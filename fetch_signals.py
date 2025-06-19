"""
fetch_signals.py  –  Lead Master  v3
• Pulls yesterday’s news via GDELT (60-s timeout); falls back to Google News RSS.
• GPT-4o summarises → JSON  {summary, sector, confidence, land_flag}
• Scrapes company site / LinkedIn (best-effort) for contact + HQ.
"""

import os, re, json, datetime, logging, textwrap, requests, feedparser
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI

from utils import get_conn, ensure_tables, cache_summary

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_COMPANIES = int(os.getenv("MAX_COMPANIES_PER_RUN", 10))

# ───────── headline collectors ─────────
def gdelt_headlines(co: str) -> list[dict]:
    yy = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    url = ("https://api.gdeltproject.org/api/v2/doc/docsearch"
           f"?query=\"{co}\" AND {yy}&maxrecords=20&format=json")
    try:
        js = requests.get(url, timeout=60).json()
        return [{"text": a["title"], "url": a["url"], "date": a["seendate"][:8],
                 "src": "GDELT"} for a in js.get("articles", [])]
    except Exception as e:
        logging.warning(f"GDELT error {e} – using Google News RSS")
        feed = feedparser.parse(f"https://news.google.com/rss/search?q={co}&hl=en-US&gl=US&ceid=US:en")
        return [{"text": ent.title, "url": ent.link, "date": yy, "src": "Google"} for ent in feed.entries[:20]]

# ───────── GPT summariser + land flag ─────────
def gpt_summarise(company: str, heads: list[dict]) -> dict | None:
    if not heads:
        return None
    bullet = "\n".join(f"- {h['text']}" for h in heads)
    prompt = textwrap.dedent(f"""
        You are a construction-lead analyst.
        Company: {company}
        Headlines (last 24 h):
        {bullet}

        1. Write one concise sentence summary.
        2. Guess the project sector (e.g., food-processing, cold-storage).
        3. Does any headline suggest a LAND PURCHASE or new site acquisition?
           Output land_flag = 1 if yes, else 0.

        Return ONLY JSON:
        {{
          "summary": "...",
          "sector": "...",
          "confidence": 0-1,
          "land_flag": 0/1
        }}
    """).strip()

    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=220,
    )
    content = rsp.choices[0].message.content.strip()
    try:
        js = json.loads(content)
    except json.JSONDecodeError:
        logging.warning("Bad JSON; wrapping raw text.")
        js = {"summary": content, "sector": "unknown", "confidence": 0.0, "land_flag": 0}
    return js

# ───────── contact & HQ scraper (best-effort, free sites) ─────────
def scrape_company_site(website: str) -> tuple[str | None, str | None, list]:
    """Return (hq_address, phone, contacts[]). contacts: list of dicts."""
    if not website:
        return None, None, []
    try:
        html = requests.get(website, timeout=20, headers={"User-Agent": "Mozilla/5.0"}).text
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # crude patterns
        phone = re.search(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", text)
        email = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
        addr = None
        for tag in soup.find_all("address"):
            addr = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
            if len(addr) > 15:
                break
        contacts = []
        if email:
            contacts.append({"department": "General", "name": "", "email": email.group(), "phone": phone.group() if phone else ""})
        return addr, phone.group() if phone else None, contacts
    except Exception as e:
        logging.warning(f"scrape error {e}")
        return None, None, []

def guess_website(co_name: str) -> str | None:
    """Ping DuckDuckGo 'site:' search to guess official domain."""
    try:
        q = {"q": f"{co_name} official site", "format": "json", "no_html": 1}
        js = requests.get("https://duckduckgo.com/i.js", params=q, timeout=15).json()
        if js["results"]:
            url = js["results"][0]["url"]
            domain = urlparse(url).netloc.replace("www.", "")
            return f"https://{domain}"
    except Exception:
        pass
    return None

def logo_from_domain(domain: str | None) -> str | None:
    return f"https://logo.clearbit.com/{domain}" if domain else None

# ───────── main run ─────────
def run():
    conn = get_conn()
    ensure_tables(conn)

    companies = (
        pd.read_sql("SELECT name FROM clients", conn)["name"].tolist()
        or ["Acme Foods"]
    )[:MAX_COMPANIES]

    for co in companies:
        heads = [h for h in gdelt_headlines(co) if not cache_summary(h["url"])]
        info = gpt_summarise(co, heads) if heads else None
        if not info:
            continue

        # ––– scrape contact / logo once –––
        row = conn.execute("SELECT website, logo_url FROM clients WHERE name=?", (co,)).fetchone()
        website = row[0] if row else guess_website(co)
        hq_addr, phone, contacts = scrape_company_site(website) if website else (None, None, [])
        logo = logo_from_domain(urlparse(website).netloc) if website else None

        # save signals
        for h in heads:
            cache_summary(h["url"], info["summary"])
            conn.execute(
                "INSERT INTO signals (company,date,headline,url,source_label,land_flag,sector_guess) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    co, h["date"], info["summary"], h["url"], h["src"],
                    info.get("land_flag", 0), info.get("sector")
                )
            )

        # upsert client
        existing = conn.execute("SELECT sector_tags FROM clients WHERE name=?", (co,)).fetchone()
        tags = json.loads(existing[0]) if existing else []
        if info["sector"] and info["sector"] not in tags:
            tags.append(info["sector"])

        conn.execute(
            """
            INSERT OR REPLACE INTO clients
            (name, summary, sector_tags, website, hq_address, phone, logo_url, contacts)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                co, info["summary"], json.dumps(tags),
                website or "", hq_addr, phone, logo, json.dumps(contacts)
            )
        )
        conn.commit()
        logging.info(f"{co}: saved, land_flag={info.get('land_flag')}")
