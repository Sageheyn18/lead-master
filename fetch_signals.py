"""
fetch_signals.py – Lead Master v4.2
• 15-second GDELT timeout + Google-News fallback
• Progress bar during national scan
• Keyword pre-filter, fuzzy dedup, GPT company/summary, budget guard
• HQ-city geocode fallback
"""

import os, json, datetime, logging, textwrap, requests, feedparser
from collections import defaultdict
from difflib import SequenceMatcher
import streamlit as st                              # NEW for progress bar
from geopy.geocoders import Nominatim
from openai import OpenAI

from utils import get_conn, ensure_tables, cache_summary

# ───────── config & constants ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")
MAX_PROSPECTS   = 50
MAX_HEADLINES   = 50
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # ≈ $3/day
BUDGET_USED     = 0

SEED_KWS = [
    "land purchase", "buys acreage", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold-storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

def _similar(a, b): return SequenceMatcher(None, a, b).ratio()

# ───────── headline fetcher with fallback ─────────
def gdelt_or_google(query: str, max_rec: int = 20):
    """Try GDELT (15 s); on timeout return Google News RSS."""
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query={query}&maxrecords={max_rec}&format=json"
    )
    try:
        js = requests.get(url, timeout=15).json()
        return js.get("articles", [])
    except Exception as e:
        logging.warning(f"GDELT timeout: {e} → switching to Google News")
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        )
        today = datetime.datetime.utcnow().strftime("%Y%m%d")
        return [
            {"title": ent.title, "url": ent.link, "seendate": today}
            for ent in feed.entries[:max_rec]
        ]

# ───────── filters & helpers ─────────
def keyword_filter(title: str) -> bool:
    low = title.lower()
    return any(kw in low for kw in SEED_KWS)

def dedup(titles: list[str]) -> list[str]:
    kept = []
    for t in titles:
        if all(_similar(t, k) < 0.8 for k in kept):
            kept.append(t)
    return kept

def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget exceeded; skipping call.")
        return False
    BUDGET_USED += cost
    return True

def gpt_company(headline: str) -> str:
    if not budget_ok(0.2): return "Unknown"
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":f"Primary company in headline: {headline}"}],
        temperature=0, max_tokens=16,
    )
    return rsp.choices[0].message.content.strip().strip('"')

def gpt_summary(company: str, heads: list[str]) -> dict:
    if not budget_ok(0.5):
        return {"summary": heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}
    bullets = "\n".join(f"- {h}" for h in heads[:10])
    prompt = textwrap.dedent(f"""
        Summarise in one sentence. Guess sector. land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {bullets}
    """)
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,max_tokens=220,
    )
    try:
        return json.loads(rsp.choices[0].message.content.strip())
    except Exception:
        txt = rsp.choices[0].message.content.strip()
        return {"summary":txt,"sector":"unknown","confidence":0,"land_flag":0}

def geocode(text: str):
    try:
        loc = geocoder.geocode(text, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return (None, None)

# ───────── write to DB ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            """
            INSERT INTO signals
            (company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                r["company"], r["date"], r["headline"], r["url"], r["src"],
                r["land_flag"], r["sector"], r["lat"], r["lon"]
            )
        )
    conn.commit()

# ───────── national scan (cron & manual) ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)

    prospects = []
    progress = st.progress(0.0)  # Streamlit progress bar
    for idx, kw in enumerate(SEED_KWS[:10], 1):
        articles = gdelt_or_google(kw.replace(" ", "%20"), 10)
        for art in articles:
            title = art["title"]
            if keyword_filter(title):
                prospects.append({
                    "headline": title,
                    "url": art["url"],
                    "date": art["seendate"][:8],
                    "src": "scan"
                })
                if len(prospects) >= MAX_PROSPECTS: break
        if len(prospects) >= MAX_PROSPECTS: break
        progress.progress(idx/10)

    progress.empty()

    prospects = [p for p in prospects if keyword_filter(p["headline"])]
    prospects = [p for p in prospects if p["headline"] in dedup([q["headline"] for q in prospects])]

    by_company = defaultdict(list)
    for p in prospects:
        p["company"] = gpt_company(p["headline"])
        by_company[p["company"]].append(p)

    rows = []
    for co, items in by_company.items():
        heads = [i["headline"] for i in items]
        info  = gpt_summary(co, heads)
        for itm in items:
            lat, lon = geocode(itm["headline"])
            itm.update(
                land_flag=info["land_flag"], sector=info["sector"],
                lat=lat, lon=lon
            )
            rows.append(itm)

        conn.execute(
            "INSERT OR REPLACE INTO clients (name, summary, sector_tags, status) VALUES (?,?,?, 'New')",
            (co, info["summary"], json.dumps([info["sector"]]))
        )

    write_signals(rows, conn)
    logging.info("Scan wrote %s signals", len(rows))


# ───────── single-company helper for UI ─────────
def headlines_for_company(company: str) -> list[dict]:
    today  = datetime.date.today()
    start  = today - datetime.timedelta(days=150)
    query  = f'"{company}" AND ({start:%Y%m%d} TO {today:%Y%m%d})'
    arts   = gdelt_or_google(query.replace(" ", "%20"), MAX_HEADLINES * 2)
    res = []
    for a in arts:
        title = a["title"]
        if keyword_filter(title):
            res.append(
                {"title": title, "url": a["url"], "date": a["seendate"][:8], "src": "search"}
            )
        if len(res) >= MAX_HEADLINES:
            break
    return res

def manual_search(company: str):
    rows = headlines_for_company(company)
    heads = dedup([r["title"] for r in rows])[:MAX_HEADLINES]
    info  = gpt_summary(company, heads)
    lat, lon = geocode(info["summary"])
    return info, heads, lat, lon


# ───────── CLI entry (optional local test) ─────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    national_scan()
