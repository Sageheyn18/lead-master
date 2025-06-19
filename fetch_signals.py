"""
fetch_signals.py  – Lead Master  v4.8   (2025-06-20)

• 15-s GDELT timeout → Google-News RSS fallback
  – Google query: "<company>" (land OR acres …)   ← URL-encoded
• Keyword OR company-name filter
• Duplicate filter: drop when title OR URL repeats
• GPT-3.5 company extraction  ·  GPT-4o bullet-list summary (throttled)
• HQ-city geocode fallback
• PDF export (logo, summary, contacts)
"""

import os, json, time, datetime, logging, textwrap, requests, feedparser
from collections import defaultdict
from difflib import SequenceMatcher
from urllib.parse import quote_plus

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")

MAX_PROSPECTS   = 50
MAX_HEADLINES   = 50
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))   # ≈ $3 / day
BUDGET_USED     = 0
_4O_LAST_CALL   = 0            # ≥ 21 s between GPT-4o calls

SEED_KWS = [
    "land purchase", "acres", "buys acreage", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

# ───────── helper utilities ─────────
def _similar(a, b): return SequenceMatcher(None, a, b).ratio()

def keyword_filter(title: str, company: str | None = None) -> bool:
    low = title.lower()
    kw_hit = any(kw in low for kw in SEED_KWS)
    co_hit = company and company.lower() in low
    return kw_hit or co_hit

def dedup(rows: list[dict]) -> list[dict]:
    """
    Remove duplicates when either title OR URL already seen (case-insensitive).
    Each row must have keys 'title' and 'url'.
    """
    seen_titles, seen_urls, out = set(), set(), []
    for r in rows:
        t = r["title"].lower(); u = r["url"].lower()
        if t in seen_titles or u in seen_urls:
            continue
        seen_titles.add(t); seen_urls.add(u); out.append(r)
    return out

def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap reached; skipping call.")
        return False
    BUDGET_USED += cost
    return True

def safe_chat(**params):
    for attempt in range(3):
        try:
            return client.chat.completions.create(**params)
        except RateLimitError:
            logging.warning("Rate-limit; wait 21 s  (%s/3)", attempt+1)
            time.sleep(21)
    logging.error("Still rate-limited after retries.")
    return None

# ───────── GPT helpers ─────────
def gpt_company(head: str) -> str:
    if not budget_ok(0.05):
        return "Unknown"
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user",
                   "content":f"Return ONLY the primary company name in: {head}"}],
        temperature=0, max_tokens=16)
    return ("Unknown" if rsp is None
            else rsp.choices[0].message.content.strip().strip('"'))

def gpt_summary(company: str, heads: list[str]) -> dict:
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No construction headlines.",
                "sector":"unknown","confidence":0,"land_flag":0}

    wait = 21 - (time.time() - _4O_LAST_CALL)
    if wait > 0: time.sleep(wait)

    if not budget_ok(0.5):
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}

    prompt = textwrap.dedent(f"""
        Summarise the project info as concise bullet points (max 6 bullets).
        Guess sector. land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {"".join("- "+h+"\\n" for h in heads[:10])}
    """)
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220)
    _4O_LAST_CALL = time.time()
    if rsp is None:
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    try:
        return json.loads(rsp.choices[0].message.content.strip())
    except Exception:
        txt = rsp.choices[0].message.content.strip()
        return {"summary":txt,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── geocode helpers ─────────
def geocode(q: str):
    try:
        loc = geocoder.geocode(q, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception: return (None, None)

def safe_geocode(head: str, company: str):
    lat, lon = geocode(head)
    return (lat, lon) if lat is not None else geocode(company)

# ───────── headline fetchers ─────────
EXTRA_KWS = [
    "land", "acres", "site", "build", "building", "construction",
    "expansion", "facility", "plant", "warehouse", "distribution center"
]

def google_news(company: str, max_rec: int = 40):
    kws = " OR ".join(EXTRA_KWS)
    q   = f'"{company}" ({kws})'
    url = ("https://news.google.com/rss/search?"
           f"q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en")
    feed = feedparser.parse(url)
    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    return [{"title":e.title, "url":e.link, "seendate":today}
            for e in feed.entries[:max_rec]]

GDELT_OK = True
def gdelt_or_google(gdelt_q: str, company: str, max_rec: int = 20):
    global GDELT_OK
    if not GDELT_OK:
        return google_news(company, max_rec)
    url = ("https://api.gdeltproject.org/api/v2/doc/docsearch"
           f"?query={gdelt_q}&maxrecords={max_rec}&format=json")
    try:
        return requests.get(url, timeout=15).json().get("articles", [])
    except Exception as e:
        logging.warning("GDELT timeout → Google (%s)", e)
        GDELT_OK = False
        return google_news(company, max_rec)

# ───────── DB writer ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals "
            "(company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (r["company"], r["date"], r["headline"], r["url"], r["src"],
             r["land_flag"], r["sector"], r["lat"], r["lon"]))
    conn.commit()

# ───────── PDF export helpers (unchanged) ─────────
def fetch_logo(company: str) -> bytes | None:
    try:
        url = f"https://www.google.com/s2/favicons?domain={company}.com&sz=64"
        r = requests.get(url, timeout=5); return r.content if r.ok else None
    except Exception: return None

def company_contacts(company: str) -> list[dict]:
    if not budget_ok(0.4): return []
    prompt = ("Give a JSON list (max 3) of procurement / construction contacts "
              f"for {company}. Fields: name, title, email, phone.")
    rsp = safe_chat(model="gpt-3.5-turbo",
                    messages=[{"role":"user","content":prompt}],
                    temperature=0, max_tokens=256)
    if rsp is None: return []
    try: return json.loads(rsp.choices[0].message.content.strip())
    except Exception: return []

def export_pdf(row: dict, bullets: str, contacts: list[dict]) -> bytes:
    pdf = FPDF(); pdf.set_auto_page_break(True, 15); pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)

    logo = fetch_logo(row["company"])
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"; open(fn, "wb").write(logo)
        pdf.image(fn, x=10, y=10, w=20); pdf.set_xy(35, 10)

    pdf.multi_cell(0, 10, row["headline"]); pdf.ln(5)
    pdf.set_font("Helvetica", "", 12)
    for b in bullets.split("•"):
        if b.strip(): pdf.multi_cell(0, 7, "• "+b.strip())
    pdf.ln(3)

    if contacts:
        pdf.set_font("Helvetica", "B", 13); pdf.cell(0, 8, "Key Contacts", ln=1)
        pdf.set_font("Helvetica", "", 11)
        for c in contacts:
            pdf.multi_cell(
                0, 6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}  {c.get('phone','')}")
            pdf.ln(1)

    pdf.set_y(-30); pdf.set_font("Helvetica", "I", 9)
    pdf.multi_cell(
        0, 5,
        f"Source: {row['url']}\nGenerated: {datetime.datetime.now():%Y-%m-%d %H:%M}")
    return pdf.output(dest="S").encode("latin-1")

# ───────── national scan (uses dedup and safe_geocode) ─────────
def national_scan():
    conn=get_conn(); ensure_tables(conn)
    prospects=[]; bar=st.progress(0.0)

    for i,kw in enumerate(SEED_KWS[:10],1):
        for art in gdelt_or_google(kw.replace(" ","%20"), "", 10):
            if keyword_filter(art["title"]):
                prospects.append({"headline":art["title"],"url":art["url"],
                                  "date":art["seendate"][:8],"src":"scan"})
                if len(prospects)>=MAX_PROSPECTS: break
        if len(prospects)>=MAX_PROSPECTS: break
        bar.progress(i/20)

    prospects = dedup(prospects)

    by_co=defaultdict(list)
    for j,p in enumerate(prospects,1):
        p["company"]=gpt_company(p["headline"])
        by_co[p["company"]].append(p)
        bar.progress(0.5+0.3*j/len(prospects))

    rows=[]; co_list=list(by_co)
    for k,co in enumerate(co_list,1):
        items=by_co[co]
        info=gpt_summary(co,[i["headline"] for i in items])
        for itm in items:
            lat,lon=safe_geocode(itm["headline"], co)
            itm.update(lat=lat,lon=lon,sector=info["sector"],
                       land_flag=info["land_flag"]); rows.append(itm)
        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name, summary, sector_tags, status) VALUES (?,?,?, 'New')",
            (co, info["summary"], json.dumps([info["sector"]]))
        )
        bar.progress(0.8+0.2*k/len(co_list))

    write_signals(rows, conn); bar.progress(1.0); bar.empty()
    logging.info("Scan wrote %s signals", len(rows))

# ───────── manual search helper ─────────
def headlines_for_company(co: str) -> list[dict]:
    today=datetime.date.today(); start=today-datetime.timedelta(days=150)
    gdelt_q = f'"{co}" AND ({start:%Y%m%d} TO {today:%Y%m%d})'
    arts = gdelt_or_google(gdelt_q.replace(" ","%20"), co,
                           MAX_HEADLINES*2)

    arts = [a for a in arts
            if a.get("seendate",
                     today.strftime("%Y%m%d")) >= start.strftime("%Y%m%d")]

    rows=[]
    for a in arts:
        if keyword_filter(a["title"], co):
            rows.append({"title":a["title"],"url":a["url"],
                         "date":a.get("seendate", today.strftime("%Y%m%d")),
                         "src":"search"})
        if len(rows)>=MAX_HEADLINES: break
    return rows

def manual_search(co: str):
    rows=headlines_for_company(co)
    rows=dedup(rows)                           # remove duplicates
    heads=[r["title"] for r in rows]
    info=gpt_summary(co, heads)
    lat, lon = safe_geocode(info["summary"], co)
    return info, rows, lat, lon
