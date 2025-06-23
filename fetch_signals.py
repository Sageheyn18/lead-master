import os, json, time, logging, datetime
import feedparser
import requests
from urllib.parse import quote_plus
from geopy.geocoders import Nominatim
from openai import OpenAI
from openai.error import RateLimitError
from fpdf import FPDF

from utils import get_conn, ensure_tables

# ───────── OpenAI client via Streamlit secrets ─────────
import streamlit as st
client = OpenAI(api_key=st.secrets["OPENAI"]["api_key"])

# ───────── Constants ─────────
SEED_KWS = [
    "land purchase", "acquired site", "build", "construction",
    "expansion", "facility", "plant", "warehouse", "distribution center"
]
MAX_HEADLINES = 60
RSS_DAYS = 30
GD_TIMEOUT = 15

# ───────── Safe Chat ─────────
def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit; skipping call.")
        return None

# ───────── Summarize Headlines ─────────
def summarise(company, heads):
    prompt = (
        f"Summarize these headlines for {company} "
        f"into a JSON object with keys summary (bullet list), sector (single word), confidence (0–1), land_flag (0/1):\n\n"
        + "\n".join(f"- {h['headline']}" for h in heads[:10])
    )
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
        max_tokens=200
    )
    if not rsp: return {}
    return json.loads(rsp.choices[0].message.content)

# ───────── RSS & GDELT Helpers ─────────
def rss_search(query: str, days:int=RSS_DAYS, maxrec:int=MAX_HEADLINES):
    q = quote_plus(f'{query} when:{days}d')
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    return feed.entries[:maxrec]

def gdelt_headlines(query:str, days:int=RSS_DAYS, maxrec:int=MAX_HEADLINES):
    dt_to = datetime.date.today()
    dt_from = dt_to - datetime.timedelta(days=days)
    url = (
        f"https://api.gdeltproject.org/api/v2/doc/docsearch?"
        f"query=\"{quote_plus(query)}\" AND {dt_from.strftime('%Y%m%d')} TO {dt_to.strftime('%Y%m%d')}"
        f"&mode=ArtList&maxrecords={maxrec}&format=json"
    )
    try:
        r = requests.get(url, timeout=GD_TIMEOUT).json()
        arts = r.get("articles", [])
        return [{"headline":a["title"], "url":a["url"], "date":a["seendate"]} for a in arts]
    except Exception:
        logging.warning("GDELT timeout → falling back to RSS")
        return []

def dedup(rows: list[dict]) -> list[dict]:
    seen_t, seen_u, out = set(), set(), []
    for r in rows:
        t,u = r["headline"].lower(), r["url"].lower()
        if t in seen_t or u in seen_u: continue
        seen_t.add(t); seen_u.add(u); out.append(r)
    return out

# ───────── Manual Search ─────────
def _set_cached_raw(seed, hits):
    db = get_conn()
    now = datetime.datetime.utcnow().isoformat()
    cur = db.cursor()
    for h in hits:
        cur.execute(
            "INSERT OR REPLACE INTO raw_cache(seed,fetched,headline,url,date) VALUES(?,?,?,?,?)",
            (seed, now, h["headline"], h["url"], h["date"])
        )
    db.commit()
    db.close()

def _fetch_for_seed(seed:str):
    raw = rss_search(seed) or gdelt_headlines(seed)
    hits = [{"headline":e.title, "url":e.link, "date":getattr(e,"published", "")} for e in raw]
    hits = dedup(hits)[:MAX_HEADLINES]
    _set_cached_raw(seed, hits)
    return hits

def manual_search(company:str):
    ensure_tables()
    # Fetch headlines
    raw = _fetch_for_seed(company)
    if not raw:
        return {}, [], None, None

    # Summarize
    summ = summarise(company, raw)

    # Geocode
    geo = Nominatim(user_agent="lead_master").geocode(company)
    lat, lon = (geo.latitude, geo.longitude) if geo else (None, None)

    # Return summary, raw list, lat, lon
    return summ, raw, lat, lon

# ───────── National Scan ─────────
def national_scan():
    ensure_tables()
    conn = get_conn()
    c = conn.cursor()

    sidebar = st.sidebar
    sidebar.markdown("### 🗂 National Scan")
    bar = sidebar.progress(0)
    status = sidebar.empty()

    all_hits = []
    for i,kw in enumerate(SEED_KWS, start=1):
        status.markdown(f"[{i}/{len(SEED_KWS)}] Scanning **{kw}**…")
        hits = rss_search(kw) or gdelt_headlines(kw)
        hits = [{"seed":kw, **h} for h in dedup(hits)[:MAX_HEADLINES]]
        all_hits += hits
        bar.progress(i/len(SEED_KWS))

    # Score and assign companies
    sidebar.markdown("📝 Scoring…")
    scored = []
    for h in all_hits:
        info = summarise(h["seed"], [h])
        if info.get("confidence",0) >= 0.5 and info.get("land_flag",0)==1:
            h.update(info)
            scored.append(h)

    # Dedup by headline
    final = dedup(scored)

    # Insert into signals table
    now = datetime.datetime.utcnow().isoformat()
    for f in final:
        c.execute("""
            INSERT OR REPLACE INTO signals(company,headline,url,date,score,read)
            VALUES(?,?,?,?,?,COALESCE((SELECT read FROM signals WHERE url=?),0))
        """, (
            f.get("seed",""),
            f["headline"], f["url"], f["date"] or now,
            f.get("confidence",0.0), f["url"]
        ))
    conn.commit()
    conn.close()
    sidebar.success(f"✅ Finished: {len(final)} new signals.")
    bar.empty()
    status.empty()

# ───────── Export to PDF ─────────
def export_pdf(company:str, headline:str, summary:str, contacts:dict):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=16, style="B")
    pdf.cell(0,10,f"{company} — Executive Summary", ln=True)
    pdf.set_font("Helvetica", size=12)
    for line in summary.strip().split("\n"):
        pdf.multi_cell(0,8,line.strip())
    pdf.ln(5)
    pdf.set_font("Helvetica", size=14, style="B")
    pdf.cell(0,8,"Contacts:", ln=True)
    pdf.set_font("Helvetica", size=12)
    for name,info in contacts.items():
        pdf.multi_cell(0,6,f"{name}: {info}")
    out = f"export_{company.replace(' ','_')}.pdf"
    pdf.output(out)
    return out
