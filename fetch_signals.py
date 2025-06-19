"""
fetch_signals.py – Lead Master v 4.6  (2025-06-19)
• 15-s GDELT timeout → sticky Google-News fallback
• GPT-3.5-turbo for company name (60 RPM) • GPT-4o-mini (throttled) for summary
• Relaxed keyword filter (keyword OR company name) • HQ-city geocode fallback
• PDF export helper (logo, bullet-list summary, contacts)
"""

import os, json, time, datetime, logging, textwrap, requests, feedparser, io
from collections import defaultdict
from difflib import SequenceMatcher

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from fpdf import FPDF
import magic                        # python-magic

from utils import get_conn, ensure_tables

# ───────── configuration ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")

MAX_PROSPECTS   = 50
MAX_HEADLINES   = 50
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # ≈ $3 / day
BUDGET_USED     = 0
_4O_LAST_CALL   = 0          # throttle GPT-4o: ≥ 21 s between calls

SEED_KWS = [
    "land purchase", "buys acreage", "acquire site", "groundbreaking",
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

def dedup(titles: list[str]) -> list[str]:
    out = []
    for t in titles:
        if all(_similar(t, k) < .8 for k in out):
            out.append(t)
    return out

def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap hit – skipping call.")
        return False
    BUDGET_USED += cost
    return True

# GPT wrapper that retries on 429
def safe_chat(**params):
    for attempt in range(3):
        try:
            return client.chat.completions.create(**params)
        except RateLimitError:
            logging.warning("Rate limit – waiting 21 s (%s/3)", attempt+1)
            time.sleep(21)
    logging.error("Rate-limit after 3 retries; returning None.")
    return None

# ───────── GPT helpers ─────────
def gpt_company(headline: str) -> str:
    if not budget_ok(0.05):
        return "Unknown"
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user",
                   "content":f"Return ONLY the primary company name in: {headline}"}],
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
        Return exact JSON {{summary, sector, confidence, land_flag}}.
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

# ───────── geocode with HQ fallback ─────────
def geocode(q: str):
    try:
        loc = geocoder.geocode(q, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception: return (None, None)

def safe_geocode(headline: str, company: str):
    lat, lon = geocode(headline)
    if lat is None:
        lat, lon = geocode(company)
    return lat, lon

# ───────── headline fetchers ─────────
def google_news(q: str, max_rec=20):
    feed = feedparser.parse(
        f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en")
    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    return [{"title":e.title, "url":e.link, "seendate":today}
            for e in feed.entries[:max_rec]]

GDELT_OK = True
def gdelt_or_google(q: str, max_rec=20):
    global GDELT_OK
    if not GDELT_OK:
        return google_news(q, max_rec)
    try:
        url = (f"https://api.gdeltproject.org/api/v2/doc/docsearch?"
               f"query={q}&maxrecords={max_rec}&format=json")
        return requests.get(url, timeout=15).json().get("articles", [])
    except Exception as e:
        logging.warning("GDELT timeout → Google (%s)", e)
        GDELT_OK = False
        return google_news(q, max_rec)

# ───────── DB writer ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals (company,date,headline,url,source_label,"
            " land_flag,sector_guess,lat,lon) VALUES (?,?,?,?,?,?,?,?,?)",
            (r["company"], r["date"], r["headline"], r["url"], r["src"],
             r["land_flag"], r["sector"], r["lat"], r["lon"]))
    conn.commit()

# ───────── PDF export helpers ─────────
def fetch_logo(company: str) -> bytes | None:
    try:
        url = f"https://www.google.com/s2/favicons?domain={company}.com&sz=64"
        r = requests.get(url, timeout=5)
        return r.content if r.ok else None
    except Exception:
        return None

def company_contacts(company: str) -> list[dict]:
    if not budget_ok(0.4): return []
    prompt = ("List max 3 procurement / construction contacts for "
              f"{company} in JSON (name, title, email, phone).")
    rsp = safe_chat(model="gpt-3.5-turbo",
                    messages=[{"role":"user","content":prompt}],
                    temperature=0, max_tokens=256)
    if rsp is None: return []
    try:
        return json.loads(rsp.choices[0].message.content.strip())
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
    pdf.multi_cell(0, 5,
        f"Source: {row['url']}\nGenerated: {datetime.datetime.now():%Y-%m-%d %H:%M}")
    return pdf.output(dest="S").encode("latin-1")

# ───────── national scan (same logic, uses safe_geocode) ─────────
def national_scan():
    conn=get_conn(); ensure_tables(conn)
    prospects=[]; bar=st.progress(0.0)

    for i,kw in enumerate(SEED_KWS[:10],1):
        for art in gdelt_or_google(kw.replace(" ","%20"),10):
            if keyword_filter(art["title"]):
                prospects.append({"headline":art["title"],"url":art["url"],
                                  "date":art["seendate"][:8],"src":"scan"})
                if len(prospects)>=MAX_PROSPECTS: break
        if len(prospects)>=MAX_PROSPECTS: break
        bar.progress(i/20)

    prospects=[p for p in prospects
               if p["headline"] in dedup([x["headline"] for x in prospects])]

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
    q=f'"{co}" AND ({start:%Y%m%d} TO {today:%Y%m%d})'
    arts=gdelt_or_google(q.replace(" ","%20"), MAX_HEADLINES*2)
    res=[]
    for a in arts:
        if keyword_filter(a["title"], co):
            res.append({"title":a["title"],"url":a["url"],
                        "date":a["seendate"][:8],"src":"search"})
        if len(res)>=MAX_HEADLINES: break
    return res

def manual_search(co: str):
    rows=headlines_for_company(co)
    heads=dedup([r["title"] for r in rows])[:MAX_HEADLINES]
    info=gpt_summary(co, heads)
    lat, lon = safe_geocode(info["summary"], co)
    return info, heads, lat, lon
