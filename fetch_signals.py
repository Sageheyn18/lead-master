# fetch_signals.py — Lead Master Google-RSS → GDELT (no NewsAPI, UI key) 2025-06-23

import json
import time
import datetime
import logging
import textwrap
import requests
import sqlite3
import csv
import threading
from collections import defaultdict
from urllib.parse import quote_plus

import feedparser
import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── USER-PROVIDED OPENAI KEY ─────────
# Prompt user to enter their key once per session:
OPENAI_KEY = st.sidebar.text_input(
    "🔑 OpenAI API Key", type="password", help="Paste your OpenAI key here"
)
if not OPENAI_KEY:
    st.sidebar.warning("⚠️ Enter your OpenAI key to enable summaries.")
client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# ───────── GLOBAL SETTINGS ─────────
MAX_HEADLINES    = 20
RELEVANCE_CUTOFF = 0.45
SUMMARY_THROTTLE = 10
_LAST_SUMMARY    = 0

KEYWORDS = [
    "land purchase", "acquired acres", "expansion",
    "construction", "facility", "plant",
    "warehouse", "distribution center"
]
EXTRA_KWS = ["land", "acres", "site", "build", "construction", "expansion", "facility"]

# ───────── DATABASE SETUP ─────────
def init_db():
    conn = get_conn()
    ensure_tables(conn)
    return conn

# ───────── HELPERS ─────────
def dedup(rows):
    seen = set(); out = []
    for r in rows:
        key = (r.get("headline","").lower(), r.get("url","").lower())
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

geocoder = Nominatim(user_agent="lead-master")
def safe_geocode(q):
    try:
        loc = geocoder.geocode(q, timeout=10)
        return loc.latitude, loc.longitude
    except:
        return None, None

def safe_chat(**kw):
    if not client:
        return None
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit; skipping call.")
        return None

# ───────── GPT SUMMARY ─────────
def gpt_summary(company, headlines):
    """Return {summary,sector,confidence,land_flag} via GPT."""
    global _LAST_SUMMARY
    wait = SUMMARY_THROTTLE - (time.time() - _LAST_SUMMARY)
    if wait > 0: time.sleep(wait)
    _LAST_SUMMARY = time.time()

    if not headlines:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}

    prompt = textwrap.dedent(f"""
        Summarise in 5 bullet points. Guess sector.
        Set land_flag=1 if a land purchase is indicated.
        Return EXACT JSON {{summary,sector,confidence,land_flag}}.

        Headlines:
        {"".join("- "+h+"\\n" for h in headlines[:10])}
    """).strip()

    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
        max_tokens=220
    )
    if not rsp:
        # fallback to first headline snippet
        return {"summary":headlines[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}

    try:
        out = json.loads(rsp.choices[0].message.content)
        out["confidence"] = float(out.get("confidence",0))
        out["land_flag"]  = int(out.get("land_flag",0))
        return out
    except:
        return {"summary":rsp.choices[0].message.content,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── GDELT FALLBACK ─────────
def gdelt_search(query, days=30, maxrecords=20):
    """Return list of {'headline','url','date'} from GDELT v2."""
    today = datetime.date.today()
    since = today - datetime.timedelta(days=days)
    q = quote_plus(f'"{query}" AND ({since:%Y%m%d} TO {today:%Y%m%d})')
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query={q}"
        "&filter=SourceCommonName:NEWS"
        "&mode=ArtList"
        f"&maxrecords={maxrecords}"
        "&format=json"
    )
    try:
        j = requests.get(url, timeout=15).json()
        docs = j.get("articles") or j.get("docs") or []
        return [{"headline":d.get("title",""), "url":d.get("url",""), "date":d.get("seendate","")} for d in docs]
    except:
        return []

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    """
    1) Google News RSS (30d)
    2) GDELT fallback
    Returns: (info, headlines, lat, lon)
    """
    # build RSS query
    kw_expr = " OR ".join(EXTRA_KWS)
    rss_q   = f'{company} ({kw_expr}) when:30d'
    rss_url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(rss_q)}&hl=en-US&gl=US&ceid=US:en"
    )
    feed    = feedparser.parse(rss_url)
    entries = feed.entries

    # fallback to GDELT
    if not entries:
        gd = gdelt_search(company, days=30, maxrecords=MAX_HEADLINES)
        entries = [{"title":g["headline"], "link":g["url"], "published":g["date"]} for g in gd]

    # normalize & filter
    rows = []
    for e in entries:
        title = e.get("title", getattr(e, "headline",""))
        link  = e.get("link", getattr(e, "url",""))
        date  = e.get("published","")
        rows.append({"headline":title, "url":link, "date":date})
    rows = [r for r in rows if any(kw in r["headline"].lower() for kw in EXTRA_KWS)]
    uniq = dedup(rows)
    heads = [u["headline"] for u in uniq][:MAX_HEADLINES]

    if not heads:
        return None, [], None, None

    info = gpt_summary(company, heads)
    lat, lon = safe_geocode(f"{company} headquarters")
    return info, heads, lat, lon

# ───────── NATIONAL SCAN ─────────
def national_scan():
    """
    For each KEYWORD:
      Google RSS → GDELT fallback → dedupe
    GPT-summarise + write into clients & signals.
    """
    conn    = init_db()
    sidebar = st.sidebar
    sidebar.header("National Scan")

    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)
    all_hits = []

    for idx, kw in enumerate(KEYWORDS, start=1):
        sidebar.write(f"[{idx}/{len(KEYWORDS)}] {kw}")

        # 1) Google RSS
        rss_url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(f'{kw} when:30d')}&hl=en-US&gl=US&ceid=US:en"
        )
        feed    = feedparser.parse(rss_url)
        hits    = feed.entries
        sidebar.write(f" • RSS: {len(hits)}")

        # 2) fallback GDELT
        if not hits:
            gd = gdelt_search(kw, days=30, maxrecords=50)
            hits = [{"title":g["headline"], "link":g["url"], "published":g["date"]} for g in gd]
            sidebar.write(f" • GDELT: {len(gd)}")

        # normalize
        raw = []
        for e in hits:
            title = e.get("title", getattr(e,"headline",""))
            link  = e.get("link",  getattr(e,"url",""))
            date  = e.get("published","")
            raw.append({"headline":title, "url":link, "date":date})
        uniq = dedup(raw)
        sidebar.write(f" • Deduped: {len(uniq)}")
        all_hits.extend(uniq)
        sidebar.progress(idx / len(KEYWORDS))

    # score + group
    sidebar.write("📝 Scoring…")
    scored = []
    for hit in all_hits:
        info = gpt_summary(hit["headline"], [hit["headline"]])
        scored.append({**hit, **info})

    by_co = defaultdict(list)
    for s in scored:
        if s["confidence"] >= RELEVANCE_CUTOFF:
            by_co[s.get("sector","Unknown")].append(s)

    # write to DB
    sidebar.write("💾 Saving…")
    for co, items in by_co.items():
        heads   = [i["headline"] for i in items]
        summary = gpt_summary(co, heads)
        lat, lon = safe_geocode(f"{co} headquarters")

        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summary["summary"], json.dumps([summary["sector"]]), "New", lat, lon)
        )
        for it in items:
            conn.execute(
                "INSERT OR REPLACE INTO signals "
                "(company,headline,url,date) VALUES(?,?,?,?)",
                (co, it["headline"], it["url"], it["date"])
            )
    conn.commit()
    sidebar.success("✅ National scan complete!")

# ───────── CONTACTS ─────────
def company_contacts(company: str):
    prompt = (
        f"List up to 3 procurement/engineering/construction "
        f"contacts for {company} as JSON array of {{name,title,email,phone}}."
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0,
        max_tokens=256
    )
    if not rsp:
        return []
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        return []

# ───────── LOGO ─────────
def fetch_logo(company: str):
    dom = company.replace(" ","")+".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{dom}", timeout=5)
        if r.ok:
            return r.content
    except:
        pass
    return None

# ───────── PDF EXPORT ─────────
def export_pdf(row, summary_text, contacts):
    pdf = FPDF()
    pdf.set_auto_page_break(True,15)
    pdf.add_page()
    # logo
    logo = fetch_logo(row.get("company",""))
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"
        open(fn,"wb").write(logo)
        pdf.image(fn,10,10,20)
        pdf.set_xy(35,10)
    # title
    pdf.set_font("Helvetica","B",16)
    pdf.multi_cell(0,10, row.get("company",""))
    pdf.ln(5)
    # summary
    pdf.set_font("Helvetica","",12)
    for l in summary_text.split("\n"):
        if l.strip():
            pdf.multi_cell(0,7,"• "+l.strip())
    pdf.ln(3)
    # contacts
    if contacts:
        pdf.set_font("Helvetica","B",13)
        pdf.cell(0,8,"Key Contacts",ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(0,6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}   {c.get('phone','')}"
            )
            pdf.ln(1)
    # footer
    pdf.set_y(-30)
    pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(0,5, f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}")
    return pdf.output(dest="S").encode("latin-1")

# ───────── PERMITS ─────────
def fetch_permits():
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out=[]
    with open(path,newline="",encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            lat, lon = safe_geocode(r.get("address",""))
            out.append({
                "company":    r.get("company",""),
                "address":    r.get("address",""),
                "date":       r.get("date",""),
                "type":       r.get("permit_type",""),
                "details_url":r.get("details_url",""),
                "lat":        lat,
                "lon":        lon
            })
    return out
