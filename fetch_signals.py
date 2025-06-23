# fetch_signals.py — Lead Master with Google News → GDELT fallback (2025-06-23)

import os
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

# ───────── CONFIG ─────────
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
client     = OpenAI(api_key=OPENAI_KEY)
geocoder   = Nominatim(user_agent="lead-master")

MAX_HEADLINES    = 20
RELEVANCE_CUTOFF = 0.45
SUMMARY_THROTTLE = 10
_LAST_SUMMARY    = 0

KEYWORDS = [
    "land purchase", "acquired acres", "expansion",
    "construction", "facility", "plant",
    "warehouse", "distribution center"
]
EXTRA_KWS = ["land", "acres", "site", "build",
             "construction", "expansion", "facility"]

# ───────── DATABASE SETUP ─────────
def init_db():
    conn = get_conn()
    ensure_tables(conn)
    return conn

# ───────── HELPERS ─────────
def dedup(rows):
    seen, out = set(), []
    for r in rows:
        key = (r.get("headline","").lower(), r.get("url","").lower())
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

def safe_geocode(q):
    try:
        loc = geocoder.geocode(q, timeout=10)
        return loc.latitude, loc.longitude
    except:
        return None, None

def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit; skipping call.")
        return None

# ───────── GPT SUMMARY ─────────
def gpt_summary(company, headlines):
    global _LAST_SUMMARY
    wait = SUMMARY_THROTTLE - (time.time() - _LAST_SUMMARY)
    if wait > 0: time.sleep(wait)
    _LAST_SUMMARY = time.time()

    if not headlines:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}

    prompt = textwrap.dedent(f"""
        Summarise in 5 bullet points. Guess sector.
        land_flag=1 if a land purchase is indicated.
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
        # fallback short
        return {
            "summary": headlines[0][:120]+"…",
            "sector": "unknown",
            "confidence": 0,
            "land_flag": 0
        }

    try:
        out = json.loads(rsp.choices[0].message.content)
        out["confidence"] = float(out.get("confidence",0))
        out["land_flag"]  = int(out.get("land_flag",0))
        return out
    except:
        return {
            "summary": rsp.choices[0].message.content,
            "sector": "unknown",
            "confidence": 0,
            "land_flag": 0
        }

# ───────── GDELT FALLBACK ─────────
def gdelt_search(query, days=30, maxrecords=20):
    """
    Query GDELT v2 docsearch API. Returns list of dicts with 'title','url','date'.
    """
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
        r = requests.get(url, timeout=15)
        j = r.json()
        docs = j.get("articles") or j.get("docs") or []
        out = []
        for d in docs:
            out.append({
                "headline": d.get("title",""),
                "url":      d.get("url",""),
                "date":     d.get("seendate","")
            })
        return out
    except:
        return []

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    """
    1) Google News RSS for past 30 days
    2) Fall back to GDELT
    Returns: (info, headlines, lat, lon)
    """
    # build RSS query with keywords
    kw_expr = " OR ".join(EXTRA_KWS)
    rss_q = f'{company} ({kw_expr}) when:30d'
    rss_url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(rss_q)}"
        "&hl=en-US&gl=US&ceid=US:en"
    )
    feed = feedparser.parse(rss_url)
    entries = feed.entries

    # fallback if no RSS or error
    if not entries:
        entries = gdelt_search(company, days=30, maxrecords=MAX_HEADLINES)

    # normalize
    rows = []
    for e in entries:
        title = e.get("title", getattr(e, "headline",""))
        link  = e.get("link",  getattr(e, "url",""))
        date  = e.get("published", e.get("date",""))
        rows.append({"headline":title, "url":link, "date":date})
    # dedupe & limit
    uniq  = dedup(rows)
    heads = [u["headline"] for u in uniq][:MAX_HEADLINES]

    if not heads:
        return None, [], None, None

    info = gpt_summary(company, heads)
    lat, lon = safe_geocode(f"{company} headquarters")
    return info, heads, lat, lon

# ───────── NATIONAL SCAN ─────────
def national_scan():
    """
    Iterate KEYWORDS, do RSS → GDELT fallback per keyword,
    dedupe, GPT-summarise and write to clients & signals tables.
    """
    conn    = init_db()
    sidebar = st.sidebar
    sidebar.header("National Scan")

    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)
    all_hits = []

    for idx, kw in enumerate(KEYWORDS, start=1):
        sidebar.write(f"[{idx}/{len(KEYWORDS)}] {kw}")

        # primary: Google News RSS
        rss_q   = f'{kw} when:30d'
        rss_url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(rss_q)}"
            "&hl=en-US&gl=US&ceid=US:en"
        )
        feed    = feedparser.parse(rss_url)
        hits    = feed.entries

        sidebar.write(f" • RSS: {len(hits)}")

        # fallback
        if not hits:
            gd = gdelt_search(kw, days=30, maxrecords=50)
            hits = [{"title":d["headline"], "link":d["url"], "published":d["date"]} for d in gd]
            sidebar.write(f" • GDELT: {len(gd)}")

        # normalize
        raw = []
        for e in hits:
            title = e.get("title", e.get("headline",""))
            link  = e.get("link",  e.get("url",""))
            date  = e.get("published", e.get("date",""))
            raw.append({"headline":title, "url":link, "date":date})
        uniq = dedup(raw)
        sidebar.write(f" • Deduped: {len(uniq)}")

        all_hits.extend(uniq)
        sidebar.progress(idx / len(KEYWORDS))

    # group by company via GPT on each headline
    sidebar.write("📝 Scoring…")
    scored = []
    for hit in all_hits:
        info = gpt_summary(hit["headline"], [hit["headline"]])
        scored.append({**hit, **info})

    by_co = defaultdict(list)
    for s in scored:
        if s["confidence"] >= RELEVANCE_CUTOFF:
            by_co[s["sector"]].append(s)

    # write to DB
    sidebar.write("💾 Saving…")
    for co, items in by_co.items():
        heads = [i["headline"] for i in items]
        summary = gpt_summary(co, heads)
        lat, lon = safe_geocode(f"{co} headquarters")

        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summary["summary"], json.dumps([summary["sector"]]), "New", lat, lon)
        )
        for i in items:
            conn.execute(
                "INSERT OR REPLACE INTO signals "
                "(company,headline,url,date) VALUES(?,?,?,?)",
                (co, i["headline"], i["url"], i["date"])
            )
    conn.commit()
    sidebar.success("✅ National scan complete!")

# ───────── CONTACTS ─────────
def company_contacts(company):
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

# ───────── LOGO FETCH ─────────
def fetch_logo(company):
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
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()

    logo = fetch_logo(row.get("company",""))
    if logo:
        ext = magic.from_buffer(logo,mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"
        open(fn,"wb").write(logo)
        pdf.image(fn,10,10,20)
        pdf.set_xy(35,10)

    pdf.set_font("Helvetica","B",16)
    pdf.multi_cell(0,10,row.get("company",""))
    pdf.ln(5)

    pdf.set_font("Helvetica","",12)
    for line in summary_text.split("\n"):
        if line.strip():
            pdf.multi_cell(0,7,"• "+line.strip())
    pdf.ln(3)

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

    pdf.set_y(-30)
    pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(0,5,f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}")

    return pdf.output(dest="S").encode("latin-1")

# ───────── PERMITS CSV ─────────
def fetch_permits():
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out = []
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
