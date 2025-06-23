# fetch_signals.py — Lead Master v10.0 (2025-06-23)

import os
import json
import time
import datetime
import logging
import textwrap
import requests
import csv
import sqlite3
import threading
from collections import defaultdict
from urllib.parse import quote_plus

import feedparser
import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from newsapi import NewsApiClient
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
client     = OpenAI(api_key=OPENAI_KEY)
geocoder   = Nominatim(user_agent="lead-master")

# NewsAPI via ENV var
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
if not NEWSAPI_KEY:
    st.sidebar.warning("⚠️ NEWSAPI_KEY not set; national scan will skip NewsAPI.")
api = NewsApiClient(api_key=NEWSAPI_KEY) if NEWSAPI_KEY else None

MAX_HEADLINES     = 20
RELEVANCE_CUTOFF  = 0.45
SUMMARY_THROTTLE  = 10
_LAST_SUMMARY     = 0

# Keywords for national scan
KEYWORDS = [
    "land purchase", "acquired acres", "expansion",
    "construction", "facility", "plant",
    "warehouse", "distribution center"
]
EXTRA_KWS = [
    "land", "acres", "site", "build",
    "construction", "expansion", "facility"
]

# ───────── DATABASE SETUP ─────────
def init_db():
    conn = get_conn()
    ensure_tables(conn)
    return conn

# ───────── HELPERS ─────────
def dedup(rows):
    seen = set()
    out  = []
    for r in rows:
        key = (r.get("headline","").lower(), r.get("url","").lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def safe_geocode(query):
    try:
        loc = geocoder.geocode(query, timeout=10)
        return loc.latitude, loc.longitude
    except:
        return None, None

def safe_chat(**kwargs):
    try:
        return client.chat.completions.create(**kwargs)
    except RateLimitError:
        logging.warning("OpenAI rate-limit reached; skipping call.")
        return None

# ───────── GPT-DRIVEN FUNCTIONS ─────────
def gpt_summary(company, headlines):
    """Return dict with summary, sector, confidence, land_flag."""
    global _LAST_SUMMARY
    # throttle
    wait = SUMMARY_THROTTLE - (time.time() - _LAST_SUMMARY)
    if wait > 0:
        time.sleep(wait)
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
        # fallback
        return {
            "summary": headlines[0][:120]+"…",
            "sector": "unknown",
            "confidence": 0,
            "land_flag": 0
        }

    try:
        out = json.loads(rsp.choices[0].message.content)
        out["confidence"] = float(out.get("confidence", 0))
        out["land_flag"]  = int(out.get("land_flag", 0))
        return out
    except:
        return {
            "summary": rsp.choices[0].message.content,
            "sector": "unknown",
            "confidence": 0,
            "land_flag": 0
        }

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    """
    Look up a single company:
    1) NewsAPI for last 30 days
    2) Fallback to Google News RSS
    Returns: (info_dict, headlines_list, lat, lon)
    """
    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)
    entries = []

    # 1) NewsAPI
    if api:
        try:
            resp = api.get_everything(
                q=company,
                from_param=since.isoformat(),
                to=today.isoformat(),
                language="en",
                page_size=MAX_HEADLINES
            )
            entries = resp.get("articles", [])
        except:
            entries = []

    # 2) RSS fallback
    if not entries:
        rss_url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(company)}%20when:30d&hl=en-US&gl=US&ceid=US:en"
        )
        feed = feedparser.parse(rss_url)
        entries = feed.entries

    # Normalize & filter by keywords
    raw = []
    for e in entries:
        title = e.get("title", getattr(e, "title", ""))
        url   = e.get("link", getattr(e, "url", ""))
        date  = getattr(e, "published", "")
        raw.append({"headline": title, "url": url, "date": date})

    filtered = [r for r in raw if any(kw in r["headline"].lower() for kw in EXTRA_KWS)]
    unique   = dedup(filtered)
    heads    = [u["headline"] for u in unique][:MAX_HEADLINES]

    if not heads:
        return None, [], None, None

    info = gpt_summary(company, heads)
    lat, lon = safe_geocode(company + " headquarters")
    return info, heads, lat, lon

# ───────── NATIONAL SCAN ─────────
def national_scan():
    """Run a 30-day national scan, show sidebar diagnostics, save to DB."""
    conn    = init_db()
    sidebar = st.sidebar
    sidebar.header("National Scan Progress")

    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)
    all_hits = []

    for idx, kw in enumerate(KEYWORDS, start=1):
        sidebar.write(f"[{idx}/{len(KEYWORDS)}] {kw}")
        hits = []

        # NewsAPI
        if api:
            try:
                resp = api.get_everything(
                    q=kw,
                    from_param=since.isoformat(),
                    to=today.isoformat(),
                    language="en",
                    sort_by="relevancy",
                    page_size=100
                )
                hits = resp.get("articles", [])
                sidebar.write(f" • NewsAPI: {len(hits)}")
            except:
                hits = []

        # RSS fallback
        if not hits:
            rss_url = (
                "https://news.google.com/rss/search?"
                f"q={quote_plus(kw)}%20when:30d&hl=en-US&gl=US&ceid=US:en"
            )
            feed = feedparser.parse(rss_url)
            hits = feed.entries
            sidebar.write(f" • RSS: {len(hits)}")

        # Normalize & dedupe
        norm = []
        for e in hits:
            title = e.get("title", getattr(e, "title", ""))
            url   = e.get("link", getattr(e, "url", ""))
            date  = getattr(e, "published", "")[:10]
            norm.append({"headline": title, "url": url, "date": date})

        uniq = dedup(norm)
        sidebar.write(f" • Deduped: {len(uniq)}")
        all_hits.extend(uniq)
        sidebar.progress(idx / len(KEYWORDS))

    # Score & group
    sidebar.write("📝 Scoring…")
    # batch via GPT for company+score
    infos = []
    for h in all_hits:
        infos.append(gpt_summary(h["headline"], [h["headline"]]))

    by_co = defaultdict(list)
    for hit, info in zip(all_hits, infos):
        score = info.get("confidence", 0)
        if score >= RELEVANCE_CUTOFF:
            hit.update(info)
            by_co[info.get("sector","Unknown")].append(hit)

    # Save to DB
    sidebar.write("💾 Saving…")
    for co, items in by_co.items():
        heads = [it["headline"] for it in items]
        summary = gpt_summary(co, heads)
        lat, lon = safe_geocode(co + " headquarters")

        conn.execute(
            "INSERT OR IGNORE INTO clients"
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summary["summary"], json.dumps([summary["sector"]]), "New", lat, lon)
        )
        for it in items:
            conn.execute(
                "INSERT OR IGNORE INTO signals"
                "(company,headline,url,date) VALUES(?,?,?,?)",
                (co, it["headline"], it["url"], it["date"])
            )
    conn.commit()
    sidebar.success("✅ Scan complete!")

# ───────── CONTACTS ─────────
def company_contacts(company: str):
    """Return list of {name,title,email,phone}."""
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts "
        f"for {company} as JSON array of {{name,title,email,phone}}."
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=256
    )
    if not rsp:
        return []
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        return []

# ───────── LOGO FETCH ─────────
def fetch_logo(company: str):
    """Fetch a logo via Clearbit."""
    domain = company.replace(" ", "") + ".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{domain}", timeout=5)
        if r.ok:
            return r.content
    except:
        pass
    return None

# ───────── PDF EXPORT ─────────
def export_pdf(row, summary_text, contacts):
    """
    Build a PDF: logo, company name, summary bullets, contacts, footer.
    Returns raw PDF bytes.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()

    # Logo
    logo = fetch_logo(row.get("company",""))
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn = f"/tmp/logo.{ext}"
        open(fn, "wb").write(logo)
        pdf.image(fn, x=10, y=10, w=20)
        pdf.set_xy(35, 10)

    # Title
    pdf.set_font("Helvetica","B",16)
    pdf.multi_cell(0,10, row.get("company",""))
    pdf.ln(5)

    # Summary
    pdf.set_font("Helvetica","",12)
    for line in summary_text.split("\n"):
        if line.strip():
            pdf.multi_cell(0,7, "• "+line.strip())
    pdf.ln(3)

    # Contacts
    if contacts:
        pdf.set_font("Helvetica","B",13)
        pdf.cell(0,8,"Key Contacts", ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(
                0,6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}   {c.get('phone','')}"
            )
            pdf.ln(1)

    # Footer
    pdf.set_y(-30)
    pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(
        0,5,
        f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}"
    )

    return pdf.output(dest="S").encode("latin-1")

# ───────── PERMITS CSV ─────────
def fetch_permits():
    """
    Read permits.csv and return list of dicts with company, address, date, type, details_url, lat, lon.
    """
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as f:
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
