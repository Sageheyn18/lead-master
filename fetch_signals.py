# fetch_signals.py — Lead Master v9.0 (2025-06-23)

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
from newsapi import NewsApiClient
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
OPENAI_KEY    = os.getenv("OPENAI_API_KEY", "")
client        = OpenAI(api_key=OPENAI_KEY)
geocoder      = Nominatim(user_agent="lead-master")

# NewsAPI via ENV var
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
if not NEWSAPI_KEY:
    st.sidebar.warning("⚠️ No NEWSAPI_KEY env var set; national scan will skip NewsAPI.")
api = NewsApiClient(api_key=NEWSAPI_KEY) if NEWSAPI_KEY else None

MAX_HEADLINES    = 20
DAILY_BUDGET_CTS = int(os.getenv("DAILY_BUDGET_CENTS","300"))
BUDGET_USED      = 0
SUMMARY_THROTTLE = 10
_LAST_SUMMARY    = 0
RELEVANCE_CUTOFF = 0.45

# National scan keywords
KEYWORDS = [
    "land purchase","acquired acres","expansion",
    "construction","facility","plant","warehouse","distribution center"
]
EXTRA_KWS = ["land","acres","site","build","construction","expansion","facility"]

# ───────── SQLITE CACHE ─────────
_cache = sqlite3.connect("rss_cache.db", check_same_thread=False)
_cache.execute("PRAGMA journal_mode=WAL;")
cache_lock = threading.Lock()
_cache.execute("""
CREATE TABLE IF NOT EXISTS cache(
  key   TEXT PRIMARY KEY,
  data  TEXT,
  ts    INTEGER
)""")
_cache.commit()

def _cached(key, ttl=86400):
    now = int(time.time())
    with cache_lock:
        row = _cache.execute("SELECT data,ts FROM cache WHERE key=?", (key,)).fetchone()
        if row and now - row[1] < ttl:
            return json.loads(row[0])
    return None

def _store(key, data):
    ts = int(time.time())
    with cache_lock:
        _cache.execute(
            "INSERT OR REPLACE INTO cache(key,data,ts) VALUES(?,?,?)",
            (key, json.dumps(data), ts)
        )
        _cache.commit()

# ───────── BUDGET & CHAT ─────────
def budget_ok(cost_cents):
    global BUDGET_USED
    if BUDGET_USED + cost_cents > DAILY_BUDGET_CTS:
        logging.warning("GPT daily budget exceeded; skipping call.")
        return False
    BUDGET_USED += cost_cents
    return True

def safe_chat(**params):
    try:
        return client.chat.completions.create(**params)
    except RateLimitError:
        logging.warning("OpenAI rate-limit reached; skipping call.")
        return None

# ───────── HELPERS ─────────
def dedup(rows):
    seen = set(); out = []
    for r in rows:
        key = (r.get("headline","").lower(), r.get("url","").lower())
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

def _geo(address):
    try:
        loc = geocoder.geocode(address, timeout=10)
        return loc.latitude, loc.longitude
    except:
        return None, None

def safe_geocode(headline, company):
    lat, lon = _geo(headline)
    if lat is None:
        lat, lon = _geo(f"{company} headquarters")
    return lat, lon

# ───────── GPT-DRIVEN FUNCTIONS ─────────
def gpt_signal_info(headline: str):
    if not budget_ok(7):  # e.g. 7¢ per call
        return {"company":"Unknown","score":0.0}
    prompt = (
        "Extract the company name and relevance score [0–1] as JSON "
        f'{{"company":<name>,"score":<0-1>}} for this headline:\n"{headline}"'
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=32
    )
    if not rsp:
        return {"company":"Unknown","score":0.0}
    try:
        data = json.loads(rsp.choices[0].message.content)
        return {"company": data.get("company","Unknown"),
                "score": float(data.get("score") or 0)}
    except:
        return {"company":"Unknown","score":0.0}

def gpt_batch_signal_info(headlines, chunk=10):
    infos = []
    for i in range(0, len(headlines), chunk):
        slice_ = headlines[i:i+chunk]
        # Batch prompt
        prompt = "Return JSON list of {headline,company,score} for:\n"
        prompt += "\n".join(f"- {h}" for h in slice_)
        rsp = safe_chat(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            temperature=0, max_tokens=256
        )
        if rsp:
            try:
                items = json.loads(rsp.choices[0].message.content)
                for itm in items:
                    itm["score"] = float(itm.get("score",0))
                infos.extend(items)
                continue
            except:
                pass
        # Fallback to individual calls
        for h in slice_:
            infos.append({"headline":h, **gpt_signal_info(h)})
    return infos

def gpt_summary(company, headlines):
    global _LAST_SUMMARY
    wait = SUMMARY_THROTTLE - (time.time() - _LAST_SUMMARY)
    if wait>0: time.sleep(wait)
    _LAST_SUMMARY = time.time()

    if not headlines:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}

    if not budget_ok(50):  # e.g. 50¢ for summary
        return {
            "summary": headlines[0][:120]+"…",
            "sector":"unknown","confidence":0,"land_flag":0
        }

    bullets = "\n".join(f"- {h}" for h in headlines[:10])
    prompt = textwrap.dedent(f"""
        Summarise in 5 bullet points. Guess sector.
        Set land_flag=1 if a land purchase is indicated.
        Return EXACT JSON {{summary,sector,confidence,land_flag}}.

        Headlines:
        {bullets}
    """).strip()

    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220
    )
    if not rsp:
        return {
            "summary": headlines[0][:120]+"…",
            "sector":"unknown","confidence":0,"land_flag":0
        }
    try:
        out = json.loads(rsp.choices[0].message.content)
        out["confidence"] = float(out.get("confidence",0))
        out["land_flag"]  = int(out.get("land_flag",0))
        return out
    except:
        return {
            "summary": rsp.choices[0].message.content,
            "sector":"unknown","confidence":0,"land_flag":0
        }

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    """Lookup headlines for one company, return (summary, headlines, lat, lon)."""
    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)

    # 1) NewsAPI
    entries = []
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
        rss = feedparser.parse(
            "https://news.google.com/rss/search?"
            f"q={quote_plus(company)}%20when:30d&hl=en-US&gl=US&ceid=US:en"
        )
        entries = rss.entries

    # Normalize & filter keywords
    raw = []
    for e in entries:
        title = e.get("title", getattr(e,"title",""))
        url   = e.get("url",   getattr(e,"link",""))
        date  = (e.get("publishedAt","")[:10]
                 or getattr(e,"published","")[:10])
        raw.append({"headline":title,"url":url,"date":date})
    # filter by our EXTRA_KWS
    raw = [r for r in raw if any(kw in r["headline"].lower() for kw in EXTRA_KWS)]
    unique = dedup(raw)
    heads  = [u["headline"] for u in unique][:MAX_HEADLINES]

    if not heads:
        return None, [], None, None

    info = gpt_summary(company, heads)
    lat, lon = safe_geocode("", company)
    return info, heads, lat, lon

# ───────── CONTACTS & LOGO ─────────
def company_contacts(company: str):
    prompt = (
        f"List up to 3 procurement, engineering, or construction "
        f"contacts at {company} as JSON array of "
        '{"name", "title", "email", "phone"}.'
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

def fetch_logo(company: str):
    """Fetch company logo via Clearbit if possible."""
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
    pdf = FPDF()
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()

    # Logo
    logo = fetch_logo(row["company"])
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn = f"/tmp/logo.{ext}"
        open(fn, "wb").write(logo)
        pdf.image(fn, x=10, y=10, w=20)
        pdf.set_xy(35, 10)

    # Headline or title
    txt = row.get("headline", row.get("company", ""))
    pdf.set_font("Helvetica","B",16)
    pdf.multi_cell(0, 10, txt)
    pdf.ln(5)

    # Summary bullets
    pdf.set_font("Helvetica","",12)
    for bullet in summary_text.split("\n"):
        if bullet.strip():
            pdf.multi_cell(0, 7, "• " + bullet.strip())
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
        f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}\n"
        f"Source URL: {row.get('url','')}"
    )

    return pdf.output(dest="S").encode("latin-1")

# ───────── PERMITS ─────────
def fetch_permits():
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            lat, lon = safe_geocode(r.get("address",""), r.get("company",""))
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

# ───────── SIGNAL WRITER & NATIONAL SCAN ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            "INSERT OR REPLACE INTO signals "
            "(company,headline,url,date,source_label,land_flag,sector_guess,lat,lon) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                r["company"], r["headline"], r["url"], r["date"],
                r.get("src","scan"), r.get("land_flag",0),
                r.get("sector",""), r.get("lat"), r.get("lon")
            )
        )
    conn.commit()

def national_scan():
    conn = get_conn()
    ensure_tables(conn)

    sidebar = st.sidebar
    sidebar.header("National Scan Progress")

    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)
    all_prospects = []

    for idx, kw in enumerate(KEYWORDS, 1):
        sidebar.write(f"🔑 [{idx}/{len(KEYWORDS)}] {kw}")

        # 1) NewsAPI
        entries = []
        if api:
            try:
                resp = api.get_everything(
                    q=kw,
                    from_param=since.isoformat(),
                    to=today.isoformat(),
                    language="en",
