# fetch_signals.py — Lead Master v11.1

import os
import json
import time
import datetime
import logging
import textwrap
import sqlite3
import csv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote_plus

import requests
import feedparser
import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── OpenAI client via Streamlit secrets ─────────
# Make sure you have .streamlit/secrets.toml with:
# OPENAI_API_KEY = "sk-..."
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# ───────── CONFIG ─────────
MAX_HEADLINES    = 60
RELEVANCE_CUTOFF = 0.45
CACHE_TABLE      = "summaries"
SEED_KWS         = [
    "manufacturing", "industrial", "food processing", "cold storage",
    "distribution center", "warehouse", "plant", "facility"
]

# ───────── CACHE TABLE SETUP ─────────
def _init_cache():
    conn = get_conn()
    conn.execute(f"""
      CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
        headline   TEXT PRIMARY KEY,
        summary    TEXT,
        sector     TEXT,
        confidence REAL,
        land_flag  INTEGER,
        company    TEXT
      )
    """)
    conn.commit()
_init_cache()

# ───────── CACHE HELPERS ─────────
def get_cached(headline: str):
    row = get_conn().execute(
        f"SELECT summary,sector,confidence,land_flag,company FROM {CACHE_TABLE} WHERE headline=?",
        (headline,),
    ).fetchone()
    if not row:
        return None
    s, sec, conf, lf, co = row
    return {"summary":s, "sector":sec, "confidence":conf, "land_flag":lf, "company":co}

def set_cached(headline: str, info: dict):
    get_conn().execute(
        f"INSERT OR REPLACE INTO {CACHE_TABLE} "
        "(headline,summary,sector,confidence,land_flag,company) VALUES(?,?,?,?,?,?)",
        (headline, info["summary"], info["sector"],
         info["confidence"], info["land_flag"], info["company"]),
    )
    get_conn().commit()

# ───────── SAFE GPT CALL ─────────
def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit – skipping")
        return None

# ───────── BATCHED GPT SUMMARY ─────────
def gpt_summary_batch(headlines: list[str]) -> dict|None:
    prompt = textwrap.dedent("""
      You are an assistant. Given these headlines indicating potential
      land purchases or new construction, please:
      • Summarize in 3 bullet points
      • Identify industry sector
      • Give a confidence score (0–1)
      • Flag land_flag=1 if land purchase is indicated, else 0
      • Extract primary COMPANY name
      Return EXACT JSON: {summary,sector,confidence,land_flag,company}.
    """).strip() + "\n\nHeadlines:\n" + "\n".join(f"- {h}" for h in headlines[:MAX_HEADLINES])

    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
        max_tokens=300,
    )
    if not rsp:
        return None
    try:
        return json.loads(rsp.choices[0].message.content)
    except Exception as e:
        logging.error(f"GPT parse error: {e}")
        return None

def gpt_summary_single(headline: str) -> dict:
    cached = get_cached(headline)
    if cached:
        return cached

    info = gpt_summary_batch([headline]) or {
        "summary":"", "sector":"unknown", "confidence":0.0,
        "land_flag":0, "company":""
    }
    set_cached(headline, info)
    return info

# ───────── WRAPPER ─────────
def gpt_summary(company: str, headlines: list[str]) -> dict:
    info = gpt_summary_batch(headlines) or {}
    return {
        "summary":    info.get("summary",""),
        "sector":     info.get("sector","unknown"),
        "confidence": info.get("confidence",0.0),
        "land_flag":  int(info.get("land_flag",0))
    }

# ───────── GEO ─────────
_geo = Nominatim(user_agent="lead-master")
def geocode_company(name: str) -> tuple[float|None,float|None]:
    try:
        loc = _geo.geocode(name + " headquarters", timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None,None)
    except:
        return (None,None)

# ───────── RSS & GDELT ─────────
def rss_search(query: str, days:int=30, maxrec:int=60):
    q     = quote_plus(f'{query} when:{days}d')
    url   = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    feed  = feedparser.parse(url)
    return feed.entries[:maxrec]

def gdelt_search(query: str, days:int=30, maxrec:int=60):
    today = datetime.date.today()
    since = today - datetime.timedelta(days=days)
    q     = quote_plus(f'"{query}" AND ({since:%Y%m%d} TO {today:%Y%m%d})')
    url   = (
      "https://api.gdeltproject.org/api/v2/doc/docsearch"
      f"?query={q}&filter=SourceCommonName:NEWS&mode=ArtList"
      f"&maxrecords={maxrec}&format=json"
    )
    try:
        j    = requests.get(url,timeout=15).json()
        docs = j.get("articles") or j.get("docs") or []
        return docs
    except:
        return []

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    kws     = ["land","acres","site","build","construction","expansion","facility"]
    expr    = company + " (" + " OR ".join(kws) + ")"
    entries = rss_search(expr) or [
        {"headline":d["headline"],"url":d["url"],"date":d["date"]}
        for d in gdelt_search(expr)
    ]

    rows = []
    for e in entries:
        title = e.get("title", getattr(e,"headline",""))
        url   = e.get("link",  getattr(e,"url",""))
        date  = e.get("published", e.get("date",""))
        rows.append({"headline":title,"url":url,"date":date})

    filtered = [r for r in rows if any(k in r["headline"].lower() for k in kws)]
    deduped  = []
    seen     = set()
    for r in filtered:
        key = r["headline"].lower()
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    deduped = deduped[:MAX_HEADLINES]

    info     = gpt_summary(company, [r["headline"] for r in deduped])
    detailed = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futs = {exe.submit(gpt_summary_single,r["headline"]): r for r in deduped}
        for fut in as_completed(futs):
            base = futs[fut]
            inf  = fut.result()
            detailed.append({**base,**inf})

    lat, lon = geocode_company(company)
    return info, detailed, lat, lon

# ───────── NATIONAL SCAN ─────────
def national_scan():
    conn = get_conn()
    ensure_tables(conn)

    raw = []
    for kw in SEED_KWS:
        raw.extend([{"headline":e.title,"url":e.link,"seed":kw}
                    for e in rss_search(kw, maxrec=MAX_HEADLINES)])
    if not raw:
        for kw in SEED_KWS:
            raw.extend([{"headline":d["headline"],"url":d["url"],"seed":kw}
                        for d in gdelt_search(kw,maxrec=MAX_HEADLINES)])

    # dedupe
    seen = set(); hits = []
    for r in raw:
        key = r["headline"].lower()
        if key not in seen:
            seen.add(key); hits.append(r)
    hits = hits[: MAX_HEADLINES * len(SEED_KWS)]

    # parallel summarize
    scored = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futs = {exe.submit(gpt_summary_single,h["headline"]): h for h in hits}
        for fut in as_completed(futs):
            base = futs[fut]; inf = fut.result()
            scored.append({**base,**inf})

    by_co = defaultdict(list)
    for s in scored:
        co = s.get("company") or s["seed"]
        by_co[co].append(s)

    for co, recs in by_co.items():
        summary = gpt_summary(co,[r["headline"] for r in recs])
        lat, lon = geocode_company(co)
        tags = json.dumps(list({r["seed"] for r in recs}))

        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summary["summary"], tags, "New", lat, lon)
        )
        for r in recs:
            conn.execute(
                "INSERT OR REPLACE INTO signals "
                "(company,headline,url,date,lat,lon) VALUES(?,?,?,?,?,?)",
                (co, r["headline"], r["url"], r.get("date"), lat, lon)
            )
    conn.commit()

# ───────── CONTACTS ─────────
def company_contacts(company: str) -> list[dict]:
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts for {company} "
        "as JSON array of {name,title,email,phone}."
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
def fetch_logo(company: str) -> bytes|None:
    dom = company.replace(" ","")+".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{dom}",timeout=5)
        return r.content if r.ok else None
    except:
        return None

# ───────── PDF EXPORT ─────────
def export_pdf(row, summary_text, contacts) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(True,15)
    pdf.add_page()

    logo = fetch_logo(row.get("company",""))
    if logo:
        ext = magic.from_buffer(logo,mime=True).split("/")[-1]
        fn = f"/tmp/logo.{ext}"
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
    pdf.multi_cell(0,5,
        f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}"
    )
    return pdf.output(dest="S").encode("latin-1")

# ───────── PERMITS ─────────
def fetch_permits() -> list[dict]:
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out=[]
    with open(path,newline="",encoding="utf-8") as f:
        for r in csv.DictReader(f):
            lat, lon = geocode_company(r.get("company",""))
            out.append({
                "company":     r.get("company",""),
                "address":     r.get("address",""),
                "date":        r.get("date",""),
                "type":        r.get("permit_type",""),
                "details_url": r.get("details_url",""),
                "lat":         lat,
                "lon":         lon
            })
    return out
