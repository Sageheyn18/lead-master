# fetch_signals.py — Lead Master (fresh build)

import os
import json
import datetime
import sqlite3
import logging
import textwrap
import csv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus

import requests
import feedparser
import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
MAX_HEADLINES     = 60
CACHE_RAW_HOURS   = 6
RAW_CACHE_TABLE   = "raw_hits"
SUMMARY_CACHE_TBL = "summaries"
SEED_KWS          = [
    "manufacturing", "industrial", "food processing", "cold storage",
    "distribution center", "warehouse", "plant", "facility"
]

# ───────── STREAMLIT & OPENAI SETUP ─────────
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# ───────── GEOCODER ─────────
_geo = Nominatim(user_agent="lead-master")
def geocode_company(name: str):
    try:
        loc = _geo.geocode(f"{name} headquarters", timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except:
        return (None, None)

# ───────── DATABASE INITIALIZATION ─────────
def _init_db():
    db = get_conn()
    # raw hits cache
    db.execute(f"""
      CREATE TABLE IF NOT EXISTS {RAW_CACHE_TABLE} (
        seed      TEXT,
        fetched   TIMESTAMP,
        headline  TEXT,
        url       TEXT,
        date      TEXT,
        PRIMARY KEY(seed, headline)
      )
    """)
    # summary cache
    db.execute(f"""
      CREATE TABLE IF NOT EXISTS {SUMMARY_CACHE_TBL} (
        headline   TEXT PRIMARY KEY,
        summary    TEXT,
        sector     TEXT,
        confidence REAL,
        land_flag  INTEGER,
        company    TEXT
      )
    """)
    ensure_tables(db)  # your existing clients & signals
    db.commit()

_init_db()

# ───────── RAW HITS CACHING ─────────
def _get_cached_raw(seed: str):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=CACHE_RAW_HOURS)
    rows = get_conn().execute(
        f"SELECT headline,url,date FROM {RAW_CACHE_TABLE} "
        "WHERE seed=? AND fetched>=?",
        (seed, cutoff)
    ).fetchall()
    return [{"headline": r[0], "url": r[1], "date": r[2]} for r in rows] if rows else []

def _set_cached_raw(seed: str, hits: list[dict]):
    db = get_conn()
    now = datetime.datetime.utcnow()
    for h in hits:
        db.execute(
            f"INSERT OR REPLACE INTO {RAW_CACHE_TABLE}"
            "(seed,fetched,headline,url,date) VALUES(?,?,?,?,?)",
            (seed, now, h["headline"], h["url"], h["date"])
        )
    db.commit()

# ───────── SAFE OPENAI CALL ─────────
def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit; skipping")
        return None

# ───────── SUMMARY CACHE HELPERS ─────────
def get_cached_summary(headline: str):
    row = get_conn().execute(
        f"SELECT summary,sector,confidence,land_flag,company "
        f"FROM {SUMMARY_CACHE_TBL} WHERE headline=?",
        (headline,)
    ).fetchone()
    if not row:
        return None
    summary, sector, confidence, land_flag, company = row
    return {
        "summary": summary,
        "sector": sector,
        "confidence": confidence,
        "land_flag": land_flag,
        "company": company
    }

def set_cached_summary(headline: str, info: dict):
    db = get_conn()
    db.execute(
        f"INSERT OR REPLACE INTO {SUMMARY_CACHE_TBL}"
        "(headline,summary,sector,confidence,land_flag,company) VALUES(?,?,?,?,?,?)",
        (
            headline,
            info["summary"],
            info["sector"],
            info["confidence"],
            info["land_flag"],
            info["company"]
        )
    )
    db.commit()

# ───────── MAP HEADLINES → COMPANY ─────────
def _map_headlines_to_company(headlines: list[str]) -> dict[str,str]:
    prompt = textwrap.dedent("""
      You are an assistant. For each of these news headlines,
      extract the primary COMPANY name mentioned.
      Return EXACT JSON array of {"headline": "...", "company": "..."}.
    """).strip() + "\n\nHeadlines:\n" + "\n".join(f"- {h}" for h in headlines)
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
        max_tokens=300
    )
    if not rsp:
        return {}
    try:
        arr = json.loads(rsp.choices[0].message.content)
        return {item["headline"]: item["company"] for item in arr}
    except:
        return {}

# ───────── GPT SUMMARY ─────────
def gpt_summary_batch(headlines: list[str]) -> dict|None:
    prompt = textwrap.dedent("""
      You are an assistant. Given these headlines, please:
      • Summarize in 3 bullet points
      • Identify the industry sector
      • Score confidence (0–1)
      • Flag land_flag=1 if land purchase is indicated
      • Extract primary COMPANY name
      Return EXACT JSON {summary,sector,confidence,land_flag,company}.
    """).strip() + "\n\nHeadlines:\n" + "\n".join(f"- {h}" for h in headlines[:MAX_HEADLINES])
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
        max_tokens=300
    )
    if not rsp:
        return None
    try:
        return json.loads(rsp.choices[0].message.content)
    except Exception as e:
        logging.error(f"GPT JSON parse error: {e}")
        return None

def gpt_summary_single(headline: str) -> dict:
    cached = get_cached_summary(headline)
    if cached:
        return cached
    info = gpt_summary_batch([headline]) or {
        "summary": "", "sector": "unknown",
        "confidence": 0.0, "land_flag": 0, "company": ""
    }
    set_cached_summary(headline, info)
    return info

def gpt_summary(company: str, headlines: list[str]) -> dict:
    info = gpt_summary_batch(headlines) or {}
    return {
        "summary":    info.get("summary", ""),
        "sector":     info.get("sector", "unknown"),
        "confidence": info.get("confidence", 0.0),
        "land_flag":  int(info.get("land_flag", 0))
    }

# ───────── RSS & GDELT FETCH ─────────
def _fetch_for_seed(seed: str) -> list[dict]:
    # try cache
    cached = _get_cached_raw(seed)
    if cached:
        return cached

    # build RSS query
    kws = ["land","acres","site","build","construction","expansion","facility"]
    expr = f'{seed} ({" OR ".join(kws)}) when:30d'
    url  = "https://news.google.com/rss/search?q=" + quote_plus(expr) + "&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    hits = []
    for e in feed.entries[:MAX_HEADLINES]:
        hits.append({
            "headline": e.title,
            "url":       e.link,
            "date":      getattr(e, "published", "")
        })

    # fallback to GDELT if none
    if not hits:
        today = datetime.date.today()
        since = today - datetime.timedelta(days=30)
        q     = quote_plus(f'"{seed}" AND ({since:%Y%m%d} TO {today:%Y%m%d})')
        api   = (
            "https://api.gdeltproject.org/api/v2/doc/docsearch"
            f"?query={q}&filter=SourceCommonName:NEWS"
            f"&mode=ArtList&maxrecords={MAX_HEADLINES}&format=json"
        )
        try:
            docs = requests.get(api, timeout=15).json().get("articles", [])
        except:
            docs = []
        for d in docs:
            hits.append({
                "headline": d.get("headline") or d.get("title",""),
                "url":       d.get("url",""),
                "date":      d.get("date","")
            })

    # dedupe & cache
    seen, out = set(), []
    for h in hits:
        key = h["headline"].lower()
        if key not in seen:
            seen.add(key)
            out.append(h)
    out = out[:MAX_HEADLINES]
    _set_cached_raw(seed, out)
    return out

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    raw = _fetch_for_seed(company)
    if not raw:
        return {"summary":""}, [], None, None

    # map headlines→companies
    mapping = _map_headlines_to_company([r["headline"] for r in raw])

    # prepare detailed rows in parallel
    detailed = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {
            exe.submit(gpt_summary_single, r["headline"]): r for r in raw
        }
        for fut in as_completed(futures):
            base = futures[fut]
            info = fut.result()
            detailed.append({**base, **info})

    # overall summary for primary company
    primary = mapping.get(raw[0]["headline"], company)
    summary = gpt_summary(primary, [r["headline"] for r in raw])
    lat, lon = geocode_company(primary)

    return summary, detailed, lat, lon

# ───────── NATIONAL SCAN ─────────
def national_scan():
    # parallel fetch
    all_hits = []
    with ThreadPoolExecutor(max_workers=len(SEED_KWS)) as exe:
        futures = {exe.submit(_fetch_for_seed, kw): kw for kw in SEED_KWS}
        for fut in as_completed(futures):
            all_hits.extend(fut.result())

    # dedupe
    seen, hits = set(), []
    for r in all_hits:
        key = r["headline"].lower()
        if key not in seen:
            seen.add(key)
            hits.append(r)
    hits = hits[: MAX_HEADLINES * len(SEED_KWS)]

    # map once
    mapping = _map_headlines_to_company([h["headline"] for h in hits])

    # group by company and summarize
    db = get_conn()
    for company, group in defaultdict(list, {
        mapping.get(h["headline"], h["headline"]): []
        for h in hits
    }).items():
        heads = [h["headline"] for h in hits if mapping.get(h["headline"], h["headline"]) == company]
        summary = gpt_summary(company, heads)
        lat, lon = geocode_company(company)
        tags = json.dumps(SEED_KWS)

        # write client
        db.execute(
            "INSERT OR REPLACE INTO clients "
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (company, summary["summary"], tags, "New", lat, lon)
        )
        # write signals
        for h in hits:
            if mapping.get(h["headline"], h["headline"]) == company:
                db.execute(
                    "INSERT OR REPLACE INTO signals "
                    "(company,headline,url,date,lat,lon) VALUES(?,?,?,?,?,?)",
                    (company, h["headline"], h["url"], h["date"], lat, lon)
                )
    db.commit()
    st.sidebar.success("✅ National scan complete!")

# ───────── CONTACTS ─────────
def company_contacts(company: str) -> list[dict]:
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts for {company} "
        "as JSON [{'{'}name,title,email,phone{'}'}]."
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
    domain = company.replace(" ", "") + ".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{domain}", timeout=5)
        return r.content if r.ok else None
    except:
        return None

# ───────── PDF EXPORT ─────────
def export_pdf(row: dict, summary_text: str, contacts: list[dict]) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()

    try:
        logo = fetch_logo(row.get("name", ""))
        if logo:
            ext = magic.from_buffer(logo, mime=True).split("/")[-1]
            fn = f"/tmp/logo.{ext}"
            with open(fn, "wb") as f:
                f.write(logo)
            pdf.image(fn, 10, 10, 20)
            pdf.set_xy(35, 10)
    except:
        pass

    pdf.set_font("Helvetica", "B", 16)
    pdf.multi_cell(0, 10, row.get("name", ""))
    pdf.ln(5)

    pdf.set_font("Helvetica", "", 12)
    for line in summary_text.split("\n"):
        if line.strip():
            pdf.multi_cell(0, 7, "• " + line.strip())
    pdf.ln(3)

    if contacts:
        pdf.set_font("Helvetica", "B", 13)
        pdf.cell(0, 8, "Key Contacts", ln=1)
        pdf.set_font("Helvetica", "", 11)
        for c in contacts:
            pdf.multi_cell(
                0,
                6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')} | {c.get('phone','')}"
            )
            pdf.ln(1)

    pdf.set_y(-20)
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 5, f"Generated on {datetime.datetime.now():%Y-%m-%d %H:%M}")

    return pdf.output(dest="S").encode("latin-1")

# ───────── PERMITS ─────────
def fetch_permits() -> list[dict]:
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            lat, lon = geocode_company(r.get("company", ""))
            out.append({
                "company":     r.get("company", ""),
                "address":     r.get("address", ""),
                "date":        r.get("date", ""),
                "type":        r.get("permit_type", ""),
                "details_url": r.get("details_url", ""),
                "lat":         lat,
                "lon":         lon
            })
    return out
