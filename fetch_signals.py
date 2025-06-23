# fetch_signals.py — Lead Master v12.0

import os
import json
import time
import datetime
import logging
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus

import feedparser
import requests
import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
MAX_HEADLINES     = 60
CACHE_RAW_HOURS   = 6
SEED_KWS          = [
    "manufacturing", "industrial", "food processing", "cold storage",
    "distribution center", "warehouse", "plant", "facility"
]
RELEVANCE_CUTOFF  = 0.45
RAW_CACHE_TABLE   = "raw_hits"
SUMMARY_CACHE_TBL = "summaries"

# ───────── STREAMLIT + OPENAI SETUP ─────────
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# ───────── GEOCODER ─────────
_geo = Nominatim(user_agent="lead-master")
def geocode_company(name: str):
    try:
        loc = _geo.geocode(name + " headquarters", timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except:
        return (None, None)

# ───────── DB → ensure all tables ─────────
def _init_db():
    c = get_conn()
    # raw hits cache
    c.execute(f"""
      CREATE TABLE IF NOT EXISTS {RAW_CACHE_TABLE} (
        seed      TEXT,
        fetched   TIMESTAMP,
        headline  TEXT,
        url       TEXT,
        date      TEXT,
        PRIMARY KEY(seed,headline)
      )
    """)
    # summary cache
    c.execute(f"""
      CREATE TABLE IF NOT EXISTS {SUMMARY_CACHE_TBL} (
        headline   TEXT PRIMARY KEY,
        summary    TEXT,
        sector     TEXT,
        confidence REAL,
        land_flag  INTEGER,
        company    TEXT
      )
    """)
    ensure_tables(c)  # your existing clients/signals
    c.commit()

_init_db()

# ───────── RAW‐HITS CACHING ─────────
def _get_cached_raw(seed):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=RAW_CACHE_TABLE == RAW_CACHE_TABLE and CACHE_RAW_HOURS or 0)
    rows = get_conn().execute(
        f"SELECT headline,url,date FROM {RAW_CACHE_TABLE} WHERE seed=? AND fetched>=?",
        (seed, cutoff)
    ).fetchall()
    return [{"headline":r[0],"url":r[1],"date":r[2]} for r in rows] if rows else []

def _set_cached_raw(seed, hits):
    db = get_conn()
    now = datetime.datetime.utcnow()
    for h in hits:
        db.execute(
            f"INSERT OR REPLACE INTO {RAW_CACHE_TABLE}"
            "(seed,fetched,headline,url,date) VALUES(?,?,?,?,?)",
            (seed, now, h["headline"], h["url"], h["date"])
        )
    db.commit()

# ───────── SAFE GPT ─────────
def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("GPT rate-limit; skipping")
        return None

# ───────── SUMMARY‐CACHE HELPERS ─────────
def get_cached_summary(headline):
    row = get_conn().execute(
        f"SELECT summary,sector,confidence,land_flag,company "
        f"FROM {SUMMARY_CACHE_TBL} WHERE headline=?", (headline,)
    ).fetchone()
    if not row:
        return None
    s, sec, conf, lf, co = row
    return {"summary":s,"sector":sec,"confidence":conf,"land_flag":lf,"company":co}

def set_cached_summary(headline, info):
    db = get_conn()
    db.execute(
        f"INSERT OR REPLACE INTO {SUMMARY_CACHE_TBL}"
        "(headline,summary,sector,confidence,land_flag,company) VALUES(?,?,?,?,?,?)",
        (headline, info["summary"], info["sector"],
         info["confidence"], info["land_flag"], info["company"])
    )
    db.commit()

# ───────── BATCHED GPT SUMMARY & MAPPING ─────────
def _map_headlines_to_company(headlines: list[str]) -> dict[str,str]:
    """
    One GPT call: input all headlines, output JSON array:
    [{"headline":"…","company":"…"}, …]
    """
    prompt = (
        "Given these news headlines, extract the primary COMPANY name for each.\n"
        "Return EXACT JSON array of objects with keys 'headline' and 'company'.\n\n"
        "Headlines:\n" + "\n".join(f"- {h}" for h in headlines)
    )
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

def gpt_summary_batch(headlines: list[str]) -> dict|None:
    """
    Summarize up to MAX_HEADLINES for one company:
    returns {summary,sector,confidence,land_flag,company}
    """
    prompt = (
        "You are an assistant. Summarize these headlines:"
        "\n• Summarize in 3 bullets"
        "\n• Identify sector"
        "\n• Confidence 0–1"
        "\n• land_flag=1 if land purchase indicated"
        "\nReturn EXACT JSON {summary,sector,confidence,land_flag,company}.\n\n"
        "Headlines:\n" + "\n".join(f"- {h}" for h in headlines)
    )
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
    except:
        return None

def gpt_summary_single(headline: str) -> dict:
    """
    Return cached or batch-summary on [headline] alone.
    """
    cached = get_cached_summary(headline)
    if cached:
        return cached

    info = gpt_summary_batch([headline])
    if not info:
        info = {
            "summary":"", "sector":"unknown",
            "confidence":0.0, "land_flag":0, "company":""
        }
    set_cached_summary(headline, info)
    return info

# ───────── RSS → GDELT FETCHER ─────────
def _fetch_for_seed(seed: str):
    """Fetch & return list of {'headline','url','date'} for one seed."""
    # 1) check cache
    cached = _get_cached_raw(seed)
    if cached:
        return cached

    kws = ["land","acres","site","build","construction","expansion","facility"]
    expr = f'{seed} ({" OR ".join(kws)}) when:30d'
    url  = "https://news.google.com/rss/search?q="+ quote_plus(expr) + "&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    entries = feed.entries[:MAX_HEADLINES]

    hits = []
    for e in entries:
        hits.append({
            "headline": e.title,
            "url":       e.link,
            "date":      getattr(e,"published", "")
        })
    # fallback?
    if not hits:
        docs = []
        # GDELT fallback
        today = datetime.date.today()
        since = today - datetime.timedelta(days=30)
        q     = quote_plus(f'"{seed}" AND ({since:%Y%m%d} TO {today:%Y%m%d})')
        api   = f"https://api.gdeltproject.org/api/v2/doc/docsearch?query={q}&filter=SourceCommonName:NEWS&mode=ArtList&maxrecords={MAX_HEADLINES}&format=json"
        try:
            docs = requests.get(api,timeout=15).json().get("articles",[])
        except:
            docs = []
        for d in docs:
            hits.append({
                "headline": d.get("title",""),
                "url":       d.get("url",""),
                "date":      d.get("seendate","")
            })

    # dedupe
    seen,hits2 = set(), []
    for h in hits:
        tl = h["headline"].lower()
        if tl not in seen:
            seen.add(tl)
            hits2.append(h)
    hits2 = hits2[:MAX_HEADLINES]

    # cache raw
    _set_cached_raw(seed, hits2)
    return hits2

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    """
    One-off lookup: RSS→GDELT → dedupe → batch summary mapping → details → geo
    Returns (info, detailed, lat, lon)
    """
    # fetch raw
    raw = _fetch_for_seed(company)
    if not raw:
        return None, [], None, None

    headlines = [r["headline"] for r in raw]
    # map to companies
    mapping = _map_headlines_to_company(headlines)

    # group raw by mapping
    groups = defaultdict(list)
    for r in raw:
        co = mapping.get(r["headline"], company)
        groups[co].append(r["headline"])

    # per-headline details in parallel
    detailed = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {
            exe.submit(gpt_summary_single, h["headline"]): h
            for h in raw
        }
        for fut in as_completed(futures):
            r = futures[fut]
            inf = fut.result()
            detailed.append({**r, **inf})

    # overall summary from headlines under primary company
    primary = list(groups.keys())[0]
    info    = gpt_summary_batch(groups[primary]) or {}
    lat,lon = geocode_company(primary)

    return info, detailed, lat, lon

# ───────── NATIONAL SCAN ─────────
def national_scan():
    """
    1) parallel‐fetch raw hits for all SEED_KWS
    2) dedupe → one mapping GPT call
    3) group headlines→company
    4) one summary GPT call per company
    5) write clients & signals
    6) UI feedback
    """
    _init_db()  # ensure all tables

    # 1) parallel fetch all seeds
    all_raw = []
    with ThreadPoolExecutor(max_workers=len(SEED_KWS)) as exe:
        futures = {exe.submit(_fetch_for_seed, kw): kw for kw in SEED_KWS}
        for fut in as_completed(futures):
            all_raw.extend(fut.result())

    # 2) dedupe
    seen,hits = set(), []
    for r in all_raw:
        key = r["headline"].lower()
        if key not in seen:
            seen.add(key)
            hits.append(r)
    hits = hits[: MAX_HEADLINES * len(SEED_KWS)]

    # 3) one mapping call
    mapping = _map_headlines_to_company([h["headline"] for h in hits])

    # 4) group by mapping
    groups = defaultdict(list)
    for h in hits:
        co = mapping.get(h["headline"], h["headline"])
        groups[co].append(h["headline"])

    # 5) summary per company + write to DB
    db = get_conn()
    for co, heads in groups.items():
        summ   = gpt_summary_batch(heads) or {}
        lat,lon= geocode_company(co)
        tags   = json.dumps(SEED_KWS)
        # clients
        db.execute(
            "INSERT OR REPLACE INTO clients "
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summ.get("summary",""), tags, "New", lat, lon)
        )
        # signals
        for h in heads:
            db.execute(
                "INSERT OR REPLACE INTO signals "
                "(company,headline,url,date,lat,lon) VALUES(?,?,?,?,?,?)",
                (co, h, next(r["url"] for r in hits if r["headline"]==h),
                 next(r["date"] for r in hits if r["headline"]==h), lat, lon)
            )
    db.commit()

    # 6) UI feedback
    st.sidebar.success("✅ National scan complete! All data written.")
