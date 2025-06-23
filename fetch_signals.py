# fetch_signals.py — Lead Master (env-var NEWSAPI_KEY + manual_search) (2025-06-23)

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
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Load NewsAPI key from environment
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
if not NEWSAPI_KEY:
    st.sidebar.warning("⚠️  No NEWSAPI_KEY env var set; national scan will skip NewsAPI.")
api = NewsApiClient(api_key=NEWSAPI_KEY) if NEWSAPI_KEY else None

MAX_PROSPECTS    = 100
MAX_HEADLINES    = 20
DAILY_BUDGET     = int(os.getenv("DAILY_BUDGET_CENTS","300"))
BUDGET_USED      = 0
SUMMARY_THROTTLE = 10
_LAST_SUMMARY    = 0
RELEVANCE_CUTOFF = 0.45

# keywords
KEYWORDS = [
    "land purchase","acquired acres","expansion",
    "construction","facility","plant","warehouse","distribution center"
]
EXTRA_KWS = ["land","acres","site","build","construction","expansion","facility"]

# ───────── SQLITE CACHE ─────────
_cache = sqlite3.connect("rss_gdelt_cache.db", check_same_thread=False)
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
        if row and now-row[1] < ttl:
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
def budget_ok(cost):
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap reached; skipping call.")
        return False
    BUDGET_USED += cost
    return True

def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit – skipping call")
        return None

# ───────── HELPERS ─────────
def dedup(rows):
    seen, out = set(), []
    for r in rows:
        key = (r.get("headline","").lower(), r.get("url","").lower())
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

def _geo(q):
    try:
        return geocoder.geocode(q, timeout=10).latitude, geocoder.geocode(q, timeout=10).longitude
    except:
        return None, None

def safe_geocode(head, company):
    lat, lon = _geo(head)
    if lat is None:
        lat, lon = _geo(f"{company} headquarters")
    return lat, lon

# ───────── GPT SIGNAL INFO ─────────
def gpt_batch_signal_info(headlines, chunk=10):
    results = []
    for i in range(0, len(headlines), chunk):
        batch = headlines[i:i+chunk]
        prompt = "For each headline return JSON array of {headline,company,score}:\n"
        for h in batch: prompt += f"- {h}\n"
        rsp = safe_chat(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            temperature=0, max_tokens=256
        )
        if rsp:
            try:
                arr = json.loads(rsp.choices[0].message.content)
                for item in arr:
                    item["score"] = float(item.get("score") or 0)
                results.extend(arr)
                continue
            except:
                pass
        for h in batch:
            results.append({"headline":h, **gpt_signal_info(h)})
    return results

def gpt_signal_info(head):
    if not budget_ok(0.07):
        return {"company":"Unknown","score":0.0}
    prompt = f'Return EXACT JSON {{"company":<name>,"score":<0-1>}} for:\n"{head}"'
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=32
    )
    if not rsp:
        return {"company":"Unknown","score":0.0}
    try:
        d = json.loads(rsp.choices[0].message.content)
        return {"company":d.get("company","Unknown"), "score":float(d.get("score") or 0)}
    except:
        return {"company":"Unknown","score":0.0}

# ───────── GPT SUMMARY ─────────
def gpt_summary(company, heads):
    global _LAST_SUMMARY
    wait = SUMMARY_THROTTLE - (time.time() - _LAST_SUMMARY)
    if wait>0: time.sleep(wait)
    _LAST_SUMMARY = time.time()

    if not heads:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}
    if not budget_ok(0.5):
        return {"summary":heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}

    prompt = textwrap.dedent(f"""
        Summarise in 5 bullets. Guess sector. land_flag=1 if land purchase.
        Return EXACT JSON {{summary,sector,confidence,land_flag}}.

        Headlines:
        {"".join("- "+h+"\\n" for h in heads[:10])}
    """).strip()
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220
    )
    if not rsp:
        return {"summary":heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}
    try:
        out = json.loads(rsp.choices[0].message.content)
        out["confidence"] = float(out.get("confidence") or 0)
        out["land_flag"]  = int(out.get("land_flag") or 0)
        return out
    except:
        return {"summary":rsp.choices[0].message.content,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    today = datetime.date.today()
    since = today - datetime.timedelta(days=30)
    arts = []
    # 1) try NewsAPI
    if api:
        try:
            resp = api.get_everything(
                q=company,
                from_param=since.isoformat(),
                to=today.isoformat(),
                language="en",
                page_size=MAX_HEADLINES
            )
            arts = resp.get("articles", [])
        except:
            arts = []
    # 2) fallback RSS
    if not arts:
        feed = feedparser.parse(
            "https://news.google.com/rss/search?"
            f"q={quote_plus(company)}%20when:30d&hl=en-US&gl=US&ceid=US:en"
        )
        arts = feed.entries

    # normalize & filter
    rows = []
    for a in arts:
        title = a.get("title", getattr(a,"title",""))
        url   = a.get("url",   getattr(a,"link",""))
        date  = a.get("publishedAt","")[:10] or getattr(a,"published","")[:10]
        rows.append({"headline":title,"url":url,"date":date})
    rows = [r for r in rows if any(ek in r["headline"].lower() for ek in EXTRA_KWS)]
    rows = dedup(rows)
    heads = [r["headline"] for r in rows][:MAX_HEADLINES]
    if not heads:
        return None, [], None, None

    info = gpt_summary(company, heads)
    lat, lon = safe_geocode("", company)
    return info, heads, lat, lon

# ───────── PERMITS ─────────
def fetch_permits():
    path = "permits.csv"
    if not os.path.exists(path):
        return []
    out=[]
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            lat, lon = safe_geocode(r.get("address",""), r.get("company",""))
            out.append({
                "company": r.get("company",""),
                "address": r.get("address",""),
                "date":    r.get("date",""),
                "type":    r.get("permit_type",""),
                "details_url": r.get("details_url",""),
                "lat":     lat,
                "lon":     lon
            })
    return out

# ───────── DB WRITE & NATIONAL SCAN ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            "INSERT OR REPLACE INTO signals"
            "(company,headline,url,date,source_label,land_flag,sector_guess,lat,lon)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
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
    all_prospects=[]

    for idx, kw in enumerate(KEYWORDS,1):
        sidebar.write(f"🔑 [{idx}/{len(KEYWORDS)}] {kw}")

        # NewsAPI
        arts=[]
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
                arts = resp.get("articles",[])
                sidebar.write(f" • NewsAPI: {len(arts)} hits")
            except:
                sidebar.write(" • NewsAPI failed, fallback to RSS")

        # RSS fallback
        if not arts:
            rss = feedparser.parse(
                "https://news.google.com/rss/search?"
                f"q={quote_plus(kw)}%20when:30d&hl=en-US&gl=US&ceid=US:en"
            )
            arts = rss.entries
            sidebar.write(f" • RSS: {len(arts)} hits")

        raw=[]
        for a in arts:
            title = a.get("title", getattr(a,"title",""))
            url   = a.get("url",   getattr(a,"link",""))
            date  = a.get("publishedAt","")[:10] or getattr(a,"published","")[:10]
            raw.append({"headline":title,"url":url,"date":date,"src":"scan"})
        uniq = dedup(raw)
        sidebar.write(f" • Deduped: {len(uniq)} kept")
        all_prospects.extend(uniq)

        sidebar.progress(idx/len(KEYWORDS))

    # Score
    sidebar.write("📝 Scoring…")
    scores = gpt_batch_signal_info([p["headline"] for p in all_prospects])

    sidebar.write("💾 Saving…")
    by_co=defaultdict(list)
    for p,info in zip(all_prospects,scores):
        if info.get("score",0) >= RELEVANCE_CUTOFF:
            p.update(info)
            by_co[info.get("company","Unknown")].append(p)

    for co, items in by_co.items():
        heads = [it["headline"] for it in items]
        summary = gpt_summary(co, heads)
        raw = summary.get("summary","")
        if isinstance(raw,list):
            summary["summary"] = "\n".join(raw)
        lat, lon = safe_geocode("", co)
        conn.execute(
            "INSERT OR REPLACE INTO clients"
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summary["summary"], json.dumps([summary["sector"]]),
             "New", lat, lon)
        )
        write_signals(items, conn)

    conn.commit()
    sidebar.success("✅ Scan complete!")
