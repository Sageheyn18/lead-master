# fetch_signals.py  – Lead Master  v7.0   (2025-06-23)

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
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
OPENAI_KEY       = os.getenv("OPENAI_API_KEY")
client           = OpenAI(api_key=OPENAI_KEY)
geocoder         = Nominatim(user_agent="lead-master")

NEWSAPI_KEY      = "cde04d56b1f7429a84cb3f834791fad7"

MAX_PROSPECTS    = 100
MAX_HEADLINES    = 20
DAILY_BUDGET     = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # in cents
BUDGET_USED      = 0
SUMMARY_THROTTLE = 10    # seconds between GPT summary calls
_LAST_SUMMARY    = 0
RELEVANCE_CUTOFF = 0.45

SEED_KWS = [
    "land purchase", "acres", "groundbreaking", "construct",
    "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold storage",
    "manufacturing facility", "industrial park",
    "relocation", "ground lease", "site plan"
]
EXTRA_KWS = [
    "land", "acres", "site", "build",
    "construction", "expansion", "facility"
]

# ───────── LOCAL CACHE SETUP ─────────
_cache = sqlite3.connect(
    os.path.join(os.getcwd(), "rss_gdelt_cache.db"),
    check_same_thread=False
)
# Enable WAL mode so readers and writers don't block each other
_cache.execute("PRAGMA journal_mode=WAL;")
cache_lock = threading.Lock()

_cache.execute("""
CREATE TABLE IF NOT EXISTS cache(
  key   TEXT PRIMARY KEY,
  data  TEXT,
  ts    INTEGER
)""")
_cache.commit()

def _cached(key: str, ttl: int = 86400):
    now = int(time.time())
    with cache_lock:
        row = _cache.execute(
            "SELECT data,ts FROM cache WHERE key=?", (key,)
        ).fetchone()
        if row and now - row[1] < ttl:
            return json.loads(row[0])
    return None

def _store(key: str, data):
    ts = int(time.time())
    serialized = json.dumps(data)
    with cache_lock:
        _cache.execute(
            "INSERT OR REPLACE INTO cache(key,data,ts) VALUES(?,?,?)",
            (key, serialized, ts)
        )
        _cache.commit()

# ───────── BUDGET & CHAT WRAPPERS ─────────
def budget_ok(cost: float) -> bool:
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

# ───────── UTILITIES ─────────
def dedup(rows: list[dict]) -> list[dict]:
    seen_t, seen_u, out = set(), set(), []
    for r in rows:
        t = (r.get("title","") or r.get("headline","")).lower()
        u = r.get("url","").lower()
        if t in seen_t or u in seen_u:
            continue
        seen_t.add(t); seen_u.add(u)
        out.append(r)
    return out

def _geo(q: str):
    try:
        loc = geocoder.geocode(q, timeout=10)
    except:
        loc = None
    return (loc.latitude, loc.longitude) if loc else (None, None)

def safe_geocode(headline: str, company: str):
    lat, lon = _geo(headline)
    if lat is None or lon is None:
        lat, lon = _geo(f"{company} headquarters")
    return lat, lon

# ───────── NEWS FETCHING ─────────
def gdelt_headlines(query: str, maxrec: int = MAX_PROSPECTS) -> list[dict]:
    key = f"newsapi:{query}:{maxrec}"
    cached = _cached(key)
    if cached is not None:
        return cached

    url = "https://newsapi.org/v2/everything"
    params = {
        "apiKey":   NEWSAPI_KEY,
        "q":        query,
        "pageSize": maxrec,
        "sortBy":   "publishedAt",
        "language": "en"
    }
    try:
        resp = requests.get(url, params=params, timeout=5).json()
        arts = resp.get("articles", [])
        out  = [{
            "title":    a.get("title",""),
            "url":      a.get("url",""),
            "seendate": a.get("publishedAt","")[:10].replace("-","")
        } for a in arts]
    except Exception as e:
        logging.warning(f"NewsAPI failed ({e}); fallback to RSS")
        out = []
    _store(key, out)
    return out

def google_news(co: str, maxrec: int = MAX_HEADLINES) -> list[dict]:
    key = f"rss:{co}:{maxrec}"
    cached = _cached(key)
    if cached is not None:
        return cached

    q   = f'"{co}" ({" OR ".join(EXTRA_KWS)})'
    url = ("https://news.google.com/rss/search?"
           f"q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en")
    try:
        feed = feedparser.parse(url, request_timeout=5)
        today = datetime.datetime.utcnow().strftime("%Y%m%d")
        out   = [{"title":e.title, "url":e.link, "seendate":today}
                 for e in feed.entries[:maxrec]]
    except Exception as e:
        logging.warning(f"RSS fetch failed ({e}); returning empty")
        out = []
    _store(key, out)
    return out

# ───────── GPT SIGNAL PROCESSING ─────────
def gpt_batch_signal_info(headlines: list[str], chunk: int = 10) -> list[dict]:
    results = []
    for i in range(0, len(headlines), chunk):
        batch = headlines[i:i+chunk]
        prompt = "For each headline return JSON array of {headline,company,score}:\n"
        for h in batch:
            prompt += f"- {h}\n"
        rsp = safe_chat(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            temperature=0, max_tokens=256
        )
        if rsp:
            try:
                arr = json.loads(rsp.choices[0].message.content)
                for item in arr:
                    item["score"] = float(item.get("score",0) or 0)
                results.extend(arr)
                continue
            except:
                pass
        # Fallback individual calls
        for h in batch:
            tmp = gpt_signal_info(h)
            results.append({"headline":h, **tmp})
    return results

def gpt_signal_info(head: str) -> dict:
    if not budget_ok(0.07):
        return {"company":"Unknown","score":0.0}
    prompt = (
        f'Return EXACT JSON {{'
        f'"company":<name>,"score":<0-1>'
        f'}} for:\n"{head}"'
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
        return {
            "company": data.get("company","Unknown"),
            "score":   float(data.get("score",0) or 0)
        }
    except:
        return {"company":"Unknown","score":0.0}

# ───────── GPT SUMMARY ─────────
def gpt_summary(co: str, heads: list[str]) -> dict:
    global _LAST_SUMMARY
    wait = SUMMARY_THROTTLE - (time.time() - _LAST_SUMMARY)
    if wait > 0:
        time.sleep(wait)
    _LAST_SUMMARY = time.time()

    if not heads:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}

    cost = 0.5
    if not budget_ok(cost):
        return {"summary":heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}

    prompt = textwrap.dedent(f"""
        Summarise in 5 bullets. Guess sector. land_flag=1 if land purchase.
        Return EXACT JSON {{summary,sector,confidence,land_flag}}.

        Headlines:
        {"".join("- "+h+"\n" for h in heads[:10])}
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
        out["confidence"] = float(out.get("confidence",0) or 0)
        out["land_flag"]  = int(out.get("land_flag",0) or 0)
        return out
    except:
        return {"summary":rsp.choices[0].message.content,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── CONTACTS ─────────
def company_contacts(co: str) -> list[dict]:
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts for {co} "
        "as JSON array of {name,title,email,phone}."
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

# ───────── LOGO & PDF ─────────
def fetch_logo(co: str) -> bytes | None:
    dom = co.replace(" ","") + ".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{dom}", timeout=5)
        if r.ok:
            return r.content
    except:
        pass
    return None

def export_pdf(row: dict, bullets: str, contacts: list[dict]) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()
    pdf.set_font("Helvetica","B",16)

    logo = fetch_logo(row["company"])
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"
        open(fn,"wb").write(logo)
        pdf.image(fn, x=10, y=10, w=20)
        pdf.set_xy(35,10)

    txt = row.get("headline", row.get("title",""))
    pdf.multi_cell(0,10, txt)
    pdf.ln(5)

    pdf.set_font("Helvetica","",12)
    for b in bullets.split("•"):
        if b.strip():
            pdf.multi_cell(0,7,"• "+b.strip())
    pdf.ln(3)

    if contacts:
        pdf.set_font("Helvetica","B",13)
        pdf.cell(0,8,"Key Contacts",ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(
                0,6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}  {c.get('phone','')}"
            )
            pdf.ln(1)

    pdf.set_y(-30)
    pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(
        0,5,
        f"Source: {row.get('url','')}\n"
        f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}"
    )

    return pdf.output(dest="S").encode("latin-1")

# ───────── MANUAL SEARCH ─────────
def manual_search(company: str):
    arts = gdelt_headlines(company, MAX_HEADLINES)
    if not arts:
        arts = google_news(company, MAX_HEADLINES)
    arts = [a for a in arts if any(ek in a["title"].lower() for ek in EXTRA_KWS)]
    arts = dedup(arts)
    heads = [a["title"] for a in arts][:MAX_HEADLINES]
    if not heads:
        return None, [], None, None

    info = gpt_summary(company, heads)
    lat, lon = safe_geocode("", company)
    return info, heads, lat, lon

# ───────── PERMITS IMPORT ─────────
def fetch_permits() -> list[dict]:
    fpath = os.path.join(os.getcwd(), "permits.csv")
    if not os.path.exists(fpath):
        return []
    permits = []
    with open(fpath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lat, lon = safe_geocode(row.get("address",""), row.get("company",""))
            permits.append({
                "company":     row.get("company",""),
                "address":     row.get("address",""),
                "date":        row.get("date",""),
                "type":        row.get("permit_type",""),
                "details_url": row.get("details_url",""),
                "lat":         lat,
                "lon":         lon
            })
    return permits

# ───────── WRITE & SCAN ─────────
def write_signals(rows: list[dict], conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals(company,date,headline,url,source_label,"
            "land_flag,sector_guess,lat,lon) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                r["company"], r["date"], r["headline"], r["url"],
                r["src"],     r["land_flag"], r["sector"],
                r["lat"],     r["lon"]
            )
        )
    conn.commit()

def national_scan():
    conn = get_conn()
    ensure_tables(conn)
    prospects = []

    # ───────── fetch seeds in parallel with status updates ─────────
    bar    = st.progress(0)
    status = st.empty()
    futures = {}
    with ThreadPoolExecutor(max_workers=len(SEED_KWS)) as pool:
        for kw in SEED_KWS:
            futures[pool.submit(
                lambda k=kw: [
                    {
                      "headline":a["title"],
                      "url":      a["url"],
                      "date":     a["seendate"][:8],
                      "src":      "scan"
                    }
                    for a in (gdelt_headlines(k, MAX_PROSPECTS)
                              or google_news(k, MAX_PROSPECTS))
                    if any(ek in a["title"].lower() for ek in EXTRA_KWS)
                ], 
                kw
            )] = kw

        total = len(SEED_KWS)
        for i, fut in enumerate(as_completed(futures), start=1):
            kw = futures[fut]
            status.text(f"Scanning {i}/{total}: “{kw}”")
            prospects.extend(fut.result())
            bar.progress(i/total)

    status.empty(); bar.empty()

    prospects = dedup(prospects)
    infos     = gpt_batch_signal_info([p["headline"] for p in prospects])

    by_co = defaultdict(list)
    for p in prospects:
        for inf in infos:
            try:
                sc = float(inf.get("score",0) or 0)
            except:
                continue
            if inf.get("headline") == p["headline"] and sc >= RELEVANCE_CUTOFF:
                p["company"] = inf.get("company","Unknown")
                by_co[p["company"]].append(p)
                break

    rows=[]
    for co, items in by_co.items():
        heads = [it["headline"] for it in items]
        info  = gpt_summary(co, heads)

        raw = info.get("summary","")
        if isinstance(raw, list):
            info["summary"] = "\n".join(raw)

        contacts = company_contacts(co)
        for c in contacts:
            conn.execute(
                "INSERT OR IGNORE INTO contacts(company,name,title,email,phone)"
                " VALUES(?,?,?,?,?)",
                (
                  co,
                  c.get("name",""),
                  c.get("title",""),
                  c.get("email",""),
                  c.get("phone","")
                )
            )

        lat, lon = safe_geocode("", co)
        conn.execute(
            "INSERT OR REPLACE INTO clients(name,summary,sector_tags,status,lat,lon)"
            " VALUES(?,?,?,?,?,?)",
            (
              co,
              info["summary"],
              json.dumps([info["sector"]]),
              "New",
              lat,
              lon
            )
        )

        for it in items:
            it.update({
                "land_flag":  info["land_flag"],
                "sector":     info["sector"],
                "confidence": info["confidence"],
                "lat":        lat,
                "lon":        lon
            })
            rows.append(it)

    write_signals(rows, conn)
    logging.info(f"Wrote {len(rows)} signals")
