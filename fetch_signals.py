# fetch_signals.py

import json
import logging
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

import feedparser
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError

from utils import get_conn, ensure_tables

# ───────── OpenAI client ─────────
client = OpenAI()

# ───────── Constants ─────────
MAX_HEADLINES = 60      # lower hit count
CACHE_TABLE = "summaries"

# ───────── Ensure summary‐cache table exists ─────────
def _init_cache_table():
    conn = get_conn()
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE}(
            headline   TEXT PRIMARY KEY,
            summary    TEXT,
            sector     TEXT,
            confidence REAL,
            land_flag  INTEGER,
            company    TEXT
        )
    """)
    conn.commit()

_init_cache_table()


# ───────── Caching Helpers ─────────
def get_cached(headline: str):
    conn = get_conn()
    row = conn.execute(
        f"SELECT summary,sector,confidence,land_flag,company FROM {CACHE_TABLE} WHERE headline = ?",
        (headline,),
    ).fetchone()
    if row:
        s, sec, conf, lf, comp = row
        return {"summary": s, "sector": sec, "confidence": conf, "land_flag": lf, "company": comp}
    return None

def set_cached(headline: str, info: dict):
    conn = get_conn()
    conn.execute(
        f"""INSERT OR REPLACE INTO {CACHE_TABLE}
            (headline, summary, sector, confidence, land_flag, company)
           VALUES (?,?,?,?,?,?)""",
        (
            headline,
            info["summary"],
            info["sector"],
            info["confidence"],
            info["land_flag"],
            info["company"],
        ),
    )
    conn.commit()


# ───────── Safe OpenAI call ─────────
def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit hit; skipping call")
        return None


# ───────── GPT summarizer (batched) ─────────
def gpt_summary_batch(headlines: list[str]) -> dict | None:
    """
    One GPT call for up to MAX_HEADLINES headlines.
    Returns a JSON dict with keys:
      summary, sector, confidence, land_flag, company
    """
    prompt = (
        "You are an assistant that reads a list of news headlines about potential "
        "construction or land purchases.  Please:\n"
        "  • Summarize them in 3 bullet points\n"
        "  • Identify the industry sector\n"
        "  • Give a confidence score (0–1)\n"
        "  • Flag with 1 if they involve land purchase or new build, else 0\n"
        "  • Extract the primary COMPANY name referenced\n"
        "Output exactly one JSON object with keys "
        '"summary","sector","confidence","land_flag","company".\n\n'
        "Headlines:\n"
        + "\n".join(f"- {h}" for h in headlines)
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=300,
    )
    if not rsp:
        return None
    try:
        return json.loads(rsp.choices[0].message.content)
    except Exception as e:
        logging.error(f"Failed to parse GPT JSON: {e}")
        return None


def gpt_summary_single(headline: str) -> dict:
    """
    Returns cached summary or calls GPT on [headline].
    """
    cached = get_cached(headline)
    if cached:
        return cached

    info = gpt_summary_batch([headline])
    if not info:
        # on failure, return empty defaults
        info = {
            "summary": "",
            "sector": "unknown",
            "confidence": 0.0,
            "land_flag": 0,
            "company": "",
        }

    set_cached(headline, info)
    return info


# ───────── Headline fetchers ─────────
def headlines_for_company(company: str) -> list[dict]:
    """
    Use Google News RSS to grab up to MAX_HEADLINES items for [company]
    with keywords related to land/construction.
    """
    q = f'"{company}" (land OR acres OR site OR build OR construction OR expansion OR facility OR plant OR warehouse OR "distribution center")'
    params = {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"}
    url = "https://news.google.com/rss/search?" + urlencode(params)
    feed = feedparser.parse(url)
    entries = feed.entries[:MAX_HEADLINES]
    return [{"headline": e.title, "url": e.link} for e in entries]


# ───────── Geocoding ─────────
_geo = Nominatim(user_agent="lead-master-app")

def geocode_company(name: str) -> tuple[float | None, float | None]:
    try:
        loc = _geo.geocode(name + " headquarters")
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return (None, None)


# ───────── Manual search (sidebar lookup) ─────────
def manual_search(company: str):
    """
    Fetch headlines for [company], summarize each in parallel,
    then geocode once.
    Returns (summary_dict, list_of_detailed_rows, lat, lon).
    """
    heads = headlines_for_company(company)

    # parallelize up to 4 summaries at a time
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(gpt_summary_single, h["headline"]): h for h in heads}
        for fut in as_completed(futures):
            h = futures[fut]
            info = fut.result()
            results.append({
                "headline": h["headline"],
                "url":      h["url"],
                **info
            })

    # overall summary: pick the batch summary of all headlines
    batch_info = gpt_summary_batch([h["headline"] for h in heads]) or {}
    lat, lon = geocode_company(company)

    return (
        batch_info,
        results,
        lat,
        lon
    )


# ───────── National scan (runs on schedule) ─────────
SEED_KWS = [
    "manufacturing", "industrial", "food processing", "cold storage",
    "distribution center", "warehouse", "plant", "facility"
]

def national_scan():
    """
    For each seed keyword, fetch headlines → summarize in batch → store in DB.
    """
    db = get_conn()
    ensure_tables()  # your existing client/signals tables

    # 1) collect all raw hits
    all_hits = []
    for kw in SEED_KWS:
        hits = headlines_for_company(kw)
        for h in hits:
            all_hits.append({**h, "seed": kw})

    # 2) dedupe by headline text
    seen = set()
    deduped = []
    for h in all_hits:
        if h["headline"] not in seen:
            seen.add(h["headline"])
            deduped.append(h)
    all_hits = deduped[: MAX_HEADLINES * len(SEED_KWS)]

    # 3) summarize each headline (parallel)
    sidebar = logging.getLogger("streamlit")  # pretend to write progress
    scored = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(gpt_summary_single, h["headline"]): h for h in all_hits}
        for i, fut in enumerate(as_completed(futures), start=1):
            info = fut.result()
            h = futures[fut]
            scored.append({
                **h,
                **info
            })
            # you can update a progress bar here if you wire it in app.py

    # 4) group by company and insert into your signals/clients tables
    by_co = defaultdict(list)
    for rec in scored:
        co = rec.get("company") or rec["seed"]
        by_co[co].append(rec)

    for co, recs in by_co.items():
        # store client
        summ = gpt_summary_batch([r["headline"] for r in recs]) or {}
        lat, lon = geocode_company(co)
        tags = json.dumps(list({r["seed"] for r in recs}))
        db.execute(
            """INSERT OR REPLACE INTO clients
               (name, summary, sector_tags, status, lat, lon)
               VALUES (?,?,?,?,?,?)""",
            (co, summ.get("summary",""), tags, "New", lat, lon)
        )

        # store each signal
        for r in recs:
            db.execute(
                """INSERT OR REPLACE INTO signals
                   (company, headline, url, date, lat, lon)
                   VALUES (?,?,?,?,?,?)""",
                (
                    co,
                    r["headline"],
                    r["url"],
                    None,  # if you parse date add here
                    lat,
                    lon
                )
            )
    db.commit()
