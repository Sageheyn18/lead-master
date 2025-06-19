"""
fetch_signals.py – Lead Master  v4.1
• Keyword pre-filter (land/expansion) + fuzzy dedup (80 % overlap)
• GPT company extraction, summary, budget guard
• Geocode city fallback → lat/lon
"""

import os, re, json, datetime, logging, textwrap, requests, feedparser
from collections import defaultdict
from difflib import SequenceMatcher
from urllib.parse import urlparse

import pandas as pd
from geopy.geocoders import Nominatim
from openai import OpenAI

from utils import get_conn, ensure_tables, cache_summary

# ───────── config & constants ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")
MAX_PROSPECTS   = 50          # scheduled scan cap
MAX_HEADLINES   = 50          # per manual search cap
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # ≈ $3 default
BUDGET_USED     = 0

SEED_KWS = [
    "land purchase", "buys acreage", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold-storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

def _similar(a, b) -> float:
    return SequenceMatcher(None, a, b).ratio()


# ───────── headline fetchers & filter ─────────
def gdelt_search(query: str, max_rec: int = 20):
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query={query}&maxrecords={max_rec}&format=json"
    )
    try:
        return requests.get(url, timeout=60).json().get("articles", [])
    except Exception:
        return []


def keyword_filter(title: str) -> bool:
    low = title.lower()
    return any(kw in low for kw in SEED_KWS)


def dedup(headlines: list[str]) -> list[str]:
    kept = []
    for h in headlines:
        if all(_similar(h, k) < 0.8 for k in kept):
            kept.append(h)
    return kept


# ───────── GPT helpers ─────────
def budget_ok(cost_cents: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost_cents > DAILY_BUDGET:
        logging.warning("Daily GPT budget exceeded – skipping call.")
        return False
    BUDGET_USED += cost_cents
    return True


def gpt_company(headline: str) -> str:
    if not budget_ok(0.2):
        return "Unknown"
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": f"Primary company in headline: {headline}"}],
        temperature=0, max_tokens=16,
    )
    return rsp.choices[0].message.content.strip().strip('"')


def gpt_summary(company: str, heads: list[str]) -> dict:
    if not budget_ok(0.5):
        return {"summary": heads[0][:120]+"…", "sector": "unknown", "confidence":0, "land_flag":0}
    bullets = "\n".join(f"- {h}" for h in heads[:10])
    prompt = textwrap.dedent(f"""
        Summarise in one sentence. Guess sector. land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}. Headlines:\n{bullets}
    """)
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2, max_tokens=220,
    )
    try:
        return json.loads(rsp.choices[0].message.content.strip())
    except Exception:
        txt = rsp.choices[0].message.content.strip()
        return {"summary": txt, "sector": "unknown", "confidence":0, "land_flag":0}


# ───────── geocode helper ─────────
def geocode(text: str):
    try:
        loc = geocoder.geocode(text, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return (None, None)


# ───────── write to DB ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            """
            INSERT INTO signals
            (company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                r["company"], r["date"], r["headline"], r["url"], r["src"],
                r["land_flag"], r["sector"], r["lat"], r["lon"]
            )
        )
    conn.commit()


# ───────── national scan (cron & button) ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    prospects = []
    for kw in SEED_KWS[:10]:
        for art in gdelt_search(kw.replace(" ", "%20"), 10):
            title = art["title"]
            if not keyword_filter(title):
                continue
            prospects.append({
                "headline": title,
                "url":      art["url"],
                "date":     art["seendate"][:8],
                "src":      "scan"
            })
            if len(prospects) >= MAX_PROSPECTS:
                break
        if len(prospects) >= MAX_PROSPECTS:
            break

    # fuzzy dedup
    unique_titles = dedup([p["headline"] for p in prospects])
    prospects = [p for p in prospects if p["headline"] in unique_titles]

    # GPT company extraction & summary per company
    by_co = defaultdict(list)
    for p in prospects:
        p["company"] = gpt_company(p["headline"])
        by_co[p["company"]].append(p)

    rows_to_write = []
    for co, items in by_co.items():
        info = gpt_summary(co, [i["headline"] for i in items])
        for itm in items:
            lat, lon = geocode(itm["headline"])
            itm.update(
                land_flag=info["land_flag"], sector=info["sector"],
                lat=lat, lon=lon
            )
            rows_to_write.append(itm)

        # upsert client
        conn.execute(
            """
            INSERT OR REPLACE INTO clients
            (name, summary, sector_tags, status)
            VALUES (?,?,?, 'New')
            """,
            (
                co, info["summary"],
                json.dumps([info["sector"]])
            )
        )

    write_signals(rows_to_write, conn)
    logging.info("Scan wrote %s signals", len(rows_to_write))


# ───────── manual search helper for UI ─────────
def manual_search(company: str):
    arts = headlines_for_company(company)
    arts = [a for a in arts if keyword_filter(a["title"])]
    arts = dedup([a["title"] for a in arts])
    heads = arts[:MAX_HEADLINES]
    info  = gpt_summary(company, heads)
    lat, lon = geocode(info["summary"])
    return info, heads, lat, lon
