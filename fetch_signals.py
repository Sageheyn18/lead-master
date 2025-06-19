"""
fetch_signals.py – Lead Master  v4.5
• GDELT 15 s timeout; sticky Google fallback
• GPT-3.5-turbo for company-name extraction (high RPM, cheap)
• GPT-4o-mini for summary with automatic 21-s throttle (org limit 3 RPM)
• Progress bar 0–100 %, fuzzy dedup, keyword filter
• HQ-city geocode fallback, daily budget guard
"""

import os, json, time, datetime, logging, textwrap, requests, feedparser
from collections import defaultdict
from difflib import SequenceMatcher

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError

from utils import get_conn, ensure_tables

# ───────── configuration ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")

MAX_PROSPECTS   = 50
MAX_HEADLINES   = 50
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # ≈ $3/day
BUDGET_USED     = 0

# simple throttle for GPT-4o summary
_4O_LAST_CALL = 0

SEED_KWS = [
    "land purchase", "buys acreage", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold-storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

# ───────── helpers ─────────
def _similar(a, b): return SequenceMatcher(None, a, b).ratio()

def keyword_filter(title: str) -> bool:
    low = title.lower()
    return any(kw in low for kw in SEED_KWS)

def dedup(titles: list[str]) -> list[str]:
    kept = []
    for t in titles:
        if all(_similar(t, k) < 0.8 for k in kept):
            kept.append(t)
    return kept

def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget exceeded; skipping call.")
        return False
    BUDGET_USED += cost
    return True

# safe GPT wrapper (handles rate limit)
def safe_chat(**params):
    for attempt in range(3):
        try:
            return client.chat.completions.create(**params)
        except RateLimitError:
            logging.warning("OpenAI rate-limit; wait 21 s (%s/3)", attempt+1)
            time.sleep(21)
    logging.error("OpenAI limit after retries; returning None")
    return None

# ───────── GPT utilities ─────────
def gpt_company(headline: str) -> str:
    """Uses GPT-3.5 (60 RPM) so it never hits 4o limit."""
    if not budget_ok(0.05):          # cheap
        return "Unknown"
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user",
                   "content":f"Give ONLY the primary company name in: {headline}"}],
        temperature=0, max_tokens=16,
    )
    if rsp is None: return "Unknown"
    return rsp.choices[0].message.content.strip().strip('"')

def gpt_summary(company: str, heads: list[str]) -> dict:
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No construction headlines.",
                "sector":"unknown","confidence":0,"land_flag":0}
    # throttle 4-o: ensure 21 s since last call
    now = time.time()
    wait = 21 - (now - _4O_LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    if not budget_ok(0.5):
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    bullets = "\n".join(f"- {h}" for h in heads[:10])
    prompt = textwrap.dedent(f"""
        Summarise in one sentence. Guess sector. land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {bullets}
    """)
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,max_tokens=220,
    )
    _4O_LAST_CALL = time.time()
    if rsp is None:
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    try:
        return json.loads(rsp.choices[0].message.content.strip())
    except Exception:
        txt = rsp.choices[0].message.content.strip()
        return {"summary":txt,"sector":"unknown","confidence":0,"land_flag":0}

def geocode(text: str):
    try:
        loc = geocoder.geocode(text, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return (None, None)

# ───────── headline fetchers ─────────
def google_news(query: str, max_rec: int = 20):
    feed = feedparser.parse(
        f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    )
    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    return [
        {"title": ent.title, "url": ent.link, "seendate": today}
        for ent in feed.entries[:max_rec]
    ]

GDELT_WORKING = True   # sticky flag

def gdelt_or_google(query: str, max_rec: int = 20):
    global GDELT_WORKING
    if not GDELT_WORKING:
        return google_news(query, max_rec)
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query={query}&maxrecords={max_rec}&format=json"
    )
    try:
        return requests.get(url, timeout=15).json().get("articles", [])
    except Exception as e:
        logging.warning("GDELT timeout: %s → Google News", e)
        GDELT_WORKING = False
        return google_news(query, max_rec)

# ───────── DB helper ─────────
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

# ───────── national scan (cron & manual) ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    prospects = []
    progress  = st.progress(0.0)

    # Phase 1 – keyword fetch (0–50 %)
    for i, kw in enumerate(SEED_KWS[:10], 1):
        arts = gdelt_or_google(kw.replace(" ", "%20"), 10)
        for art in arts:
            title = art["title"]
            if keyword_filter(title):
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
        progress.progress(i / 20)          # 0–0.5

    prospects = [p for p in prospects
                 if p["headline"] in dedup([q["headline"] for q in prospects])]

    # Phase 2 – company extract (50–80 %)
    by_co = defaultdict(list)
    for j, p in enumerate(prospects, 1):
        p["company"] = gpt_company(p["headline"])
        by_co[p["company"]].append(p)
        progress.progress(0.5 + 0.3 * j / len(prospects))

    # Phase 3 – summary + geocode (80–100 %)
    rows = []
    co_list = list(by_co)
    for k, co in enumerate(co_list, 1):
        items = by_co[co]
        info  = gpt_summary(co, [i["headline"] for i in items])
        for itm in items:
            lat, lon = geocode(itm["headline"] or co)
            itm.update(
                land_flag=info["land_flag"], sector=info["sector"],
                lat=lat, lon=lon
            )
            rows.append(itm)

        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name, summary, sector_tags, status) VALUES (?,?,?, 'New')",
            (co, info["summary"], json.dumps([info["sector"]]))
        )
        progress.progress(0.8 + 0.2 * k / len(co_list))

    write_signals(rows, conn)
    progress.progress(1.0); progress.empty()
    logging.info("Scan wrote %s signals", len(rows))

# ───────── UI helper (manual search) ─────────
def headlines_for_company(company: str) -> list[dict]:
    today  = datetime.date.today()
    start  = today - datetime.timedelta(days=150)
    query  = f'"{company}" AND ({start:%Y%m%d} TO {today:%Y%m%d})'
    arts   = gdelt_or_google(query.replace(" ", "%20"), MAX_HEADLINES * 2)
    res = []
    for a in arts:
        title = a["title"]
        if keyword_filter(title):
            res.append(
                {"title": title, "url": a["url"],
                 "date": a["seendate"][:8], "src": "search"}
