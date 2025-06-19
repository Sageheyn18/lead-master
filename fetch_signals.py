"""
fetch_signals.py – Lead Master  v4.3
• 15-second GDELT timeout; after first failure we stick to Google News
• Progress bar covers all phases (0 → 100 %)
• Keyword pre-filter, fuzzy dedup (≥ 80 % overlap)
• GPT company extraction + summary with daily budget guard
• HQ-city geocode fallback → lat/lon
"""

import os, json, datetime, logging, textwrap, requests, feedparser
from collections import defaultdict
from difflib import SequenceMatcher

import streamlit as st                     # for the progress bar
from geopy.geocoders import Nominatim
from openai import OpenAI

from utils import get_conn, ensure_tables, cache_summary

# ───────── configuration ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")

MAX_PROSPECTS   = 50          # per scheduled scan
MAX_HEADLINES   = 50          # per manual search
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # ≈ $3/day
BUDGET_USED     = 0           # tracked each run

SEED_KWS = [
    "land purchase", "buys acreage", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold-storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

# ───────── utility helpers ─────────
def _similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def keyword_filter(title: str) -> bool:
    low = title.lower()
    return any(kw in low for kw in SEED_KWS)

def dedup(titles: list[str]) -> list[str]:
    kept = []
    for t in titles:
        if all(_similar(t, k) < 0.8 for k in kept):
            kept.append(t)
    return kept

def budget_ok(cost_cents: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost_cents > DAILY_BUDGET:
        logging.warning("GPT budget exceeded – skipping call.")
        return False
    BUDGET_USED += cost_cents
    return True

# ───────── GPT helpers ─────────
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
        return {"summary": heads[0][:120]+"…", "sector": "unknown",
                "confidence": 0, "land_flag": 0}
    bullets = "\n".join(f"- {h}" for h in heads[:10])
    prompt = textwrap.dedent(f"""
        Summarise in one sentence. Guess sector. land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {bullets}
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
        return {"summary": txt, "sector": "unknown",
                "confidence": 0, "land_flag": 0}

# ───────── geocode helper ─────────
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

GDELT_WORKING = True   # sticky flag for this run

def gdelt_or_google(query: str, max_rec: int = 20):
    """Try GDELT (15 s). After first timeout, permanently switch to Google News."""
    global GDELT_WORKING
    if not GDELT_WORKING:
        return google_news(query, max_rec)

    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query={query}&maxrecords={max_rec}&format=json"
    )
    try:
        js = requests.get(url, timeout=15).json()
        return js.get("articles", [])
    except Exception as e:
        logging.warning(f"GDELT timeout: {e} → switching to Google News")
        GDELT_WORKING = False
        return google_news(query, max_rec)

# ───────── DB writer ─────────
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

# ───────── national scan (cron or manual button) ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    prospects = []
    progress  = st.progress(0.0)

    # Phase 1: keyword fetch (0–50 %)
    for i, kw in enumerate(SEED_KWS[:10], 1):
        for art in gdelt_or_google(kw.replace(" ", "%20"), 10):
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
        progress.progress(i / 20)             # 10 keywords maps to 0–0.5

    # Fuzzy dedup
    prospects = [p for p in prospects
                 if p["headline"] in dedup([q["headline"] for q in prospects])]

    # Phase 2: GPT company extraction (50–80 %)
    by_company = defaultdict(list)
    for j, p in enumerate(prospects, 1):
        p["company"] = gpt_company(p["headline"])
        by_company[p["company"]].append(p)
        progress.progress(0.5 + 0.3 * j / len(prospects))

    # Phase 3: summary + geocode (80–100 %)
    rows = []
    companies = list(by_company)
    for k, co in enumerate(companies, 1):
        items = by_company[co]
        heads = [i["headline"] for i in items]
        info  = gpt_summary(co, heads)
        for itm in items:
            lat, lon = geocode(itm["headline"])
            itm.update(
                land_flag=info["land_flag"], sector=info["sector"],
                lat=lat, lon=lon
            )
            rows.append(itm)

        # upsert client
        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name, summary, sector_tags, status) VALUES (?,?,?, 'New')",
            (co, info["summary"], json.dumps([info["sector"]]))
        )
        progress.progress(0.8 + 0.2 * k / len(companies))

    write_signals(rows, conn)
    progress.progress(1.0)
    progress.empty()
    logging.info("Scan wrote %s signals", len(rows))

# ───────── single-company helper for the UI ─────────
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
            )
        if len(res) >= MAX_HEADLINES:
            break
    return res

def manual_search(company: str):
    rows  = headlines_for_company(company)
    heads = dedup([r["title"] for r in rows])[:MAX_HEADLINES]
    info  = gpt_summary(company, heads)
    lat, lon = geocode(info["summary"])
    return info, heads, lat, lon

# ───────── local test entry ─────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(message)s")
    national_scan()
