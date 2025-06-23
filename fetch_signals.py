# fetch_signals.py
import logging
import datetime
import json
from collections import defaultdict
from urllib.parse import quote_plus

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, OpenAIError

from utils import get_conn, ensure_tables

# ───────── OpenAI client via Streamlit secrets ─────────
# Try two common styles in .streamlit/secrets.toml:
# 1) [OPENAI] api_key="sk-..."
# 2) OPENAI_API_KEY="sk-..."
api_key = (
    st.secrets.get("OPENAI", {}).get("api_key")
    or st.secrets.get("OPENAI_API_KEY")
)
if not api_key:
    st.error(
        "❌ **OpenAI API key not found!**\n\n"
        "Please create `.streamlit/secrets.toml` with either:\n\n"
        "```toml\n"
        "[OPENAI]\n"
        'api_key = "sk-…"\n'
        "```\n\nor:\n\n```toml\n"
        'OPENAI_API_KEY = "sk-…"\n'
        "```"
    )
    st.stop()

client = OpenAI(api_key=api_key)


# ───────── Constants ─────────
SEED_KWS = [
    "land purchase", "acquired site", "build", "construction",
    "expansion", "facility", "plant", "warehouse", "distribution center"
]
MAX_HEADLINES = 60


# ───────── Helpers ─────────
def safe_chat(**kwargs):
    """Wrap client.chat.completions.create and skip on rate‐limit / errors."""
    try:
        return client.chat.completions.create(**kwargs)
    except OpenAIError as e:
        logging.warning(f"OpenAI error: {e!r} – skipping call")
        return None

def rss_search(query: str, days: int = 30, maxrec: int = MAX_HEADLINES):
    """Query Google News RSS for the last `days` days."""
    q = quote_plus(f'{query} when:{days}d')
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    import feedparser
    feed = feedparser.parse(url)
    return feed.entries[:maxrec]


# ───────── Manual search ─────────
def _fetch_for_seed(seed: str):
    """Fetch headlines (RSS → dedupe → cache → return)."""
    conn = get_conn()
    # load fresh if >1h old, else reuse cache…
    # (left intact from your existing logic)
    # …
    # return list of dicts {"headline":…, "url":…, "date": datetime, "seed": seed}
    pass  # … your existing cache logic here

def manual_search(company: str):
    """Returns (summary, rows, lat, lon) for a single‐company lookup."""
    raw = _fetch_for_seed(company)
    if not raw:
        return {"summary": ""}, [], None, None

    # 1) run GPT summary
    prompt = (
        f"Summarize these headlines for {company} with focus on land/reactive construction:\n\n"
        + "\n".join(f"- {h['headline']}" for h in raw[:MAX_HEADLINES])
    )
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=200,
    )
    summary = json.loads(rsp.choices[0].message.content) if rsp else {"summary": ""}
    # 2) geocode
    geolocator = Nominatim(user_agent="lead_master_app")
    loc = geolocator.geocode(company, timeout=10)
    lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)

    return summary, raw, lat, lon


# ───────── National scan ─────────
def national_scan():
    """
    Runs through SEED_KWS→RSS→dedupe→score via GPT→group by company→
    save to DB signals + clients table.
    """
    conn = get_conn()
    ensure_tables(conn)

    sidebar = st.sidebar
    sidebar.write("🔍 **National scan in progress**")
    progress = sidebar.progress(0)
    all_hits = []

    # 1) Fetch + dedupe
    for i, kw in enumerate(SEED_KWS, start=1):
        sidebar.write(f"[{i}/{len(SEED_KWS)}] Searching `{kw}`…")
        hits = rss_search(kw)
        # dedupe by title+url
        seen = set()
        deduped = []
        for h in hits:
            key = (h.title.lower(), h.link.lower())
            if key in seen:
                continue
            seen.add(key)
            deduped.append({
                "headline": h.title,
                "url": h.link,
                "seed": kw,
                "date": getattr(h, "published", None),
            })
        all_hits.extend(deduped)
        progress.progress(i / len(SEED_KWS))

    # 2) Score each via GPT
    sidebar.write("✍️ **Scoring headlines…**")
    scored = []
    for hit in all_hits[:MAX_HEADLINES]:
        info = safe_chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content":
                f"Identify company name and confidence (0–1) from this headline:\n\n{hit['headline']}"
            }],
            temperature=0.2,
            max_tokens=50
        )
        if not info:
            continue
        body = info.choices[0].message.content
        try:
            parsed = json.loads(body)
            hit.update(parsed)
            scored.append(hit)
        except Exception:
            continue

    # 3) Group by company, write to DB
    by_co = defaultdict(list)
    for s in scored:
        co = s.get("company")
        if not co:
            continue
        by_co[co].append(s)

    for co, projects in by_co.items():
        # take first project as the “latest”
        row = projects[0]
        geolocator = Nominatim(user_agent="lead_master_app")
        loc = geolocator.geocode(co, timeout=10)
        lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)

        # upsert client
        conn.execute(
            """
            INSERT OR REPLACE INTO clients
              (name, summary, sector_tags, status, lat, lon)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                co,
                row.get("summary", ""),
                json.dumps([row.get("seed")]),
                "New",
                lat,
                lon,
            ),
        )
        # insert each signal
        for p in projects:
            conn.execute(
                """
                INSERT OR REPLACE INTO signals
                  (company, headline, url, date, lat, lon)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    co,
                    p["headline"],
                    p["url"],
                    p.get("date"),
                    lat,
                    lon,
                ),
            )
    conn.commit()
    sidebar.success("✅ National scan complete!")


# ───────── (Optional) other helpers ─────────
def company_contacts(company: str):
    """You can fill in your scrape‐LinkedIn‐or‐site logic here."""
    return {"procurement": None, "facilities": None}


def export_pdf(company: str, headline: str, contacts: dict):
    """Build a one‐page PDF summary. (Implement via fpdf/magic as you like)"""
    pdf = st.cache_data(lambda: FPDF())()
    # … your PDF generation …
    return pdf
