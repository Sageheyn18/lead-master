import logging
import json
from urllib.parse import quote_plus
import datetime

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, OpenAIError

from utils import get_conn

# ───────── OpenAI client via Streamlit secrets ─────────
api_key = (
    st.secrets.get("OPENAI_API_KEY") or
    st.secrets.get("OPENAI", {}).get("api_key")
)
if not api_key:
    st.error(
        "❌ **OpenAI API key not found!**\n"
        "Add OPENAI_API_KEY to .streamlit/secrets.toml"
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
    try:
        return client.chat.completions.create(**kwargs)
    except OpenAIError as e:
        logging.warning(f"OpenAI error: {e!r}")
        return None


def rss_search(query: str, days: int = 30, maxrec: int = MAX_HEADLINES):
    q = quote_plus(f"{query} when:{days}d")
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    import feedparser
    feed = feedparser.parse(url)
    entries = []
    for e in feed.entries[:maxrec]:
        entries.append({
            "headline": e.title,
            "url": e.link,
            "date": getattr(e, "published", None)
        })
    return entries

# ───────── Manual search ─────────
def manual_search(company: str):
    # fetch recent headlines
    raw = rss_search(company)
    # summarize via GPT
    prompt = (
        f"Summarize these headlines for {company} focusing on potential construction leads."
        " Return JSON with keys: summary (array of bullets), sector (string), confidence (0-1). Headlines:\n" +
        "\n".join(f"- {h['headline']}" for h in raw[:MAX_HEADLINES])
    )
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=200,
    )
    summary = {"summary": [], "sector": "unknown", "confidence": 0.0}
    if rsp:
        try:
            content = rsp.choices[0].message.content
            parsed = json.loads(content)
            summary.update(parsed)
        except Exception:
            pass
    # geocode
    locator = Nominatim(user_agent="lead_master")
    loc = locator.geocode(company, timeout=10)
    lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)
    return summary, raw, lat, lon

# ───────── National scan ─────────
def national_scan():
    conn = get_conn()
    sidebar = st.sidebar
    sidebar.info("🔍 Running national scan…")
    prog = sidebar.progress(0)
    all_hits = []
    # fetch & dedupe
    for i, kw in enumerate(SEED_KWS, start=1):
        sidebar.write(f"[{i}/{len(SEED_KWS)}] {kw}")
        hits = rss_search(kw)
        seen = set()
        dedup = []
        for h in hits:
            key = (h["headline"].lower(), h["url"].lower())
            if key in seen: continue
            seen.add(key)
            h["seed"] = kw
            dedup.append(h)
        all_hits.extend(dedup)
        prog.progress(i/len(SEED_KWS))
    # score & group
    scored = []
    for hit in all_hits[:MAX_HEADLINES]:
        info = safe_chat(
            model="gpt-4o-mini",
            messages=[{"role": "user",
                       "content":
                       f"Extract company and confidence from this headline as JSON: {hit['headline']}"}],
            temperature=0.2,
            max_tokens=50
        )
        if not info: continue
        try:
            parsed = json.loads(info.choices[0].message.content)
            hit.update(parsed)
            scored.append(hit)
        except Exception:
            continue
    # write to DB
    from collections import defaultdict
    by_co = defaultdict(list)
    for s in scored:
        co = s.get("company")
        if co: by_co[co].append(s)
    for co, items in by_co.items():
        first = items[0]
        # geocode company
        locator = Nominatim(user_agent="lead_master")
        loc = locator.geocode(co, timeout=10)
        lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)
        # upsert client
        conn.execute(
            "INSERT OR REPLACE INTO clients (name, summary, sector_tags, status, lat, lon)"
            " VALUES (?,?,?,?,?,?)",
            (co, "", json.dumps([first['seed']]), 'New', lat, lon)
        )
        # insert signals
        for rec in items:
            conn.execute(
                "INSERT OR REPLACE INTO signals (company, headline, url, date, lat, lon)"
                " VALUES (?,?,?,?,?,?)",
                (co, rec['headline'], rec['url'], rec.get('date'), lat, lon)
            )
    conn.commit()
    sidebar.success("✅ Scan complete!")

# ───────── Contacts & PDF ─────────
def company_contacts(company: str):
    return {"procurement": None, "facilities": None}

def export_pdf(company: str, headline: str, contacts: dict):
    # stub: return a fake path
    out = Path('.').resolve() / f"{company[:10]}_report.pdf"
    return str(out)
