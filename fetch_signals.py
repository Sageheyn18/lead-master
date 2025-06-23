# fetch_signals.py
import os
import logging
import datetime
import json
from collections import defaultdict
from urllib.parse import quote_plus

import streamlit as st
import feedparser
from geopy.geocoders import Nominatim
from openai import OpenAI, OpenAIError
from fpdf import FPDF

from utils import get_conn, ensure_tables

# ───────── OpenAI Client ─────────
api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY")
if not api_key:
    st.error(
        "❌ OpenAI API key not found. Set OPENAI_API_KEY environment variable,"
        " or add OPENAI_API_KEY to .streamlit/secrets.toml under [default]."
    )
    st.stop()

client = OpenAI(api_key=api_key)

# ───────── Constants ─────────
SEED_KWS = [
    "land purchase", "acquired site", "build", "construction",
    "expansion", "facility", "plant", "warehouse", "distribution center"
]
MAX_HEADLINES = 60  # limit results for performance

# ───────── Helpers ─────────
def safe_chat(**kwargs):
    """Call OpenAI and skip errors."""
    try:
        return client.chat.completions.create(**kwargs)
    except OpenAIError as e:
        logging.warning(f"OpenAI error: {e}")
        return None


def rss_search(query: str, days: int = 30, maxrec: int = MAX_HEADLINES):
    """Fetch Google News RSS for the past `days` days."""
    q = quote_plus(f"{query} when:{days}d")
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    return feed.entries[:maxrec]

# ───────── Manual Search ─────────
def manual_search(company: str):
    """Lookup a single company: RSS → summarize → geocode."""
    # Fetch headlines
    raw = []
    for entry in rss_search(company, days=150):
        raw.append({
            "headline": entry.title,
            "url": entry.link,
            "date": getattr(entry, "published", None),
        })

    # Summarize via GPT
    if raw:
        prompt = (
            f"Summarize these headlines for {company}, focusing on land purchases or construction leads:\n"
            + "\n".join(f"- {r['headline']}" for r in raw)
        )
        rsp = safe_chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=200,
        )
        summary = rsp.choices[0].message.content.strip() if rsp else ""
    else:
        summary = "No recent headlines found."

    # Geocode
    locator = Nominatim(user_agent="lead_master_app")
    loc = locator.geocode(company, timeout=10)
    lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)

    return summary, raw, lat, lon

# ───────── National Scan ─────────
def national_scan():
    """Fetch, dedupe, score, group by company, and save to SQLite."""
    conn = get_conn()
    ensure_tables()
    sidebar = st.sidebar
    sidebar.info("🔍 Running national scan…")
    progress = sidebar.progress(0)
    all_hits = []

    # 1) Fetch and dedupe
    for i, kw in enumerate(SEED_KWS, start=1):
        sidebar.write(f"[{i}/{len(SEED_KWS)}] Searching '{kw}'…")
        hits = rss_search(kw)
        seen, deduped = set(), []
        for h in hits:
            key = (h.title.lower(), h.link.lower())
            if key in seen: continue
            seen.add(key)
            deduped.append({
                "headline": h.title,
                "url": h.link,
                "date": getattr(h, "published", None),
            })
        all_hits.extend(deduped)
        progress.progress(i / len(SEED_KWS))

    # 2) Score each via GPT
    sidebar.info("✍️ Scoring headlines…")
    scored = []
    for hit in all_hits[:MAX_HEADLINES]:
        info = safe_chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content":
                f"Extract company name and confidence (0–1) from this headline:\n{hit['headline']}"
            }],
            temperature=0.2,
            max_tokens=50,
        )
        if not info: continue
        try:
            parsed = json.loads(info.choices[0].message.content)
            co = parsed.get("company")
            conf = float(parsed.get("confidence", 0))
            if co and conf >= 0.3:
                hit["company"] = co
                hit["confidence"] = conf
                scored.append(hit)
        except Exception:
            continue

    # 3) Group by company and save
    by_co = defaultdict(list)
    for s in scored:
        by_co[s["company"]].append(s)

    for co, projects in by_co.items():
        proj = projects[0]
        locator = Nominatim(user_agent="lead_master_app")
        loc = locator.geocode(co, timeout=10)
        lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)

        # Upsert client
        conn.execute(
            """
            INSERT OR REPLACE INTO clients
              (name, summary, sector_tags, status, lat, lon)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                co,
                proj.get("headline", ""),
                json.dumps([p.get("headline") for p in projects]),
                "New",
                lat,
                lon,
            ),
        )
        # Insert signals
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

# ───────── Company Contacts ─────────
def company_contacts(company: str):
    """Stub for procurement/engineering contacts lookup."""
    return {"procurement": None, "engineering": None, "construction": None}

# ───────── Export PDF ─────────
def export_pdf(company: str, headline: str, contacts: dict):
    """Generate a one-page PDF executive summary."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, f"Executive Summary: {company}", ln=True)
    pdf.set_font("Arial", "", 12)
    pdf.multi_cell(0, 8, f"Headline: {headline}")
    for role, contact in contacts.items():
        pdf.multi_cell(0, 8, f"{role.title()}: {contact or 'N/A'}")
    fname = f"lead_{company}_{datetime.datetime.utcnow():%Y%m%d%H%M%S}.pdf"
    path = f"/mnt/data/{fname}"
    pdf.output(path)
    return path
