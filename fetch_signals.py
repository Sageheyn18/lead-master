# fetch_signals.py
import logging
import json
import datetime
from urllib.parse import quote_plus
from collections import defaultdict

import streamlit as st
import feedparser
import requests
from geopy.geocoders import Nominatim
from openai import OpenAI
from fpdf import FPDF

from utils import get_conn, ensure_tables

# ───────── OpenAI client ─────────
client = OpenAI(api_key=st.secrets["OPENAI"]["api_key"])

# ───────── Constants ─────────
MAX_HEADLINES = 60          # how many to fetch per seed
SEED_KWS = [
    "land purchase", "acres", "site acquisition", "plant",
    "warehouse", "distribution center", "factory", "expansion"
]
RELEVANCE_THRESHOLD = 0.3  # for national_scan scoring

# ───────── Helpers ─────────
def safe_chat(**kw):
    """Wrapper around client.chat.completions.create that never raises."""
    try:
        return client.chat.completions.create(**kw)
    except Exception as e:
        logging.warning(f"OpenAI error (skipping call): {e}")
        return None

def rss_search(query: str, days: int = 30, maxrec: int = MAX_HEADLINES):
    """Pull Google News RSS for the past `days` days."""
    q = quote_plus(f'{query} when:{days}d')
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    return feed.entries[:maxrec]

def gdelt_search(query: str, days: int = 30, maxrec: int = MAX_HEADLINES):
    """Fallback to GDELT v2 if RSS is empty."""
    dt = datetime.date.today()
    dt0 = dt - datetime.timedelta(days=days)
    gquery = f'"{query}" AND ({dt0:%Y%m%d} TO {dt:%Y%m%d})'
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query={quote_plus(gquery)}&mode=ArtList&maxrecords={maxrec}&format=json"
    )
    try:
        resp = requests.get(url, timeout=10).json()
        return resp.get("articles", [])
    except Exception:
        logging.warning("GDELT timeout or parse error, returning empty.")
        return []

def fetch_for_seed(seed: str):
    """Get raw headlines for a single keyword seed."""
    # 1) Try RSS
    hits = []
    for e in rss_search(seed):
        hits.append({
            "headline": e.title,
            "url": e.link,
            "date": e.published
        })
    # 2) If none, fall back to GDELT
    if not hits:
        for art in gdelt_search(seed):
            hits.append({
                "headline": art.get("title", ""),
                "url": art.get("url", ""),
                "date": art.get("seendate", "")
            })
    return hits[:MAX_HEADLINES]

def summarise_headlines(company: str, headlines: list[str]):
    """Ask GPT to score & extract company from each headline."""
    prompt = (
        f"Company: {company}\n\n"
        "For each of these headlines:\n"
        + "\n".join(f"- {h}" for h in headlines[:10])
        + "\n\nReturn JSON list of objects with keys: headline, score (0–1), company."
    )
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=300
    )
    if not rsp:
        return []
    try:
        return json.loads(rsp.choices[0].message.content)
    except Exception:
        logging.warning("Failed to parse GPT summary JSON")
        return []

def geocode_company(name: str):
    """Return (lat, lon) or (None, None)."""
    geo = Nominatim(user_agent="lead_master")
    try:
        loc = geo.geocode(name, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return (None, None)

# ───────── Manual search (UI) ─────────
def manual_search(company: str):
    """Fetch, dedupe, summarise & return (info, raw, lat, lon)."""
    raw = fetch_for_seed(company)
    # dedupe by headline/url
    seen = set(); out = []
    for h in raw:
        key = (h["headline"], h["url"])
        if key in seen: continue
        seen.add(key); out.append(h)
    heads = out[:MAX_HEADLINES]
    # GPT summary just returns a short executive summary + sector/confidence
    info = {"summary": "", "sector": "unknown", "confidence": 0}
    if heads:
        # very basic summarisation prompt
        prompt = (
            f"Summarize these {len(heads)} headlines about {company} "
            "focusing on land or construction signals. Return JSON "
            '{"summary": "...", "sector": "...", "confidence": 0-1}.'
        )
        rsp = safe_chat(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0.2, max_tokens=200
        )
        if rsp:
            try:
                info = json.loads(rsp.choices[0].message.content)
            except Exception:
                logging.warning("Failed to parse exec summary JSON")
    lat, lon = geocode_company(company)
    return info, heads, lat, lon

# ───────── National scan (sidebar) ─────────
def national_scan():
    """
    Run through each seed KW:
      • RSS → GDELT
      • dedupe
      • get gpt_summary = summarise_headlines(...)
      • group by company
      • save into SQLite (clients + signals tables)
    """
    conn = get_conn()
    ensure_tables(conn)
    sidebar = st.sidebar
    sidebar.info("📡 Fetching…")
    all_hits = []
    for i, kw in enumerate(SEED_KWS, 1):
        sidebar.progress((i-1)/len(SEED_KWS), text=f"[{i}/{len(SEED_KWS)}] {kw}")
        hits = fetch_for_seed(kw)
        # attach seed for debugging
        for h in hits:
            h["seed"] = kw
        all_hits.extend(hits)
    sidebar.success("✅ Fetched")
    # dedupe globally
    seen = set(); prospects = []
    for h in all_hits:
        key = (h["headline"], h["url"])
        if key in seen: continue
        seen.add(key); prospects.append(h)
    sidebar.info(f"🌀 Scoring {len(prospects)}…")
    scored = []
    for idx, p in enumerate(prospects, 1):
        sidebar.progress(idx/len(prospects))
        info = summarise_headlines(p["headline"], [p["headline"]])
        if not info: continue
        rec = {**p, **info[0]}  # take first object from GPT
        if rec.get("score", 0) >= RELEVANCE_THRESHOLD:
            scored.append(rec)
    sidebar.success(f"🏷 {len(scored)} relevant")

    # group by company and persist
    by_co = defaultdict(list)
    for r in scored:
        co = r.get("company") or r["seed"]
        by_co[co].append(r)

    for co, recs in by_co.items():
        # save client summary with latest
        summ = recs[0]
        lat, lon = geocode_company(co)
        conn.execute(
            "INSERT OR REPLACE INTO clients(name,summary,sector_tags,status,lat,lon) "
            "VALUES(?,?,?,?,?,?)",
            (co, summ.get("summary",""), json.dumps([summ.get("sector","")]), "New", lat, lon)
        )
        # save each signal
        for r in recs:
            conn.execute(
                "INSERT OR REPLACE INTO signals(company,headline,url,date) VALUES(?,?,?,?)",
                (co, r["headline"], r["url"], r.get("date",""))
            )
    conn.commit()
    sidebar.success("💾 Saved to library")
    st.experimental_rerun()

# ───────── Export PDF ─────────
def export_pdf(company: str, headline: str, url: str, contacts: dict):
    """
    Create a one-page PDF summarizing:
      company, headline, URL, contact info
    """
    pdf = FPDF()
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, f"{company}", ln=True)
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(0, 8, f"• Headline: {headline}")
    pdf.multi_cell(0, 8, f"• URL: {url}")
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Contacts:", ln=True)
    pdf.set_font("Helvetica", size=12)
    for role, info in contacts.items():
        pdf.multi_cell(0, 8, f"– {role}: {info}")
    return pdf.output(dest="S").encode("latin1")

# ───────── Company contacts stub ─────────
def company_contacts(company: str):
    """
    Scrape or stub out procurement/facilities contacts.
    For now we just return an empty dict to be filled manually.
    """
    return {
        "Procurement": "",
        "Facilities": "",
        "Engineering": ""
    }
