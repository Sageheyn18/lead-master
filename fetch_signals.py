# fetch_signals.py — Lead Master v10.2 (2025-06-23)
import json
import logging
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

import feedparser
import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError
from fpdf import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── OpenAI client via Streamlit secrets ─────────
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# ───────── Constants ─────────
MAX_HEADLINES = 60
CACHE_TABLE   = "summaries"
SEED_KWS      = [
    "manufacturing", "industrial", "food processing", "cold storage",
    "distribution center", "warehouse", "plant", "facility"
]
RELEVANCE_CUTOFF = 0.45

# ───────── Initialize summary‐cache table ─────────
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
        f"SELECT summary,sector,confidence,land_flag,company FROM {CACHE_TABLE} WHERE headline=?",
        (headline,)
    ).fetchone()
    if row:
        summary, sector, confidence, land_flag, company = row
        return {
            "summary": summary,
            "sector": sector,
            "confidence": confidence,
            "land_flag": land_flag,
            "company": company
        }
    return None

def set_cached(headline: str, info: dict):
    conn = get_conn()
    conn.execute(
        f"""INSERT OR REPLACE INTO {CACHE_TABLE}
            (headline,summary,sector,confidence,land_flag,company)
           VALUES (?,?,?,?,?,?)""",
        (
            headline,
            info["summary"],
            info["sector"],
            info["confidence"],
            info["land_flag"],
            info["company"],
        )
    )
    conn.commit()

# ───────── Safe OpenAI call ─────────
def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit hit; skipping call.")
        return None

# ───────── GPT batch summary ─────────
def gpt_summary_batch(headlines: list[str]) -> dict | None:
    prompt = (
        "You are an assistant that reads news headlines about potential construction "
        "or land purchases. Please:\n"
        "  • Summarize them in 3 bullet points\n"
        "  • Identify the industry sector\n"
        "  • Give a confidence score (0–1)\n"
        "  • Flag with 1 if they involve land purchase or new build, else 0\n"
        "  • Extract the primary COMPANY name referenced\n"
        "Output exactly one JSON with keys "
        '"summary","sector","confidence","land_flag","company".\n\n'
        "Headlines:\n"
        + "\n".join(f"- {h}" for h in headlines[:MAX_HEADLINES])
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
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
    cached = get_cached(headline)
    if cached:
        return cached

    info = gpt_summary_batch([headline]) or {
        "summary": "",
        "sector": "unknown",
        "confidence": 0.0,
        "land_flag": 0,
        "company": ""
    }
    set_cached(headline, info)
    return info

# ───────── Headline fetcher ─────────
def headlines_for_query(q: str) -> list[dict]:
    params = {
        "q": f'"{q}" (land OR acres OR site OR build OR construction OR expansion OR facility OR warehouse OR "distribution center")',
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en"
    }
    url = "https://news.google.com/rss/search?" + urlencode(params)
    feed = feedparser.parse(url)
    entries = feed.entries[:MAX_HEADLINES]
    return [{"headline": e.title, "url": e.link} for e in entries]

# ───────── Geocoding ─────────
_geo = Nominatim(user_agent="lead-master-app")
def geocode_company(name: str) -> tuple[float|None, float|None]:
    try:
        loc = _geo.geocode(name + " headquarters", timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except:
        return (None, None)

# ───────── Manual search ─────────
def manual_search(company: str):
    """Lookup one company: RSS → GPT summaries → geocode."""
    heads = headlines_for_query(company)
    detailed = []

    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(gpt_summary_single, h["headline"]): h for h in heads}
        for fut in as_completed(futures):
            h = futures[fut]
            info = fut.result()
            detailed.append({
                "headline": h["headline"],
                "url":      h["url"],
                **info
            })

    batch_info = gpt_summary_batch([h["headline"] for h in heads]) or {}
    lat, lon   = geocode_company(company)
    return batch_info, detailed, lat, lon

# ───────── National scan ─────────
def national_scan():
    """
    Runs RSS → GPT summaries in parallel for each seed keyword,
    groups by extracted company, and writes to DB.
    """
    conn = get_conn()
    ensure_tables(conn)

    # 1) collect raw hits
    all_hits = []
    for kw in SEED_KWS:
        for h in headlines_for_query(kw):
            all_hits.append({**h, "seed": kw})

    # 2) dedupe
    seen, deduped = set(), []
    for h in all_hits:
        if h["headline"] not in seen:
            seen.add(h["headline"])
            deduped.append(h)
    all_hits = deduped[: MAX_HEADLINES * len(SEED_KWS)]

    # 3) summarize each in parallel
    scored = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(gpt_summary_single, h["headline"]): h for h in all_hits}
        for fut in as_completed(futures):
            h = futures[fut]
            info = fut.result()
            scored.append({**h, **info})

    # 4) group by company
    by_co = defaultdict(list)
    for r in scored:
        co = r.get("company") or r["seed"]
        by_co[co].append(r)

    # 5) write clients & signals
    for co, recs in by_co.items():
        heads = [r["headline"] for r in recs]
        summary = gpt_summary_batch(heads) or {}
        lat, lon = geocode_company(co)
        tags = json.dumps(list({r["seed"] for r in recs}))

        conn.execute(
            "INSERT OR REPLACE INTO clients "
            "(name,summary,sector_tags,status,lat,lon) VALUES(?,?,?,?,?,?)",
            (co, summary.get("summary",""), tags, "New", lat, lon)
        )
        for r in recs:
            conn.execute(
                "INSERT OR REPLACE INTO signals "
                "(company,headline,url,date,lat,lon) VALUES(?,?,?,?,?,?)",
                (co, r["headline"], r["url"], None, lat, lon)
            )
    conn.commit()

# ───────── Contacts ─────────
def company_contacts(company: str) -> list[dict]:
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts "
        f"for {company} as JSON array of {{name,title,email,phone}}."
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0,
        max_tokens=256
    )
    if not rsp:
        return []
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        return []

# ───────── Logo fetch ─────────
def fetch_logo(company: str) -> bytes|None:
    domain = company.replace(" ","") + ".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{domain}", timeout=5)
        if r.ok:
            return r.content
    except:
        pass
    return None

# ───────── PDF export ─────────
def export_pdf(row, summary_text, contacts) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()

    logo = fetch_logo(row.get("company",""))
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"
        open(fn, "wb").write(logo)
        pdf.image(fn, 10, 10, 20)
        pdf.set_xy(35, 10)

    pdf.set_font("Helvetica","B",16)
    pdf.multi_cell(0, 10, row.get("company",""))
    pdf.ln(5)

    pdf.set_font("Helvetica","",12)
    for line in summary_text.split("\n"):
        if line.strip():
            pdf.multi_cell(0, 7, "• " + line.strip())
    pdf.ln(3)

    if contacts:
        pdf.set_font("Helvetica","B",13)
        pdf.cell(0, 8, "Key Contacts", ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(
                0, 6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}   {c.get('phone','')}"
            )
            pdf.ln(1)

    pdf.set_y(-30)
    pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(0,5,f"Generated: {datetime.datetime.now():%Y-%m-%d %H:%M}")

    return pdf.output(dest="S").encode("latin-1")

# ───────── Permits CSV ─────────
def fetch_permits() -> list[dict]:
    path = "permits.csv"
    if not __import__("os").path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            lat, lon = geocode_company(r.get("company",""))
            out.append({
                "company":     r.get("company",""),
                "address":     r.get("address",""),
                "date":        r.get("date",""),
                "type":        r.get("permit_type",""),
                "details_url": r.get("details_url",""),
                "lat":         lat,
                "lon":         lon
            })
    return out
