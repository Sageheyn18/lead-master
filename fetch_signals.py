import logging
import datetime
import json
from collections import defaultdict
from urllib.parse import quote_plus
from pathlib import Path

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, OpenAIError

from utils import get_conn, ensure_tables, RAW_CACHE_TABLE

# ───────── OpenAI client via Streamlit secrets ─────────
api_key = (
    st.secrets.get("OPENAI", {}).get("api_key")
    or st.secrets.get("OPENAI_API_KEY")
)
if not api_key:
    st.error(
        "❌ OpenAI API key not found!\n\n"
        "Add to `.streamlit/secrets.toml` either:\n\n"
        "```toml\n[OPENAI]\napi_key = \"sk-...\"\n```\nor\n```toml\nOPENAI_API_KEY = \"sk-...\"\n```"
    )
    st.stop()
client = OpenAI(api_key=api_key)

# ───────── Constants ─────────
SEED_KWS = [
    "land purchase",
    "acquired site",
    "build",
    "construction",
    "expansion",
    "facility",
    "plant",
    "warehouse",
    "distribution center",
]
MAX_HEADLINES = 60

# ───────── Helpers ─────────
def safe_chat(**kwargs):
    try:
        return client.chat.completions.create(**kwargs)
    except OpenAIError as e:
        logging.warning(f"OpenAI error {e!r}; skipping call")
        return None

def rss_search(query: str, days: int = 30, maxrec: int = MAX_HEADLINES):
    """Fetch Google News RSS entries from the past `days` days."""
    q = quote_plus(f'{query} when:{days}d')
    url = (
        f"https://news.google.com/rss/search"
        f"?q={q}&hl=en-US&gl=US&ceid=US:en"
    )
    import feedparser
    feed = feedparser.parse(url)
    return feed.entries[:maxrec]

def _fetch_for_seed(seed: str):
    """Fetch & cache raw RSS hits for a given seed."""
    conn = get_conn()
    now = datetime.datetime.utcnow()
    hits = rss_search(seed)
    out = []
    for h in hits:
        headline = h.title
        url      = h.link
        date     = getattr(h, "published", None)
        out.append({"headline": headline, "url": url, "date": date, "seed": seed})
        conn.execute(
            f"INSERT OR REPLACE INTO {RAW_CACHE_TABLE}"
            "(seed,fetched,headline,url,date) VALUES(?,?,?,?,?)",
            (seed, now, headline, url, date),
        )
    conn.commit()
    conn.close()
    return out

# ───────── Manual Search ─────────
def manual_search(company: str):
    """
    1) Fetch raw headlines via _fetch_for_seed
    2) Summarize + extract JSON via GPT
    3) Geocode the company
    """
    raw = _fetch_for_seed(company)
    if not raw:
        return {"summary":"", "sector":"unknown", "confidence":0}, [], None, None

    # build prompt
    prompt = (
        f"Summarize these headlines for {company}, focusing on potential "
        "construction leads. Return JSON with keys "
        "`summary` (list or single string), `sector`, and `confidence`:\n\n"
    )
    for h in raw[:MAX_HEADLINES]:
        prompt += f"- {h['headline']}\n"

    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
        max_tokens=200,
    )
    try:
        summary = json.loads(rsp.choices[0].message.content) if rsp else {}
    except Exception:
        summary = {}

    # geocode
    geolocator = Nominatim(user_agent="lead_master_app")
    loc = geolocator.geocode(company, timeout=10)
    lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)

    return summary, raw, lat, lon

# ───────── National Scan ─────────
def national_scan():
    """
    1) loop SEED_KWS → rss_search → dedupe
    2) safe_chat to extract {"company","confidence"} from each headline
    3) group by company → upsert clients + signals tables
    """
    ensure_tables()
    conn = get_conn()

    sidebar = st.sidebar
    sidebar.write("🔍 **Running national scan…**")
    progress = sidebar.progress(0)

    all_hits = []
    for i, kw in enumerate(SEED_KWS, start=1):
        sidebar.write(f"[{i}/{len(SEED_KWS)}] Searching `{kw}`…")
        hits = rss_search(kw)
        seen = set()
        deduped = []
        for h in hits:
            key = (h.title.lower(), h.link.lower())
            if key in seen:
                continue
            seen.add(key)
            deduped.append({
                "headline": h.title,
                "url":      h.link,
                "seed":     kw,
                "date":     getattr(h, "published", None),
            })
        all_hits.extend(deduped)
        progress.progress(i / len(SEED_KWS))

    sidebar.write("✍️ **Scoring headlines…**")
    scored = []
    for hit in all_hits[:MAX_HEADLINES]:
        info = safe_chat(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":
                f"Extract JSON with keys `company` and `confidence` "
                f"from this headline:\n\n{hit['headline']}"
            }],
            temperature=0.2,
            max_tokens=50,
        )
        if not info:
            continue

        try:
            parsed = json.loads(info.choices[0].message.content)
            hit.update(parsed)
            scored.append(hit)
        except Exception:
            continue

    # group by company
    by_co = defaultdict(list)
    for s in scored:
        co = s.get("company")
        if co:
            by_co[co].append(s)

    # upsert into DB
    for co, projects in by_co.items():
        first = projects[0]
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
                first.get("summary",""),
                json.dumps([ first.get("seed") ]),
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
    conn.close()
    sidebar.success("✅ National scan complete!")

# ───────── Company Contacts Stub ─────────
def company_contacts(company: str):
    """Fill in your own site/LinkedIn scrape logic here."""
    return {
        "procurement": None,
        "engineering": None,
        "construction": None,
    }

# ───────── Export PDF Stub ─────────
def export_pdf(company: str, headline: str, contacts: dict):
    """
    Build and return a one‐page PDF path.
    You can expand this with fpdf, PIL/magic to embed logos, etc.
    """
    from fpdf import FPDF

    out_path = Path("data") / f"{company.replace(' ','_')}.pdf"
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial","B",16)
    pdf.cell(0,10,f"{company} — Lead Summary", ln=1)
    pdf.set_font("Arial","",12)
    pdf.multi_cell(0,8,headline)
    pdf.ln(5)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,8,"Contacts:", ln=1)
    pdf.set_font("Arial","",10)
    for role,val in contacts.items():
        pdf.cell(0,6,f"{role.title()}: {val or 'N/A'}", ln=1)

    pdf.output(str(out_path))
    return out_path
