"""
fetch_signals.py – Lead Master (caps + GPT company extract + read_flag)
"""

import os, json, datetime, logging, textwrap, re, requests, feedparser
import pandas as pd
from openai import OpenAI
from geopy.geocoders import Nominatim
from utils import get_conn, ensure_tables, cache_summary

client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geoloc   = Nominatim(user_agent="lead-master")
MAX_HEADLINES   = 50      # per manual search
MAX_PROSPECTS   = 50      # per scheduled scan
SEED_KWS = ["plant expansion","groundbreaking","distribution center","warehouse",
            "cold storage","factory","manufacturing facility","acquire site",
            "buys land","facility renovation"]


# ---------- helper: geocode city/state ----------
def geocode_place(text: str) -> tuple[float|None, float|None]:
    try:
        loc = geoloc.geocode(text, timeout=10)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return (None, None)


# ---------- headline collectors ----------
def gdelt_search(query: str, max_rec: int = 20):
    url = f"https://api.gdeltproject.org/api/v2/doc/docsearch?query={query}&maxrecords={max_rec}&format=json"
    try:
        return requests.get(url, timeout=60).json().get("articles", [])
    except Exception:
        return []


def headlines_for_company(co: str) -> list[dict]:
    yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    arts = gdelt_search(f'\"{co}\" AND {yday}', 40)
    if not arts:
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={co}&hl=en-US&gl=US&ceid=US:en"
        )
        return [{"title": ent.title, "url": ent.link, "date": yday, "src": "Google"} for ent in feed.entries[:MAX_HEADLINES]]
    return [
        {"title": a["title"], "url": a["url"], "date": a["seendate"][:8], "src": "GDELT"}
        for a in arts[:MAX_HEADLINES]
    ]


# ---------- GPT helpers ----------
def gpt_company_name(headline: str) -> str:
    prompt = f"From this headline give ONLY the primary company name: {headline}"
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0, max_tokens=20,
    )
    return rsp.choices[0].message.content.strip().replace('"', '')


def summarise(company: str, heads: list[dict]) -> dict | None:
    if not heads: return None
    bullets = "\n".join(f"- {h['title']}" for h in heads)
    prompt = textwrap.dedent(f"""
        Summarise in one sentence. Guess sector. land_flag=1 if headline implies land purchase/new site.
        Return JSON {{summary, sector, confidence, land_flag}}
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
        return {"summary": txt, "sector": "unknown", "confidence": 0, "land_flag": 0}


# ---------- national scan (cron & manual) ----------
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    # build keyword list (SEED_KWS + GPT expansion weekly)
    kws = SEED_KWS
    # grab top prospects
    prospects = []
    for kw in kws[:10]:
        for art in gdelt_search(kw, 10):
            headline = art["title"]
            company  = gpt_company_name(headline)
            prospects.append({
                "company": company,
                "headline": headline,
                "url": art["url"],
                "date": art["seendate"][:8],
                "src": "scan"
            })
            if len(prospects) >= MAX_PROSPECTS:
                break
        if len(prospects) >= MAX_PROSPECTS:
            break
    write_signals(prospects, conn)


# ---------- write signals & upsert client ----------
def write_signals(arts: list[dict], conn):
    ensure_tables(conn)
    for art in arts:
        co = art["company"]
        lat, lon = geocode_place(art["headline"])
        conn.execute(
            """
            INSERT INTO signals
            (company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)
            VALUES (?,?,?,?,?,?,?, ?, ?)
            """,
            (
                co, art["date"], art["headline"], art["url"], art["src"],
                1 if "land" in art["headline"].lower() else 0, "",
                lat, lon
            )
        )
    conn.commit()


if __name__ == "__main__":
    national_scan()
