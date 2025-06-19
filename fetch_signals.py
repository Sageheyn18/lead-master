"""
fetch_signals.py – Lead Master  v4.5-fixed
• 15-s GDELT timeout; sticky Google-News fallback
• GPT-3.5-turbo for company-name (60 RPM) • GPT-4o-mini for summary (throttled)
• Progress bar 0-100 % • Keyword pre-filter, fuzzy dedup
• HQ-city geocode fallback • Daily budget guard
"""

import os, json, time, datetime, logging, textwrap, requests, feedparser
from collections import defaultdict
from difflib import SequenceMatcher

import streamlit as st
from geopy.geocoders import Nominatim
from openai import OpenAI, RateLimitError

from utils import get_conn, ensure_tables

# ───────── config ─────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")

MAX_PROSPECTS   = 50
MAX_HEADLINES   = 50
DAILY_BUDGET    = int(os.getenv("DAILY_BUDGET_CENTS", "300"))  # ≈ $3/day
BUDGET_USED     = 0
_4O_LAST_CALL   = 0          # throttle 1 call / 21 s

SEED_KWS = [
    "land purchase", "buys acreage", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold-storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

# ───────── small helpers ─────────
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

# --- safe GPT wrapper (retries x3, 21 s back-off) ---
def safe_chat(**params):
    for attempt in range(3):
        try:
            return client.chat.completions.create(**params)
        except RateLimitError:
            wait = 21
            logging.warning("OpenAI rate-limit; wait %s s (%s/3)", wait, attempt+1)
            time.sleep(wait)
    logging.error("OpenAI still rate-limited → giving up.")
    return None
# ----------------------------------------

def gpt_company(head: str) -> str:
    if not budget_ok(0.05):           # 3.5-turbo cheap
        return "Unknown"
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":f"Give ONLY the primary company name in: {head}"}],
        temperature=0, max_tokens=16,
    )
    if rsp is None: return "Unknown"
    return rsp.choices[0].message.content.strip().strip('"')

def gpt_summary(company: str, heads: list[str]) -> dict:
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No construction headlines","sector":"unknown",
                "confidence":0,"land_flag":0}
    # throttle 4-o
    wait = 21 - (time.time() - _4O_LAST_CALL)
    if wait > 0: time.sleep(wait)
    if not budget_ok(0.5):
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    prompt = textwrap.dedent(f"""
        Summarise in one sentence. Guess sector. land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {chr(10).join('- '+h for h in heads[:10])}
    """)
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220,
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
def google_news(q: str, max_rec=20):
    feed = feedparser.parse(
        f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    )
    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    return [{"title":e.title,"url":e.link,"seendate":today} for e in feed.entries[:max_rec]]

GDELT_OK = True          # sticky per-scan

def gdelt_or_google(q: str, max_rec=20):
    global GDELT_OK
    if not GDELT_OK:
        return google_news(q, max_rec)
    url = f"https://api.gdeltproject.org/api/v2/doc/docsearch?query={q}&maxrecords={max_rec}&format=json"
    try:
        return requests.get(url, timeout=15).json().get("articles", [])
    except Exception as e:
        logging.warning("GDELT timeout → Google (%s)", e)
        GDELT_OK = False
        return google_news(q, max_rec)

# ───────── db helper ─────────
def write_signals(rows, conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals (company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (r["company"], r["date"], r["headline"], r["url"], r["src"],
             r["land_flag"], r["sector"], r["lat"], r["lon"])
        )
    conn.commit()

# ───────── national scan ─────────
def national_scan():
    conn=get_conn(); ensure_tables(conn)
    prospects=[]; progress=st.progress(0.0)

    # Phase 1 – fetch (0-50 %)
    for i,kw in enumerate(SEED_KWS[:10],1):
        for art in gdelt_or_google(kw.replace(" ","%20"),10):
            title=art["title"]
            if keyword_filter(title):
                prospects.append({"headline":title,"url":art["url"],
                                  "date":art["seendate"][:8],"src":"scan"})
                if len(prospects)>=MAX_PROSPECTS: break
        if len(prospects)>=MAX_PROSPECTS: break
        progress.progress(i/20)

    prospects=[p for p in prospects if p["headline"] in
               dedup([x["headline"] for x in prospects])]

    # Phase 2 – company (50-80 %)
    by_co=defaultdict(list)
    for j,p in enumerate(prospects,1):
        p["company"]=gpt_company(p["headline"])
        by_co[p["company"]].append(p)
        progress.progress(0.5+0.3*j/len(prospects))

    # Phase 3 – summary+geocode (80-100 %)
    rows=[]; co_list=list(by_co)
    for k,co in enumerate(co_list,1):
        items=by_co[co]
        info=gpt_summary(co,[i["headline"] for i in items])
        for itm in items:
            lat,lon=geocode(itm["headline"]) or geocode(co)
            itm.update(lat=lat,lon=lon,sector=info["sector"],
                       land_flag=info["land_flag"])
            rows.append(itm)
        conn.execute(
            "INSERT OR REPLACE INTO clients (name,summary,sector_tags,status)"
            " VALUES (?,?,?, 'New')",
            (co,info["summary"],json.dumps([info["sector"]]))
        )
        progress.progress(0.8+0.2*k/len(co_list))

    write_signals(rows,conn)
    progress.progress(1.0); progress.empty()
    logging.info("Scan wrote %s signals",len(rows))

# ───────── manual search helper ─────────
def headlines_for_company(co:str)->list[dict]:
    today=datetime.date.today(); start=today-datetime.timedelta(days=150)
    q=f'"{co}" AND ({start:%Y%m%d} TO {today:%Y%m%d})'
    arts=gdelt_or_google(q.replace(" ","%20"),MAX_HEADLINES*2)
    res=[]
    for a in arts:
        if keyword_filter(a["title"]):
            res.append({"title":a["title"],"url":a["url"],
                        "date":a["seendate"][:8],"src":"search"})
        if len(res)>=MAX_HEADLINES: break
    return res

def manual_search(co:str):
    rows=headlines_for_company(co)
    heads=dedup([r["title"] for r in rows])[:MAX_HEADLINES]
    info=gpt_summary(co,heads); lat,lon=geocode(info["summary"])
    return info,heads,lat,lon

# ───────── local test ─────────
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(message)s")
    national_scan()
