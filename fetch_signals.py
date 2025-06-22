# fetch_signals.py – Lead Master v5.11  (2025-06-22)

import os, json, time, datetime, logging, textwrap, requests, sqlite3
from collections import defaultdict
from urllib.parse     import quote_plus

import feedparser
import streamlit as st
from geopy.geocoders  import Nominatim
from openai           import OpenAI, RateLimitError

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
OPENAI_KEY       = os.getenv("OPENAI_API_KEY")
client           = OpenAI(api_key=OPENAI_KEY)
geocoder         = Nominatim(user_agent="lead-master")

MAX_PROSPECTS    = 100
MAX_HEADLINES    = 20
DAILY_BUDGET     = int(os.getenv("DAILY_BUDGET_CENTS","300"))
BUDGET_USED      = 0
_4O_LAST_CALL    = 0
RELEVANCE_CUTOFF = 0.45

SEED_KWS = [
    "land purchase","acres","groundbreaking","construct",
    "construction project","plant expansion","build new",
    "distribution center","warehouse","cold storage",
    "manufacturing facility","industrial park",
    "relocation","ground lease","site plan"
]
EXTRA_KWS = ["land","acres","site","build","construction","expansion","facility"]

# ───────── CACHES ─────────
_cache = sqlite3.connect(os.path.join(os.getcwd(),"rss_gdelt_cache.db"), check_same_thread=False)
_cache.execute("""
CREATE TABLE IF NOT EXISTS cache(
  key   TEXT PRIMARY KEY,
  data  TEXT,
  ts    INTEGER
)""")
_cache.commit()

# ───────── BUDGET & SAFE_CHAT ─────────
def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap reached; skipping call.")
        return False
    BUDGET_USED += cost
    return True

def safe_chat(**params):
    try:
        return client.chat.completions.create(**params)
    except RateLimitError:
        logging.warning("OpenAI rate-limit – skipping call")
        return None

# ───────── UTILITIES ─────────
def dedup(rows: list[dict]) -> list[dict]:
    seen_t, seen_u, out = set(), set(), []
    for r in rows:
        t = r.get("title","").lower()
        u = r.get("url","").lower()
        if t in seen_t or u in seen_u: continue
        seen_t.add(t); seen_u.add(u); out.append(r)
    return out

def _geo(q:str):
    try: loc = geocoder.geocode(q, timeout=10)
    except: loc = None
    return (loc.latitude,loc.longitude) if loc else (None,None)

def safe_geocode(head:str, co:str):
    lat,lon = _geo(head)
    return (lat,lon) if lat is not None else _geo(co)

# ───────── CACHED FETCH ─────────
def _cached(key:str, ttl:int=86400):
    row = _cache.execute("SELECT data,ts FROM cache WHERE key=?", (key,)).fetchone()
    now = int(time.time())
    if row and now-row[1]<ttl:
        return json.loads(row[0])
    return None

def _store(key:str, data, ttl:int=86400):
    _cache.execute(
      "INSERT OR REPLACE INTO cache(key,data,ts) VALUES(?,?,?)",
      (key, json.dumps(data), int(time.time()))
    )
    _cache.commit()

# ───────── GDELT FETCH ─────────
def gdelt_headlines(query:str, maxrec:int=MAX_PROSPECTS) -> list[dict]:
    key = f"gdelt:{query}:{maxrec}"
    cached = _cached(key)
    if cached is not None:
        return cached
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch?"
        f"query={quote_plus(query)}&mode=ArtList"
        f"&maxrecords={maxrec}&format=json"
    )
    try:
        r = requests.get(url, timeout=15)
        j = r.json()
        arts = j.get("articles",[])
        out  = [
            {"title":a.get("title",""), "url":a.get("url",""),
             "seendate":datetime.datetime.utcnow().strftime("%Y%m%d")}
            for a in arts
        ]
    except Exception as e:
        logging.warning(f"GDELT failed: {e} – fallback to RSS")
        out = []
    _store(key, out)
    return out

# ───────── RSS FETCH ─────────
def google_news(co:str, maxrec:int=MAX_HEADLINES) -> list[dict]:
    key = f"rss:{co}:{maxrec}"
    cached = _cached(key)
    if cached is not None:
        return cached
    q = f'"{co}" ({" OR ".join(EXTRA_KWS)})'
    url = ("https://news.google.com/rss/search?"
           f"q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en")
    feed = feedparser.parse(url)
    today= datetime.datetime.utcnow().strftime("%Y%m%d")
    out = [
      {"title":e.title, "url":e.link, "seendate":today}
      for e in feed.entries[:maxrec]
    ]
    _store(key, out)
    return out

# ───────── GPT SIGNAL INFO ─────────
def gpt_signal_info(head:str) -> dict:
    if not budget_ok(0.07): return {"company":"Unknown","score":0.0}
    prompt = (
        f"Return EXACT JSON {{company:<name>,score:<0-1>}} for the headline:\n\"{head}\""
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=32
    )
    if rsp is None: return {"company":"Unknown","score":0.0}
    try:
        data = json.loads(rsp.choices[0].message.content)
        return {"company":data.get("company","Unknown"),
                "score":float(data.get("score",0.0))}
    except:
        return {"company":"Unknown","score":0.0}

# ───────── GPT SUMMARY ─────────
def gpt_summary(co:str, heads:list[str]) -> dict:
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}
    wait = 21-(time.time()-_4O_LAST_CALL)
    if wait>0: time.sleep(wait)
    cost = 0.5
    if not budget_ok(cost):
        return {"summary":heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}
    prompt = textwrap.dedent(f"""
        Summarise in 5 bullets. Guess sector. land_flag=1 if land purchase.
        Return EXACT JSON {{summary,sector,confidence,land_flag}}.

        Headlines:
        {"".join("- "+h+"\\n" for h in heads[:10])}
    """).strip()
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220
    )
    _4O_LAST_CALL = time.time()
    if rsp is None: 
        return {"summary":heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        txt = rsp.choices[0].message.content
        return {"summary":txt,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── CONTACTS ─────────
def company_contacts(co:str) -> list[dict]:
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts "
        f"for {co} as JSON array of {{name,title,email,phone}}."
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=256
    )
    if rsp is None: return []
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        return []

# ───────── DB WRITERS ─────────
def write_signals(rows:list[dict], conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals(company,date,headline,url,source_label,"
            "land_flag,sector_guess,lat,lon) VALUES(?,?,?,?,?,?,?,?,?)",
            (r["company"], r["date"], r["headline"], r["url"],
             r["src"], r["land_flag"], r["sector"],
             r["lat"], r["lon"])
        )
    conn.commit()

# ───────── NATIONAL SCAN ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    prospects = []
    bar = st.progress(0.0)

    for i, kw in enumerate(SEED_KWS, start=1):
        # first try GDELT
        arts = gdelt_headlines(kw, MAX_PROSPECTS)
        if not arts:
            arts = google_news(kw, MAX_PROSPECTS)
        for a in arts:
            if any(ek in a["title"].lower() for ek in EXTRA_KWS):
                prospects.append({
                    "headline": a["title"],
                    "url":       a["url"],
                    "date":      a["seendate"][:8],
                    "src":       "scan"
                })
        bar.progress(i/len(SEED_KWS))

    prospects = dedup(prospects)
    logging.info(f"Found {len(prospects)} unique prospects")

    by_co = defaultdict(list)
    for p in prospects:
        info = gpt_signal_info(p["headline"])
        if info["score"] >= RELEVANCE_CUTOFF:
            p.update({
                "company":    info["company"],
                "land_flag":  1 if "land" in p["headline"].lower() else 0,
                "sector":     "",
                "confidence": info["score"],
            })
            by_co[info["company"]].append(p)

    rows = []
    for co, items in by_co.items():
        heads = [it["headline"] for it in items]
        summ = gpt_summary(co, heads)
        contacts = company_contacts(co)
        # save contacts
        for c in contacts:
            conn.execute(
                "INSERT OR IGNORE INTO contacts(company,name,title,email,phone)"
                " VALUES(?,?,?,?,?)",
                (co,c.get("name",""),c.get("title",""),
                 c.get("email",""),c.get("phone",""))
            )
        for it in items:
            lat,lon = safe_geocode(it["headline"], co)
            it.update({
                "land_flag":   summ["land_flag"],
                "sector":      summ["sector"],
                "confidence":  summ["confidence"],
                "lat":         lat, "lon": lon
            })
            rows.append(it)
        conn.execute(
            "INSERT OR REPLACE INTO clients(name,summary,sector_tags,status)"
            " VALUES(?,?,?, 'New')",
            (co, summ["summary"], json.dumps([summ["sector"]]))
        )

    write_signals(rows, conn)
    bar.progress(1.0)
    bar.empty()
    logging.info(f"Wrote {len(rows)} signals to DB")
