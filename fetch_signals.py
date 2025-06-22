# fetch_signals.py  – Lead Master  v6.3   (2025-06-22)

import os, json, time, datetime, logging, textwrap, requests, sqlite3, csv
from collections import defaultdict
from urllib.parse     import quote_plus

import feedparser, streamlit as st
from geopy.geocoders  import Nominatim
from openai           import OpenAI, RateLimitError
from fpdf             import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
OPENAI_KEY       = os.getenv("OPENAI_API_KEY")
client           = OpenAI(api_key=OPENAI_KEY)
geocoder         = Nominatim(user_agent="lead-master")

NEWSAPI_KEY      = "cde04d56b1f7429a84cb3f834791fad7"

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

# ───────── LOCAL CACHE ─────────
_cache = sqlite3.connect(
    os.path.join(os.getcwd(),"rss_gdelt_cache.db"),
    check_same_thread=False
)
_cache.execute("""
CREATE TABLE IF NOT EXISTS cache(
  key   TEXT PRIMARY KEY,
  data  TEXT,
  ts    INTEGER
)""")
_cache.commit()

def _cached(key, ttl=86400):
    row = _cache.execute(
        "SELECT data,ts FROM cache WHERE key=?", (key,)
    ).fetchone()
    now = int(time.time())
    if row and now-row[1] < ttl:
        return json.loads(row[0])
    return None

def _store(key, data):
    _cache.execute(
      "INSERT OR REPLACE INTO cache(key,data,ts) VALUES(?,?,?)",
      (key, json.dumps(data), int(time.time()))
    )
    _cache.commit()

# ───────── BUDGET & CHAT ─────────
def budget_ok(cost):
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap reached; skipping call.")
        return False
    BUDGET_USED += cost
    return True

def safe_chat(**kw):
    try:
        return client.chat.completions.create(**kw)
    except RateLimitError:
        logging.warning("OpenAI rate-limit – skipping call")
        return None

# ───────── HELPERS ─────────
def dedup(rows):
    seen_t, seen_u, out = set(), set(), []
    for r in rows:
        t = (r.get("title","") or r.get("headline","")).lower()
        u = r.get("url","").lower()
        if t in seen_t or u in seen_u: 
            continue
        seen_t.add(t); seen_u.add(u)
        out.append(r)
    return out

def _geo(q):
    try:
        loc = geocoder.geocode(q, timeout=10)
    except:
        loc = None
    return (loc.latitude,loc.longitude) if loc else (None,None)

def safe_geocode(head, co):
    lat, lon = _geo(head)
    if lat is None or lon is None:
        lat, lon = _geo(f"{co} headquarters")
    return lat, lon

# ───────── NEWSAPI VIA REQUESTS ─────────
def gdelt_headlines(query, maxrec=MAX_PROSPECTS):
    key = f"newsapi:{query}:{maxrec}"
    cached = _cached(key)
    if cached is not None:
        return cached

    url    = "https://newsapi.org/v2/everything"
    params = {
        "apiKey": NEWSAPI_KEY,
        "q":      query,
        "pageSize": maxrec,
        "sortBy":   "publishedAt",
        "language": "en"
    }
    try:
        resp = requests.get(url, params=params, timeout=15).json()
        arts = resp.get("articles", [])
        out  = [
            {
              "title":    a.get("title",""),
              "url":      a.get("url",""),
              "seendate": a.get("publishedAt","")[:10].replace("-","")
            }
            for a in arts
        ]
    except Exception as e:
        logging.warning(f"NewsAPI failed ({e}); fallback to RSS")
        out = []
    _store(key, out)
    return out

# ───────── RSS FETCH ─────────
def google_news(co, maxrec=MAX_HEADLINES):
    key = f"rss:{co}:{maxrec}"
    cached = _cached(key)
    if cached is not None:
        return cached

    q   = f'"{co}" ({" OR ".join(EXTRA_KWS)})'
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

# ───────── BATCHED SIGNAL INFO ─────────
def gpt_batch_signal_info(headlines, chunk=10):
    results=[]
    for i in range(0, len(headlines), chunk):
        batch = headlines[i:i+chunk]
        prompt = "For each headline return JSON array of {headline,company,score}:\n"
        for h in batch:
            prompt += f"- {h}\n"
        rsp = safe_chat(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            temperature=0, max_tokens=256
        )
        if rsp:
            try:
                arr = json.loads(rsp.choices[0].message.content)
                results.extend(arr)
                continue
            except:
                pass
        # fallback to individual calls
        for h in batch:
            tmp = gpt_signal_info(h)
            results.append({"headline":h, **tmp})
    return results

def gpt_signal_info(head):
    if not budget_ok(0.07):
        return {"company":"Unknown","score":0.0}
    prompt = f'Return EXACT JSON {{"company":<name>,"score":<0-1>}} for:\n"{head}"'
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=32
    )
    if not rsp:
        return {"company":"Unknown","score":0.0}
    try:
        data = json.loads(rsp.choices[0].message.content)
        return {"company":data.get("company","Unknown"),
                "score":float(data.get("score",0.0))}
    except:
        return {"company":"Unknown","score":0.0}

# ───────── GPT SUMMARY ─────────
def gpt_summary(co, heads):
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No signals","sector":"unknown","confidence":0,"land_flag":0}
    wait = 21 - (time.time() - _4O_LAST_CALL)
    if wait>0:
        time.sleep(wait)
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
    if not rsp:
        return {"summary":heads[0][:120]+"…","sector":"unknown","confidence":0,"land_flag":0}
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        return {"summary":rsp.choices[0].message.content,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── CONTACTS ─────────
def company_contacts(co):
    prompt = (
        f"List up to 3 procurement/engineering/construction contacts for {co} "
        "as JSON array of {name,title,email,phone}."
    )
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=256
    )
    if not rsp:
        return []
    try:
        return json.loads(rsp.choices[0].message.content)
    except:
        return []

# ───────── LOGO & PDF ─────────
def fetch_logo(co: str) -> bytes | None:
    dom = co.replace(" ","") + ".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{dom}", timeout=6)
        if r.ok:
            return r.content
    except:
        pass
    return None

def export_pdf(row: dict, bullets: str, contacts: list[dict]) -> bytes:
    pdf = FPDF(); pdf.set_auto_page_break(True,15); pdf.add_page()
    pdf.set_font("Helvetica","B",16)
    logo = fetch_logo(row["company"])
    if logo:
        ext = magic.from_buffer(logo, mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"
        open(fn,"wb").write(logo)
        pdf.image(fn, x=10, y=10, w=20); pdf.set_xy(35,10)
    txt = row.get("headline", row.get("title",""))
    pdf.multi_cell(0,10, txt); pdf.ln(5)
    pdf.set_font("Helvetica","",12)
    for b in bullets.split("•"):
        if b.strip():
            pdf.multi_cell(0,7,"• "+b.strip())
    pdf.ln(3)
    if contacts:
        pdf.set_font("Helvetica","B",13); pdf.cell(0,8,"Key Contacts",ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(0,6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}  {c.get('phone','')}"
            ); pdf.ln(1)
    pdf.set_y(-30); pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(0,5,
        f"Source: {row.get('url','')}\nGenerated: {datetime.datetime.now():%Y-%m-%d %H:%M}"
    )
    return pdf.output(dest="S").encode("latin-1")

# ───────── MANUAL SEARCH FOR UI ─────────
def manual_search(company: str):
    arts = gdelt_headlines(company, MAX_HEADLINES)
    if not arts:
        arts = google_news(company, MAX_HEADLINES)
    arts = [a for a in arts if any(ek in a["title"].lower() for ek in EXTRA_KWS)]
    arts = dedup(arts)
    heads = [a["title"] for a in arts][:MAX_HEADLINES]
    if not heads:
        return None, [], None, None
    info = gpt_summary(company, heads)
    lat, lon = safe_geocode("", company)
    return info, heads, lat, lon

# ───────── PERMITS IMPORT ─────────
def fetch_permits():
    """
    Expects a 'permits.csv' in the app root with columns:
      company,address,date,permit_type,details_url
    """
    fpath = os.path.join(os.getcwd(), "permits.csv")
    if not os.path.exists(fpath):
        return []
    permits = []
    with open(fpath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lat, lon = safe_geocode(row.get("address",""), row.get("company",""))
            permits.append({
                "company":     row.get("company",""),
                "address":     row.get("address",""),
                "date":        row.get("date",""),
                "type":        row.get("permit_type",""),
                "details_url": row.get("details_url",""),
                "lat":         lat,
                "lon":         lon
            })
    return permits

# ───────── DB WRITERS ─────────
def write_signals(rows: list[dict], conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals(company,date,headline,url,source_label,"
            "land_flag,sector_guess,lat,lon) VALUES(?,?,?,?,?,?,?,?,?)",
            (r["company"],r["date"],r["headline"],r["url"],
             r["src"],r["land_flag"],r["sector"],r["lat"],r["lon"])
        )
    conn.commit()

# ───────── NATIONAL SCAN ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    prospects=[]; bar=st.progress(0.0)

    for i, kw in enumerate(SEED_KWS, start=1):
        arts = gdelt_headlines(kw, MAX_PROSPECTS)
        if not arts:
            arts = google_news(kw, MAX_PROSPECTS)
        for a in arts:
            if any(ek in a["title"].lower() for ek in EXTRA_KWS):
                prospects.append({
                    "headline":a["title"],"url":a["url"],
                    "date":a["seendate"][:8],"src":"scan"
                })
        bar.progress(i/len(SEED_KWS))

    prospects = dedup(prospects)
    logging.info(f"Found {len(prospects)} prospects")

    infos = gpt_batch_signal_info([p["headline"] for p in prospects])
    by_co = defaultdict(list)
    for p in prospects:
        for inf in infos:
            if inf["headline"]==p["headline"] and inf["score"]>=RELEVANCE_CUTOFF:
                p["company"]=inf["company"]
                by_co[inf["company"]].append(p)
                break

    rows=[]
    for co, items in by_co.items():
        heads = [it["headline"] for it in items]
        summ  = gpt_summary(co, heads)
        contacts = company_contacts(co)
        for c in contacts:
            conn.execute(
                "INSERT OR IGNORE INTO contacts(company,name,title,email,phone)"
                " VALUES(?,?,?,?,?)",
                (co,c.get("name",""),c.get("title",""),
                 c.get("email",""),c.get("phone",""))
            )
        lat, lon = safe_geocode("", co)
        conn.execute(  # store or update HQ coords
            "INSERT OR REPLACE INTO clients(name,summary,sector_tags,status,lat,lon)"
            " VALUES(?,?,?,?,?,?)",
            (co, summ["summary"], json.dumps([summ["sector"]]), "New", lat, lon)
        )
        for it in items:
            it.update({
                "land_flag":  summ["land_flag"],
                "sector":     summ["sector"],
                "confidence": summ["confidence"],
                "lat":        lat,
                "lon":        lon
            })
            rows.append(it)

    write_signals(rows, conn)
    bar.progress(1.0); bar.empty()
    logging.info(f"Wrote {len(rows)} signals")
