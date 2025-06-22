"""
fetch_signals.py  – Lead Master  v5.1   (2025-06-22)

• Fail-fast on OpenAI rate limits (no 21 s back-offs)
• Manual-search uses only relevance (not company-guess) to include headlines
• All other v5.0 behavior (RSS cache, batch GPT-3.5, single GPT-4o summary, etc.)
"""

import os, json, time, datetime, logging, textwrap, requests, feedparser, sqlite3
from collections import defaultdict
from urllib.parse    import quote_plus

import streamlit as st
from geopy.geocoders import Nominatim
from openai          import OpenAI, RateLimitError
from fpdf            import FPDF
import magic

from utils import get_conn, ensure_tables

# ───────── CONFIG ─────────
OPENAI_KEY       = os.getenv("OPENAI_API_KEY")
client           = OpenAI(api_key=OPENAI_KEY)
geocoder         = Nominatim(user_agent="lead-master")

MAX_PROSPECTS    = 50
MAX_HEADLINES    = 20
DAILY_BUDGET     = int(os.getenv("DAILY_BUDGET_CENTS","300"))
BUDGET_USED      = 0
_4O_LAST_CALL    = 0
RELEVANCE_CUTOFF = 0.55

SEED_KWS = [
    "land purchase","acres","acquire site","groundbreaking",
    "construct","construction project","plant expansion","build new",
    "distribution center","warehouse","cold storage","manufacturing facility",
    "industrial park","facility renovation"
]
EXTRA_KWS = [
    "land","acres","site","build","construction","expansion",
    "facility","plant","warehouse","distribution center"
]

# ───────── SQLITE RSS CACHE ─────────
_cache = get_conn()
_cache.execute("""
CREATE TABLE IF NOT EXISTS rss_cache(
  query TEXT PRIMARY KEY,
  data  TEXT,
  ts    INTEGER
)""")
_cache.commit()

# ───────── HELPERS ─────────
def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap reached; skipping call.")
        return False
    BUDGET_USED += cost; return True

def safe_chat(**params):
    """
    Fail-fast on rate limits. Return None immediately if over limit.
    """
    try:
        return client.chat.completions.create(**params)
    except RateLimitError:
        logging.warning("OpenAI rate-limit – skipping call")
        return None

def keyword_filter(title: str, co: str | None=None) -> bool:
    low = title.lower()
    return any(kw in low for kw in SEED_KWS) or (co and co.lower() in low)

def dedup(rows: list[dict]) -> list[dict]:
    seen_t, seen_u, out = set(), set(), []
    for r in rows:
        t, u = r["title"].lower(), r["url"].lower()
        if t in seen_t or u in seen_u: continue
        seen_t.add(t); seen_u.add(u); out.append(r)
    return out

# ───────── GEOCODE ─────────
def _geo(q: str):
    try: loc=geocoder.geocode(q, timeout=10)
    except: loc=None
    return (loc.latitude, loc.longitude) if loc else (None, None)

def safe_geocode(head: str, co: str):
    lat, lon = _geo(head)
    return (lat, lon) if lat is not None else _geo(co)

# ───────── RSS FETCH (CACHED) ─────────
def google_news(co: str, max_rec: int=MAX_HEADLINES) -> list[dict]:
    now = int(time.time())
    query = f'"{co}" ({" OR ".join(EXTRA_KWS)})'
    row = _cache.execute(
        "SELECT data, ts FROM rss_cache WHERE query=?", (query,)
    ).fetchone()
    if row and now - row[1] < 86400:
        items = json.loads(row[0])
    else:
        url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        )
        feed = feedparser.parse(url)
        today = datetime.datetime.utcnow().strftime("%Y%m%d")
        items = [
            {"title":e.title,"url":e.link,"seendate":today}
            for e in feed.entries[:max_rec]
        ]
        _cache.execute(
            "INSERT OR REPLACE INTO rss_cache(query,data,ts) VALUES(?,?,?)",
            (query, json.dumps(items), now)
        )
        _cache.commit()
    return items[:max_rec]

# ───────── BATCH GPT-3.5 PER HEADLINE ─────────
def gpt_signal_info(head: str) -> dict:
    if not budget_ok(0.07):
        return {"company":"Unknown","score":0.0}
    prompt = textwrap.dedent(f"""
        Return EXACTLY {{ "company":<name>,"score":<0-1> }} for:
        "{head}"
    """)
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=32
    )
    if not rsp:
        return {"company":"Unknown","score":0.0}
    try:
        data = json.loads(rsp.choices[0].message.content.strip())
        return {"company":data.get("company","Unknown"),
                "score":float(data.get("score",0.0))}
    except:
        return {"company":"Unknown","score":0.0}

# ───────── GPT-4o SUMMARY ─────────
def gpt_summary(co: str, heads: list[str]) -> dict:
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No headlines.","sector":"unknown",
                "confidence":0,"land_flag":0}
    wait = 21 - (time.time() - _4O_LAST_CALL)
    if wait>0: time.sleep(wait)
    if not budget_ok(0.5):
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}

    prompt = textwrap.dedent(f"""
        Summarise ≤6 bullets. Guess sector.
        land_flag=1 if site purchase.
        Return EXACT JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {"".join("- "+h+"\\n" for h in heads[:10])}
    """)
    rsp = safe_chat(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220
    )
    _4O_LAST_CALL = time.time()
    if not rsp:
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    try:
        return json.loads(rsp.choices[0].message.content.strip())
    except:
        txt = rsp.choices[0].message.content.strip()
        return {"summary":txt,"sector":"unknown","confidence":0,"land_flag":0}

# ───────── DB WRITERS ─────────
def write_signals(rows: list[dict], conn):
    for r in rows:
        conn.execute(
            "INSERT INTO signals(company,date,headline,url,source_label,"
            "land_flag,sector_guess,lat,lon) VALUES(?,?,?,?,?,?,?,?,?)",
            (r["company"],r["date"],r["headline"],r["url"],r["src"],
             r["land_flag"],r["sector"],r["lat"],r["lon"])
        )
    conn.commit()

def write_contacts(co: str, people: list[dict], conn):
    for p in people:
        conn.execute(
            "INSERT OR IGNORE INTO contacts"
            "(company,name,title,email,phone) VALUES(?,?,?,?,?)",
            (co,p.get("name",""),p.get("title",""),
             p.get("email",""),p.get("phone",""))
        )
    conn.commit()

# ensure contacts table
_conn = get_conn()
_conn.execute("""
CREATE TABLE IF NOT EXISTS contacts(
  company TEXT, name TEXT, title TEXT, email TEXT, phone TEXT,
  UNIQUE(company,name,title,email)
)""")
_conn.commit()

# ───────── CONTACTS VIA GPT-3.5 ─────────
def company_contacts(co: str) -> list[dict]:
    if not budget_ok(0.4): return []
    prompt = (f"Return JSON array (≤3) of procurement/engineering/"
              f"construction contacts for {co}. Fields:name,title,email,phone.")
    rsp = safe_chat(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=256
    )
    if not rsp: return []
    try: return json.loads(rsp.choices[0].message.content.strip())
    except: return []

# ───────── CLEARBIT LOGO ─────────
@st.cache_data(show_spinner=False)
def fetch_logo(co: str) -> bytes | None:
    dom = co.replace(" ","")+".com"
    try:
        r = requests.get(f"https://logo.clearbit.com/{dom}", timeout=6)
        return r.content if r.ok else None
    except:
        return None

# ───────── PDF EXPORT ─────────
def export_pdf(row: dict, bullets: str, contacts: list[dict]) -> bytes:
    pdf = FPDF(); pdf.set_auto_page_break(True,15); pdf.add_page()
    pdf.set_font("Helvetica","B",16)
    logo = fetch_logo(row["company"])
    if logo:
        ext = magic.from_buffer(logo,mime=True).split("/")[-1]
        fn  = f"/tmp/logo.{ext}"; open(fn,"wb").write(logo)
        pdf.image(fn,x=10,y=10,w=20); pdf.set_xy(35,10)
    pdf.multi_cell(0,10,row["headline"]); pdf.ln(5)
    pdf.set_font("Helvetica","",12)
    for b in bullets.split("•"):
        if b.strip(): pdf.multi_cell(0,7,"• "+b.strip())
    pdf.ln(3)
    if contacts:
        pdf.set_font("Helvetica","B",13); pdf.cell(0,8,"Key Contacts",ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(
                0,6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}  {c.get('phone','')}"
            ); pdf.ln(1)
    pdf.set_y(-30); pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(
        0,5,
        f"Source: {row['url']}\nGenerated:{datetime.datetime.now():%Y-%m-%d %H:%M}"
    )
    return pdf.output(dest="S").encode("latin-1")

# ───────── NATIONAL SCAN ─────────
def national_scan():
    conn = get_conn(); ensure_tables(conn)
    prospects=[]; bar=st.progress(0.0)

    # fetch & filter
    for i,kw in enumerate(SEED_KWS[:10],1):
        for a in google_news("", MAX_PROSPECTS):
            if keyword_filter(a["title"]):
                prospects.append({
                    "headline":a["title"],"url":a["url"],
                    "date":a["seendate"][:8],"src":"scan"
                })
        bar.progress(i/len(SEED_KWS))

    prospects = dedup(prospects)

    # batch GPT per headline
    scored=[]
    for j,p in enumerate(prospects,1):
        inf = gpt_signal_info(p["headline"])
        if inf["score"] >= RELEVANCE_CUTOFF:
            p["company"] = inf["company"]; scored.append(p)
        bar.progress(0.5 + 0.5*j/len(prospects))

    # group & summarize
    by_co=defaultdict(list)
    for p in scored: by_co[p["company"]].append(p)

    rows=[]
    for co, items in by_co.items():
        heads    = [it["headline"] for it in items]
        summ     = gpt_summary(co, heads)
        contacts = company_contacts(co)
        write_contacts(co, contacts, conn)

        for it in items:
            lat, lon = safe_geocode(it["headline"], co)
            it.update(
                land_flag  = summ["land_flag"],
                sector     = summ["sector"],
                confidence = summ["confidence"],
                lat        = lat,
                lon        = lon
            )
            rows.append(it)

        conn.execute(
            "INSERT OR REPLACE INTO clients (name,summary,sector_tags,status)"
            " VALUES(?,?,?, 'New')",
            (co, summ["summary"], json.dumps([summ["sector"]]))
        )

    write_signals(rows, conn)
    bar.progress(1.0); bar.empty()
    logging.info("national_scan wrote %s signals", len(rows))

# ───────── MANUAL SEARCH ─────────
def manual_search(co: str):
    conn = get_conn()
    arts = google_news(co, MAX_HEADLINES)
    rows = [
        {"title":a["title"],"url":a["url"],"date":a["seendate"][:8],"src":"search"}
        for a in arts if keyword_filter(a["title"], co)
    ]
    rows = dedup(rows)

    # batch GPT per headline (no company match filter)
    scored=[]
    for r in rows:
        inf = gpt_signal_info(r["title"])
        if inf["score"] >= RELEVANCE_CUTOFF:
            r["company"] = co; scored.append(r)

    heads   = [r["title"] for r in scored]
    summ    = gpt_summary(co, heads)
    lat,lon = safe_geocode(summ["summary"], co)

    return summ, scored, lat, lon
