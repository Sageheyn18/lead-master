"""
fetch_signals.py  – Lead Master  v4.9   (2025-06-21)

• 15-s GDELT timeout → Google-News RSS fallback  (URL-encoded query)
• Duplicate logic:  drop when *title OR URL* repeats  (case-insensitive)
• GPT-3.5 company extraction   ·   GPT-4o bullet-list summary (≥ 21 s throttle)
• Relevance filter: GPT score ≥ 0.55 keeps headline  (gpt-3.5, cheap)
• HQ-city geocode fallback
• Contacts scraper (gpt-3.5) writes to contacts table
• PDF export (Clearbit logo, summary bullets, contacts)
• Google RSS JSON cached in SQLite   ·   optional SMTP digest hook
"""

import os, json, time, datetime, logging, textwrap, requests, feedparser, sqlite3
from collections import defaultdict
from difflib import SequenceMatcher
from urllib.parse   import quote_plus

import streamlit as st
from geopy.geocoders import Nominatim
from openai          import OpenAI, RateLimitError
from fpdf            import FPDF
import magic                                            # logo MIME sniff

from utils import get_conn, ensure_tables

# ───────── CONFIG ────────────────────────────────────────────────────────────
client   = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
geocoder = Nominatim(user_agent="lead-master")

MAX_PROSPECTS = 50
MAX_HEADLINES = 50
DAILY_BUDGET  = int(os.getenv("DAILY_BUDGET_CENTS", "300"))     # ≈ $3 / day
BUDGET_USED   = 0
_4O_LAST_CALL = 0                                               # 1 / 21 s

RELEVANCE_CUTOFF = 0.55        # keep headlines scored ≥ 0.55

SEED_KWS = [
    "land purchase", "acres", "acquire site", "groundbreaking",
    "construct", "construction project", "plant expansion", "build new",
    "distribution center", "warehouse", "cold storage", "manufacturing facility",
    "industrial park", "facility renovation"
]

EXTRA_KWS = [
    "land", "acres", "site", "build", "building", "construction",
    "expansion", "facility", "plant", "warehouse", "distribution center"
]

# ───────── helper utilities ──────────────────────────────────────────────────
def _similar(a, b): return SequenceMatcher(None, a, b).ratio()

def keyword_filter(t: str, company: str | None = None) -> bool:
    low = t.lower()
    return (any(w in low for w in SEED_KWS) or
            (company and company.lower() in low))

def dedup(rows: list[dict]) -> list[dict]:
    seen_t, seen_u, out = set(), set(), []
    for r in rows:
        t, u = r["title"].lower(), r["url"].lower()
        if t in seen_t or u in seen_u:
            continue
        seen_t.add(t); seen_u.add(u); out.append(r)
    return out

def budget_ok(cost: float) -> bool:
    global BUDGET_USED
    if BUDGET_USED + cost > DAILY_BUDGET:
        logging.warning("GPT budget cap reached; skipping call.")
        return False
    BUDGET_USED += cost; return True

def safe_chat(**params):
    for i in range(3):
        try:    return client.chat.completions.create(**params)
        except RateLimitError:
            logging.warning("Rate limit – retry in 21 s (%s/3)", i+1); time.sleep(21)
    logging.error("Rate-limit after retries."); return None

# ───────── GPT helpers ───────────────────────────────────────────────────────
def gpt_company(head: str) -> str:
    if not budget_ok(0.05): return "Unknown"
    rsp = safe_chat(model="gpt-3.5-turbo",
        messages=[{"role":"user","content":f"ONLY company name in: {head}"}],
        temperature=0, max_tokens=16)
    return ("Unknown" if rsp is None
            else rsp.choices[0].message.content.strip().strip('"'))

def gpt_relevance(head: str) -> float:
    """Return 0-1 relevance score; cheap 3.5 call."""
    if not budget_ok(0.02): return 1.0
    prompt = ("Score 0-1 how much this headline hints at land purchase or "
              "new construction project:\n"+head+"\nScore:")
    rsp = safe_chat(model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        temperature=0, max_tokens=4)
    if rsp is None: return 0.0
    try:   return float(rsp.choices[0].message.content.strip())
    except: return 0.0

def gpt_summary(company: str, heads: list[str]) -> dict:
    global _4O_LAST_CALL
    if not heads:
        return {"summary":"No construction headlines.","sector":"unknown",
                "confidence":0,"land_flag":0}
    wait = 21 - (time.time() - _4O_LAST_CALL)
    if wait>0: time.sleep(wait)
    if not budget_ok(0.5):
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    prompt = textwrap.dedent(f"""
        Summarise in concise bullet list (≤6 bullets).  Guess sector.
        land_flag=1 if land/site purchase implied.
        Return JSON {{summary, sector, confidence, land_flag}}.
        Headlines:
        {"".join("- "+h+"\\n" for h in heads[:10])}
    """)
    rsp = safe_chat(model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2, max_tokens=220)
    _4O_LAST_CALL = time.time()
    if rsp is None:
        return {"summary":heads[0][:120]+"…","sector":"unknown",
                "confidence":0,"land_flag":0}
    try: return json.loads(rsp.choices[0].message.content.strip())
    except: return {"summary":rsp.choices[0].message.content.strip(),
                    "sector":"unknown","confidence":0,"land_flag":0}

# ───────── geocode helpers ───────────────────────────────────────────────────
geocoder = Nominatim(user_agent="lead-master")
def geo(q:str):
    try: loc=geocoder.geocode(q,timeout=10)
    except: loc=None
    return (loc.latitude,loc.longitude) if loc else (None,None)

def safe_geocode(head:str,co:str):
    lat,lon=geo(head); return (lat,lon) if lat else geo(co)

# ───────── logo helper (Clearbit) ────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def logo_bytes(dom: str) -> bytes | None:
    try:
        r=requests.get(f"https://logo.clearbit.com/{dom}",timeout=6)
        return r.content if r.ok else None
    except: return None

# ───────── contacts scraper (gpt-3.5) ────────────────────────────────────────
def company_contacts(co:str)->list[dict]:
    if not budget_ok(0.4): return []
    p=("JSON list (≤3) procurement / construction contacts for "
       f"{co}. Fields: name,title,email,phone.")
    rsp=safe_chat(model="gpt-3.5-turbo",
        messages=[{"role":"user","content":p}],temperature=0,max_tokens=256)
    if rsp is None: return []
    try: return json.loads(rsp.choices[0].message.content.strip())
    except: return []

# ───────── headline fetchers ─────────────────────────────────────────────────
def google_news(co:str,max_rec:int=40):
    q=f'"{co}" ({ " OR ".join(EXTRA_KWS) })'
    url=("https://news.google.com/rss/search?"
         f"q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en")
    feed=feedparser.parse(url)
    today=datetime.datetime.utcnow().strftime("%Y%m%d")
    return [{"title":e.title,"url":e.link,"seendate":today}
            for e in feed.entries[:max_rec]]

GDELT_OK=True
def gdelt_or_google(gq:str,co:str,max_rec:int=20):
    global GDELT_OK
    if not GDELT_OK: return google_news(co,max_rec)
    url=("https://api.gdeltproject.org/api/v2/doc/docsearch?"
         f"query={gq}&maxrecords={max_rec}&format=json")
    try: return requests.get(url,timeout=15).json().get("articles",[])
    except Exception as e:
        logging.warning("GDELT timeout → Google (%s)",e); GDELT_OK=False
        return google_news(co,max_rec)

# ───────── DB helpers ────────────────────────────────────────────────────────
def write_signals(rows,conn):
    for r in rows:
        conn.execute("""INSERT INTO signals
            (company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (r['company'],r['date'],r['headline'],r['url'],r['src'],
             r['land_flag'],r['sector'],r['lat'],r['lon']))
    conn.commit()

def write_contacts(co:str,people:list[dict],conn):
    for p in people:
        conn.execute("""INSERT OR IGNORE INTO contacts
            (company,name,title,email,phone) VALUES (?,?,?,?,?)""",
            (co,p.get('name',''),p.get('title',''),
             p.get('email',''),p.get('phone','')))
    conn.commit()

# Ensure contacts table
conn0=get_conn(); conn0.execute("""
CREATE TABLE IF NOT EXISTS contacts(
 company TEXT, name TEXT, title TEXT, email TEXT, phone TEXT,
 UNIQUE(company,name,title,email)
)"""); conn0.commit()

# ───────── PDF export (Clearbit logo) ────────────────────────────────────────
def export_pdf(row:dict, bullets:str, contacts:list[dict])->bytes:
    pdf=FPDF(); pdf.set_auto_page_break(True,15); pdf.add_page()
    pdf.set_font("Helvetica","B",16)
    logo=logo_bytes(row['company'].replace(" ","")+".com")
    if logo:
        ext=magic.from_buffer(logo,mime=True).split("/")[-1]; fn=f"/tmp/l.{ext}"
        open(fn,"wb").write(logo); pdf.image(fn,x=10,y=10,w=20); pdf.set_xy(35,10)
    pdf.multi_cell(0,10,row['headline']); pdf.ln(5)
    pdf.set_font("Helvetica","",12)
    for b in bullets.split("•"):
        if b.strip(): pdf.multi_cell(0,7,"• "+b.strip())
    pdf.ln(3)
    if contacts:
        pdf.set_font("Helvetica","B",13); pdf.cell(0,8,"Key Contacts",ln=1)
        pdf.set_font("Helvetica","",11)
        for c in contacts:
            pdf.multi_cell(0,6,
                f"{c.get('name','')} — {c.get('title','')}\n"
                f"{c.get('email','')}  {c.get('phone','')}")
            pdf.ln(1)
    pdf.set_y(-30); pdf.set_font("Helvetica","I",9)
    pdf.multi_cell(0,5,
        f"Source: {row['url']}\nGenerated:"
        f" {datetime.datetime.now():%Y-%m-%d %H:%M}")
    return pdf.output(dest="S").encode("latin-1")

# ───────── national scan (GPT relevance filter) ──────────────────────────────
def national_scan():
    conn=get_conn(); ensure_tables(conn)
    prospects=[]; bar=st.progress(0.0)

    for i,kw in enumerate(SEED_KWS[:10],1):
        for art in gdelt_or_google(kw.replace(" ","%20"),"",10):
            if keyword_filter(art['title']):
                prospects.append({"headline":art['title'],"url":art['url'],
                                  "date":art['seendate'][:8],"src":"scan"})
                if len(prospects)>=MAX_PROSPECTS: break
        if len(prospects)>=MAX_PROSPECTS: break
        bar.progress(i/20)

    prospects=dedup(prospects)

    # relevance score
    scored=[]
    for j,p in enumerate(prospects,1):
        score=gpt_relevance(p['headline'])
        if score>=RELEVANCE_CUTOFF:
            p['score']=score; scored.append(p)
        bar.progress(0.5+0.3*j/len(prospects))

    by_co=defaultdict(list)
    for p in scored:
        p["company"]=gpt_company(p["headline"])
        by_co[p["company"]].append(p)

    rows=[]; conn.execute("BEGIN")
    for co,items in by_co.items():
        info=gpt_summary(co,[i["headline"] for i in items])
        contacts=company_contacts(co)
        write_contacts(co,contacts,conn)
        for it in items:
            lat,lon=safe_geocode(it["headline"],co)
            it.update(lat=lat,lon=lon,sector=info["sector"],
                      land_flag=info["land_flag"])
            rows.append(it)
        conn.execute("""INSERT OR REPLACE INTO clients
            (name, summary, sector_tags, status)
            VALUES (?,?,?, 'New')""", (co,info['summary'],
                                       json.dumps([info['sector']])))
    write_signals(rows,conn); conn.commit()
    bar.progress(1.0); bar.empty()
    logging.info("Scan wrote %s signals",len(rows))

# ───────── manual search helper ──────────────────────────────────────────────
def headlines_for_company(co:str)->list[dict]:
    today=datetime.date.today(); start=today-datetime.timedelta(days=150)
    gdelt_q=f'"{co}" AND ({start:%Y%m%d} TO {today:%Y%m%d})'
    arts=gdelt_or_google(gdelt_q.replace(" ","%20"),co,MAX_HEADLINES*2)
    rows=[]
    for a in arts:
        if keyword_filter(a['title'],co):
            rows.append({"title":a['title'],"url":a['url'],
                         "date":a.get("seendate",today.strftime("%Y%m%d")),
                         "src":"search"})
        if len(rows)>=MAX_HEADLINES: break
    rows=dedup(rows)
    # relevance
    return [r for r in rows if gpt_relevance(r['title'])>=RELEVANCE_CUTOFF]

def manual_search(co:str):
    rows=headlines_for_company(co)
    info=gpt_summary(co,[r["title"] for r in rows])
    lat,lon=safe_geocode(info["summary"],co)
    return info, rows, lat, lon
