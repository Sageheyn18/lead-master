
import os, json, datetime, logging, textwrap, re, requests, feedparser
from urllib.parse import urlparse
import pandas as pd
from geopy.geocoders import Nominatim
from openai import OpenAI
from utils import get_conn, ensure_tables, cache_summary

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_PROSPECTS = 50
SEED_KWS = ["plant expansion","groundbreaking","distribution center","warehouse","cold storage","factory","manufacturing facility","acquire site","buys land","facility renovation"]

def get_keywords(conn):
    row = conn.execute("SELECT keywords, updated FROM kw_cache WHERE id=1").fetchone()
    today=datetime.date.today()
    if row:
        last=datetime.datetime.fromisoformat(row[1]).date()
        if (today - last).days < 7:
            return json.loads(row[0])
    prompt="Expand the following construction-related phrases to detect news about land purchase and facility expansion (max 60 unique phrases):\n"+", ".join(SEED_KWS)
    rsp=client.chat.completions.create(model="gpt-4o-mini",messages=[{"role":"user","content":prompt}],temperature=0.3,max_tokens=300)
    kws=[k.strip() for k in rsp.choices[0].message.content.split(",") if k.strip()]
    conn.execute("INSERT OR REPLACE INTO kw_cache(id,keywords,updated) VALUES(1,?,?)",(json.dumps(kws[:60]), today.isoformat()))
    conn.commit()
    return kws[:60]

def headline_matches(text,kws):
    low=text.lower()
    return any(kw.lower() in low for kw in kws)

def national_scan():
    conn=get_conn()
    ensure_tables(conn)
    kws=get_keywords(conn)
    # use GDELT Events API quick search for broad phrases
    prospects=[]
    for kw in kws[:10]:
        url=f"https://api.gdeltproject.org/api/v2/doc/docsearch?query={kw}&maxrecords=5&format=json"
        try:
            js=requests.get(url,timeout=30).json()
            for a in js.get("articles",[]):
                title=a["title"]
                if not headline_matches(title,kws): continue
                prospects.append({"company":a.get("source"),"headline":title,"url":a["url"],"date":a["seendate"][:8]})
        except Exception as e:
            logging.warning(e)
    prospects=prospects[:MAX_PROSPECTS]
    # write to signals temp table
    for p in prospects:
        conn.execute("INSERT INTO signals(company,date,headline,url,source_label,land_flag) VALUES(?,?,?,?,?,0)",(p["company"],p["date"],p["headline"],p["url"],"scan"))
    conn.commit()

if __name__=="__main__":
    national_scan()
