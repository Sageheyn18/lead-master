
import os, json, datetime, requests, logging, sqlite3, textwrap
import openai, pandas as pd
from utils import get_conn, ensure_tables, cache_summary

openai.api_key = os.getenv("OPENAI_API_KEY")
MAX_COMPANIES = int(os.getenv("MAX_COMPANIES_PER_RUN", 10))
GDELT = ("https://api.gdeltproject.org/api/v2/doc/docsearch?"
         "query=\"{company}\"&filter=SourceCommonName:NEWS&mode=ArtList&maxrecords=20&format=json")

def gdelt_headlines(company):
    yesterday=(datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y%m%d")
    try:
        js=requests.get(GDELT.format(company=company), timeout=20).json()
        return [{"text":a["title"],"url":a["url"],"date":a["seendate"][:8]} for a in js.get("articles",[])]
    except Exception as e:
        logging.warning(f"gdelt error {e}")
        return []

def summarise(company, headlines):
    if not headlines: return None
    txt="\n".join(f"- {h['text']}" for h in headlines)
    prompt=textwrap.dedent(f"""Summarise in one sentence and guess sector for company {company}. Headlines:\n{txt}
    Return JSON with keys summary, sector, confidence.""")
    try:
        r=openai.ChatCompletion.create(model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}], temperature=0.2, max_tokens=120)
        return json.loads(r.choices[0].message.content)
    except Exception as e:
        logging.warning(f"gpt error {e}")
        return None

def run():
    conn=get_conn()
    ensure_tables(conn)
    companies=pd.read_sql("SELECT name FROM clients LIMIT ?", conn, params=(MAX_COMPANIES,))["name"].tolist()
    if not companies:
        companies=["Acme Foods"]  # seed example
        conn.execute("INSERT OR IGNORE INTO clients(name) VALUES(?)", (companies[0],))
        conn.commit()
    for co in companies:
        heads=[h for h in gdelt_headlines(co) if not cache_summary(h["url"])]
        if not heads: continue
        info=summarise(co, heads)
        if not info: continue
        for h in heads:
            cache_summary(h["url"], info["summary"])
            conn.execute("INSERT INTO signals(company,date,headline,url,confidence,sector_guess) VALUES(?,?,?,?,?,?)",
                         (co,h["date"],info["summary"],h["url"],info.get("confidence",0),info.get("sector")))
        tags=json.loads(conn.execute("SELECT sector_tags FROM clients WHERE name=?", (co,)).fetchone()[0])
        if info.get("sector") and info["sector"] not in tags:
            tags.append(info["sector"])
        conn.execute("UPDATE clients SET last_signal=?, sector_tags=? WHERE name=?",
                     (info["summary"], json.dumps(tags), co))
        conn.commit()
        logging.info(f"{co}: {info['sector']}")
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    run()
