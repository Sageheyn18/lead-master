"""
Lead Master – fetch_signals.py (OpenAI v1+)
 • Gets yesterday’s news from GDELT, falls back to Google News RSS.
 • Summarises headlines with GPT-4o using the new OpenAI client.
 • Writes signals + sector tags into SQLite.
"""

import os, json, datetime, logging, textwrap, sqlite3
import requests, feedparser, pandas as pd
from openai import OpenAI                           # ← new import

from utils import get_conn, ensure_tables, cache_summary

# ─────────────────── configuration ───────────────────
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_COMPANIES = int(os.getenv("MAX_COMPANIES_PER_RUN", 10))

# ─────────────────── helpers ─────────────────────────
def gdelt_headlines(company: str) -> list[dict]:
    """Try GDELT; if it times out, fall back to Google News RSS."""
    yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    gdelt_url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query=\"{company}\" AND {yday}&maxrecords=20&format=json"
    )
    try:
        js = requests.get(gdelt_url, timeout=60).json()
        return [
            {"text": a["title"], "url": a["url"], "date": a["seendate"][:8]}
            for a in js.get("articles", [])
        ]
    except Exception as e:
        logging.warning(f"GDELT error {e} – switching to Google News RSS")
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={company}"
            "&hl=en-US&gl=US&ceid=US:en"
        )
        return [
            {"text": ent.title, "url": ent.link, "date": yday}
            for ent in feed.entries[:20]
        ]

def summarise(company: str, headlines: list[dict]) -> dict | None:
    """One GPT-4o call returns JSON summary & sector guess."""
    if not headlines:
        return None
    bullets = "\n".join(f"- {h['text']}" for h in headlines)
    prompt = textwrap.dedent(
        f"""
        You are a construction-lead analyst.
        Company: {company}
        Headlines (last 24 h):
        {bullets}

        Return JSON with keys:
        summary     – one concise sentence
        sector      – guessed sector (e.g. food-processing, cold-storage)
        confidence  – 0-1 float
        """
    ).strip()

    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=200,
    )
    return json.loads(rsp.choices[0].message.content)

# ─────────────────── main routine ────────────────────
def run():
    conn = get_conn()
    ensure_tables(conn)

    companies = pd.read_sql(
        "SELECT name FROM clients LIMIT ?", conn, params=(MAX_COMPANIES,)
    )["name"].tolist() or ["Acme Foods"]  # seed if DB empty

    for co in companies:
        heads = [h for h in gdelt_headlines(co) if not cache_summary(h["url"])]
        if not heads:
            continue
        info = summarise(co, heads)
        if not info:
            continue

        # write signals
        for h in heads:
            cache_summary(h["url"], info["summary"])
            conn.execute(
                """
                INSERT INTO signals
                (company, date, headline, url, confidence, sector_guess)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    co,
                    h["date"],
                    info["summary"],
                    h["url"],
                    info.get("confidence", 0),
                    info.get("sector"),
                ),
            )

        # update / insert client row
        existing = conn.execute(
            "SELECT sector_tags FROM clients WHERE name=?", (co,)
        ).fetchone()
        tags = json.loads(existing[0]) if existing else []
        if info["sector"] and info["sector"] not in tags:
            tags.append(info["sector"])
        conn.execute(
            """
            INSERT OR REPLACE INTO clients
            (name, last_signal, sector_tags)
            VALUES (?,?,?)
            """,
            (co, info["summary"], json.dumps(tags)),
        )
        conn.commit()
        logging.info(f"{co}: {info['sector']}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
