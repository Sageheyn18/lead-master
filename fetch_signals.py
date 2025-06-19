"""
Lead Master – fetch_signals.py (OpenAI v1, resilient JSON)

• Grabs yesterday’s news from GDELT; if slow, falls back to Google News RSS.
• Summarises all headlines for one company with GPT-4o-mini (new OpenAI client).
• Handles bad GPT JSON gracefully.
• Writes signals + sector tags into SQLite.
"""

import os, json, datetime, logging, textwrap, sqlite3
import requests, feedparser, pandas as pd
from openai import OpenAI                     # ← v1 client

from utils import get_conn, ensure_tables, cache_summary

# ────────────────────── config ──────────────────────
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_COMPANIES = int(os.getenv("MAX_COMPANIES_PER_RUN", 10))

# ────────────────── helpers ─────────────────────────
def gdelt_headlines(company: str) -> list[dict]:
    """Try GDELT first; fall back to Google News RSS if GDELT times out."""
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
            f"https://news.google.com/rss/search?q={company}&hl=en-US&gl=US&ceid=US:en"
        )
        return [
            {"text": ent.title, "url": ent.link, "date": yday}
            for ent in feed.entries[:20]
        ]

def summarise(company: str, headlines: list[dict]) -> dict | None:
    """Call GPT-4o once; return dict even if GPT gives plain text."""
    if not headlines:
        return None

    bullets = "\n".join(f"- {h['text']}" for h in headlines)
    prompt = textwrap.dedent(
        f"""
        You are a construction-lead analyst.
        Company: {company}
        Headlines (last 24 h):
        {bullets}

        Return ONLY JSON like:
        {{
          "summary": "...",
          "sector": "...",
          "confidence": 0.8
        }}
        """
    ).strip()

    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=200,
    )

    content = rsp.choices[0].message.content.strip()

    try:                                     # robust parse
        js = json.loads(content)
    except json.JSONDecodeError:
        logging.warning("GPT returned non-JSON; using raw text.")
        js = {"summary": content, "sector": "unknown", "confidence": 0.0}

    return js

# ────────────────── main routine ───────────────────
def run() -> None:
    conn = get_conn()
    ensure_tables(conn)

    companies = (
        pd.read_sql("SELECT name FROM clients LIMIT ?", conn, params=(MAX_COMPANIES,))
        ["name"]
        .tolist()
        or ["Acme Foods"]  # seed if DB empty
    )

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
                (company,date,headline,url,confidence,sector_guess)
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
        tags = (
            json.loads(
                conn.execute(
                    "SELECT sector_tags FROM clients WHERE name=?", (co,)
                ).fetchone()[0]
            )
            if conn.execute(
                "SELECT 1 FROM clients WHERE name=?", (co,)
            ).fetchone()
            else []
        )
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
