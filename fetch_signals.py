"""
fetch_signals.py – Lead Master (map edition)
• Pulls yesterday’s news with GDELT; falls back to Google News RSS.
• Summarises headlines with GPT-4o (OpenAI v1 client).
• Handles non-JSON GPT replies gracefully.
• Writes signals + sector tags into SQLite.
"""

import os, json, datetime, logging, textwrap
import requests, feedparser, pandas as pd
from openai import OpenAI
from utils import get_conn, ensure_tables, cache_summary

# ───────── configuration ─────────
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_PROSPECTS = 50          # used by daily scan

# ───────── headline fetchers ─────────
def gdelt_headlines(company: str) -> list[dict]:
    """Return yesterday’s headlines for the company."""
    yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    url = (
        "https://api.gdeltproject.org/api/v2/doc/docsearch"
        f"?query=\"{company}\" AND {yday}&maxrecords=20&format=json"
    )
    try:
        js = requests.get(url, timeout=60).json()
        return [
            {"text": art["title"], "url": art["url"], "date": art["seendate"][:8], "src": "GDELT"}
            for art in js.get("articles", [])
        ]
    except Exception as e:
        logging.warning(f"GDELT error {e} – switching to Google News RSS")
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={company}&hl=en-US&gl=US&ceid=US:en"
        )
        return [
            {"text": ent.title, "url": ent.link, "date": yday, "src": "Google"}
            for ent in feed.entries[:20]
        ]

# ───────── GPT summariser ─────────
def summarise(company: str, headlines: list[dict]) -> dict | None:
    """Return dict even if GPT responds with plain text."""
    if not headlines:
        return None

    bullets = "\n".join(f"- {h['text']}" for h in headlines)
    prompt = textwrap.dedent(
        f"""
        You are a construction-lead analyst.
        Company: {company}
        Headlines (last 24 h):
        {bullets}

        Respond ONLY with JSON:
        {{
          "summary": "one concise sentence …",
          "sector": "…",
          "confidence": 0.8,
          "land_flag": 0
        }}
        """
    ).strip()

    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=220,
    )
    content = rsp.choices[0].message.content.strip()

    try:
        js = json.loads(content)
    except json.JSONDecodeError:
        logging.warning("GPT returned non-JSON; wrapping raw text.")
        js = {"summary": content, "sector": "unknown", "confidence": 0.0, "land_flag": 0}

    return js

# ───────── main daily run (used by workflows) ─────────
def run():
    conn = get_conn()
    ensure_tables(conn)

    companies = (
        pd.read_sql("SELECT name FROM clients", conn)["name"].tolist()
        or ["Acme Foods"]
    )

    for co in companies:
        heads = [h for h in gdelt_headlines(co) if not cache_summary(h["url"])]
        info = summarise(co, heads) if heads else None
        if not info:
            continue

        # save signals
        for h in heads:
            cache_summary(h["url"], info["summary"])
            conn.execute(
                """
                INSERT INTO signals
                (company, date, headline, url, source_label, land_flag, sector_guess)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    co,
                    h["date"],
                    info["summary"],
                    h["url"],
                    h["src"],
                    info["land_flag"],
                    info["sector"],
                ),
            )

        # upsert client basic data
        existing = conn.execute(
            "SELECT sector_tags FROM clients WHERE name=?", (co,)
        ).fetchone()
        tags = json.loads(existing[0]) if existing else []
        if info["sector"] and info["sector"] not in tags:
            tags.append(info["sector"])

        conn.execute(
            """
            INSERT OR REPLACE INTO clients
            (name, summary, sector_tags, status)
            VALUES (?,?,?, 'New')
            """,
            (co, info["summary"], json.dumps(tags)),
        )
        conn.commit()
        logging.info(f"{co}: saved")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
