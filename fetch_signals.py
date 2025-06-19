# fetch_signals.py  – v2 with GDELT-or-Google fallback
import os, json, datetime, requests, logging, sqlite3, textwrap, feedparser
import openai, pandas as pd
from utils import get_conn, ensure_tables, cache_summary

openai.api_key = os.getenv("OPENAI_API_KEY")
MAX_COMPANIES = int(os.getenv("MAX_COMPANIES_PER_RUN", 10))

# ───────────────── helper ─────────────────
def gdelt_headlines(company: str) -> list[dict]:
    """Try GDELT first; fall back to Google News RSS if GDELT times out."""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    api = ("https://api.gdeltproject.org/api/v2/doc/docsearch"
           f"?query=\"{company}\" AND {yesterday}&maxrecords=20&format=json")
    try:
        js = requests.get(api, timeout=60).json()        # 60-sec timeout
        return [
            {"text": a["title"], "url": a["url"], "date": a["seendate"][:8]}
            for a in js.get("articles", [])
        ]
    except Exception as e:
        logging.warning(f"GDELT error {e} – switching to Google News")
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={company}"
            "&hl=en-US&gl=US&ceid=US:en"
        )
        return [
            {"text": entry.title, "url": entry.link, "date": yesterday}
            for entry in feed.entries[:20]
        ]

# … rest of the file stays the same …
