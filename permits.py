"""
permits.py – Permit alerts for Lead Master

Returns a hybrid list of building-permit headlines
(national + top 15 county feeds), filtered out awarded notices.
"""

import datetime, feedparser, json
from collections import defaultdict
from urllib.parse import quote_plus
from utils import get_conn

def fetch_permits(max_rec=10) -> list[dict]:
    # import the same google_news + COUNTY_DOMAINS from fetch_signals
    from fetch_signals import google_news, dedup, COUNTY_DOMAINS

    results = []
    # national feed
    nat = google_news("building permit site:gov", max_rec)
    for a in nat:
        results.append({**a, "src":"national"})

    # county feeds
    for dom in COUNTY_DOMAINS:
        q   = f'"building permit" site:{dom}'
        url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
        )
        feed = feedparser.parse(url)
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        for e in feed.entries[:max_rec]:
            results.append({
                "title": e.title,
                "url":   e.link,
                "seendate": date,
                "src":     dom
            })

    # filter out awarded (mentions contractor)
    results = [r for r in results
               if "contractor" not in r["title"].lower()]

    # dedup & return
    from fetch_signals import dedup
    return dedup(results)
