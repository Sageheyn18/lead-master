# utils.py

import sqlite3
from pathlib import Path

# ───────── Paths & constants ─────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)                       # ensure data/ exists
DB_PATH = DATA_DIR / "leadmaster.db"                # your SQLite file

RAW_CACHE_TABLE = "raw_cache"
CLIENTS_TABLE   = "clients"
SIGNALS_TABLE   = "signals"

# ───────── Connection helper ─────────
def get_conn():
    """
    Return a sqlite3.Connection to the DB in data/leadmaster.db.
    check_same_thread=False so Streamlit can share it across reruns.
    """
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# ───────── Bootstrap all tables ─────────
def ensure_tables():
    """
    Create clients, signals, and raw_cache tables if they don't exist.
    Call this once at app startup.
    """
    conn = get_conn()
    c = conn.cursor()

    # Clients table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLIENTS_TABLE} (
            name        TEXT    PRIMARY KEY,
            summary     TEXT,
            sector_tags TEXT,         -- JSON-encoded list of tags
            status      TEXT,         -- e.g. 'New', 'Contacted', etc.
            lat         REAL,
            lon         REAL
        )
    """)

    # Signals table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {SIGNALS_TABLE} (
            company  TEXT,
            headline TEXT,
            url      TEXT,
            date     TEXT,
            lat      REAL,
            lon      REAL,
            PRIMARY KEY(company, headline)
        )
    """)

    # Raw cache table for manual_search/RSS caching
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_CACHE_TABLE} (
            seed      TEXT,
            fetched   TIMESTAMP,
            headline  TEXT,
            url       TEXT,
            date      TEXT,
            PRIMARY KEY(seed, headline, url)
        )
    """)

    conn.commit()
    conn.close()
