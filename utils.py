import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "lead_master.db"
PERMITS_CSV = Path(__file__).parent / "data" / "permits.csv"

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return conn

def ensure_tables():
    conn = get_conn()
    c = conn.cursor()
    # Clients table
    c.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        name TEXT PRIMARY KEY,
        summary TEXT,
        sector_tags TEXT,
        status TEXT,
        lat REAL,
        lon REAL,
        contacts TEXT
    )
    """)
    # Signals table (national scan & manual cache)
    c.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company TEXT,
        headline TEXT,
        url TEXT,
        date TEXT,
        score REAL,
        read INTEGER DEFAULT 0
    )
    """)
    # Raw-cache for manual scan
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_cache (
        seed TEXT,
        fetched TIMESTAMP,
        headline TEXT,
        url TEXT,
        date TEXT,
        PRIMARY KEY(seed, headline)
    )
    """)
    conn.commit()
    conn.close()
