
import sqlite3, os, logging, hashlib, datetime

DB = "lead_radar.db"
CACHE = "cache.sqlite"

def get_conn(db=DB):
    return sqlite3.connect(db, check_same_thread=False)

def ensure_tables(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS clients(
        name TEXT PRIMARY KEY,
        last_signal TEXT,
        sector_tags TEXT DEFAULT '[]',
        status TEXT DEFAULT 'New',
        next_touch TEXT,
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company TEXT,
        date TEXT,
        headline TEXT,
        url TEXT,
        lat REAL,
        lon REAL,
        confidence REAL,
        sector_guess TEXT
    );
    """)
    conn.commit()

def hash_url(url:str)->str:
    import hashlib
    return hashlib.sha256(url.encode()).hexdigest()

def cache_summary(url:str, summary:str=None):
    c = get_conn(CACHE)
    c.execute("CREATE TABLE IF NOT EXISTS cache(urlhash TEXT PRIMARY KEY, summary TEXT)")
    if summary is None:
        row=c.execute("SELECT summary FROM cache WHERE urlhash=?", (hash_url(url),)).fetchone()
        return None if row is None else row[0]
    c.execute("INSERT OR REPLACE INTO cache VALUES(?,?)",(hash_url(url), summary))
    c.commit()
