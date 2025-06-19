
import sqlite3, json, hashlib, os, logging

DB = "lead_master.db"
CACHE = "cache.sqlite"

def get_conn(db: str = DB):
    return sqlite3.connect(db, check_same_thread=False)

def ensure_tables(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS clients(
        name TEXT PRIMARY KEY,
        summary TEXT,
        sector_tags TEXT DEFAULT '[]',
        status TEXT DEFAULT 'New',
        hq_address TEXT,
        phone TEXT,
        website TEXT,
        logo_url TEXT,
        facilities TEXT DEFAULT '[]',
        contacts TEXT DEFAULT '[]',
        next_touch TEXT,
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company TEXT,
        date TEXT,
        headline TEXT,
        url TEXT,
        source_label TEXT,
        land_flag INTEGER DEFAULT 0,
        sector_guess TEXT
    );
    CREATE TABLE IF NOT EXISTS kw_cache(
        id INTEGER PRIMARY KEY,
        keywords TEXT,
        updated TEXT
    );
    """)
    conn.commit()

def hash_url(url:str)->str:
    return hashlib.sha256(url.encode()).hexdigest()

def cache_summary(url:str, summary:str=None):
    c=get_conn("cache.sqlite")
    c.execute("CREATE TABLE IF NOT EXISTS cache(urlhash TEXT PRIMARY KEY, summary TEXT)")
    if summary is None:
        row=c.execute("SELECT summary FROM cache WHERE urlhash=?", (hash_url(url),)).fetchone()
        return None if row is None else row[0]
    c.execute("INSERT OR REPLACE INTO cache VALUES(?,?)",(hash_url(url),summary))
    c.commit()
