"""
utils.py – Lead Master  v4.1
• SQLite helpers   • Table creator (lat / lon / read_flag)
• Tiny URL→summary cache
"""

import sqlite3, hashlib, json, os, logging, datetime

DB_PATH  = "lead_master.db"
CACHE_DB = "cache.sqlite"


def get_conn(db: str = DB_PATH) -> sqlite3.Connection:
    """Return thread-safe SQLite connection."""
    return sqlite3.connect(db, check_same_thread=False)


# ───────── ensure tables (adds cols if missing) ─────────
def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS clients(
            name          TEXT PRIMARY KEY,
            summary       TEXT,
            sector_tags   TEXT DEFAULT '[]',
            status        TEXT DEFAULT 'New',
            hq_address    TEXT,
            phone         TEXT,
            website       TEXT,
            logo_url      TEXT,
            facilities    TEXT DEFAULT '[]',
            contacts      TEXT DEFAULT '[]',
            next_touch    TEXT,
            notes         TEXT
        );

        CREATE TABLE IF NOT EXISTS signals(
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            company       TEXT,
            date          TEXT,
            headline      TEXT,
            url           TEXT,
            source_label  TEXT,
            land_flag     INTEGER DEFAULT 0,
            sector_guess  TEXT,
            lat           REAL,
            lon           REAL,
            read_flag     INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS kw_cache(
            id       INTEGER PRIMARY KEY,
            keywords TEXT,
            updated  TEXT
        );
        """
    )

    # ── add missing columns in existing DBs ──
    cols = {c[1] for c in conn.execute("PRAGMA table_info(signals)")}
    for col, sql in [
        ("lat",  "ALTER TABLE signals ADD COLUMN lat REAL"),
        ("lon",  "ALTER TABLE signals ADD COLUMN lon REAL"),
        ("read_flag", "ALTER TABLE signals ADD COLUMN read_flag INTEGER DEFAULT 0"),
    ]:
        if col not in cols:
            conn.execute(sql)

    conn.commit()


# ───────── tiny cache for GPT summaries ─────────
def _h(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def cache_summary(url: str, summary: str | None = None) -> str | None:
    c = get_conn(CACHE_DB)
    c.execute(
        "CREATE TABLE IF NOT EXISTS cache(urlhash TEXT PRIMARY KEY, summary TEXT)"
    )
    if summary is None:
        row = c.execute(
            "SELECT summary FROM cache WHERE urlhash=?", (_h(url),)
        ).fetchone()
        return None if row is None else row[0]
    c.execute(
        "INSERT OR REPLACE INTO cache(urlhash, summary) VALUES(?,?)",
        (_h(url), summary),
    )
    c.commit()
