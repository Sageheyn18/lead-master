"""
utils.py  –  Lead Master

• SQLite helpers
• Table creator (with lat / lon added)
• Simple content-hash cache for headline summaries
"""

import sqlite3, hashlib, json, logging, os
from pathlib import Path

# ---------- configuration ----------
DB_PATH   = "lead_master.db"      # main data
CACHE_DB  = "cache.sqlite"        # tiny URL→summary cache


# ---------- connection helpers ----------
def get_conn(db: str = DB_PATH) -> sqlite3.Connection:
    """Return a sqlite3 connection with safe threading."""
    return sqlite3.connect(db, check_same_thread=False)


# ---------- table creator ----------
def ensure_tables(conn: sqlite3.Connection) -> None:
    """Create tables if they don’t exist (idempotent)."""
    conn.executescript(
        """
        -- ===== clients =====
        CREATE TABLE IF NOT EXISTS clients (
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

        -- ===== signals =====
        CREATE TABLE IF NOT EXISTS signals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            company       TEXT,
            date          TEXT,
            headline      TEXT,
            url           TEXT,
            source_label  TEXT,
            land_flag     INTEGER DEFAULT 0,
            sector_guess  TEXT,
            lat           REAL,          --  NEW: latitude
            lon           REAL           --  NEW: longitude
        );

        -- ===== keyword cache (for GPT-expanded phrases) =====
        CREATE TABLE IF NOT EXISTS kw_cache (
            id       INTEGER PRIMARY KEY,
            keywords TEXT,
            updated  TEXT
        );
        """
    )
    conn.commit()


# ---------- tiny headline-summary cache ----------
def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def cache_summary(url: str, summary: str | None = None) -> str | None:
    """
    • Pass (url, summary)   → store.
    • Pass (url, None)      → return cached summary or None.
    """
    c = get_conn(CACHE_DB)
    c.execute(
        "CREATE TABLE IF NOT EXISTS cache(urlhash TEXT PRIMARY KEY, summary TEXT)"
    )

    if summary is None:                         # --- lookup ---
        row = c.execute(
            "SELECT summary FROM cache WHERE urlhash=?", (_hash_url(url),)
        ).fetchone()
        return None if row is None else row[0]

    # --- store / update ---
    c.execute(
        "INSERT OR REPLACE INTO cache(urlhash, summary) VALUES(?, ?)",
        (_hash_url(url), summary),
    )
    c.commit()
