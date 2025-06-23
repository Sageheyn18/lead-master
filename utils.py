# utils.py — SQLite helpers

import sqlite3

DB_PATH = "lead_master.db"

def get_conn():
    """Return a SQLite connection (thread‐safe)."""
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_tables(conn):
    """Create clients, signals, and pipeline tables if they don’t exist."""
    conn.execute("""
      CREATE TABLE IF NOT EXISTS clients (
        name        TEXT PRIMARY KEY,
        summary     TEXT,
        sector_tags TEXT,
        status      TEXT,
        lat         REAL,
        lon         REAL
      )
    """)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS signals (
        company   TEXT,
        headline  TEXT,
        url       TEXT,
        date      TEXT,
        lat       REAL,
        lon       REAL,
        PRIMARY KEY(company, headline)
      )
    """)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS pipeline (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        company   TEXT,
        headline  TEXT,
        status    TEXT
      )
    """)
    conn.commit()
