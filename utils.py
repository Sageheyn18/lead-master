import os
import sqlite3

# Database file path (change as needed)
DB_PATH = os.getenv("DB_PATH", "leadmaster.db")

def get_conn():
    """
    Return a SQLite connection (thread-safe).
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables(conn):
    """
    Create all required tables if they don't exist:
     • signals
     • clients
     • contacts
     • rss_cache
    """
    conn.execute("""
    CREATE TABLE IF NOT EXISTS signals (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      company       TEXT,
      date          TEXT,
      headline      TEXT,
      url           TEXT,
      source_label  TEXT,
      land_flag     INTEGER,
      sector_guess  TEXT,
      lat           REAL,
      lon           REAL,
      read_flag     INTEGER DEFAULT 0
    )""")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS clients (
      name         TEXT PRIMARY KEY,
      summary      TEXT,
      sector_tags  TEXT,
      status       TEXT,
      notes        TEXT
    )""")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS contacts (
      company  TEXT,
      name     TEXT,
      title    TEXT,
      email    TEXT,
      phone    TEXT,
      UNIQUE(company,name,title,email)
    )""")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS rss_cache (
      query  TEXT PRIMARY KEY,
      data   TEXT,
      ts     INTEGER
    )""")

    conn.commit()
