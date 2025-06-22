# utils.py
import os, sqlite3

DB_PATH = os.path.join(os.getcwd(), "leadmaster.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return conn

def ensure_tables(conn):
    # Clients with HQ lat/lon
    conn.execute("""
    CREATE TABLE IF NOT EXISTS clients(
      name TEXT PRIMARY KEY,
      summary TEXT,
      sector_tags TEXT,
      status TEXT,
      lat REAL,
      lon REAL
    )
    """)
    # Signals (individual headlines)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company TEXT,
      date TEXT,
      headline TEXT,
      url TEXT,
      source_label TEXT,
      land_flag INTEGER,
      sector_guess TEXT,
      lat REAL,
      lon REAL
    )
    """)
    # Contacts
    conn.execute("""
    CREATE TABLE IF NOT EXISTS contacts(
      company TEXT,
      name TEXT,
      title TEXT,
      email TEXT,
      phone TEXT,
      UNIQUE(company,name,title,email)
    )
    """)
    conn.commit()
