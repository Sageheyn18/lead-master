# app.py  –  Lead Master  v3
# Dashboard (instant lookup + grid)  •  Companies page (logo cards + profile)

import streamlit as st
import pandas as pd
import sqlite3
import json
import datetime
import feedparser                               # used in quick_lookup helper

from utils import get_conn, ensure_tables
from fetch_signals import gdelt_headlines, summarise  # reuse robot helpers

# ────────────────── page setup ──────────────────
st.set_page_config(page_title="Lead Master", layout="wide")

# simple page switcher
PAGE = st.sidebar.radio("Pages", ["Dashboard", "Companies"])

conn = get_conn()
ensure_tables(conn)

# ───────── cached client list (1 h) ─────────
@st.cache_data(ttl=3600)
def load_clients():
    df = pd.read_sql("SELECT * FROM clients", conn)
    df["sector_tags"] = df["sector_tags"].apply(json.loads)
    return df

clients = load_clients()

# ───────── instant lookup helper (same as before) ─────────
@st.cache_data(ttl=3600)
def quick_lookup(name: str):
    heads = gdelt_headlines(name)
    return summarise(name, heads) if heads else None

# ───────────────────────── Dashboard page ─────────────────────────
if PAGE == "Dashboard":
    st.title("Lead Master – Dashboard")

    # sidebar instant lookup
    st.sidebar.header("Look up a company")
    search = st.sidebar.text_input("Type a company name")

    if search and not (clients.name == search).any():
        info = quick_lookup(search)
        if info:
            st.sidebar.success(info["summary"])
            if st.sidebar.button("Save to library"):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO clients
                    (name, summary, sector_tags)
                    VALUES (?,?,?)
                    """,
                    (search, info["summary"], json.dumps([info.get("sector")])),
                )
                conn.commit()
                st.sidebar.success("Saved! Reloading …")
                st.cache_data.clear()   # clear client list cache
                st.rerun()
        else:
            st.sidebar.warning("No recent signals found.")

    # filters
    status_sel = st.sidebar.multiselect("Status", ["New", "Contacted", "Proposal", "Won", "Lost"])
    sector_sel = st.sidebar.multiselect(
        "Sector tag", sorted({t for tags in clients.sector_tags for t in tags})
    )
    overdue = st.sidebar.checkbox("Next-touch overdue")

    def filter_df(df):
        if search:
            df = df[
                df.name.str.contains(search, case=False)
                | df.summary.str.contains(search, case=False)
            ]
        if status_sel:
            df = df[df.status.isin(status_sel)]
        if sector_sel:
            df = df[df.sector_tags.apply(lambda ts: any(t in ts for t in sector_sel))]
        if overdue:
            df = df[pd.to_datetime(df.next_touch, errors="coerce") < pd.Timestamp.today()]
        return df

    filtered = filter_df(clients)

    st.subheader("Saved leads")
    st.dataframe(filtered[["name", "summary", "next_touch"]], height=320)

# ───────────────────────── Companies page ─────────────────────────
else:
    st.title("Saved Companies")

    # ---------- logo card grid ----------
    cols = st.columns(3)
    idx = 0
    for _, row in clients.iterrows():
        with cols[idx]:
            with st.container(border=True):
                if row.logo_url:
                    st.image(row.logo_url, width=96)
                st.markdown(f"### {row.name}")
                st.caption(row.summary or "—")
                if st.button("Open profile ▸", key=f"open_{row.name}"):
                    st.session_state["view_company"] = row.name
                    st.rerun()
        idx = (idx + 1) % 3

    # ---------- profile page ----------
    if "view_company" in st.session_state:
        target = st.session_state["view_company"]
        row = clients[clients.name == target].iloc[0]

        st.divider()
        st.header(f"📄 {target} – profile")

        # basic info block
        info_cols = st.columns([1, 3])
        if row.logo_url:
            info_cols[0].image(row.logo_url, width=120)
        with info_cols[1]:
            st.markdown(f"**Sector tags:** {', '.join(json.loads(row.sector_tags)) or '—'}")
            st.markdown(f"**HQ:** {row.hq_address or '—'}")
            st.markdown(f"**Phone:** {row.phone or '—'}")
            if row.website:
                st.markdown(f"**Website:** [{row.website}]({row.website})")

        # contacts
        contacts = json.loads(row.contacts)
        if contacts:
            st.subheader("Contacts")
            st.table(pd.json_normalize(contacts))

        # recent signals with [source] links
        sig = pd.read_sql(
            "SELECT date, headline, url FROM signals WHERE company=? ORDER BY date DESC",
            conn,
            params=(target,),
        )
        if not sig.empty:
            st.subheader("Recent Signals")
            for _, s in sig.iterrows():
                st.markdown(f"*{s.date}* – {s.headline}  [[source]({s.url})]")

        # notes
        new_notes = st.text_area("Notes", row.notes or "", height=120)
        if st.button("Save notes"):
            conn.execute("UPDATE clients SET notes=? WHERE name=?", (new_notes, target))
            conn.commit()
            st.success("Notes saved – reload to view.")
