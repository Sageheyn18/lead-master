import streamlit as st
import pandas as pd
import json, datetime
from utils import get_conn, ensure_tables
from fetch_signals import gdelt_headlines, summarise  # re-use robot helpers

# ────────────────── setup ──────────────────
st.set_page_config(page_title="Lead Master", layout="wide")
st.title("Lead Master")

conn = get_conn()
ensure_tables(conn)

# cache the full client list for 1 hour
@st.cache_data(ttl=3600)
def load_clients():
    df = pd.read_sql("SELECT * FROM clients", conn)
    df["sector_tags"] = df["sector_tags"].apply(json.loads)
    return df

clients = load_clients()

# ────────────────── instant lookup helper ──────────────────
@st.cache_data(ttl=3600)
def quick_lookup(name: str):
    """Pull yesterday’s headlines and GPT summary for a brand-new company."""
    heads = gdelt_headlines(name)
    return summarise(name, heads) if heads else None

# ────────────────── sidebar ──────────────────
st.sidebar.header("Look up or filter")
search = st.sidebar.text_input("Type a company name")

# if the name is NOT already in our table → do a quick lookup
if search and not (clients.name == search).any():
    info = quick_lookup(search)
    if info:
        st.sidebar.success(info["summary"])
        if st.sidebar.button("Save to library"):
            conn.execute(
                "INSERT OR IGNORE INTO clients(name, last_signal, sector_tags) VALUES(?,?,?)",
                (search, info["summary"], json.dumps([info.get("sector")])),
            )
            conn.commit()
            st.sidebar.success("Saved! Reloading…")
            st.rerun()
    else:
        st.sidebar.warning("No recent signals found for that name.")

# regular filters
status_sel = st.sidebar.multiselect("Status", ["New","Contacted","Proposal","Won","Lost"])
sector_sel = st.sidebar.multiselect(
    "Sector tag", sorted({t for tags in clients.sector_tags for t in tags})
)
overdue = st.sidebar.checkbox("Next-touch overdue")

def filter_df(df):
    if search:
        df = df[df.name.str.contains(search, case=False) | df.last_signal.str.contains(search, case=False)]
    if status_sel:
        df = df[df.status.isin(status_sel)]
    if sector_sel:
        df = df[df.sector_tags.apply(lambda ts: any(t in ts for t in sector_sel))]
    if overdue:
        df = df[pd.to_datetime(df.next_touch, errors='coerce') < pd.Timestamp.today()]
    return df

filtered = filter_df(clients)

# ────────────────── main grid ──────────────────
st.dataframe(filtered[["name","last_signal","next_touch"]], height=300)

# ────────────────── detail panel ──────────────────
sel = st.selectbox("Select a customer", filtered.name) if not filtered.empty else None
if sel:
    row = clients[clients.name == sel].iloc[0]
    st.subheader(sel)

    hist = pd.read_sql(
        "SELECT date, headline FROM signals WHERE company=? ORDER BY date DESC",
        conn, params=(sel,)
    )
    st.table(hist)

    coords = pd.read_sql(
        "SELECT lat, lon FROM signals WHERE company=? AND lat IS NOT NULL",
        conn, params=(sel,)
    )
    if not coords.empty:
        st.map(coords)

    new_notes = st.text_area("Notes", row.notes or "")
    new_status = st.selectbox(
        "Status",
        ["New","Contacted","Proposal","Won","Lost"],
        index=["New","Contacted","Proposal","Won","Lost"].index(row.status)
    )
    new_tags = st.text_input("Sector tags (comma-separated)", ", ".join(row.sector_tags))

    if st.button("Save changes"):
        conn.execute(
            "UPDATE clients SET notes=?, status=?, sector_tags=? WHERE name=?",
            (
                new_notes,
                new_status,
                json.dumps([t.strip() for t in new_tags.split(",") if t.strip()]),
                sel,
            ),
        )
        conn.commit()
        st.success("Saved – reload to view.")
