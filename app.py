# app.py — Lead Master v3.2 (updated 2025-06-23)

import os
import json
import datetime
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium

from utils import get_conn, ensure_tables
from fetch_signals import (
    client,
    manual_search,
    national_scan,
    export_pdf,
    company_contacts,
    fetch_logo,
    gpt_summary,
    fetch_permits
)

# ───────── Page Setup ─────────
st.set_page_config(page_title="Lead Master", layout="wide")

# ───────── Sidebar ─────────
st.sidebar.title("Controls")

# Theme toggle
theme = st.sidebar.radio("Theme", ["Light", "Dark"], index=0)
if theme == "Dark":
    st.markdown(
        "<style>body {background-color: #333; color: #eee;} </style>",
        unsafe_allow_html=True
    )

# National scan button
if st.sidebar.button("Run national scan now"):
    with st.spinner("Scanning…"):
        national_scan()
    # force a rerun so new data appears:
    st.experimental_rerun()

# Heatmap toggle
heatmap_on = st.sidebar.checkbox("Heatmap overlay", value=False)

# Back‐to‐map button (when not already on Map)
if st.sidebar.button("Back to map"):
    st.session_state["page"] = "Map"

# Page selector
menu = st.sidebar.expander("Menu", expanded=True)
with menu:
    page = st.radio(
        "", 
        ["Map", "Companies", "Pipeline", "Permits"],
        index=["Map","Companies","Pipeline","Permits"]
        .index(st.session_state.get("page", "Map")),
        key="page"
    )
st.session_state["page"] = page

# Search company input (collapsed label to avoid warnings)
search_co = st.sidebar.text_input(
    "Search company", 
    key="search_co", 
    label_visibility="collapsed"
)
if st.sidebar.button("Go", key="go"):
    st.session_state["overlay"] = search_co
    st.experimental_rerun()

# Show current GPT spend
spent = client.usage.today().get("total_cents", 0)
st.sidebar.markdown(f"GPT spend: {spent}¢ / 300¢")

# ───────── Main Area ─────────
conn = get_conn()
ensure_tables(conn)

if st.session_state["page"] == "Map":
    st.title("Lead Master — Project Map")

    # If an overlay search is active
    if overlay := st.session_state.get("overlay"):
        info, heads, lat, lon = manual_search(overlay)
        if info:
            st.subheader(overlay)
            for b in info["summary"].split("\n"):
                if b.strip():
                    st.write("• " + b.strip())
            st.write(f"**Sector:** {info['sector']}  •  "
                     f"**Confidence:** {info['confidence']}")
            if st.button("Save to library"):
                # Insert as a new client
                conn.execute(
                    "INSERT OR IGNORE INTO clients(name,summary,sector_tags,status,lat,lon) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        overlay,
                        info["summary"],
                        json.dumps([info["sector"]]),
                        "New",
                        lat,
                        lon
                    )
                )
                conn.commit()
                st.success("Saved!")
        else:
            st.info("No signals found for that company.")

        if st.button("Close", key="close_overlay"):
            del st.session_state["overlay"]
            st.experimental_rerun()
        st.stop()

    # Otherwise show the full map
    clients = pd.read_sql("SELECT * FROM clients", conn)
    # Apply heatmap or markers
    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    if heatmap_on and not clients.empty:
        pts = clients[["lat","lon"]].dropna().values.tolist()
        folium.plugins.HeatMap(pts).add_to(m)

    for _, r in clients.iterrows():
        # >>> FIX: use bracket access instead of attribute <<<
        if r["lat"] is None or r["lon"] is None:
            continue
        folium.Marker(
            [r["lat"], r["lon"]],
            popup=(
                f"<b>{r['name']}</b><br>"
                f"{r['summary'][:120]}…<br>"
                f"<a href='https://maps.google.com/?q={r['lat']},{r['lon']}' target='_blank'>Map</a>"
            )
        ).add_to(m)

    st_folium(m, width=700, height=500)

elif st.session_state["page"] == "Companies":
    st.title("Companies")
    df = pd.read_sql("SELECT * FROM clients", conn)
    if df.empty:
        st.info("No companies yet. Save some from Map!")
    else:
        sel = st.selectbox("Select company", df["name"].tolist())
        row = df[df["name"] == sel].iloc[0]
        st.subheader(sel)
        st.markdown(row["summary"])
        st.write(f"**Sector tags:** {', '.join(json.loads(row['sector_tags']))}")
        st.write(f"**Status:** {row['status']}")
        st.write(f"**Location:** {row['lat']}, {row['lon']}")

        # Contacts
        contacts = pd.read_sql(
            "SELECT * FROM contacts WHERE company=?", conn, params=(sel,)
        )
        if not contacts.empty:
            st.markdown("**Key contacts:**")
            for _, c in contacts.iterrows():
                st.write(f"- {c['name']}, {c['title']}, {c['email']}, {c['phone']}")

        if st.button("Export profile PDF"):
            pdf_bytes = export_pdf(row, row["summary"], contacts.to_dict("records"))
            st.download_button("Download PDF", data=pdf_bytes, file_name=f"{sel}.pdf")

elif st.session_state["page"] == "Pipeline":
    st.title("Pipeline")
    # Kanban / Trello-style could go here
    st.info("No leads to show yet.")

elif st.session_state["page"] == "Permits":
    st.title("Permits")
    permits = fetch_permits()
    if not permits:
        st.info("No permits.csv found or no permits to display.")
    else:
        for p in permits:
            st.markdown(f"**{p['company']}** — {p['type']} — {p['date']}")
            st.write(p["address"])
            st.write(f"[Details]({p['details_url']})")

else:
    st.error("Unknown page selected!")
