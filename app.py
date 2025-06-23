# app.py — Lead Master v3.5 (2025-06-23)

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
    st.experimental_rerun()

# Heatmap toggle
heatmap_on = st.sidebar.checkbox("Heatmap overlay", value=False)

# Back-to-map button
if st.sidebar.button("Back to map"):
    st.session_state.pop("overlay", None)
    st.experimental_rerun()

# Search company input (collapsed label)
overlay_input = st.sidebar.text_input(
    "Search company", key="overlay_input", label_visibility="collapsed"
)
if st.sidebar.button("Go", key="go"):
    st.session_state["overlay"] = overlay_input
    st.experimental_rerun()

# (Removed GPT spend display — client.usage isn’t available)

# ───────── Page Selector ─────────
page = st.sidebar.radio(
    "Pages",
    ["Map", "Companies", "Pipeline", "Permits"],
    index=["Map", "Companies", "Pipeline", "Permits"]
          .index(st.session_state.get("page", "Map")),
    key="page"
)

# ───────── Main Content ─────────
conn = get_conn()
ensure_tables(conn)

if page == "Map":
    st.title("Lead Master — Project Map")

    # Overlay search
    if overlay := st.session_state.get("overlay"):
        info, heads, lat, lon = manual_search(overlay)
        if info:
            st.subheader(overlay)
            for line in info["summary"].split("\n"):
                if line.strip():
                    st.write("• " + line.strip())
            st.write(f"**Sector:** {info['sector']}  •  "
                     f"**Confidence:** {info['confidence']}")
            if st.button("Save to library"):
                conn.execute(
                    "INSERT OR IGNORE INTO clients "
                    "(name,summary,sector_tags,status,lat,lon) "
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

        if st.button("Close"):
            st.session_state.pop("overlay", None)
            st.experimental_rerun()
        st.stop()

    # Draw the map
    clients = pd.read_sql("SELECT * FROM clients", conn)
    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")

    if heatmap_on and not clients.empty:
        pts = clients[["lat", "lon"]].dropna().values.tolist()
        folium.plugins.HeatMap(pts).add_to(m)

    for _, r in clients.iterrows():
        if pd.isna(r["lat"]) or pd.isna(r["lon"]):
            continue
        folium.Marker(
            [r["lat"], r["lon"]],
            popup=(
                f"<b>{r['name']}</b><br>"
                f"{r['summary'][:120]}…<br>"
                f"<a href='https://maps.google.com/?q={r['lat']},{r['lon']}' "
                f"target='_blank'>Map</a>"
            )
        ).add_to(m)

    st_folium(m, width=700, height=500)

elif page == "Companies":
    st.title("Companies")
    df = pd.read_sql("SELECT * FROM clients", conn)
    if df.empty:
        st.info("No companies yet. Use Map to add some.")
    else:
        sel = st.selectbox("Select company", df["name"].tolist())
        row = df[df["name"] == sel].iloc[0]

        st.subheader(sel)
        st.markdown(row["summary"])
        tags = json.loads(row["sector_tags"])
        st.write(f"**Sector tags:** {', '.join(tags)}")
        st.write(f"**Status:** {row['status']}")
        st.write(f"**Location:** {row['lat']}, {row['lon']}")

        contacts = pd.read_sql(
            "SELECT * FROM contacts WHERE company=?", 
            conn, params=(sel,)
        )
        if not contacts.empty:
            st.markdown("**Key contacts:**")
            for _, c in contacts.iterrows():
                st.write(
                    f"- {c['name']} ({c['title']}), "
                    f"{c['email']}, {c['phone']}"
                )

        if st.button("Export profile PDF"):
            pdf_data = export_pdf(
                {"company": sel, "headline": "", "url": ""},
                row["summary"],
                contacts.to_dict("records")
            )
            st.download_button(
                "Download PDF", pdf_data, file_name=f"{sel}.pdf"
            )

elif page == "Pipeline":
    st.title("Pipeline")
    st.info("No leads in pipeline yet.")

elif page == "Permits":
    st.title("Permits")
    permits = fetch_permits()
    if not permits:
        st.info("No permits.csv found or no relevant permits.")
    else:
        for p in permits:
            st.markdown(f"**{p['company']}** — {p['type']} — {p['date']}")
            st.write(p["address"])
            st.write(f"[Details]({p['details_url']})")

else:
    st.error("Unknown page!")
