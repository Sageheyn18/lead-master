# app.py
import datetime
import json

import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium

from utils import get_conn, ensure_tables
from fetch_signals import (
    manual_search,
    national_scan,
    company_contacts,
    fetch_logo,
    gpt_summary,
)

# ───────── page config ─────────
st.set_page_config(page_title="Lead Master", layout="wide", initial_sidebar_state="expanded")

# ensure our SQLite tables exist
conn = get_conn()
ensure_tables(conn)

# ───────── session state defaults ─────────
if "overlay" not in st.session_state:
    st.session_state.overlay = None
if "page" not in st.session_state:
    st.session_state.page = "Map"

# ───────── sidebar ─────────
with st.sidebar:
    st.title("Search Company")
    co = st.text_input("Name", key="search_co")
    if st.button("Go", key="go"):
        st.session_state.overlay = co.strip()
    if st.session_state.overlay:
        if st.button("Back to map", key="back"):
            st.session_state.overlay = None

    st.markdown("---")
    if st.button("Run national scan now"):
        with st.spinner("Scanning… this can take a minute…"):
            national_scan()
        st.experimental_rerun()

    heat = st.checkbox("Heatmap overlay")
    st.markdown("---")
    st.subheader("View")
    page = st.selectbox("", ["Map", "Companies", "Pipeline"], index=["Map", "Companies", "Pipeline"].index(st.session_state.page))
    st.session_state.page = page

# ───────── search overlay ─────────
if st.session_state.overlay:
    company = st.session_state.overlay
    info, rows, lat, lon = manual_search(company)
    st.title(f"{company} — Overview")
    st.subheader("Executive summary")
    for bullet in info["summary"].split("•"):
        b = bullet.strip()
        if b:
            st.write("• " + b)
    st.write(f"**Sector:** {info['sector']}  •  **Confidence:** {info['confidence']:.2f}")
    st.subheader("Headlines (tick to save)")
    sel = st.multiselect("Select which headlines to add", [r["headline"] for r in rows])
    tag = st.selectbox("Sector tag", ["Manufacturing","Industrial","Retail","Logistics","Energy","Other"])
    if st.button("Save selected"):
        for h in rows:
            if h["headline"] in sel:
                conn.execute(
                    "INSERT OR REPLACE INTO signals(company,headline,url,date) VALUES(?,?,?,?)",
                    (company, h["headline"], h["url"], h["date"])
                )
        conn.commit()
        st.success("Saved!")
    st.stop()

# ───────── Map page ─────────
if page == "Map":
    st.title("Lead Master — Project Map")
    clients = pd.read_sql("SELECT * FROM clients", conn)
    signals = pd.read_sql("SELECT company,lat,lon,date FROM signals WHERE date>=date('now','-5 months')", conn)
    df = clients.merge(
        signals.groupby("company").first().reset_index(),
        on="company", how="inner"
    )

    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    if heat and not df.empty:
        from folium.plugins import HeatMap
        pts = df[["lat","lon"]].dropna().values.tolist()
        HeatMap(pts).add_to(m)

    for _, r in df.iterrows():
        folium.Marker(
            [r["lat"], r["lon"]],
            popup=(
                f"<b>{r['company']}</b><br>"
                f"{r['summary'][:120]}…<br>"
                f"<a href='{r['url']}' target='_blank'>Read more</a>"
            )
        ).add_to(m)

    st_folium(m, width=700, height=500)

# ───────── Companies page ─────────
elif page == "Companies":
    st.title("Companies")
    clients = pd.read_sql("SELECT * FROM clients", conn)
    if clients.empty:
        st.info("No companies yet.")
    else:
        for _, c in clients.iterrows():
            logo = fetch_logo(c["company"])
            cols = st.columns([1,3,1])
            with cols[0]:
                if logo: st.image(logo, width=60)
            with cols[1]:
                st.subheader(c["company"])
                st.write(c["summary"])
                st.write("• Sector:", ", ".join(json.loads(c["sector_tags"])))
            with cols[2]:
                if st.button("View details", key=f"view_{c['company']}"):
                    st.session_state.overlay = c["company"]
                    st.session_state.page = "Map"
                    st.experimental_rerun()

# ───────── Pipeline page ─────────
elif page == "Pipeline":
    st.title("Pipeline")
    pipeline = pd.read_sql("SELECT * FROM pipeline", conn)
    if pipeline.empty:
        st.info("No pipeline entries yet.")
    else:
        st.dataframe(pipeline)

# ───────── utilities ─────────
st.markdown("---")
st.caption("© Sage's Lead Master")
