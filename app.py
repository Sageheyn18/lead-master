# app.py — Lead Master UI v12.0

import streamlit as st
import folium
import pandas as pd
import json

from streamlit_folium import st_folium
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

st.set_page_config(page_title="Lead Master", layout="wide")

# ───────── SIDEBAR: Manual Search ─────────
st.sidebar.header("Search Company")
overlay_input = st.sidebar.text_input("Name")
if st.sidebar.button("Go", key="go"):
    st.session_state["overlay"] = overlay_input

# ───────── SIDEBAR: National Scan ─────────
if st.sidebar.button("Run national scan now"):
    with st.spinner("Scanning…"):
        national_scan()
    # Streamlit will auto‐rerun and show the sidebar success message

# ───────── MAIN RENDER ─────────
conn = get_conn()
ensure_tables(conn)
clients = pd.read_sql("SELECT * FROM clients", conn)

if "overlay" in st.session_state:
    company = st.session_state["overlay"]
    info, rows, lat, lon = manual_search(company)
    st.title(f"{company} — Overview")
    st.write("**Summary:**", info["summary"])
    st.write(f"**Sector:** {info['sector']} — **Confidence:** {info['confidence']}")
    st.write(f"**Land Flag:** {info['land_flag']}")
    # Details
    st.subheader("Signals")
    for r in rows:
        with st.expander(r["headline"]):
            st.write("•", r["summary"])
            st.write("[View article]("+r["url"]+")")
            if st.button(f"Save to {company}", key=r["headline"]):
                conn.execute(
                    "INSERT OR REPLACE INTO signals(company,headline,url,date) VALUES(?,?,?,?)",
                    (company, r["headline"], r["url"], r["date"])
                )
                conn.commit()
    if st.button("Back to map"):
        del st.session_state["overlay"]

else:
    # Map view of all clients
    st.title("Lead Master — Project Map")
    df = clients.dropna(subset=["lat","lon"])
    m  = folium.Map(location=[37,-96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in df.iterrows():
        folium.Marker([r.lat, r.lon], popup=r.name).add_to(m)
    st_folium(m, width=700, height=500)

    st.sidebar.header("Companies")
    choice = st.sidebar.selectbox("View company", clients["name"].tolist())
    if choice:
        info = clients[clients.name==choice].iloc[0]
        st.sidebar.subheader(choice)
        st.sidebar.write(info.summary)
        if st.sidebar.button("Export PDF"):
            contacts = company_contacts(choice)
            pdfdata  = export_pdf(info, info.summary, contacts)
            st.sidebar.download_button(
                "Download Proposal PDF", data=pdfdata,
                file_name=f"{choice}-proposal.pdf", mime="application/pdf"
            )

# ───────── PERMITS ─────────
permits = fetch_permits()
if permits:
    st.sidebar.header("Recent Permits")
    for p in permits:
        st.sidebar.write(f"{p['date']} — {p['company']} — {p['type']}")
