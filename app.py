import streamlit as st
import pandas as pd
import folium
from pathlib import Path
from streamlit_folium import st_folium

from utils import get_conn, ensure_tables
from fetch_signals import (
    manual_search,
    national_scan,
    company_contacts,
    export_pdf,
)

# ───────── App Setup ─────────
st.set_page_config(layout="wide")
ensure_tables()

# ───────── Sidebar ─────────
st.sidebar.title("Lead Master")

# 1) Manual company lookup overlay
search_co = st.sidebar.text_input("Search Company")
if st.sidebar.button("Go"):
    st.session_state["overlay"] = search_co

# 2) National scan trigger
if st.sidebar.button("Run national scan now"):
    national_scan()

# 3) Main view selector
page = st.sidebar.selectbox("View", ["Map", "Companies", "Pipeline", "Permits"])

# ───────── Main Content ─────────
if page == "Map":
    st.header("Lead Master — Project Map")
    conn = get_conn()
    df_clients = pd.read_sql("SELECT * FROM clients", conn)
    df_signals = pd.read_sql("SELECT company, lat, lon, date FROM signals", conn)
    conn.close()

    # unify column names for merge
    df_signals = df_signals.rename(columns={"company": "name"})
    merged = (
        df_clients
        .merge(df_signals.groupby("name").first().reset_index(),
               on="name", how="inner")
    )

    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    for _, row in merged.iterrows():
        lat, lon = row["lat"], row["lon"]
        if pd.notna(lat) and pd.notna(lon):
            folium.Marker(
                [lat, lon],
                popup=folium.Popup(f"<b>{row['name']}</b><br>{row['summary']}",
                                   max_width=250)
            ).add_to(m)

    st_folium(m, width=700, height=500)

elif page == "Companies":
    st.header("Companies")
    conn = get_conn()
    df_clients = pd.read_sql("SELECT * FROM clients", conn)
    conn.close()

    if df_clients.empty:
        st.info("No companies yet. Run a national scan or do a manual lookup.")
    else:
        company = st.selectbox("Select company", df_clients["name"].tolist())
        if company:
            data = df_clients.set_index("name").loc[company]
            st.subheader(company)
            st.markdown(f"**Summary:** {data['summary']}")
            st.markdown(f"**Sector tags:** {data['sector_tags']}")

            conn = get_conn()
            df_sigs = pd.read_sql(
                "SELECT headline, url, date FROM signals WHERE company=?",
                conn,
                params=(company,),
            )
            conn.close()

            st.markdown("**Headlines:**")
            for idx, sig in df_sigs.iterrows():
                with st.expander(sig["headline"]):
                    st.write(f"Date: {sig['date']}")
                    st.markdown(f"[Read Article]({sig['url']})")
                    contacts = company_contacts(company)
                    st.markdown("**Contacts:**")
                    for role, val in contacts.items():
                        st.write(f"- {role.title()}: {val or 'N/A'}")
                    if st.button("Export as PDF", key=f"pdf_{idx}"):
                        pdf_path = export_pdf(company, sig["headline"], contacts)
                        st.success(f"PDF saved to {pdf_path}")

elif page == "Pipeline":
    st.header("Pipeline")
    conn = get_conn()
    df_pipeline = pd.read_sql("SELECT * FROM clients", conn)
    conn.close()
    st.dataframe(df_pipeline)

elif page == "Permits":
    st.header("Permits")
    permits_file = Path(__file__).parent / "data" / "permits.csv"
    if permits_file.exists():
        df_permits = pd.read_csv(permits_file)
        st.dataframe(df_permits)
    else:
        st.info("No permits.csv found in data/ folder.")

# ───────── Overlay (Manual Search) ─────────
if "overlay" in st.session_state:
    company = st.session_state.pop("overlay")
    summary, rows, lat, lon = manual_search(company)

    st.subheader(f"{company} — Overview")
    st.markdown("**Summary:**")
    raw = summary.get("summary", "")
    if isinstance(raw, list):
        lines = raw
    else:
        lines = raw.split("\n")
    for line in lines:
        if line.strip():
            st.write(f"- {line.strip()}")

    st.markdown(
        f"**Sector:** {summary.get('sector','unknown')} | "
        f"**Confidence:** {summary.get('confidence',0)}"
    )

    st.markdown("**Headlines (tick to save):**")
    if rows:
        choices = [h["headline"] for h in rows]
        selected = st.multiselect("Pick headlines to save", choices)
        if st.button("Save selected"):
            conn = get_conn()
            for h in rows:
                if h["headline"] in selected:
                    conn.execute(
                        "INSERT OR IGNORE INTO signals(company,headline,url,date,lat,lon) VALUES (?,?,?,?,?,?)",
                        (company, h["headline"], h["url"], h["date"], lat, lon),
                    )
            conn.commit()
            conn.close()
            st.success("Saved!")

