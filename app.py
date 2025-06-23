import json
import datetime
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium

from utils import get_conn, ensure_tables
from fetch_signals import (
    manual_search, national_scan, export_pdf
)

# ───────── App Setup ─────────
st.set_page_config(layout="wide")
ensure_tables()

# ───────── Sidebar ─────────
st.sidebar.title("Lead Master")
if st.sidebar.button("Map"):
    st.session_state.page = "map"
if st.sidebar.button("Companies"):
    st.session_state.page = "companies"
if st.sidebar.button("Pipeline"):
    st.session_state.page = "pipeline"
if st.sidebar.button("Permits"):
    st.session_state.page = "permits"

# Search company
search = st.sidebar.text_input("Search Company", key="search_co")
if st.sidebar.button("Go"):
    st.session_state.overlay = search

# National scan
if st.sidebar.button("Run national scan now"):
    national_scan()

# Heatmap toggle
heat = st.sidebar.checkbox("Heatmap overlay")

# ───────── Layout ─────────
main, sidebar_head = st.columns([3,1], gap="large")

# ───────── Latest Headlines Panel ─────────
with sidebar_head:
    st.header("Latest headlines")
    conn = get_conn()
    dfh = pd.read_sql("SELECT * FROM signals ORDER BY date DESC LIMIT 100", conn)
    for _,r in dfh.groupby("company"):
        comp = r.company.iloc[0]
        st.subheader(comp)
        for i,row in r.iterrows():
            dot = "🟢" if row.read==0 else "⚪"
            if st.button(f"{dot} {row.headline[:50]}…", key=row.id):
                st.session_state.sel_headline = row.id
    conn.close()

# ───────── Overlay (manual search) ─────────
if "overlay" in st.session_state:
    co = st.session_state.overlay
    summ, rows, lat, lon = manual_search(co)
    main.title(f"{co} — Details")
    # summary
    main.subheader("Executive summary")
    for b in summ.get("summary","").split("\n"):
        main.write(f"• {b.strip()}")
    main.write(f"**Sector:** {summ.get('sector','')} | **Confidence:** {summ.get('confidence',0)}")
    # map
    if lat and lon:
        m = folium.Map(location=[lat,lon], zoom_start=10)
        folium.Marker([lat,lon], popup=co).add_to(m)
        st_folium(m, width=700, height=400)
    # headlines list & save
    main.subheader("Headlines (tick to add)")
    sel = main.multiselect("Select headlines to save", rows, format_func=lambda x: x["headline"])
    tag = main.selectbox("Sector tag", [summ.get("sector","")])
    if main.button("Save selected"):
        conn = get_conn()
        c = conn.cursor()
        for h in sel:
            c.execute("""
                INSERT OR REPLACE INTO signals(company,headline,url,date,score,read)
                VALUES(?,?,?,?,?,0)
            """,(co, h["headline"], h["url"], h["date"], summ.get("confidence",0)))
        # Upsert client
        c.execute("""
            INSERT OR REPLACE INTO clients(name,summary,sector_tags,status,lat,lon,contacts)
            VALUES(?,?,?,?,?,?,?)
        """,(
            co, summ.get("summary",""),
            json.dumps([summ.get("sector","")]),
            "New", lat, lon, json.dumps({})
        ))
        conn.commit()
        conn.close()
        st.success("Saved!")
    if main.button("Back to map"):
        del st.session_state.overlay

# ───────── Headline Detail & Export ─────────
if "sel_headline" in st.session_state:
    hid = st.session_state.sel_headline
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT company,headline,score,contacts FROM signals WHERE id=?", (hid,))
    comp, head, score, contacts = cur.fetchone()
    contacts = json.loads(contacts or "{}")
    # mark read
    cur.execute("UPDATE signals SET read=1 WHERE id=?", (hid,))
    conn.commit()
    conn.close()

    main.header(head)
    main.write(f"**Score:** {score}")
    # export PDF
    if main.button("Export to PDF"):
        path = export_pdf(comp, head, summarise(comp, [{"headline":head}])["summary"], contacts)
        main.success(f"PDF saved to `{path}`")

# ───────── Pages ─────────
page = st.session_state.get("page","map")

if page=="map":
    main.title("Lead Master — Project Map")
    conn = get_conn()
    dfc = pd.read_sql("SELECT * FROM clients", conn)
    dfs = pd.read_sql("SELECT company,lat,lon,date FROM signals", conn)
    conn.close()
    df = dfc.merge(dfs.groupby("company").first().reset_index(), on="name", how="inner")
    # map
    m = folium.Map(location=[37,-96], zoom_start=4, tiles="CartoDB Positron")
    if heat:
        pts = df[["lat","lon"]].dropna().values.tolist()
        folium.plugins.HeatMap(pts).add_to(m)
    for _,r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat,r.lon],
                popup=(f"<b>{r.name}</b><br>{r.summary[:120]}...<br>"
                       f"<a href='{r.url}' target='_blank'>View article</a>")
            ).add_to(m)
    st_folium(m, width=700, height=500)

elif page=="companies":
    main.title("Companies")
    conn = get_conn()
    df = pd.read_sql("SELECT name,summary,sector_tags,status FROM clients", conn)
    conn.close()
    if df.empty:
        main.info("No companies yet.")
    else:
        sel = main.selectbox("View company", df.name.tolist())
        row = df[df.name==sel].iloc[0]
        main.subheader(sel)
        main.write(f"**Summary:** {row.summary}")
        main.write(f"**Sector tags:** {json.loads(row.sector_tags)}")
        main.write(f"**Status:** {row.status}")

elif page=="pipeline":
    main.title("Pipeline")
    main.info("No leads to show.")

elif page=="permits":
    main.title("Permits")
    dfp = pd.read_csv(utils.PERMITS_CSV)
    # Filter for keywords
    kw = ["land","build","construction","expansion","site"]
    dfp = dfp[dfp.Description.str.contains("|".join(kw), case=False, na=False)]
    main.dataframe(dfp)

