# app.py  – Lead Master (map + search overlay + news bar)

import streamlit as st
import pandas as pd, datetime, json, folium
from streamlit_folium import st_folium

from utils import get_conn, ensure_tables
from fetch_signals import summarise, headlines_for_company, national_scan

# ---------- styling ----------
GOLD = "#B7932F"; CHAR = "#41413F"
st.set_page_config(page_title="Lead Master", layout="wide")
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    html, body, [class*="css"] {font-family: 'Inter', sans-serif;}
    .stButton button {background-color:%s;color:white;border:none;border-radius:4px;padding:6px 12px;font-weight:600}
    .news-dot {height:10px;width:10px;background:%s;border-radius:50%%;display:inline-block;margin-right:6px}
    </style>""" % (GOLD, GOLD),
    unsafe_allow_html=True,
)

# ---------- DB ----------
conn = get_conn(); ensure_tables(conn)
clients = pd.read_sql("SELECT * FROM clients", conn)

# ---------- sidebar ----------
st.sidebar.header("Controls")
if st.sidebar.button("Run national scan now"):
    with st.spinner("Scanning…"):
        national_scan()
    st.experimental_rerun()

PAGE = st.sidebar.radio("Pages", ["Map", "Companies"])

# ---------- right-hand News bar ----------
st.sidebar.header("Latest headlines")
news_df = pd.read_sql("SELECT id, headline, company, read_flag FROM signals ORDER BY date DESC LIMIT 100", conn)
for _, r in news_df.iterrows():
    dot = "" if r.read_flag else "<span class='news-dot'></span>"
    if st.sidebar.markdown(f"{dot}{r.headline[:60]}…", unsafe_allow_html=True):
        st.session_state["view_headline"] = int(r.id)

# ---------- Search overlay ----------
def search_overlay(name: str):
    heads = headlines_for_company(name)
    info  = summarise(name, heads)
    st.markdown("### " + name)
    st.write(info["summary"])
    if st.button("Save to library"):
        conn.execute(
            "INSERT OR IGNORE INTO clients (name, summary, sector_tags, status) VALUES (?,?,?, 'New')",
            (name, info["summary"], json.dumps([info["sector"]])),
        )
        conn.commit()
    st.divider()
    st.write("#### Related headlines (last 5 months)")
    for h in heads:
        st.markdown(f"- {h['title']}  [[source]({h['url']})]")
    if st.button("Close"):
        st.session_state.pop("overlay", None)

search_query = st.sidebar.text_input("Search company (auto list)")
if search_query and st.sidebar.button("Go"):
    st.session_state["overlay"] = search_query

# ---------- Main --------------------
if "overlay" in st.session_state:
    search_overlay(st.session_state["overlay"])

elif PAGE == "Map":
    st.title("Lead Master — Project Map")
    sector = st.sidebar.multiselect(
        "Sector tag",
        sorted({t for tags in clients.sector_tags.apply(json.loads) for t in tags}),
    )
    today = datetime.date.today()
    start = st.sidebar.date_input("Start date", today - datetime.timedelta(days=150))
    end   = st.sidebar.date_input("End date",   today)

    df = clients.copy()
    if sector:
        df = df[df.sector_tags.apply(lambda s: any(t in json.loads(s) for t in sector))]

    sig = pd.read_sql(
        "SELECT company, lat, lon, date FROM signals WHERE date BETWEEN ? AND ?",
        conn,
        params=(start.strftime("%Y%m%d"), end.strftime("%Y%m%d")),
    )
    df = df.merge(sig.groupby("company").first().reset_index(), left_on="name", right_on="company", how="left")

    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat, r.lon],
                popup=f"<b>{r.name}</b><br>{r.summary}",
                icon=folium.Icon(color="orange", icon="info-sign"),
            ).add_to(m)
    st_folium(m, height=600)

else:
    st.title("Companies")
    for _, r in clients.iterrows():
        st.subheader(r.name)
        st.caption(r.summary or "—")
