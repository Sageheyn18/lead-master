# app.py – Lead Master  v4.1
import json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium
from utils import get_conn, ensure_tables
from fetch_signals import manual_search, national_scan

# ───────── theme ----------
GOLD = "#B7932F"; CHAR = "#41413F"
st.set_page_config(page_title="Lead Master", layout="wide")
st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    html, body, [class*="css"] {{font-family:'Inter',sans-serif}}
    .stButton>button {{background:{GOLD};color:#fff;border:none;padding:6px 12px;border-radius:4px;font-weight:600}}
    .news-dot {{height:10px;width:10px;background:{GOLD};border-radius:50%;display:inline-block;margin-right:6px}}
    </style>""",
    unsafe_allow_html=True,
)

# ───────── DB ----------
conn = get_conn(); ensure_tables(conn)
def load_clients(): return pd.read_sql("SELECT * FROM clients", conn)

# ───────── sidebar ----------
st.sidebar.header("Controls")
if st.sidebar.button("Run national scan now"):
    with st.spinner("Scanning…"):
        national_scan()
    st.experimental_rerun()

budget = st.sidebar.empty()

PAGE = st.sidebar.radio("Pages",["Map","Companies"])

# ───────── right-hand News bar ----------
st.sidebar.header("Latest headlines")
news_df = pd.read_sql(
    "SELECT id, headline, company, read_flag FROM signals ORDER BY date DESC LIMIT 100",
    conn,
)
for _, r in news_df.iterrows():
    dot = "" if r.read_flag else "<span class='news-dot'></span>"
    if st.sidebar.markdown(f"{dot}{r.headline[:60]}…", unsafe_allow_html=True):
        st.session_state["view_headline"] = int(r.id)

# ───────── search overlay ----------
search_q = st.sidebar.text_input("Search company")
if search_q and st.sidebar.button("Go"):
    st.session_state["overlay"] = search_q

def search_overlay(name: str):
    info, heads, lat, lon = manual_search(name)
    st.markdown(f"### {name}")
    st.write(info["summary"])
    if st.button("Save to library"):
        conn.execute(
            "INSERT OR IGNORE INTO clients (name, summary, sector_tags, status) VALUES (?,?,?, 'New')",
            (name, info["summary"], json.dumps([info["sector"]])),
        )
        for h in heads:
            conn.execute(
                "INSERT INTO signals (company,date,headline,url,source_label,land_flag,sector_guess,lat,lon) VALUES (?,?,?,?,?,?,?,?,?)",
                (name, datetime.date.today().strftime("%Y%m%d"), h,"", "search", info["land_flag"], info["sector"], lat, lon)
            )
        conn.commit()
        st.success("Saved.")
    st.divider()
    st.write("#### Headlines (5 mo)")
    for h in heads:
        st.markdown(f"- {h}")
    if st.button("Close"):
        st.session_state.pop("overlay", None)
        st.experimental_rerun()

# ───────── Main content ----------
if "overlay" in st.session_state:
    search_overlay(st.session_state["overlay"])

elif PAGE=="Map":
    st.title("Lead Master — Project Map")
    clients = load_clients()
    sector = st.sidebar.multiselect(
        "Sector tag",
        sorted({t for tags in clients.sector_tags.apply(json.loads) for t in tags}),
    )
    today = datetime.date.today()
    start = st.sidebar.date_input("Start", today - datetime.timedelta(days=150))
    end   = st.sidebar.date_input("End", today)

    sig = pd.read_sql(
        "SELECT company,max(date) as date,lat,lon FROM signals WHERE date BETWEEN ? AND ? GROUP BY company",
        conn, params=(start.strftime("%Y%m%d"), end.strftime("%Y%m%d")),
    )

    df = clients.merge(sig, left_on="name", right_on="company", how="inner")
    if sector:
        df = df[df.sector_tags.apply(lambda s:any(t in json.loads(s) for t in sector))]

    m = folium.Map(location=[37,-96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat, r.lon],
                popup=f"<b>{r.name}</b><br>{r.summary}",
                icon=folium.Icon(color="orange", icon="info-sign"),
            ).add_to(m)
    st_folium(m, height=600)

else:  # Companies page
    st.title("Companies")

    sort_by = st.selectbox("Sort by",["Alphabetical","Status","Sector tag","Next-touch"])
    clients = load_clients()

    if sort_by == "Alphabetical":
        clients = clients.sort_values("name")
    elif sort_by == "Status":
        clients = clients.sort_values("status")
    elif sort_by == "Sector tag":
        clients["first_sector"] = clients.sector_tags.apply(lambda s: json.loads(s)[0] if json.loads(s) else "")
        clients = clients.sort_values("first_sector")
    else:
        clients = clients.sort_values("next_touch", na_position="last")

    for _, r in clients.iterrows():
        st.subheader(r.name)
        st.caption(r.summary or "—")

# ---------- budget display ----------
from fetch_signals import BUDGET_USED, DAILY_BUDGET
budget.markdown(f"**GPT spend today:** {BUDGET_USED:.1f} ¢ / {DAILY_BUDGET} ¢")
