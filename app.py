
import streamlit as st, pandas as pd, json, folium, datetime
from streamlit_folium import st_folium
from utils import get_conn, ensure_tables
from fetch_signals import summarise, gdelt_headlines
from pathlib import Path

st.set_page_config(page_title="Lead Master", layout="wide")
st.markdown("<style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap'); html, body, [class*='css']  {font-family: 'Inter', sans-serif;}</style>", unsafe_allow_html=True)
# color scheme
GOLD="#B7932F"; CHAR="#41413F"
st.markdown(f"<style>.css-18e3th9 {{background-color:#F9F9F9}} .stButton>button{{background:{GOLD};color:white;border:none;border-radius:4px;padding:6px 12px;font-weight:600}}</style>", unsafe_allow_html=True)

PAGE = st.sidebar.radio("Pages",["Map","Companies"])

conn=get_conn(); ensure_tables(conn)
clients=pd.read_sql("SELECT * FROM clients",conn)

if PAGE=="Map":
    st.title("Lead Master — Project Map")
    # filters
    sector=st.sidebar.multiselect("Sector tag", sorted({t for tags in clients.sector_tags.apply(json.loads) for t in tags}))
    today=datetime.date.today()
    start=st.sidebar.date_input("Start date", today-datetime.timedelta(days=30))
    end=st.sidebar.date_input("End date", today)
    df=clients.copy()
    if sector:
        df=df[df.sector_tags.apply(lambda s:any(t in json.loads(s) for t in sector))]
    sig=pd.read_sql("SELECT company,lat,lon,date FROM signals WHERE date BETWEEN ? AND ?",conn,params=(start.strftime("%Y%m%d"),end.strftime("%Y%m%d")))
    df=df.merge(sig.groupby("company").first().reset_index(),left_on="name",right_on="company",how="left")
    m=folium.Map(location=[37,-96],zoom_start=4,tiles="CartoDB Positron")
    for _,r in df.iterrows():
        if pd.notna(r.lon) and pd.notna(r.lat):
            folium.Marker(
                location=[r.lat,r.lon],
                popup=f"<b>{r.name}</b><br>{r.summary}",
                icon=folium.Icon(color='orange',icon='info-sign')
            ).add_to(m)
    st_folium(m,height=600)
else:
    st.title("Companies")
    for _,r in clients.iterrows():
        st.subheader(r.name)
        st.caption(r.summary or "—")
