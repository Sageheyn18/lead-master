# app.py  – Lead Master (2025-06-19 R-panel + PDF)

import json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium
from utils import get_conn, ensure_tables
from fetch_signals import (
    manual_search, national_scan,
    export_pdf, company_contacts,
    BUDGET_USED, DAILY_BUDGET,
)

# universal rerun (works on any Streamlit version)
def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        from streamlit.runtime.scriptrunner import RerunException, get_script_run_ctx
        raise RerunException(get_script_run_ctx())

# ───────── theme ─────────
GOLD = "#B7932F"; CHAR = "#41413F"
st.set_page_config(page_title="Lead Master", layout="wide")
st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    html, body, [class*="css"] {{font-family:'Inter',sans-serif}}
    .stButton>button {{background:{GOLD};color:#fff;border:none;
                       padding:6px 12px;border-radius:4px;font-weight:600}}
    .news-dot {{height:10px;width:10px;background:{GOLD};border-radius:50%;
               display:inline-block;margin-right:6px}}
    </style>""",
    unsafe_allow_html=True,
)

# ───────── DB ─────────
conn = get_conn(); ensure_tables(conn)
def load_clients(): return pd.read_sql("SELECT * FROM clients", conn)

# ───────── sidebar (left) ─────────
with st.sidebar:
    st.header("Controls")
    if st.button("Run national scan now"):
        with st.spinner("Scanning…"):
            national_scan()
        _rerun()

    if "overlay" in st.session_state and st.button("Back to map"):
        st.session_state.pop("overlay"); _rerun()

    PAGE = st.radio("Pages", ["Map", "Companies"])

    search_q = st.text_input("Search company")
    if search_q and st.button("Go"):
        st.session_state["overlay"] = search_q

    st.write(f"**GPT spend today:** {BUDGET_USED:.1f} ¢ / {DAILY_BUDGET} ¢")

# ───────── layout 75 / 25 ─────────
left, right = st.columns([0.75, 0.25], gap="medium")

# ───────── right-hand news panel ─────────
with right:
    st.header("Latest headlines")
    news_df = pd.read_sql(
        """SELECT id, company, headline, url, date, source_label,
                  land_flag, sector_guess, read_flag
           FROM signals ORDER BY date DESC LIMIT 100""",
        conn)
    sum_by_co = pd.read_sql("SELECT name, summary FROM clients", conn)\
                  .set_index("name")["summary"].to_dict()

    if news_df.empty:
        st.info("No headlines yet — run a scan.")
    else:
        for _, r in news_df.iterrows():
            dot = "" if r.read_flag else "<span class='news-dot'></span>"
            head = f"{dot}{r.headline[:60]}…"
            with st.expander(head, expanded=False):
                st.markdown(f"**{r.headline}**")
                st.write(f"Date : {r.date}")
                st.write(f"Source : {r.source_label}")
                if r.land_flag:
                    st.write("🏷️ **Land / new site**")
                st.write(f"Sector : {r.sector_guess or '–'}")
                st.markdown(f"[Open article]({r.url})", unsafe_allow_html=True)
                st.write("---")
                st.write("**Company summary**")
                st.write(sum_by_co.get(r.company, '—'))

                # Export PDF
                if st.button("Export PDF", key=f"pdf{r.id}"):
                    pdf_bytes = export_pdf(
                        r._asdict(),
                        sum_by_co.get(r.company, r.headline),
                        company_contacts(r.company)
                    )
                    st.download_button(
                        "Download PDF",
                        data=pdf_bytes,
                        file_name=f"{r.company}_{r.date}.pdf",
                        mime="application/pdf",
                        key=f"dl{r.id}")

                # Mark read toggle
                if not r.read_flag:
                    if st.button("Mark read", key=f"mark{r.id}"):
                        conn.execute("UPDATE signals SET read_flag=1 WHERE id=?",
                                     (int(r.id),)); conn.commit(); _rerun()

# ───────── helper: overlay view ─────────
def search_overlay(company: str):
    info, heads, lat, lon = manual_search(company)
    left.markdown(f"### {company}")
    left.write(info["summary"])
    if left.button("Save to library"):
        conn.execute(
            "INSERT OR IGNORE INTO clients (name, summary, sector_tags, status)"
            " VALUES (?,?,?, 'New')",
            (company, info["summary"], json.dumps([info["sector"]]))
        )
        for h in heads:
            conn.execute(
                "INSERT INTO signals (company,date,headline,url,source_label,"
                " land_flag,sector_guess,lat,lon) VALUES (?,?,?,?,?,?,?,?,?)",
                (company, datetime.date.today().strftime("%Y%m%d"), h, "", "search",
                 info["land_flag"], info["sector"], lat, lon)
            )
        conn.commit(); left.success("Saved.")
    left.divider(); left.write("#### Headlines (5 mo)")
    for h in heads: left.markdown(f"- {h}")
    if left.button("Close"):
        st.session_state.pop("overlay"); _rerun()

# ───────── main content ─────────
if "overlay" in st.session_state:
    search_overlay(st.session_state["overlay"])

elif PAGE == "Map":
    left.title("Lead Master — Project Map")

    clients = load_clients()
    sector = st.sidebar.multiselect(
        "Sector tag", sorted({t for tags in clients.sector_tags.apply(json.loads)
                              for t in tags}))

    today=datetime.date.today()
    start = st.sidebar.date_input("Start", today - datetime.timedelta(days=150))
    end   = st.sidebar.date_input("End", today)

    sig = pd.read_sql(
        "SELECT company, MAX(date) AS date, lat, lon "
        "FROM signals WHERE date BETWEEN ? AND ? GROUP BY company",
        conn, params=(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    )
    df = clients.merge(sig, left_on="name", right_on="company", how="inner")
    if sector:
        df = df[df.sector_tags.apply(lambda s: any(t in json.loads(s) for t in sector))]

    fmap = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat, r.lon],
                popup=f"<b>{r.name}</b><br>{r.summary}",
                icon=folium.Icon(color="orange", icon="info-sign")
            ).add_to(fmap)
    st_folium(fmap, height=600, width="100%")

else:  # Companies
    left.title("Companies")

    sort_by = st.selectbox("Sort by",
        ["Alphabetical","Status","Sector tag","Next-touch"])
    clients = load_clients()
    if sort_by == "Alphabetical":
        clients = clients.sort_values("name")
    elif sort_by == "Status":
        clients = clients.sort_values("status")
    elif sort_by == "Sector tag":
        clients["first_sector"] = clients.sector_tags.apply(
            lambda s: json.loads(s)[0] if json.loads(s) else "")
        clients = clients.sort_values("first_sector")
    else:
        clients = clients.sort_values("next_touch", na_position="last")

    for _, r in clients.iterrows():
        left.subheader(r.name); left.caption(r.summary or "—")
