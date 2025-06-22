# app.py  – Lead Master v5.2  (2025-06-21)
# • All previously agreed features
# • AI‐created logo in sidebar
# • Heatmap on/off toggle
# • Dark/light mode toggle
# • Fixed‐height scrollable panels
# • Clean, professional layout

import os, json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium
from PIL import Image

from utils          import get_conn, ensure_tables
from fetch_signals  import (
    manual_search, national_scan, export_pdf, company_contacts,
    BUDGET_USED, DAILY_BUDGET
)

# ───────── page config & theming ─────────
st.set_page_config(
    page_title="Sage's Lead Master",
    layout="wide",
    initial_sidebar_state="expanded",
)

# sidebar logo & mode toggle
with st.sidebar:
    # logo (place logo.png in your repo)
    try:
        logo = Image.open("logo.png")
        st.image(logo, width=140)
    except Exception:
        pass

    st.markdown("## Controls")
    # dark/light mode
    theme = st.radio("Theme", ["Light","Dark"], index=0)
    if theme=="Dark":
        st.markdown(
            "<style>body {background-color:#222; color:#ddd;} </style>",
            unsafe_allow_html=True
        )

# ───────── rerun helper ─────────
def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        from streamlit.runtime.scriptrunner import RerunException, get_script_run_ctx
        raise RerunException(get_script_run_ctx())

# ───────── DB ─────────
conn = get_conn(); ensure_tables(conn)
def load_clients(): return pd.read_sql("SELECT * FROM clients", conn)

# ───────── sidebar continued ─────────
with st.sidebar:
    # national scan
    if st.button("Run national scan now"):
        with st.spinner("Scanning…"):
            national_scan()
        _rerun()

    # heatmap toggle
    show_heat = st.checkbox("Heatmap overlay", value=False)

    # back-to-map while overlay open
    if "overlay" in st.session_state and st.button("Back to map"):
        st.session_state.pop("overlay")
        _rerun()

    # page selector
    target = st.session_state.pop("page_override", None)
    PAGE = st.radio("Pages", ["Map","Companies","Pipeline"],
                    index= ["Map","Companies","Pipeline"].index(target)
                             if target in ["Companies","Pipeline"] else 0)

    # manual search
    q = st.text_input("Search company")
    if q and st.button("Go"):
        st.session_state["overlay"] = q

    # budget display
    st.markdown(f"**GPT spend:** {BUDGET_USED:.1f}¢ / {DAILY_BUDGET}¢")

# ───────── layout ─────────
main, news = st.columns([0.75,0.25], gap="small")

# ───────── NEWS panel (fixed height) ─────────
with news:
    st.header("Latest headlines")
    news_df = pd.read_sql(
        """SELECT id, company, headline, url, date, source_label,
                  land_flag, sector_guess, read_flag
           FROM signals ORDER BY date DESC LIMIT 100""", conn
    )
    summaries = pd.read_sql("SELECT name, summary FROM clients",
                             conn).set_index("name")["summary"].to_dict()

    container = st.container()
    with st.container():
        for _, r in news_df.iterrows():
            dot = "" if r.read_flag else "🟡 "
            header = f"{dot}{r.headline[:60]}…"
            with st.expander(header):
                st.markdown(f"**{r.headline}**")
                st.write(f"- Date: {r.date}")
                st.write(f"- Source: {r.source_label}")
                if r.land_flag: st.write("- 🏷️ Land/site project")
                st.write(f"- Sector: {r.sector_guess or '–'}")
                st.markdown(f"[Open article]({r.url})", unsafe_allow_html=True)
                st.write("---")
                st.write("**Summary**:")
                st.write(summaries.get(r.company, "—"))

                cols = st.columns(2)
                if cols[0].button("Export PDF", key=f"pdf{r.id}"):
                    pdf = export_pdf(r._asdict(),
                                     summaries.get(r.company,""),
                                     company_contacts(r.company))
                    cols[1].download_button(
                        "Download PDF", pdf,
                        file_name=f"{r.company}_{r.date}.pdf",
                        mime="application/pdf",
                        key=f"dl{r.id}"
                    )
                if not r.read_flag and cols[1].button("Mark read", key=f"mr{r.id}"):
                    conn.execute("UPDATE signals SET read_flag=1 WHERE id=?",
                                 (int(r.id),))
                    conn.commit(); _rerun()

# make news panel scrollable
st.markdown(
    """
    <style>
    .css-1lcbmhc { overflow-y: auto; height: 800px; }
    </style>
    """, unsafe_allow_html=True
)

# ───────── SEARCH overlay ─────────
def search_overlay(co: str):
    info, rows, lat, lon = manual_search(co)
    with main:
        st.subheader(co)
        st.write("**Executive summary**")
        for b in info["summary"].split("•"):
            if b.strip(): st.write("• "+b.strip())
        st.write(f"- Sector: {info['sector']}  |  Confidence: {info['confidence']}")
        st.write("---")
        st.write("### Headlines (tick to save)")
        checks = []
        for i, r in enumerate(rows):
            with st.expander(r["title"][:90]):
                st.markdown(f"[Open article]({r['url']})", unsafe_allow_html=True)
                checks.append(st.checkbox("Save this headline", key=f"chk{i}"))

        sector = st.selectbox("Sector tag", ["Manufacturing","Food processing",
            "Cold storage","Industrial","Retail","Logistics","Other"])
        if st.button("Save selected"):
            saved=False
            for i, keep in enumerate(checks):
                if not keep: continue
                row = rows[i]
                dup = conn.execute(
                    "SELECT 1 FROM signals WHERE headline=? LIMIT 1",
                    (row["title"],)
                ).fetchone()
                if dup: continue
                conn.execute(
                    "INSERT INTO signals (company,date,headline,url,"
                    "source_label,land_flag,sector_guess,lat,lon)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (co, datetime.date.today().strftime("%Y%m%d"),
                     row["title"], row["url"], "search",
                     info["land_flag"], sector, lat, lon)
                )
                saved=True
            if saved:
                conn.execute(
                    "INSERT OR REPLACE INTO clients (name,summary,sector_tags,status)"
                    " VALUES (?,?,?, 'New')",
                    (co, info["summary"], json.dumps([sector]))
                )
                conn.commit()
                st.success("Saved")
                st.session_state.pop("overlay")
                st.session_state["page_override"]="Companies"
                _rerun()
            else:
                st.warning("No headlines selected.")

        if st.button("Close"):
            st.session_state.pop("overlay"); _rerun()

# ───────── MAIN pages ─────────
if "overlay" in st.session_state:
    search_overlay(st.session_state["overlay"])

elif PAGE=="Map":
    main.title("Lead Master — Project Map")
    # map & heatmap
    clients = load_clients()
    sig = pd.read_sql(
        "SELECT company, MAX(date) AS date, lat, lon FROM signals "
        "GROUP BY company", conn
    )
    df = clients.merge(sig, left_on="name", right_on="company", how="inner")

    m = folium.Map(location=[37,-96], zoom_start=4, tiles="CartoDB Positron")
    if show_heat:
        from folium.plugins import HeatMap
        points = df[["lat","lon"]].dropna().values.tolist()
        HeatMap(points).add_to(m)

    for _, r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat, r.lon],
                popup=(
                    f"<b>{r.name}</b><br>"
                    f"{r.summary[:120]}…<br>"
                    f"<a href='https://www.google.com/maps/search/?api=1&query={r.lat},{r.lon}' target='_blank'>"
                    "Open in Google Maps</a>"
                ),
            ).add_to(m)
    st_folium(m, height=700, width="100%")

elif PAGE=="Companies":
    main.title("Companies")
    clients = load_clients()
    if clients.empty:
        main.info("No companies yet.")
    else:
        sel, detail = main.columns([0.3,0.7], gap="medium")
        df_sel = clients[["name","status"]].rename(
            columns={"name":"Company","status":"Status"}
        )
        sel.dataframe(df_sel, height=600, use_container_width=True)
        pick = sel.selectbox("Select company", clients.name.tolist())
        row = clients.set_index("name").loc[pick]

        detail.subheader(pick)
        detail.write(f"**Status:** {row.status}")
        detail.write(row.summary)
        detail.write("**Sector:** "+", ".join(json.loads(row.sector_tags)))

        # contacts
        c_df = pd.read_sql(
            "SELECT name,title,email,phone FROM contacts WHERE company=?",
            conn, params=(pick,)
        )
        if not c_df.empty:
            detail.write("**Contacts**")
            detail.table(c_df)

        # recent signals
        s_df = pd.read_sql(
            "SELECT date,headline,url FROM signals WHERE company=? "
            "ORDER BY date DESC LIMIT 7", conn, params=(pick,)
        )
        if not s_df.empty:
            detail.write("**Last 7 signals**")
            for _, s in s_df.iterrows():
                detail.markdown(f"- {s.date} – [{s.headline}]({s.url})")

elif PAGE=="Pipeline":
    main.title("Pipeline")
    from st_aggrid import AgGrid, GridOptionsBuilder
    # Kanban-like: we'll simulate with AgGrid plus status dropdown + drag not supported
    clients = load_clients()
    if clients.empty:
        main.info("No leads to show.")
    else:
        gb = GridOptionsBuilder.from_dataframe(clients[["name","status"]])
        gb.configure_selection("single")
        gb.configure_column("status", editable=True, cellEditor="agSelectCellEditor",
                            cellEditorParams={"values":["Lead","Qualified","Proposal","Negotiation","Won","Lost"]})
        grid = AgGrid(clients[["name","status"]], gb.build(), height=600)
        sel = grid["selected_rows"]
        if sel:
            name = sel[0]["name"]
            idx  = clients[clients.name==name].index[0]
            new_status = clients.at[idx,"status"]
            conn.execute("UPDATE clients SET status=? WHERE name=?",
                         (new_status, name))
            conn.commit()
            st.success(f"{name} moved to {new_status}")
