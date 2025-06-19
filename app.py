# app.py  – Lead Master 5.0  (2025-06-20)
# • Overlay: clean summary, confidence, sector tag, selectable headlines
# • Manual sector dropdown on save
# • Back-to-map button always visible
# • Companies page: table selector → detail pane
# • Duplicate insert guard

import json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium
from utils import get_conn, ensure_tables
from fetch_signals import (
    manual_search, national_scan,
    export_pdf, company_contacts, dedup,
    BUDGET_USED, DAILY_BUDGET,
)

# rerun helper (works on any Streamlit version)
def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        from streamlit.runtime.scriptrunner import RerunException, get_script_run_ctx
        raise RerunException(get_script_run_ctx())

# ───────── THEME ─────────
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

# ───────── left SIDEBAR ─────────
with st.sidebar:
    st.header("Controls")
    if st.button("Run national scan now"):
        with st.spinner("Scanning…"):
            national_scan()
        _rerun()

    # Back-to-map always visible when overlay open
    if "overlay" in st.session_state and st.button("Back to map"):
        st.session_state.pop("overlay"); _rerun()

    PAGE = st.radio("Pages", ["Map", "Companies"])
    search_q = st.text_input("Search company")
    if search_q and st.button("Go"):
        st.session_state["overlay"] = search_q

    st.write(f"**GPT spend today:** {BUDGET_USED:.1f} ¢ / {DAILY_BUDGET} ¢")

# ───────── layout 75 / 25 ─────────
left, right = st.columns([0.75, 0.25], gap="medium")

# ───────── right-hand NEWS panel ─────────
with right:
    st.header("Latest headlines")
    news_df = pd.read_sql(
        """SELECT id, company, headline, url, date, source_label,
                  land_flag, sector_guess, read_flag
           FROM signals ORDER BY date DESC LIMIT 100""", conn)
    sum_by_co = pd.read_sql("SELECT name, summary FROM clients", conn)\
                   .set_index("name")["summary"].to_dict()

    if news_df.empty:
        st.info("No headlines yet — run a scan.")
    else:
        for _, r in news_df.iterrows():
            dot = "" if r.read_flag else "<span class='news-dot'></span>"
            header = f"{dot}{r.headline[:60]}…"
            with st.expander(header, expanded=False):
                st.markdown(f"**{r.headline}**")
                st.write(f"Date : {r.date}")
                st.write(f"Source : {r.source_label}")
                if r.land_flag: st.write("🏷️ **Land / new site**")
                st.write(f"Sector : {r.sector_guess or '–'}")
                st.markdown(f"[Open article]({r.url})", unsafe_allow_html=True)
                st.write("---")
                st.write("**Company summary**")
                st.write(sum_by_co.get(r.company, '—'))

                # export
                if st.button("Export PDF", key=f"pdf{r.id}"):
                    pdf_bytes = export_pdf(
                        r._asdict(), sum_by_co.get(r.company, ''),
                        company_contacts(r.company))
                    st.download_button("Download PDF", pdf_bytes,
                        file_name=f"{r.company}_{r.date}.pdf",
                        mime="application/pdf", key=f"dl{r.id}")

                # mark read
                if not r.read_flag:
                    if st.button("Mark read", key=f"mark{r.id}"):
                        conn.execute("UPDATE signals SET read_flag=1 WHERE id=?",
                                     (int(r.id),)); conn.commit(); _rerun()

# ───────── SEARCH overlay helper ─────────
def search_overlay(co: str):
    info, raw_heads, lat, lon = manual_search(co)
    # dedup row dicts
    rows = dedup([{"title":t, "url":""} for t in raw_heads])

    left.markdown(f"## {co}")
    bullets = info["summary"].split("•")
    left.write("**Executive summary**")
    for b in bullets:
        if b.strip(): left.write("• "+b.strip())
    left.write(f"**Sector guess:** {info['sector']}  |  **Confidence:** {info['confidence']}")
    left.divider()

    left.write("### Headlines (select to save)")
    save_flags = []
    for i, r in enumerate(rows):
        with left.expander(r["title"][:80]):
            st.markdown(f"[Open link]({r['url'] or 'https://news.google.com'})")
            save_flags.append(st.checkbox("Save this headline", key=f"chk{i}"))

    manual_sector = left.selectbox("Sector tag for this company",
        ["Manufacturing","Food processing","Cold storage","Industrial","Retail",
         "Logistics","Other"], index=0)

    if left.button("Save selected"):
        for i, keep in enumerate(save_flags):
            if keep:
                row = rows[i]
                # prevent exact duplicate insert
                dupe = conn.execute(
                    "SELECT 1 FROM signals WHERE headline=? LIMIT 1",
                    (row["title"],)).fetchone()
                if dupe: continue
                conn.execute(
                    "INSERT INTO signals (company,date,headline,url,source_label,"
                    " land_flag,sector_guess,lat,lon) VALUES (?,?,?,?,?,?,?,?,?)",
                    (co, datetime.date.today().strftime("%Y%m%d"), row["title"],
                     row["url"], "search", info["land_flag"], manual_sector,
                     lat, lon))
        conn.execute(
            "INSERT OR REPLACE INTO clients (name,summary,sector_tags,status)"
            " VALUES (?,?,?, 'New')",
            (co, info["summary"], json.dumps([manual_sector])))
        conn.commit(); left.success("Saved.")
        left.button("Go to Companies", on_click=lambda: st.session_state.update(
            overlay=None, page_override="Companies"))

    if left.button("Close"):
        st.session_state.pop("overlay"); _rerun()

# ───────── main PAGES ─────────
if "overlay" in st.session_state:
    search_overlay(st.session_state["overlay"])

elif PAGE == "Map":
    left.title("Lead Master — Project Map")

    clients = load_clients()
    sector = st.sidebar.multiselect(
        "Sector tag", sorted({t for tags in clients.sector_tags.apply(json.loads)
                              for t in tags}))
    today = datetime.date.today()
    start = st.sidebar.date_input("Start", today - datetime.timedelta(days=150))
    end   = st.sidebar.date_input("End", today)

    sig = pd.read_sql(
        "SELECT company, MAX(date) AS date, lat, lon "
        "FROM signals WHERE date BETWEEN ? AND ? GROUP BY company",
        conn, params=(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    )
    df = clients.merge(sig, left_on="name", right_on="company", how="inner")
    if sector:
        df = df[df.sector_tags.apply(
            lambda s: any(t in json.loads(s) for t in sector))]

    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat, r.lon],
                popup=f"<b>{r.name}</b><br>{r.summary}",
                icon=folium.Icon(color="orange", icon="info-sign")
            ).add_to(m)
    st_folium(m, height=600, width="100%")

else:  # COMPANIES
    left.title("Companies")
    clients = load_clients()
    if clients.empty:
        left.info("No companies saved yet.")
    else:
        # left pane selector
        sel_col, detail_col = left.columns([0.35, 0.65], gap="medium")
        sel_table = clients[["name","status"]].rename(
            columns={"name":"Company","status":"Status"})
        sel_col.dataframe(sel_table, height=520, use_container_width=True)
        selected = sel_col.selectbox("Select company", clients.name.tolist())
        row = clients.set_index("name").loc[selected]

        # right pane details
        detail_col.subheader(selected)
        detail_col.write(f"**Status:** {row.status}")
        detail_col.write(row.summary or "—")
        detail_col.write("**Sector tags:** "+
            ", ".join(json.loads(row.sector_tags)))
        # signals table
        sigs = pd.read_sql(
            "SELECT date, headline, url FROM signals WHERE company=? "
            "ORDER BY date DESC LIMIT 20",
            conn, params=(selected,))
        if not sigs.empty:
            detail_col.write("**Latest signals**")
            for _, s in sigs.iterrows():
                detail_col.markdown(
                    f"- {s.date} – [{s.headline[:90]}…]({s.url or '#'})")
