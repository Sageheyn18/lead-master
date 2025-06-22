# app.py  – Lead Master 5.1
# • Map / Companies pages
# • Search overlay with bullet summary, sector/confidence, selectable headlines
# • Manual sector dropdown on save
# • Right-hand news panel with expanders + PDF export
# • Back-to-map always visible while overlay active
# • Duplicate-safe inserts
# • Works on any Streamlit ≥ 1.26 (fallback _rerun helper)

import json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium

from utils          import get_conn, ensure_tables
from fetch_signals  import (
    manual_search, national_scan, export_pdf, company_contacts,
    BUDGET_USED, DAILY_BUDGET
)

# ───────── universal rerun ─────────
def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:  # older Streamlit
        from streamlit.runtime.scriptrunner import RerunException, get_script_run_ctx
        raise RerunException(get_script_run_ctx())

# ───────── theme / brand colours (Fisher Construction) ─────────
GOLD = "#B7932F"; CHAR = "#41413F"
st.set_page_config(page_title="Lead Master", layout="wide")
st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    html, body, [class*="css"] {{font-family:'Inter',sans-serif}}
    .stButton>button {{background:{GOLD};color:#fff;border:none;
                       padding:6px 12px;border-radius:4px;font-weight:600}}
    .news-dot {{height:10px;width:10px;background:{GOLD};
               border-radius:50%;display:inline-block;margin-right:6px}}
    </style>""",
    unsafe_allow_html=True,
)

# ───────── DB connection ─────────
conn = get_conn(); ensure_tables(conn)
def load_clients(): return pd.read_sql("SELECT * FROM clients", conn)

# ───────── sidebar (always visible) ─────────
with st.sidebar:
    st.header("Controls")

    # national scan
    if st.button("Run national scan now"):
        with st.spinner("Scanning…"):
            national_scan()
        _rerun()

    # back-to-map (only when overlay active)
    if "overlay" in st.session_state and st.button("Back to map"):
        st.session_state.pop("overlay"); _rerun()

    # page selector
    page_default = st.session_state.pop("page_override", None)
    PAGE = st.radio("Pages", ["Map", "Companies"],
                    index=0 if page_default != "Companies" else 1)

    # manual search
    search_q = st.text_input("Search company")
    if search_q and st.button("Go"):
        st.session_state["overlay"] = search_q

    # budget
    st.write(f"**GPT spend today:** {BUDGET_USED:.1f} ¢ / {DAILY_BUDGET} ¢")

# layout: main + right-hand news panel
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
            with st.expander(header):
                st.markdown(f"**{r.headline}**")
                st.write(f"Date : {r.date}")
                st.write(f"Source : {r.source_label}")
                if r.land_flag: st.write("🏷️ **Land / new site**")
                st.write(f"Sector : {r.sector_guess or '–'}")
                st.markdown(f"[Open article]({r.url})", unsafe_allow_html=True)
                st.write("---")
                st.write("**Company summary**")
                st.write(sum_by_co.get(r.company, '—'))

                # export PDF
                if st.button("Export PDF", key=f"pdf{r.id}"):
                    pdf_bytes = export_pdf(
                        r._asdict(),
                        sum_by_co.get(r.company, ''),
                        company_contacts(r.company))
                    st.download_button("Download PDF", pdf_bytes,
                        file_name=f"{r.company}_{r.date}.pdf",
                        mime="application/pdf", key=f"dl{r.id}")

                # mark read
                if not r.read_flag:
                    if st.button("Mark read", key=f"mark{r.id}"):
                        conn.execute(
                            "UPDATE signals SET read_flag=1 WHERE id=?", (int(r.id),))
                        conn.commit(); _rerun()

# ───────── SEARCH overlay helper ─────────
def search_overlay(company: str):
    info, rows, lat, lon = manual_search(company)  # rows already deduped!

    left.markdown(f"## {company}")

    left.write("**Executive summary**")
    for bullet in info["summary"].split("•"):
        if bullet.strip():
            left.write("• " + bullet.strip())
    left.write(
        f"**Sector guess:** {info['sector']} &nbsp;&nbsp; "
        f"**Confidence:** {info['confidence']}")

    left.divider(); left.write("### Headlines (tick to save)")

    save_flags = []
    for i, r in enumerate(rows):
        with left.expander(r["title"][:90]):
            st.markdown(f"[Open article]({r['url'] or 'https://news.google.com'})")
            save_flags.append(
                st.checkbox("Save this headline", key=f"chk{i}"))

    manual_sector = left.selectbox(
        "Sector tag", ["Manufacturing","Food processing","Cold storage",
                       "Industrial","Retail","Logistics","Other"], index=0)

    if left.button("Save selected"):
        saved_any = False
        for i, keep in enumerate(save_flags):
            if not keep: continue
            row = rows[i]
            dup = conn.execute(
                "SELECT 1 FROM signals WHERE headline=? LIMIT 1",
                (row["title"],)).fetchone()
            if dup: continue
            conn.execute(
                "INSERT INTO signals (company,date,headline,url,source_label,"
                " land_flag,sector_guess,lat,lon) VALUES (?,?,?,?,?,?,?,?,?)",
                (company, datetime.date.today().strftime("%Y%m%d"),
                 row["title"], row["url"], "search",
                 info["land_flag"], manual_sector, lat, lon))
            saved_any = True

        if saved_any:
            conn.execute(
                "INSERT OR REPLACE INTO clients (name,summary,sector_tags,status)"
                " VALUES (?,?,?, 'New')",
                (company, info["summary"], json.dumps([manual_sector])))
            conn.commit()
            left.success("Saved.")
            st.session_state.pop("overlay")
            st.session_state["page_override"] = "Companies"
            _rerun()
        else:
            left.warning("No headlines ticked.")

    if left.button("Close"):
        st.session_state.pop("overlay"); _rerun()

# ───────── MAIN AREA ─────────
if "overlay" in st.session_state:
    search_overlay(st.session_state["overlay"])

elif PAGE == "Map":
    left.title("Lead Master — Project Map")

    clients = load_clients()
    sector_filter = st.sidebar.multiselect(
        "Sector tag", sorted({t for tags in clients.sector_tags.apply(json.loads)
                              for t in tags}))

    today = datetime.date.today()
    start = st.sidebar.date_input("Start", today - datetime.timedelta(days=150))
    end   = st.sidebar.date_input("End",   today)

    sig = pd.read_sql(
        "SELECT company, MAX(date) AS date, lat, lon "
        "FROM signals WHERE date BETWEEN ? AND ? GROUP BY company",
        conn, params=(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    )
    df = clients.merge(sig, left_on="name", right_on="company", how="inner")
    if sector_filter:
        df = df[df.sector_tags.apply(
            lambda s: any(t in json.loads(s) for t in sector_filter))]

    fmap = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in df.iterrows():
        if pd.notna(r.lat) and pd.notna(r.lon):
            folium.Marker(
                [r.lat, r.lon],
                popup=f"<b>{r.name}</b><br>{r.summary}",
                icon=folium.Icon(color="orange", icon="info-sign")
            ).add_to(fmap)
    st_folium(fmap, height=600, width="100%")

else:  # Companies page
    left.title("Companies")
    clients = load_clients()
    if clients.empty:
        left.info("No companies saved yet.")
    else:
        sel_col, info_col = left.columns([0.35, 0.65], gap="medium")
        table = clients[["name","status"]].rename(
            columns={"name":"Company","status":"Status"})
        sel_col.dataframe(table, height=520, use_container_width=True)
        selection = sel_col.selectbox("Select company", clients.name.tolist())
        row = clients.set_index("name").loc[selection]

        info_col.subheader(selection)
        info_col.write(f"**Status:** {row.status}")
        info_col.write(row.summary or "—")
        info_col.write("**Sector tags:** "+", ".join(json.loads(row.sector_tags)))

        sigs = pd.read_sql(
            "SELECT date, headline, url FROM signals WHERE company=? "
            "ORDER BY date DESC LIMIT 20", conn, params=(selection,))
        if not sigs.empty:
            info_col.write("**Latest signals**")
            for _, s in sigs.iterrows():
                info_col.markdown(
                    f"- {s.date} – [{s.headline[:90]}]({s.url or '#'})")
