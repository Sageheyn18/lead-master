# app.py – Lead Master v5.3 (2025-06-22)
import os, json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium
from PIL import Image
from utils          import get_conn, ensure_tables
from fetch_signals  import (
    manual_search, national_scan, export_pdf,
    company_contacts, fetch_logo, gpt_summary, google_news
)
from permits        import fetch_permits

# ───────── Page config ─────────
st.set_page_config(
    page_title="Sage's Lead Master",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ───────── Sidebar – logo & controls ─────────
with st.sidebar:
    # logo
    try:
        img = Image.open("logo.png"); st.image(img, width=140)
    except: pass

    st.header("Controls")
    # Dark/Light
    theme = st.radio("Theme", ["Light","Dark"], index=0)
    if theme=="Dark":
        st.markdown("<style>body{background:#222;color:#ddd;}</style>",
                    unsafe_allow_html=True)

    # Voice-command stub
    st.button("🎙️ Start voice (beta)")

    # National scan
    if st.button("Run national scan now"):
        with st.spinner("Scanning…"):
            national_scan()
        st.experimental_rerun()

    # Heatmap
    heat = st.checkbox("Heatmap overlay", value=False)

    # Search company
    st.header("Search company")
    co = st.text_input("Company name")
    if st.button("Go"):
        st.session_state["search_co"] = co
        st.experimental_rerun()

    # Back to map if overlay
    if "search_co" in st.session_state:
        if st.button("Back to map"):
            del st.session_state["search_co"]
            st.experimental_rerun()

    # Pages
    st.header("Pages")
    if "search_co" not in st.session_state:
        page = st.radio("", ["Map","Companies","Pipeline"])
    else:
        page = "Map"
    st.write(f"")  # spacer

    # Permits toggle
    st.header("Permits")
    permits = fetch_permits(10)
    for p in permits[:5]:
        st.markdown(f"- [{p['title']}]({p['url']})")

    st.write("---")
    st.markdown(f"**GPT spend:** *{round(os.getenv('BUDGET_USED',0),1)}¢ / {os.getenv('DAILY_BUDGET_CENTS',300)}¢*")

# ───────── Main vs. Overlay ─────────
conn = get_conn(); ensure_tables(conn)

if "search_co" in st.session_state:
    # ───────── Search overlay ─────────
    co = st.session_state["search_co"]
    st.header(co)
    # toggle all vs filtered
    show_all = st.checkbox("Show all headlines", value=False)
    if show_all:
        arts = google_news(co, MAX_HEADLINES)
        rows = [{"title":a["title"],"url":a["url"]} for a in arts]
        lat = lon = None
        summary = {"summary":"Showing all RSS headlines.","sector":"mixed","confidence":0,"land_flag":0}
    else:
        summary, rows, lat, lon = manual_search(co)

    st.subheader("Executive summary")
    for b in summary["summary"].split("•"):
        if b.strip(): st.write("• "+b.strip())
    st.write(f"**Sector:** {summary['sector']}  |  **Confidence:** {summary['confidence']}")

    st.subheader("Headlines (tick & save)")
    checks = []
    for i,r in enumerate(rows):
        with st.expander(r["title"][:100]):
            st.markdown(f"[Open article]({r['url']})", unsafe_allow_html=True)
            checks.append(st.checkbox("Save this", key=f"chk{i}"))

    sect = st.selectbox("Sector tag",
        ["Manufacturing","Food processing","Cold storage","Industrial","Retail","Logistics","Other"]
    )
    if st.button("Save selected"):
        saved=False
        for i,keep in enumerate(checks):
            if not keep: continue
            row = rows[i]
            conn.execute(
                "INSERT INTO signals(company,date,headline,url,source_label,land_flag,sector_guess,lat,lon)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (co, datetime.date.today().strftime("%Y%m%d"),
                 row["title"], row["url"], "search",
                 summary["land_flag"], sect, lat, lon)
            )
            saved=True
        if saved:
            conn.execute(
                "INSERT OR REPLACE INTO clients(name,summary,sector_tags,status)"
                " VALUES(?,?,?, 'New')",
                (co, summary["summary"], json.dumps([sect]))
            )
            conn.commit()
            st.success("Saved.")
            del st.session_state["search_co"]
            st.session_state["page"] = "Companies"
            st.experimental_rerun()
        else:
            st.warning("No selections made.")

    st.button("Close overlay", on_click=lambda: st.session_state.pop("search_co"))

else:
    # ───────── Page: Map ─────────
    if page=="Map":
        st.title("Lead Master — Project Map")
        df_clients = pd.read_sql("SELECT * FROM clients", conn)
        sig_df     = pd.read_sql(
            "SELECT company,MAX(date) AS date,lat,lon FROM signals GROUP BY company",
            conn
        )
        df = df_clients.merge(sig_df, left_on="name", right_on="company", how="inner")
        m  = folium.Map(location=[37,-96], zoom_start=4, tiles="CartoDB Positron")
        if heat:
            from folium.plugins import HeatMap
            pts = df[["lat","lon"]].dropna().values.tolist()
            HeatMap(pts).add_to(m)
        for _,r in df.iterrows():
            folium.Marker(
                [r.lat,r.lon],
                popup=(f"<b>{r.name}</b><br>{r.summary[:120]}…"
                       f"<br><a href='https://maps.google.com/?q={r.lat},{r.lon}' target='_blank'>Google Maps</a>")
            ).add_to(m)
        st_folium(m, height=700)

    # ───────── Page: Companies ─────────
    elif page=="Companies":
        st.title("Companies")
        clients = pd.read_sql("SELECT * FROM clients", conn)
        if clients.empty:
            st.info("No companies yet.")
        else:
            col1, col2 = st.columns([0.3,0.7])
            sel = col1.selectbox("Select a company", clients.name.tolist())
            row = clients.set_index("name").loc[sel]
            col2.subheader(sel)
            col2.write(row.summary)
            col2.write(f"**Sector:** {', '.join(json.loads(row.sector_tags))}")
            # contacts
            cdf = pd.read_sql(
                "SELECT name,title,email,phone FROM contacts WHERE company=?",
                conn, params=(sel,)
            )
            if not cdf.empty:
                col2.write("**Contacts**"); col2.table(cdf)
            # last 7 signals
            sdf = pd.read_sql(
                "SELECT date,headline,url FROM signals WHERE company=? "
                "ORDER BY date DESC LIMIT 7", conn, params=(sel,)
            )
            if not sdf.empty:
                col2.write("**Last 7 signals**")
                for _,s in sdf.iterrows():
                    col2.markdown(f"- {s.date} – [{s.headline}]({s.url})")
            # proposal draft
            if col2.button("Create proposal draft"):
                sections = [
                  "Scope of Work","Timeline & Milestones","Key Contacts",
                  "High-Level Budget Estimate","Site Logistics & Assumptions",
                  "Deliverables","Risk & Contingencies",
                  "Permitting & Approvals Timeline","Safety & Compliance Plan",
                  "Warranty & Maintenance","Cost Breakdown by Trade",
                  "Change-Order Process","Communication & Reporting"
                ]
                choice = col2.multiselect("Include sections:", sections, default=sections[:6])
                # generate via GPT-4o-mini
                prompt = "Create an executive proposal for " + sel + ":\n"
                for sec in choice:
                    prompt += f"## {sec}\n\n"
                prompt += "\nKeep each section concise."
                rsp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role":"user","content":prompt}],
                    temperature=0.2, max_tokens=500
                )
                draft = rsp.choices[0].message.content
                col2.text_area("Proposal Draft", draft, height=400)

    # ───────── Page: Pipeline ─────────
    elif page=="Pipeline":
        st.title("Pipeline")
        # using react-trello
        cards = []
        clients = pd.read_sql("SELECT * FROM clients", conn)
        for _,r in clients.iterrows():
            cards.append({
                "id": r.name,
                "title": r.name,
                "description": r.summary[:80]+"…"
            })
        board = {
            "lanes":[
                {"id":"Lead","title":"Lead","cards":[]},
                {"id":"Qualified","title":"Qualified","cards":[]},
                {"id":"Proposal","title":"Proposal","cards":[]},
                {"id":"Negotiation","title":"Negotiation","cards":[]},
                {"id":"Won","title":"Won","cards":[]},
                {"id":"Lost","title":"Lost","cards":[]},
            ]
        }
        for c in cards:
            status = clients.set_index("name").loc[c["id"]].status
            for lane in board["lanes"]:
                if lane["id"]==status:
                    lane["cards"].append(c)
        import streamlit.components.v1 as components
        components.html(f"""
        <!DOCTYPE html>
        <html><head>
          <link rel="stylesheet"
            href="https://unpkg.com/react-trello/dist/styles.css"/>
        </head><body>
          <div id="root"></div>
          <script src="https://unpkg.com/react/umd/react.production.min.js"></script>
          <script src="https://unpkg.com/react-dom/umd/react-dom.production.min.js"></script>
          <script src="https://unpkg.com/prop-types/prop-types.min.js"></script>
          <script src="https://unpkg.com/react-trello/dist/react-trello.min.js"></script>
          <script>
            const data = {json.dumps(board)};
            ReactDOM.render(
              React.createElement(window.TrelloBoard, {{
                data: data,
                draggable: true,
                editable: true,
                onDataChange: d => {{
                  window.parent.postMessage({{type:'PIPELINE_UPDATE', data: d}}, '*');
                }}
              }}),
              document.getElementById('root')
            );
          </script>
        </body></html>
        """, height=600)

        # listen for updates
        msg = st.experimental_get_query_params().get("PIPELINE_UPDATE")
        if msg:
            new_board = json.loads(msg[0])
            # extract and write back statuses
            for lane in new_board["lanes"]:
                for card in lane["cards"]:
                    conn.execute(
                      "UPDATE clients SET status=? WHERE name=?",
                      (lane["id"], card["id"])
                    )
            conn.commit()
            st.experimental_rerun()
