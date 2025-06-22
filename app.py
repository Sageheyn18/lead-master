# app.py – Lead Master v5.5  (2025-06-22)
import os, json, datetime, pandas as pd, folium, streamlit as st
from streamlit_folium import st_folium
from PIL import Image

from utils          import get_conn, ensure_tables
from fetch_signals  import (
    manual_search, national_scan, export_pdf,
    company_contacts, fetch_logo, gpt_summary,
    google_news, fetch_permits,
    MAX_HEADLINES, BUDGET_USED, DAILY_BUDGET
)

# ───────── page config ─────────
st.set_page_config(
    page_title="Sage's Lead Master",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ───────── rerun helper ─────────
def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        from streamlit.runtime.scriptrunner import RerunException, get_script_run_ctx
        raise RerunException(get_script_run_ctx())

# ───────── sidebar ─────────
with st.sidebar:
    # logo
    try:
        img = Image.open("logo.png"); st.image(img, width=140)
    except: pass

    st.header("Controls")

    # dark/light
    theme=st.radio("Theme",["Light","Dark"])
    if theme=="Dark":
        st.markdown("<style>body{background:#222;color:#ddd;}</style>",
                    unsafe_allow_html=True)

    # voice-command stub
    st.markdown("""
    <button onclick="startVoice()">🎙️ Start voice</button>
    <script>
      const Speech = window.SpeechRecognition||window.webkitSpeechRecognition;
      if(Speech){
        const recog=new Speech();
        recog.onresult=e=>{
          let cmd=e.results[0][0].transcript.toLowerCase();
          window.parent.postMessage({type:'VOICE',cmd},'*');
        };
        window.startVoice=()=>recog.start();
      }
    </script>
    """, unsafe_allow_html=True)

    # national scan
    if st.button("Run national scan now"):
        with st.spinner("Scanning…"):
            national_scan()
        _rerun()

    # heatmap
    show_heat = st.checkbox("Heatmap overlay")

    # Search overlay state
    search_co = st.session_state.get("search_co","")

    # ───────── Page Navigation (GPT-style) ─────────
    st.markdown("## Menu")
    pages = [("🗺️ Map","Map"),("🏭 Companies","Companies"),
             ("📊 Pipeline","Pipeline"),("🏗️ Permits","Permits")]
    for icon,name in pages:
        if st.session_state.get("page","Map")==name:
            st.markdown(f"<div style='padding:6px;background:#ddd;border-radius:4px'>"
                        f"{icon} {name}</div>", unsafe_allow_html=True)
        else:
            if st.button(f"{icon}  {name}", key=name):
                st.session_state["page"] = name
                _rerun()

    st.markdown("---")
    st.write(f"**GPT spend:** {BUDGET_USED:.1f}¢ / {DAILY_BUDGET}¢")

    # search input
    st.header("Search company")
    co = st.text_input("", value=search_co, key="in")
    if st.button("Go", key="go"):
        if co.strip():
            st.session_state["search_co"]=co.strip()
            _rerun()

    # back to map (overlay close)
    if search_co:
        if st.button("Back to map"):
            del st.session_state["search_co"]
            _rerun()

# ───────── main content ─────────
conn = get_conn(); ensure_tables(conn)
page = st.session_state.get("page","Map")

# handle voice commands
for msg in st.experimental_get_query_params().get("VOICE",[]):
    cmd = msg.lower()
    if "national scan" in cmd:
        national_scan()
    for _,nm in pages:
        if f"go to {nm.lower()}" in cmd:
            st.session_state["page"]=nm
    _rerun()

if "search_co" in st.session_state:
    # ───────── Search overlay ─────────
    company = st.session_state["search_co"]
    st.header(company)

    show_all = st.checkbox("Show all headlines", value=False)
    if show_all:
        arts = google_news(company, MAX_HEADLINES)
        rows = [{"title":a["title"],"url":a["url"]} for a in arts]
        summary={"summary":"All RSS headlines","sector":"mixed","confidence":0,"land_flag":0}
        lat=lon=None
    else:
        summary, rows, lat, lon = manual_search(company)

    st.subheader("Executive summary")
    for b in summary["summary"].split("•"):
        if b.strip(): st.write("• "+b.strip())
    st.write(f"**Sector:** {summary['sector']}  |  **Confidence:** {summary['confidence']}")

    st.subheader("Headlines (tick & save)")
    checks=[]
    for i,r in enumerate(rows):
        with st.expander(r["title"][:100]):
            st.markdown(f"[Open article]({r['url']})")
            checks.append(st.checkbox("Save",key=f"chk{i}"))

    sect = st.selectbox("Sector tag",[
        "Manufacturing","Food processing","Cold storage",
        "Industrial","Retail","Logistics","Other"
    ])
    if st.button("Save selected"):
        any_saved=False
        for i,keep in enumerate(checks):
            if not keep: continue
            any_saved=True
            rr=rows[i]
            conn.execute(
                "INSERT INTO signals(company,date,headline,url,source_label,"
                "land_flag,sector_guess,lat,lon) VALUES(?,?,?,?,?,?,?,?,?)",
                (company,datetime.date.today().strftime("%Y%m%d"),
                 rr["title"],rr["url"],"search",
                 summary["land_flag"],sect,lat,lon)
            )
        if any_saved:
            conn.execute(
                "INSERT OR REPLACE INTO clients(name,summary,sector_tags,status)"
                " VALUES(?,?,?, 'New')",
                (company, summary["summary"], json.dumps([sect]))
            )
            conn.commit()
            st.success("Saved.")
            del st.session_state["search_co"]
            st.session_state["page"]="Companies"
            _rerun()
        else:
            st.warning("Select at least one headline.")

    if st.button("Close"):
        del st.session_state["search_co"]
        _rerun()

else:
    # ───────── Map ─────────
    if page=="Map":
        st.title("Lead Master — Project Map")
        dfc=pd.read_sql("SELECT * FROM clients",conn)
        dfs=pd.read_sql(
            "SELECT company,MAX(date) AS date,lat,lon FROM signals GROUP BY company",
            conn
        )
        df = dfc.merge(dfs,left_on="name",right_on="company",how="inner")

        m=folium.Map(location=[37,-96],zoom_start=4,tiles="CartoDB Positron")
        if show_heat:
            from folium.plugins import HeatMap
            pts=df[["lat","lon"]].dropna().values.tolist()
            HeatMap(pts).add_to(m)
        for _,r in df.iterrows():
            folium.Marker(
                [r.lat,r.lon],
                popup=(f"<b>{r.name}</b><br>{r.summary[:120]}…"
                       f"<br><a href='https://maps.google.com/?q={r.lat},{r.lon}' target='_blank'>Map</a>")
            ).add_to(m)
        st_folium(m,height=700)

    # ───────── Companies ─────────
    elif page=="Companies":
        st.title("Companies")
        cl=pd.read_sql("SELECT * FROM clients",conn)
        if cl.empty:
            st.info("No companies yet.")
        else:
            c1,c2=st.columns([0.3,0.7])
            sel=c1.selectbox("Pick",cl.name.tolist())
            data=cl.set_index("name").loc[sel]
            c2.subheader(sel); c2.write(data.summary)
            c2.write(f"**Sector:** {', '.join(json.loads(data.sector_tags))}")
            cont=pd.read_sql(
                "SELECT name,title,email,phone FROM contacts WHERE company=?",
                conn,params=(sel,)
            )
            if not cont.empty:
                c2.write("**Contacts**"); c2.table(cont)
            sigs=pd.read_sql(
                "SELECT date,headline,url FROM signals WHERE company=? "
                "ORDER BY date DESC LIMIT 7",conn,params=(sel,)
            )
            if not sigs.empty:
                c2.write("**Last 7 signals**")
                for _,s in sigs.iterrows():
                    c2.markdown(f"- {s.date} – [{s.headline}]({s.url})")
            if c2.button("Create proposal"):
                secs=[
                    "Scope of Work","Timeline & Milestones","Key Contacts",
                    "High-Level Budget Estimate","Site Logistics & Assumptions",
                    "Deliverables","Risk & Contingencies",
                    "Permitting & Approvals Timeline","Safety & Compliance Plan",
                    "Warranty & Maintenance","Cost Breakdown by Trade",
                    "Change-Order Process","Communication & Reporting"
                ]
                pick= c2.multiselect("Sections",secs,default=secs[:6])
                prompt=f"Draft exec proposal for {sel}:\n"
                for s in pick: prompt+=f"## {s}\n\n"
                rsp=client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role":"user","content":prompt}],
                    temperature=0.2,max_tokens=500
                )
                c2.text_area("Proposal",rsp.choices[0].message.content,height=400)

    # ───────── Pipeline ─────────
    elif page=="Pipeline":
        st.title("Pipeline")
        cl=pd.read_sql("SELECT * FROM clients",conn)
        lanes=["Lead","Qualified","Proposal","Negotiation","Won","Lost"]
        board={"lanes":[{"id":l,"title":l,"cards":[]} for l in lanes]}
        for _,r in cl.iterrows():
            status=r.status
            card={"id":r.name,"title":r.name,
                  "description":r.summary[:80]+"…"}
            for lane in board["lanes"]:
                if lane["id"]==status: lane["cards"].append(card)

        import streamlit.components.v1 as components
        components.html(f"""
        <!DOCTYPE html><html><head>
          <link rel="stylesheet"
           href="https://unpkg.com/react-trello/dist/styles.css"/>
        </head><body><div id="root"></div>
        <script src="https://unpkg.com/react/umd/react.production.min.js"></script>
        <script src="https://unpkg.com/react-dom/umd/react-dom.production.min.js"></script>
        <script src="https://unpkg.com/prop-types/prop-types.min.js"></script>
        <script src="https://unpkg.com/react-trello/dist/react-trello.min.js"></script>
        <script>
          const data={json.dumps(board)};
          ReactDOM.render(
            React.createElement(window.TrelloBoard,{{
              data:data,draggable:true,editable:true,
              onDataChange:d=>window.parent.postMessage({{
                type:'PIPELINE',data:JSON.stringify(d)
              }},'*')
            }}),document.getElementById('root')
          );
          window.addEventListener('message',e=>{
            if(e.data.type==='PIPELINE') {
              const qs=new URLSearchParams(window.location.search);
              qs.set('PIPELINE',e.data.data);
              window.history.replaceState(null,'',`?${qs}`);
            }
          });
        </script>
        </body></html>
        """,height=600)

        if 'PIPELINE' in st.experimental_get_query_params():
            new = json.loads(st.experimental_get_query_params()['PIPELINE'][0])
            for lane in new.lanes:
                for c in lane.cards:
                    conn.execute(
                      "UPDATE clients SET status=? WHERE name=?",
                      (lane.id, c.id)
                    )
            conn.commit()
            _rerun()
