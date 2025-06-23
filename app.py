```python
# app.py
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from utils import get_conn, ensure_tables
from fetch_signals import manual_search, national_scan, company_contacts, export_pdf

# ───────── Page setup ─────────
st.set_page_config(layout="wide")
ensure_tables()

# ───────── Sidebar ─────────
st.sidebar.title("Lead Master")

# Company search overlay
search_co = st.sidebar.text_input("Search Company", key="search_co")
if st.sidebar.button("Go"):
    st.session_state.overlay = search_co
    st.experimental_rerun()

# National scan trigger
if st.sidebar.button("Run national scan now"):
    national_scan()
    st.experimental_rerun()

# View selector
page = st.sidebar.selectbox("View", ["Map", "Companies", "Pipeline", "Permits"], key="page")

# ───────── Main content ─────────
if page == "Map":
    st.header("Lead Master — Project Map")
    conn = get_conn()
    dfc = pd.read_sql("SELECT * FROM clients", conn)
    dfs = pd.read_sql("SELECT company, lat, lon, date FROM signals", conn)
    conn.close()
    # align keys
    dfs = dfs.rename(columns={"company": "name"})
    # merge clients + one signal per company
    merged = dfc.merge(
        dfs.groupby("name").first().reset_index(), on="name", how="inner"
    )
    # build map
    m = folium.Map(location=[37, -96], zoom_start=4, tiles="CartoDB Positron")
    for _, r in merged.iterrows():
        if pd.isna(r["lat"]) or pd.isna(r["lon"]):
            continue
        folium.Marker(
            [r["lat"], r["lon"]], 
            popup=folium.Popup(f"<b>{r['name']}</b><br>{r['summary']}", max_width=250)
        ).add_to(m)
    st_folium(m, width=700, height=500)

elif page == "Companies":
    st.header("Companies")
    conn = get_conn()
    dfc = pd.read_sql("SELECT * FROM clients", conn)
    conn.close()
    company = st.selectbox("Select company", dfc["name"].tolist())
    if company:
        data = dfc.set_index("name").loc[company]
        st.subheader(company)
        st.markdown(f"**Summary:** {data['summary']}")
        st.markdown(f"**Sector tags:** {data['sector_tags']}")
        # show saved signals
        conn = get_conn()
        sigs = pd.read_sql(
            f"SELECT headline, url, date FROM signals WHERE company='{company}'", conn
        )
        conn.close()
        st.markdown("**Headlines:**")
        for idx, row in sigs.iterrows():
            with st.expander(row['headline']):
                st.write(f"Date: {row['date']}")
                st.markdown(f"[Read Article]({row['url']})")
                contacts = company_contacts(company)
                st.markdown("**Contacts:**")
                for role, val in contacts.items():
                    st.write(f"- {role.title()}: {val or 'N/A'}")
                if st.button("Export as PDF", key=f"pdf_{idx}"):
                    pdf_path = export_pdf(company, row['headline'], contacts)
                    st.success(f"PDF saved to {pdf_path}")

elif page == "Pipeline":
    st.header("Pipeline")
    conn = get_conn()
    dfp = pd.read_sql("SELECT * FROM clients", conn)
    conn.close()
    st.dataframe(dfp)

elif page == "Permits":
    st.header("Permits")
    import pandas as pd
    df_permits = pd.read_csv("data/permits.csv")
    st.dataframe(df_permits)
```
