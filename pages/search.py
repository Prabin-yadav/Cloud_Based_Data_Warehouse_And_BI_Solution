import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db import qry

PL = dict(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
          font=dict(family="Outfit",color="#f1f5f9"),margin=dict(t=36,b=36,l=10,r=10),
          xaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45"),
          yaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45"),
          legend=dict(bgcolor="rgba(0,0,0,0)"))
C = ["#38bdf8","#818cf8","#22c55e","#eab308","#ef4444","#a78bfa","#34d399","#fb923c"]

def show():
    st.markdown('<div class="page-title">Search & Filter</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Find any drug, customer, or sales record from live database</div>', unsafe_allow_html=True)
    st.markdown("---")

    tab1,tab2,tab3 = st.tabs(["💊 Drug Lookup","🏥 Customer Lookup","📋 Sales Records"])

    with tab1:
        c1,c2,c3 = st.columns(3)
        with c1: dq = st.text_input("🔎 Drug Name",placeholder="e.g. Ramipril", key="sr_drug_name")
        with c2:
            cls = qry("SELECT DISTINCT therapeutic_class FROM dim_therapeutic_class ORDER BY 1")
            cl = ["All"]+(cls["therapeutic_class"].tolist() if not cls.empty else [])
            scl = st.selectbox("Therapeutic Class",cl, key="sr_therapeutic_class")
        with c3:
            frm = qry("SELECT DISTINCT dosage_form FROM dim_drug WHERE dosage_form IS NOT NULL ORDER BY 1")
            fl = ["All"]+(frm["dosage_form"].tolist() if not frm.empty else [])
            sf = st.selectbox("Dosage Form",fl, key="sr_dosage_form")

        w=["1=1"]
        if dq:    w.append(f"LOWER(d.drug_name) LIKE LOWER('%{dq}%')")
        if scl!="All": w.append(f"t.therapeutic_class='{scl}'")
        if sf!="All":  w.append(f"d.dosage_form='{sf}'")

        df_d = qry(f"""
            SELECT d.drug_code,d.drug_name,d.dosage_form,d.dosage_strength,
                   t.therapeutic_class AS class, t.regulatory_category AS regulatory,
                   CAST(d.unit_price AS FLOAT) AS price,
                   CAST(d.unit_cost  AS FLOAT) AS cost,
                   d.drug_status AS status, d.manufacturer
            FROM dim_drug d
            JOIN dim_therapeutic_class t ON d.therapeutic_class_key=t.therapeutic_class_key
            WHERE {' AND '.join(w)} ORDER BY d.drug_name LIMIT 100
        """)
        if not df_d.empty:
            st.success(f"Found **{len(df_d)}** drugs")
            st.dataframe(df_d,use_container_width=True)
            sel = st.selectbox("View full sales history for",["--"]+df_d["drug_name"].tolist(), key="sr_drug_history")
            if sel!="--":
                df_h = qry(f"""
                    SELECT d2.calendar_year AS yr, d2.month_name AS mo, d2.month_num,
                           SUM(f.units_sold) AS units,
                           CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                          CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                            ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS margin
                    FROM fact_drug_sales f JOIN dim_drug dr ON f.drug_key=dr.drug_key
                    JOIN dim_date d2 ON f.order_date_key=d2.date_key
                    WHERE dr.drug_name='{sel}'
                    GROUP BY d2.calendar_year,d2.month_num,d2.month_name ORDER BY 1,3
                """)
                if not df_h.empty:
                    c_a,c_b = st.columns(2)
                    with c_a:
                        fig=go.Figure()
                        fig.add_trace(go.Scatter(x=df_h["mo"],y=df_h["rev"],
                            mode="lines+markers",name="Revenue",
                            line=dict(color="#38bdf8",width=2),fill="tozeroy",
                            fillcolor="rgba(56,189,248,.07)"))
                        fig.update_layout(**PL,height=300,title=f"Revenue — {sel}")
                        st.plotly_chart(fig,use_container_width=True)
                    with c_b:
                        fig2=go.Figure(go.Bar(x=df_h["mo"],y=df_h["units"],
                            marker_color="#818cf8",
                            text=df_h["units"],textposition="outside"))
                        fig2.update_layout(**PL,height=300,title="Units Sold",showlegend=False)
                        st.plotly_chart(fig2,use_container_width=True)

                    df_inv = qry(f"""
                        SELECT fi.stock_status, SUM(fi.units_on_hand) AS on_hand,
                               CAST(SUM(fi.stock_value) AS FLOAT) AS value
                        FROM fact_inventory fi JOIN dim_drug dr ON fi.drug_key=dr.drug_key
                        WHERE dr.drug_name='{sel}' GROUP BY fi.stock_status
                    """)
                    if not df_inv.empty:
                        st.markdown('<div class="sec-hdr">CURRENT STOCK</div>', unsafe_allow_html=True)
                        st.dataframe(df_inv,use_container_width=True)
        else:
            st.info("No drugs found. Try clearing filters.")

    with tab2:
        c1,c2,c3 = st.columns(3)
        with c1: cq = st.text_input("🔎 Customer Name",placeholder="e.g. Hospital", key="sr_customer_name")
        with c2:
            ty = qry("SELECT DISTINCT customer_type FROM dim_customer ORDER BY 1")
            tl = ["All"]+(ty["customer_type"].tolist() if not ty.empty else [])
            st_ = st.selectbox("Type",tl, key="sr_customer_type")
        with c3:
            co = qry("SELECT DISTINCT country_region FROM dim_geography ORDER BY 1")
            col_ = ["All"]+(co["country_region"].tolist() if not co.empty else [])
            sc_ = st.selectbox("Country",col_, key="sr_country")

        wc=["1=1"]
        if cq:    wc.append(f"LOWER(c.customer_name) LIKE LOWER('%{cq}%')")
        if st_!="All": wc.append(f"c.customer_type='{st_}'")
        if sc_!="All": wc.append(f"g.country_region='{sc_}'")

        df_c = qry(f"""
            SELECT c.customer_code, c.customer_name, c.customer_type, c.customer_segment,
                   c.email, c.payment_terms, c.customer_status, g.country_region, g.city
            FROM dim_customer c LEFT JOIN dim_geography g ON c.geography_key=g.geography_key
            WHERE {' AND '.join(wc)} ORDER BY c.customer_name LIMIT 200
        """)
        if not df_c.empty:
            st.success(f"Found **{len(df_c)}** customers")
            st.dataframe(df_c,use_container_width=True)
        else:
            st.info("No customers found.")

    with tab3:
        c1,c2,c3,c4 = st.columns(4)
        with c1:
            yrs = qry("SELECT DISTINCT calendar_year FROM dim_date ORDER BY 1")
            yl = yrs["calendar_year"].tolist() if not yrs.empty else []
            if "sr_yr" not in st.session_state:
                st.session_state["sr_yr"] = yl.copy()
            else:
                current_years = st.session_state["sr_yr"]
                filtered_years = [y for y in current_years if y in yl]
                if not filtered_years and yl:
                    filtered_years = yl.copy()
                # Only write when the value actually changes to avoid rerun loops.
                if filtered_years != current_years:
                    st.session_state["sr_yr"] = filtered_years
            yr_sel = st.multiselect("Year",yl,key="sr_yr")
        with c2:
            sys_sel = st.selectbox("Source System",
                ["All","AW_INTERNET","AW_RESELLER","DAILY_UPLOAD","FRONTEND_UPLOAD"], key="sr_source_system")
        with c3: minr = st.number_input("Min Revenue",0,value=0,step=100,key="sr_min_revenue")
        with c4: maxr = st.number_input("Max Revenue",0,value=500000,step=1000,key="sr_max_revenue")

        ws=["1=1"]
        if yr_sel: ws.append(f"d.calendar_year IN ({','.join(str(y) for y in yr_sel)})")
        if sys_sel!="All": ws.append(f"f.source_system='{sys_sel}'")
        ws.append(f"f.net_revenue BETWEEN {minr} AND {maxr}")

        df_s = qry(f"""
            SELECT f.source_order_number AS order_no, d.full_date AS date,
                   dr.drug_name, c.customer_name, c.customer_type,
                   f.units_sold, CAST(f.unit_price AS FLOAT) AS price,
                   CAST(f.net_revenue AS FLOAT) AS revenue,
                   CAST(f.gross_margin_pct AS FLOAT) AS margin,
                   f.source_system
            FROM fact_drug_sales f
            JOIN dim_date d ON f.order_date_key=d.date_key
            JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_customer c ON f.customer_key=c.customer_key
            WHERE {' AND '.join(ws)}
            ORDER BY d.full_date DESC LIMIT 1000
        """)
        if not df_s.empty:
            st.success(f"Showing **{len(df_s):,}** records (max 1000)")
            st.dataframe(df_s,use_container_width=True)
        else:
            st.info("No records found for selected filters.")


if __name__ == "__main__":
    show()
