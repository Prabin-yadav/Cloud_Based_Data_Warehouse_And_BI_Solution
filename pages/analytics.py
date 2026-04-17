import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db import qry

C = ["#38bdf8","#818cf8","#22c55e","#eab308","#ef4444","#a78bfa","#34d399","#fb923c","#f472b6","#60a5fa"]
PL = dict(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
          font=dict(family="Outfit",color="#f1f5f9"),
          margin=dict(t=36,b=36,l=10,r=10),
          xaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45"),
          yaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45"),
          legend=dict(bgcolor="rgba(0,0,0,0)"))

def show():
    st.markdown('<div class="page-title">Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Deep-dive analysis with dynamic date filtering</div>', unsafe_allow_html=True)

    # Date filter in sidebar
    with st.sidebar:
        st.markdown("---")
        st.markdown('<div style="font-family:\'JetBrains Mono\';font-size:11px;color:#38bdf8;letter-spacing:2px;">DATE FILTER</div>', unsafe_allow_html=True)
        preset = st.selectbox("Analytics Date Preset",["All Time","Today","Last 7 Days","Last 30 Days",
            "Last 90 Days","Last 6 Months","This Year","Last Year","Custom"],
            label_visibility="collapsed", key="analytics_preset")
        today = date.today()
        if   preset=="Today":         s,e = today,today
        elif preset=="Last 7 Days":   s,e = today-timedelta(7),today
        elif preset=="Last 30 Days":  s,e = today-timedelta(30),today
        elif preset=="Last 90 Days":  s,e = today-timedelta(90),today
        elif preset=="Last 6 Months": s,e = today-timedelta(180),today
        elif preset=="This Year":     s,e = date(today.year,1,1),today
        elif preset=="Last Year":     s,e = date(today.year-1,1,1),date(today.year-1,12,31)
        elif preset=="Custom":
            s = st.date_input("From",date(2010,1,1),key="an_from")
            e = st.date_input("To",today,key="an_to")
        else: s,e = date(2005,1,1),today

    dw  = f"AND d.full_date BETWEEN '{s}' AND '{e}'"
    dw2 = f"AND d2.full_date BETWEEN '{s}' AND '{e}'"
    st.markdown(f"**Period:** `{s}` → `{e}`")
    st.markdown("---")

    tab1,tab2,tab3,tab4,tab5 = st.tabs([
        "📈 Sales Trends","📦 Inventory","🏥 Customers","💊 Drug Performance","🌍 Geography"
    ])

    # ════ TAB 1 ════════════════════════════════════════════════
    with tab1:
        c1,c2 = st.columns(2)
        with c1:
            st.markdown('<div class="sec-hdr">MONTHLY REVENUE vs PROFIT</div>', unsafe_allow_html=True)
            df = qry(f"""
                SELECT d.calendar_year||'-'||LPAD(d.month_num::text,2,'0') AS p,
                       CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                       CAST(SUM(f.gross_profit) AS FLOAT) AS prof,
                       SUM(f.units_sold) AS units
                FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key
                WHERE 1=1 {dw} GROUP BY d.calendar_year,d.month_num ORDER BY 1
            """)
            if not df.empty:
                fig=go.Figure()
                fig.add_trace(go.Bar(x=df["p"],y=df["rev"],name="Revenue",
                    marker_color="#38bdf8",opacity=0.85))
                fig.add_trace(go.Bar(x=df["p"],y=df["prof"],name="Profit",
                    marker_color="#22c55e",opacity=0.85))
                fig.update_layout(**PL,height=340,barmode="group")
                st.plotly_chart(fig,use_container_width=True)

        with c2:
            st.markdown('<div class="sec-hdr">QUARTERLY PERFORMANCE</div>', unsafe_allow_html=True)
            df2 = qry(f"""
                SELECT d.quarter_label AS q, d.calendar_year AS yr,
                       CAST(SUM(f.net_revenue) AS FLOAT) AS rev
                FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key
                WHERE 1=1 {dw} GROUP BY d.calendar_year,d.quarter_num,d.quarter_label
                ORDER BY d.calendar_year,d.quarter_num
            """)
            if not df2.empty:
                fig2=px.bar(df2,x="q",y="rev",color="yr",barmode="group",
                    color_discrete_sequence=C,labels={"q":"Quarter","rev":"Revenue","yr":"Year"})
                fig2.update_layout(**PL,height=340)
                st.plotly_chart(fig2,use_container_width=True)

        st.markdown('<div class="sec-hdr">YEAR-OVER-YEAR ANALYSIS</div>', unsafe_allow_html=True)
        df3=qry(f"""
            SELECT d.calendar_year AS yr,
                   CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                   CAST(SUM(f.gross_profit) AS FLOAT) AS prof,
                   COUNT(*) AS orders, SUM(f.units_sold) AS units
            FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key
            WHERE 1=1 {dw} GROUP BY d.calendar_year ORDER BY 1
        """)
        if not df3.empty:
            df3["growth"]=df3["rev"].pct_change()*100
            c3,c4=st.columns(2)
            with c3:
                fig3=go.Figure()
                fig3.add_trace(go.Scatter(x=df3["yr"],y=df3["rev"],name="Revenue",
                    mode="lines+markers+text",line=dict(color="#38bdf8",width=3),
                    marker=dict(size=12,color="#38bdf8",line=dict(color="#07090f",width=2)),
                    text=[f"${v/1e6:.1f}M" for v in df3["rev"]],textposition="top center",
                    textfont=dict(color="#94a3b8",size=11)))
                fig3.update_layout(**PL,height=320)
                st.plotly_chart(fig3,use_container_width=True)
            with c4:
                fig4=go.Figure(go.Bar(
                    x=df3["yr"],y=df3["growth"].fillna(0),
                    marker_color=["#22c55e" if v>=0 else "#ef4444" for v in df3["growth"].fillna(0)],
                    text=[f"{v:.1f}%" for v in df3["growth"].fillna(0)],textposition="outside",
                    textfont=dict(color="#94a3b8",size=11)
                ))
                fig4.update_layout(**PL,height=320,showlegend=False,yaxis_title="Growth %")
                st.plotly_chart(fig4,use_container_width=True)

        st.markdown('<div class="sec-hdr">REVENUE BY DAY OF WEEK</div>', unsafe_allow_html=True)
        df_dow=qry(f"""
            SELECT TRIM(TO_CHAR(d.full_date,'Day')) AS dow,
                   EXTRACT(DOW FROM d.full_date) AS dow_num,
                   CAST(AVG(f.net_revenue) AS FLOAT) AS avg_rev,
                   COUNT(*) AS orders
            FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key
            WHERE 1=1 {dw} GROUP BY dow,dow_num ORDER BY dow_num
        """)
        if not df_dow.empty:
            fig5=go.Figure(go.Bar(x=df_dow["dow"],y=df_dow["avg_rev"],
                marker=dict(color="#818cf8"),
                text=[f"${v:.0f}" for v in df_dow["avg_rev"]],textposition="outside"))
            fig5.update_layout(**PL,height=280,showlegend=False,
                yaxis_title="Avg Revenue per Order")
            st.plotly_chart(fig5,use_container_width=True)

    # ════ TAB 2 ════════════════════════════════════════════════
    with tab2:
        c1,c2=st.columns(2)
        sc={"In Stock":"#22c55e","Low Stock":"#eab308","Critical":"#fb923c",
            "Out of Stock":"#ef4444","Overstock":"#818cf8"}
        df_s=qry("SELECT stock_status AS s,COUNT(*) AS n,CAST(SUM(stock_value) AS FLOAT) AS v FROM fact_inventory GROUP BY s ORDER BY n DESC")

        with c1:
            st.markdown('<div class="sec-hdr">STOCK STATUS PIE</div>', unsafe_allow_html=True)
            if not df_s.empty:
                fig=go.Figure(go.Pie(labels=df_s["s"],values=df_s["n"],hole=0.55,
                    marker=dict(colors=[sc.get(x,"#64748b") for x in df_s["s"]],
                                line=dict(color="#07090f",width=3))))
                fig.update_layout(**PL,height=320)
                st.plotly_chart(fig,use_container_width=True)

        with c2:
            st.markdown('<div class="sec-hdr">STOCK VALUE BY STATUS</div>', unsafe_allow_html=True)
            if not df_s.empty:
                fig2=go.Figure(go.Bar(x=df_s["s"],y=df_s["v"],
                    marker_color=[sc.get(x,"#64748b") for x in df_s["s"]],
                    text=[f"${v:,.0f}" for v in df_s["v"]],textposition="outside"))
                fig2.update_layout(**PL,height=320,showlegend=False)
                st.plotly_chart(fig2,use_container_width=True)

        st.markdown('<div class="sec-hdr">INVENTORY MOVEMENT TREND</div>', unsafe_allow_html=True)
        df_mv=qry(f"""
            SELECT d2.calendar_year||'-'||LPAD(d2.month_num::text,2,'0') AS p,
                   SUM(fi.units_dispatched) AS dispatched,
                   SUM(fi.units_ordered) AS ordered,
                   SUM(fi.units_on_hand) AS on_hand
            FROM fact_inventory fi JOIN dim_date d2 ON fi.snapshot_date_key=d2.date_key
            WHERE 1=1 {dw2} GROUP BY d2.calendar_year,d2.month_num ORDER BY 1
        """)
        if not df_mv.empty:
            fig3=go.Figure()
            fill_map = {
                "#ef4444": "rgba(239,68,68,0.08)",
                "#22c55e": "rgba(34,197,94,0.08)",
                "#38bdf8": "rgba(56,189,248,0.08)",
            }
            for col,c,n in [("dispatched","#ef4444","Dispatched"),("ordered","#22c55e","Ordered"),("on_hand","#38bdf8","On Hand")]:
                fig3.add_trace(go.Scatter(x=df_mv["p"],y=df_mv[col],name=n,
                    line=dict(color=c,width=2),mode="lines",fill="tozeroy",
                    fillcolor=fill_map.get(c, "rgba(100,116,139,0.08)")))
            fig3.update_layout(**PL,height=320)
            st.plotly_chart(fig3,use_container_width=True)

        st.markdown('<div class="sec-hdr">🚨 STOCK ALERTS</div>', unsafe_allow_html=True)
        df_al=qry("""
            SELECT dr.drug_name AS drug, t.therapeutic_class AS cls,
                   fi.stock_status AS status, SUM(fi.units_on_hand) AS units,
                   CAST(SUM(fi.stock_value) AS FLOAT) AS value
            FROM fact_inventory fi
            JOIN dim_drug dr ON fi.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            WHERE fi.stock_status IN ('Out of Stock','Critical','Low Stock')
            GROUP BY dr.drug_name,t.therapeutic_class,fi.stock_status
            ORDER BY CASE fi.stock_status WHEN 'Out of Stock' THEN 1 WHEN 'Critical' THEN 2 ELSE 3 END
        """)
        if not df_al.empty:
            st.warning(f"⚠️ **{len(df_al)} alerts** — {(df_al['status']=='Out of Stock').sum()} Out of Stock, {(df_al['status']=='Critical').sum()} Critical")
            st.dataframe(df_al,use_container_width=True)
        else:
            st.success("✅ All stock levels are healthy!")

    # ════ TAB 3 ════════════════════════════════════════════════
    with tab3:
        c1,c2=st.columns(2)
        with c1:
            st.markdown('<div class="sec-hdr">REVENUE BY CUSTOMER TYPE</div>', unsafe_allow_html=True)
            df_ct=qry(f"""
                SELECT c.customer_type AS type,
                       CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                       COUNT(DISTINCT f.customer_key) AS custs,
                       COUNT(*) AS orders,
                       CAST(AVG(f.net_revenue) AS FLOAT) AS aov
                FROM fact_drug_sales f JOIN dim_customer c ON f.customer_key=c.customer_key
                JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
                GROUP BY c.customer_type ORDER BY rev DESC
            """)
            if not df_ct.empty:
                fig=go.Figure(go.Bar(x=df_ct["type"],y=df_ct["rev"],
                    marker_color=C[:len(df_ct)],
                    text=[f"${v:,.0f}" for v in df_ct["rev"]],textposition="outside"))
                fig.update_layout(**PL,height=320,showlegend=False)
                st.plotly_chart(fig,use_container_width=True)
                st.dataframe(df_ct.rename(columns={"type":"Type","rev":"Revenue","custs":"Customers","orders":"Orders","aov":"Avg Order Value"}),use_container_width=True)

        with c2:
            st.markdown('<div class="sec-hdr">CUSTOMER SEGMENT DONUT</div>', unsafe_allow_html=True)
            df_sg=qry(f"""
                SELECT c.customer_segment AS seg, CAST(SUM(f.net_revenue) AS FLOAT) AS rev
                FROM fact_drug_sales f JOIN dim_customer c ON f.customer_key=c.customer_key
                JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
                AND c.customer_segment IS NOT NULL GROUP BY c.customer_segment ORDER BY rev DESC
            """)
            if not df_sg.empty:
                fig2=go.Figure(go.Pie(labels=df_sg["seg"],values=df_sg["rev"],hole=0.5,
                    marker=dict(colors=C[:len(df_sg)],line=dict(color="#07090f",width=3))))
                fig2.update_layout(**PL,height=320)
                st.plotly_chart(fig2,use_container_width=True)

        st.markdown('<div class="sec-hdr">TOP 20 CUSTOMERS</div>', unsafe_allow_html=True)
        df_tc=qry(f"""
            SELECT c.customer_name AS name, c.customer_type AS type,
                   CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                   COUNT(*) AS orders,
                   CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                        ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS margin
            FROM fact_drug_sales f JOIN dim_customer c ON f.customer_key=c.customer_key
            JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
            GROUP BY c.customer_name,c.customer_type ORDER BY rev DESC LIMIT 20
        """)
        if not df_tc.empty:
            st.dataframe(df_tc,use_container_width=True)

    # ════ TAB 4 ════════════════════════════════════════════════
    with tab4:
        df_dp=qry(f"""
            SELECT dr.drug_name AS drug, t.therapeutic_class AS cls, dr.dosage_form AS form,
                   CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                   CAST(SUM(f.gross_profit) AS FLOAT) AS prof,
                 CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                   ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS mgn,
                   SUM(f.units_sold) AS units
            FROM fact_drug_sales f JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
            GROUP BY dr.drug_name,t.therapeutic_class,dr.dosage_form ORDER BY rev DESC LIMIT 25
        """)
        if not df_dp.empty:
            c1,c2=st.columns(2)
            with c1:
                st.markdown('<div class="sec-hdr">REVENUE TREEMAP BY CLASS</div>', unsafe_allow_html=True)
                fig=px.treemap(df_dp,path=["cls","drug"],values="rev",color="mgn",
                    color_continuous_scale=[[0,"#0c1a2e"],[0.5,"#0284c7"],[1,"#38bdf8"]],
                    hover_data={"units":True,"mgn":":.1f"},
                    labels={"mgn":"Margin %","rev":"Revenue"})
                fig.update_layout(**PL,height=420)
                st.plotly_chart(fig,use_container_width=True)
            with c2:
                st.markdown('<div class="sec-hdr">REVENUE vs MARGIN SCATTER</div>', unsafe_allow_html=True)
                fig2=px.scatter(df_dp,x="rev",y="mgn",size="units",color="cls",
                    hover_name="drug",color_discrete_sequence=C,
                    labels={"rev":"Revenue","mgn":"Margin %","cls":"Class"})
                fig2.update_traces(marker=dict(line=dict(width=1,color="#07090f")))
                fig2.update_layout(**PL,height=420)
                st.plotly_chart(fig2,use_container_width=True)

            st.markdown('<div class="sec-hdr">DRUG PERFORMANCE TABLE</div>', unsafe_allow_html=True)
            st.dataframe(df_dp,use_container_width=True)

    # ════ TAB 5 ════════════════════════════════════════════════
    with tab5:
        c1,c2=st.columns(2)
        with c1:
            st.markdown('<div class="sec-hdr">REVENUE BY COUNTRY</div>', unsafe_allow_html=True)
            df_g=qry(f"""
                SELECT g.country_region AS c, CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                       SUM(f.units_sold) AS units
                FROM fact_drug_sales f JOIN dim_geography g ON f.geography_key=g.geography_key
                JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
                GROUP BY g.country_region ORDER BY rev DESC
            """)
            if not df_g.empty:
                fig=go.Figure(go.Bar(x=df_g["c"],y=df_g["rev"],marker_color=C[:len(df_g)],
                    text=[f"${v:,.0f}" for v in df_g["rev"]],textposition="outside"))
                fig.update_layout(**PL,height=360,showlegend=False)
                st.plotly_chart(fig,use_container_width=True)

        with c2:
            st.markdown('<div class="sec-hdr">DISTRIBUTION ZONE SPLIT</div>', unsafe_allow_html=True)
            df_z=qry(f"""
                SELECT g.distribution_zone AS z, CAST(SUM(f.net_revenue) AS FLOAT) AS rev
                FROM fact_drug_sales f JOIN dim_geography g ON f.geography_key=g.geography_key
                JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
                GROUP BY g.distribution_zone ORDER BY rev DESC
            """)
            if not df_z.empty:
                fig2=go.Figure(go.Pie(labels=df_z["z"],values=df_z["rev"],hole=0.45,
                    marker=dict(colors=C[:len(df_z)],line=dict(color="#07090f",width=3))))
                fig2.update_layout(**PL,height=360)
                st.plotly_chart(fig2,use_container_width=True)


if __name__ == "__main__":
    show()
