import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db import qry

C = ["#38bdf8","#818cf8","#22c55e","#eab308","#ef4444","#a78bfa","#34d399","#fb923c","#f472b6","#60a5fa"]
PL = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
          font=dict(family="Outfit", color="#f1f5f9"),
          margin=dict(t=36,b=36,l=10,r=10),
          xaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45",showgrid=True),
          yaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45",showgrid=True),
          legend=dict(bgcolor="rgba(0,0,0,0)",bordercolor="rgba(0,0,0,0)"))


def date_bar():
    c1,c2,c3,c4 = st.columns([2,1.5,1.5,1])
    with c1:
        preset = st.selectbox("📅 Date Range", [
            "All Time","Today","Yesterday","Last 7 Days","Last 30 Days",
            "Last 90 Days","Last 6 Months","This Year","Last Year","Custom Range"
        ], label_visibility="collapsed")
    today = date.today()
    if   preset=="Today":         s,e = today,today
    elif preset=="Yesterday":     s,e = today-timedelta(1),today-timedelta(1)
    elif preset=="Last 7 Days":   s,e = today-timedelta(7),today
    elif preset=="Last 30 Days":  s,e = today-timedelta(30),today
    elif preset=="Last 90 Days":  s,e = today-timedelta(90),today
    elif preset=="Last 6 Months": s,e = today-timedelta(180),today
    elif preset=="This Year":     s,e = date(today.year,1,1),today
    elif preset=="Last Year":     s,e = date(today.year-1,1,1),date(today.year-1,12,31)
    elif preset=="Custom Range":
        with c2: s = st.date_input("From", value=date(2010,1,1), label_visibility="collapsed")
        with c3: e = st.date_input("To",   value=today,          label_visibility="collapsed")
    else: s,e = date(2005,1,1),today
    with c4:
        st.markdown(f"""<div style="background:#0d1117;border:1px solid #1e2d45;border-radius:8px;
            padding:8px 12px;font-family:'JetBrains Mono';font-size:11px;color:#64748b;margin-top:2px;">
            {s} → {e}</div>""", unsafe_allow_html=True)
    return s, e


def kpi_card(val, lbl, sub="", color="#38bdf8", grad="linear-gradient(90deg,#38bdf8,#818cf8)", shadow="rgba(56,189,248,.15)"):
    return f"""<div class="kpi-card" style="--grad:{grad};--hover-c:{color};--shadow:{shadow};">
        <div class="kpi-val" style="color:{color}">{val}</div>
        <div class="kpi-lbl">{lbl}</div>
        {"<div class='kpi-sub' style='color:#64748b'>"+sub+"</div>" if sub else ""}
    </div>"""


def show():
    st.markdown("""
    <style>
    .hero-wrap {
        position: relative;
        border: 1px solid #1e2d45;
        border-radius: 16px;
        padding: 22px 22px 18px;
        margin-bottom: 12px;
        background:
          radial-gradient(1200px 380px at 10% -20%, rgba(56,189,248,.20), rgba(56,189,248,0) 45%),
          radial-gradient(1000px 320px at 90% -30%, rgba(129,140,248,.18), rgba(129,140,248,0) 48%),
          linear-gradient(135deg, #0d1117 0%, #0b1222 55%, #0d1117 100%);
        overflow: hidden;
    }
    .hero-kicker {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        letter-spacing: 2.4px;
        text-transform: uppercase;
        color: #38bdf8;
        margin-bottom: 8px;
    }
    .hero-title {
        font-size: 34px;
        line-height: 1.12;
        font-weight: 700;
        color: #f8fafc;
        margin: 0;
    }
    .hero-sub {
        margin-top: 8px;
        color: #94a3b8;
        font-size: 14px;
    }
    .hero-chip-row {
        margin-top: 14px;
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }
    .hero-chip {
        font-size: 11px;
        color: #cbd5e1;
        border: 1px solid #243447;
        background: rgba(15, 23, 42, .65);
        border-radius: 999px;
        padding: 5px 10px;
    }
    .insight-strip {
        margin: 14px 0 4px;
        border: 1px solid #1e2d45;
        border-radius: 14px;
        background: linear-gradient(180deg, rgba(15,23,42,.55), rgba(10,15,26,.55));
        padding: 12px;
    }
    .insight-card {
        border: 1px solid #22324f;
        border-radius: 12px;
        padding: 12px;
        background: rgba(15,23,42,.45);
        min-height: 76px;
    }
    .insight-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 10px;
        letter-spacing: 1.2px;
        text-transform: uppercase;
        color: #64748b;
    }
    .insight-value {
        margin-top: 4px;
        font-size: 22px;
        font-weight: 700;
        color: #f8fafc;
        line-height: 1.05;
    }
    .insight-note {
        margin-top: 4px;
        font-size: 12px;
        color: #94a3b8;
    }
    </style>
    """, unsafe_allow_html=True)

    s, e = date_bar()
    dw = f"AND d.full_date BETWEEN '{s}' AND '{e}'"
    st.markdown("---")

    # ── KPIs ──────────────────────────────────────────────────
    kpi = qry(f"""
        SELECT CAST(SUM(f.net_revenue)       AS FLOAT) AS rev,
               CAST(SUM(f.gross_profit)      AS FLOAT) AS prof,
               CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                    ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS mgn,
               COUNT(*)                                AS orders,
               SUM(f.units_sold)                       AS units,
               COUNT(DISTINCT f.customer_key)          AS custs,
               COUNT(DISTINCT f.drug_key)              AS drugs
        FROM fact_drug_sales f
        JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
    """)
    oos = qry("SELECT COUNT(*) AS n FROM fact_inventory WHERE stock_status='Out of Stock'")
    low = qry("SELECT COUNT(*) AS n FROM fact_inventory WHERE stock_status IN ('Low Stock','Critical')")

    if not kpi.empty and kpi["rev"].iloc[0]:
        rev   = float(kpi["rev"].iloc[0] or 0)
        prof  = float(kpi["prof"].iloc[0] or 0)
        mgn   = float(kpi["mgn"].iloc[0] or 0)
        ords  = int(kpi["orders"].iloc[0] or 0)
        units = int(kpi["units"].iloc[0] or 0)
        custs = int(kpi["custs"].iloc[0] or 0)
        drugs = int(kpi["drugs"].iloc[0] or 0)
        oos_n = int(oos["n"].iloc[0] if not oos.empty else 0)
        low_n = int(low["n"].iloc[0] if not low.empty else 0)

        r1 = st.columns(4)
        cards1 = [
            (f"${rev:,.0f}",  "Total Revenue",  f"Gross ${rev+rev*0.1:,.0f}", "#38bdf8","linear-gradient(90deg,#38bdf8,#0284c7)","rgba(56,189,248,.12)"),
            (f"${prof:,.0f}", "Gross Profit",   f"{mgn:.1f}% margin",        "#22c55e","linear-gradient(90deg,#22c55e,#16a34a)","rgba(34,197,94,.12)"),
            (f"{mgn:.1f}%",   "Avg Gross Margin","Target >50%",              "#818cf8","linear-gradient(90deg,#818cf8,#6366f1)","rgba(129,140,248,.12)"),
            (f"{ords:,}",     "Total Orders",   f"{units:,} units",          "#eab308","linear-gradient(90deg,#eab308,#ca8a04)","rgba(234,179,8,.12)"),
        ]
        for col,(v,l,s2,c,g,sh) in zip(r1,cards1):
            col.markdown(kpi_card(v,l,s2,c,g,sh), unsafe_allow_html=True)

        st.markdown("")
        r2 = st.columns(4)
        cards2 = [
            (f"{custs:,}",  "Active Customers","Unique buyers",            "#a78bfa","linear-gradient(90deg,#a78bfa,#7c3aed)","rgba(167,139,250,.12)"),
            (f"{drugs:,}",  "Drugs Sold",      "Distinct SKUs",            "#34d399","linear-gradient(90deg,#34d399,#059669)","rgba(52,211,153,.12)"),
            (f"{oos_n:,}",  "Out of Stock",    "⚠ Immediate action",       "#ef4444","linear-gradient(90deg,#ef4444,#dc2626)","rgba(239,68,68,.12)"),
            (f"{low_n:,}",  "Low / Critical",  "⚠ Monitor closely",        "#fb923c","linear-gradient(90deg,#fb923c,#ea580c)","rgba(251,146,60,.12)"),
        ]
        for col,(v,l,s2,c,g,sh) in zip(r2,cards2):
            col.markdown(kpi_card(v,l,s2,c,g,sh), unsafe_allow_html=True)

        st.markdown('<div class="insight-strip">', unsafe_allow_html=True)
        i1, i2, i3 = st.columns(3)
        avg_order_value = rev / ords if ords else 0
        profit_per_unit = prof / units if units else 0
        with i1:
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-label">Average Order Value</div>
                <div class="insight-value">${avg_order_value:,.0f}</div>
                <div class="insight-note">Revenue per order in selected period</div>
            </div>
            """, unsafe_allow_html=True)
        with i2:
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-label">Profit Per Unit</div>
                <div class="insight-value">${profit_per_unit:,.2f}</div>
                <div class="insight-note">Gross profit generated per unit sold</div>
            </div>
            """, unsafe_allow_html=True)
        with i3:
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-label">Inventory Risk Ratio</div>
                <div class="insight-value">{(low_n+oos_n):,}</div>
                <div class="insight-note">SKUs requiring active stock attention</div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.warning("No sales data for the selected date range. Try **All Time**.")

    st.markdown("---")

    # ── Revenue Trend + Top Drugs ──────────────────────────────
    st.markdown('<div class="sec-hdr">REVENUE & PROFITABILITY TRENDS</div>', unsafe_allow_html=True)
    c1,c2 = st.columns([3,2])

    with c1:
        df_t = qry(f"""
            SELECT d.calendar_year||'-'||LPAD(d.month_num::text,2,'0') AS period,
                   d.month_num, d.calendar_year,
                   CAST(SUM(f.net_revenue)  AS FLOAT) AS revenue,
                   CAST(SUM(f.gross_profit) AS FLOAT) AS profit
            FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key
            WHERE 1=1 {dw} GROUP BY d.calendar_year,d.month_num ORDER BY 1
        """)
        if not df_t.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df_t["period"],y=df_t["revenue"],name="Revenue",
                line=dict(color="#38bdf8",width=2.5),fill="tozeroy",fillcolor="rgba(56,189,248,.07)",
                mode="lines+markers",marker=dict(size=4,color="#38bdf8")))
            fig.add_trace(go.Scatter(x=df_t["period"],y=df_t["profit"],name="Profit",
                line=dict(color="#22c55e",width=2.5),fill="tozeroy",fillcolor="rgba(34,197,94,.07)",
                mode="lines+markers",marker=dict(size=4,color="#22c55e")))
            fig.update_layout(**PL,height=320)
            st.plotly_chart(fig,use_container_width=True)

    with c2:
        df_top = qry(f"""
            SELECT dr.drug_name AS drug, CAST(SUM(f.net_revenue) AS FLOAT) AS rev
            FROM fact_drug_sales f JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
            GROUP BY dr.drug_name ORDER BY rev DESC LIMIT 8
        """)
        if not df_top.empty:
            fig2 = go.Figure(go.Bar(
                x=df_top["rev"], y=df_top["drug"], orientation="h",
                marker=dict(color=df_top["rev"],colorscale=[[0,"#0c1a2e"],[1,"#38bdf8"]],
                            line=dict(color="rgba(56,189,248,.3)",width=1)),
                text=[f"${v:,.0f}" for v in df_top["rev"]],
                textposition="outside",textfont=dict(color="#94a3b8",size=10)
            ))
            fig2.update_layout(**PL, height=320, showlegend=False)
            fig2.update_yaxes(autorange="reversed")
            st.plotly_chart(fig2,use_container_width=True)

    # ── Customer Type + Therapeutic Class ─────────────────────
    st.markdown('<div class="sec-hdr">CUSTOMER & THERAPEUTIC CLASS BREAKDOWN</div>', unsafe_allow_html=True)
    c3,c4 = st.columns(2)

    with c3:
        df_ct = qry(f"""
            SELECT c.customer_type AS type, CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
                   COUNT(*) AS orders
            FROM fact_drug_sales f JOIN dim_customer c ON f.customer_key=c.customer_key
            JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
            GROUP BY c.customer_type ORDER BY rev DESC
        """)
        if not df_ct.empty:
            fig3 = go.Figure(go.Pie(
                labels=df_ct["type"],values=df_ct["rev"],hole=0.58,
                marker=dict(colors=C[:len(df_ct)],line=dict(color="#07090f",width=3)),
                textfont=dict(size=12,family="Outfit")
            ))
            fig3.update_layout(**PL,height=320,
                annotations=[dict(text="Revenue",x=0.5,y=0.5,showarrow=False,
                    font=dict(size=13,color="#64748b",family="JetBrains Mono"))])
            st.plotly_chart(fig3,use_container_width=True)

    with c4:
        df_cls = qry(f"""
            SELECT t.therapeutic_class AS cls, CAST(SUM(f.net_revenue) AS FLOAT) AS rev
            FROM fact_drug_sales f JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
            GROUP BY t.therapeutic_class ORDER BY rev DESC
        """)
        if not df_cls.empty:
            fig4 = go.Figure(go.Bar(
                x=df_cls["cls"],y=df_cls["rev"],
                marker=dict(color=C[:len(df_cls)],line=dict(color="rgba(0,0,0,0)")),
                text=[f"${v/1e6:.1f}M" for v in df_cls["rev"]],
                textposition="outside",textfont=dict(color="#94a3b8",size=11)
            ))
            fig4.update_layout(**PL,height=320,showlegend=False)
            st.plotly_chart(fig4,use_container_width=True)

    # ── Stock Status + Inventory Health ───────────────────────
    st.markdown('<div class="sec-hdr">INVENTORY HEALTH</div>', unsafe_allow_html=True)
    c5,c6 = st.columns(2)

    with c5:
        df_ss = qry("""
            SELECT stock_status AS s, COUNT(*) AS n, CAST(SUM(stock_value) AS FLOAT) AS v
            FROM fact_inventory GROUP BY s ORDER BY n DESC
        """)
        if not df_ss.empty:
            sc = {"In Stock":"#22c55e","Low Stock":"#eab308","Critical":"#fb923c",
                  "Out of Stock":"#ef4444","Overstock":"#818cf8"}
            fig5 = go.Figure(go.Pie(
                labels=df_ss["s"],values=df_ss["n"],hole=0.55,
                marker=dict(colors=[sc.get(x,"#64748b") for x in df_ss["s"]],
                            line=dict(color="#07090f",width=3))
            ))
            fig5.update_layout(**PL,height=300)
            st.plotly_chart(fig5,use_container_width=True)

    with c6:
        if not df_ss.empty:
            sc = {"In Stock":"#22c55e","Low Stock":"#eab308","Critical":"#fb923c",
                  "Out of Stock":"#ef4444","Overstock":"#818cf8"}
            fig6 = go.Figure(go.Bar(
                x=df_ss["s"],y=df_ss["v"],
                marker_color=[sc.get(x,"#64748b") for x in df_ss["s"]],
                text=[f"${v:,.0f}" for v in df_ss["v"]],
                textposition="outside",textfont=dict(color="#94a3b8",size=11)
            ))
            fig6.update_layout(**PL,height=300,showlegend=False,
                               xaxis_title="Stock Status",yaxis_title="Total Value (USD)")
            st.plotly_chart(fig6,use_container_width=True)

    # ── Revenue vs Margin scatter + YoY ───────────────────────
    st.markdown('<div class="sec-hdr">DRUG PERFORMANCE MATRIX</div>', unsafe_allow_html=True)
    df_sc = qry(f"""
        SELECT dr.drug_name AS drug, t.therapeutic_class AS cls,
               CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
             CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                  ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS mgn,
               SUM(f.units_sold) AS units
        FROM fact_drug_sales f JOIN dim_drug dr ON f.drug_key=dr.drug_key
        JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
        JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
        GROUP BY dr.drug_name,t.therapeutic_class ORDER BY rev DESC LIMIT 40
    """)
    if not df_sc.empty:
        fig7 = px.scatter(df_sc,x="rev",y="mgn",size="units",color="cls",
            hover_name="drug",color_discrete_sequence=C,
            labels={"rev":"Revenue (USD)","mgn":"Gross Margin %","cls":"Therapeutic Class"})
        fig7.update_traces(marker=dict(line=dict(width=1,color="#07090f")))
        fig7.update_layout(**PL,height=400)
        st.plotly_chart(fig7,use_container_width=True)

    # ── Country + territory ────────────────────────────────────
    st.markdown('<div class="sec-hdr">GEOGRAPHIC DISTRIBUTION</div>', unsafe_allow_html=True)
    c7,c8 = st.columns(2)
    with c7:
        df_geo = qry(f"""
            SELECT g.country_region AS country, SUM(f.units_sold) AS units,
                   CAST(SUM(f.net_revenue) AS FLOAT) AS rev
            FROM fact_drug_sales f JOIN dim_geography g ON f.geography_key=g.geography_key
            JOIN dim_date d ON f.order_date_key=d.date_key WHERE 1=1 {dw}
            GROUP BY g.country_region ORDER BY rev DESC
        """)
        if not df_geo.empty:
            fig8 = go.Figure(go.Bar(
                x=df_geo["country"],y=df_geo["rev"],
                marker=dict(color=C[:len(df_geo)]),
                text=[f"${v:,.0f}" for v in df_geo["rev"]],
                textposition="outside",textfont=dict(color="#94a3b8",size=11)
            ))
            fig8.update_layout(**PL,height=320,showlegend=False,xaxis_title="Country",yaxis_title="Revenue")
            st.plotly_chart(fig8,use_container_width=True)

    with c8:
        df_yr = qry(f"""
            SELECT d.calendar_year AS yr, CAST(SUM(f.net_revenue) AS FLOAT) AS rev
            FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key
            WHERE 1=1 {dw} GROUP BY d.calendar_year ORDER BY 1
        """)
        if not df_yr.empty:
            df_yr["growth"] = df_yr["rev"].pct_change()*100
            fig9 = go.Figure()
            fig9.add_trace(go.Bar(x=df_yr["yr"],y=df_yr["rev"],name="Revenue",
                marker_color="#38bdf8",opacity=0.8,yaxis="y"))
            fig9.add_trace(go.Scatter(x=df_yr["yr"],
                y=df_yr["growth"].fillna(0),name="Growth %",
                line=dict(color="#eab308",width=2.5),mode="lines+markers",
                marker=dict(size=8),yaxis="y2"))
            fig9.update_layout(**PL, height=320)
            fig9.update_yaxes(title_text="Revenue")
            fig9.update_layout(
                legend=dict(x=0, y=1, bgcolor="rgba(0,0,0,0)"),
                yaxis2=dict(title="Growth %", overlaying="y", side="right",
                            gridcolor="#1e2d45", linecolor="#1e2d45")
            )
            st.plotly_chart(fig9,use_container_width=True)


if __name__ == "__main__":
    show()
