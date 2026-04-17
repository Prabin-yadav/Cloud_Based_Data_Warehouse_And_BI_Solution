import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db import qry, ping

PL = dict(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
          font=dict(family="Outfit",color="#f1f5f9"),margin=dict(t=36,b=36,l=10,r=10),
          xaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45"),
          yaxis=dict(gridcolor="#1e2d45",linecolor="#1e2d45"),
          legend=dict(bgcolor="rgba(0,0,0,0)"))
C=["#38bdf8","#818cf8","#22c55e","#eab308","#ef4444","#a78bfa"]


def show():
    st.markdown('<div class="page-title">ETL Monitor</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Pipeline health, audit logs, and data quality validation</div>', unsafe_allow_html=True)
    st.markdown("---")

    # ── System Health ──────────────────────────────────────────
    st.markdown('<div class="sec-hdr">SYSTEM STATUS</div>', unsafe_allow_html=True)
    c1,c2,c3,c4,c5 = st.columns(5)
    for col,sql,lbl,clr in [
        (c1,"SELECT COUNT(*) FROM fact_drug_sales",   "Sales Rows",   "#38bdf8"),
        (c2,"SELECT COUNT(*) FROM fact_inventory",    "Inventory Rows","#22c55e"),
        (c3,"SELECT COUNT(*) FROM dim_drug",           "Drugs",        "#818cf8"),
        (c4,"SELECT COUNT(*) FROM dim_customer",       "Customers",    "#eab308"),
        (c5,"SELECT COUNT(*) FROM etl_audit_log",      "ETL Runs",     "#a78bfa"),
    ]:
        df = qry(sql)
        val = int(df.iloc[0,0]) if not df.empty else 0
        col.markdown(f"""<div class="kpi-card" style="--grad:linear-gradient(90deg,{clr},{clr}88);--hover-c:{clr};--shadow:{clr}22;">
            <div class="kpi-val" style="color:{clr}">{val:,}</div>
            <div class="kpi-lbl">{lbl}</div></div>""", unsafe_allow_html=True)

    st.markdown("")
    conn_ok = ping()
    st.markdown(
        f'<div class="{"conn-ok" if conn_ok else "conn-err"}" style="display:inline-block;">{"● AWS RDS — CONNECTED & HEALTHY" if conn_ok else "● AWS RDS — CONNECTION FAILED"}</div>',
        unsafe_allow_html=True
    )
    st.markdown("")

    # ── Last ETL runs ──────────────────────────────────────────
    st.markdown('<div class="sec-hdr">RECENT ETL PIPELINE RUNS</div>', unsafe_allow_html=True)
    df_audit = qry("""
        SELECT batch_id, pipeline_step AS step, table_name AS table,
               rows_read, rows_inserted, rows_updated, rows_rejected, status,
               started_at, duration_seconds AS secs
        FROM etl_audit_log ORDER BY started_at DESC LIMIT 30
    """)
    if not df_audit.empty:
        st.dataframe(df_audit,use_container_width=True)
    else:
        st.info("No ETL runs recorded yet.")

    # ── Upload source breakdown ────────────────────────────────
    st.markdown('<div class="sec-hdr">DATA BY SOURCE SYSTEM</div>', unsafe_allow_html=True)
    df_src = qry("""
        SELECT source_system, COUNT(*) AS rows,
               CAST(SUM(net_revenue) AS FLOAT) AS revenue,
               MIN(etl_batch_id) AS first_batch, MAX(etl_batch_id) AS last_batch
        FROM fact_drug_sales GROUP BY source_system ORDER BY rows DESC
    """)
    if not df_src.empty:
        c1,c2 = st.columns(2)
        with c1:
            fig=go.Figure(go.Pie(labels=df_src["source_system"],values=df_src["rows"],hole=0.5,
                marker=dict(colors=C[:len(df_src)],line=dict(color="#07090f",width=3))))
            fig.update_layout(**PL,height=300,title="Rows by Source")
            st.plotly_chart(fig,use_container_width=True)
        with c2:
            st.dataframe(df_src,use_container_width=True)

    # ── Data Quality Check ─────────────────────────────────────
    st.markdown('<div class="sec-hdr">DATA QUALITY VALIDATION</div>', unsafe_allow_html=True)
    col1,col2 = st.columns([1,4])
    with col1:
        run_val = st.button("▶ Run All Checks",type="primary")

    if run_val:
        checks = {
            "Null drug_key in sales":           "SELECT COUNT(*) FROM fact_drug_sales WHERE drug_key IS NULL",
            "Negative net_revenue":             "SELECT COUNT(*) FROM fact_drug_sales WHERE net_revenue<0",
            "Zero units_sold":                  "SELECT COUNT(*) FROM fact_drug_sales WHERE units_sold<=0",
            "Null drug_key in inventory":       "SELECT COUNT(*) FROM fact_inventory WHERE drug_key IS NULL",
            "Duplicate sales order lines":      "SELECT COUNT(*) FROM (SELECT source_order_number,source_order_line_num,source_system,COUNT(*) FROM fact_drug_sales GROUP BY 1,2,3 HAVING COUNT(*)>1) x",
            "Drugs with null price":            "SELECT COUNT(*) FROM dim_drug WHERE unit_price IS NULL OR unit_price=0",
            "Out of stock items":               "SELECT COUNT(DISTINCT drug_key) FROM fact_inventory WHERE stock_status='Out of Stock'",
            "Customers without geography":      "SELECT COUNT(*) FROM dim_customer WHERE geography_key IS NULL",
            "Orders outside dim_date range":    "SELECT COUNT(*) FROM fact_drug_sales WHERE order_date_key NOT IN (SELECT date_key FROM dim_date)",
        }
        rows=[]
        for name,sql in checks.items():
            df=qry(sql); val=int(df.iloc[0,0]) if not df.empty else -1
            is_alert = name not in ["Out of stock items","Customers without geography"]
            status = "✅ PASS" if (val==0 and is_alert) else (f"⚠️ {val:,} found" if val>0 else "ℹ️ Info")
            rows.append({"Check":name,"Count":val,"Status":status})
        df_chk=pd.DataFrame(rows)
        st.dataframe(df_chk,use_container_width=True)
        passed = sum(1 for r in rows if "PASS" in r["Status"])
        st.info(f"**{passed}/{len(rows)} checks passed**")

    # ── Timeline of inserts ────────────────────────────────────
    st.markdown('<div class="sec-hdr">ETL RUNS TIMELINE</div>', unsafe_allow_html=True)
    df_tl = qry("""
        SELECT DATE(started_at) AS day, COUNT(*) AS runs,
               SUM(rows_inserted) AS inserted, SUM(rows_rejected) AS rejected
        FROM etl_audit_log
        WHERE started_at IS NOT NULL
        GROUP BY DATE(started_at) ORDER BY day DESC LIMIT 30
    """)
    if not df_tl.empty:
        fig2=go.Figure()
        fig2.add_trace(go.Bar(x=df_tl["day"],y=df_tl["inserted"],name="Inserted",
            marker_color="#22c55e",opacity=0.85))
        fig2.add_trace(go.Bar(x=df_tl["day"],y=df_tl["rejected"],name="Rejected",
            marker_color="#ef4444",opacity=0.85))
        fig2.update_layout(**PL,height=300,barmode="stack")
        st.plotly_chart(fig2,use_container_width=True)


if __name__ == "__main__":
    show()
