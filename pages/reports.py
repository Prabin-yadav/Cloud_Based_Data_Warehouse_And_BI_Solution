import streamlit as st
import pandas as pd
import io
from datetime import date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db import qry


def to_excel(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    return buf.getvalue()


def show():
    st.markdown('<div class="page-title">Download Reports</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Generate filtered reports — export as CSV or Excel</div>', unsafe_allow_html=True)
    st.markdown("---")

    c1,c2,c3 = st.columns([2,1.5,1.5])
    today = date.today()
    with c1:
        preset = st.selectbox("Date Range",
            ["All Time","Last 30 Days","Last 90 Days","This Year","Last Year","Custom"])
    if   preset=="Last 30 Days": s,e = today-timedelta(30),today
    elif preset=="Last 90 Days": s,e = today-timedelta(90),today
    elif preset=="This Year":    s,e = date(today.year,1,1),today
    elif preset=="Last Year":    s,e = date(today.year-1,1,1),date(today.year-1,12,31)
    elif preset=="Custom":
        with c2: s = st.date_input("From",date(2010,1,1))
        with c3: e = st.date_input("To",today)
    else: s,e = date(2005,1,1),today
    dw = f"AND d.full_date BETWEEN '{s}' AND '{e}'"
    st.markdown(f"**Period:** `{s}` → `{e}`")
    st.markdown("---")

    REPS = [
        ("📊 Sales Summary Report","sales_summary", f"""
            SELECT d.calendar_year AS year, d.month_name AS month, dr.drug_name AS drug,
                   t.therapeutic_class AS class, c.customer_type AS cust_type,
                   SUM(f.units_sold) AS units,
                   CAST(SUM(f.gross_revenue)  AS FLOAT) AS gross_rev,
                   CAST(SUM(f.net_revenue)    AS FLOAT) AS net_rev,
                   CAST(SUM(f.gross_profit)   AS FLOAT) AS profit,
                 CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                   ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS avg_margin
            FROM fact_drug_sales f
            JOIN dim_date d ON f.order_date_key=d.date_key
            JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            JOIN dim_customer c ON f.customer_key=c.customer_key
            WHERE 1=1 {dw}
            GROUP BY d.calendar_year,d.month_name,d.month_num,dr.drug_name,t.therapeutic_class,c.customer_type
            ORDER BY d.calendar_year,d.month_num,net_rev DESC
        """),
        ("📦 Inventory Status Report","inventory_status", """
            SELECT dr.drug_code, dr.drug_name, dr.dosage_form,
                   t.therapeutic_class AS class, fi.stock_status,
                   SUM(fi.units_on_hand) AS on_hand,
                   SUM(fi.units_ordered) AS ordered,
                   CAST(SUM(fi.stock_value) AS FLOAT) AS value,
                   CAST(AVG(fi.days_of_supply) AS FLOAT) AS days_supply
            FROM fact_inventory fi
            JOIN dim_drug dr ON fi.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            GROUP BY dr.drug_code,dr.drug_name,dr.dosage_form,t.therapeutic_class,fi.stock_status
            ORDER BY fi.stock_status,dr.drug_name
        """),
        ("🚨 Stock Alerts Report","stock_alerts", """
            SELECT dr.drug_name, t.therapeutic_class AS class, fi.stock_status,
                   SUM(fi.units_on_hand) AS units,
                   CAST(SUM(fi.stock_value) AS FLOAT) AS value
            FROM fact_inventory fi
            JOIN dim_drug dr ON fi.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            WHERE fi.stock_status IN ('Out of Stock','Critical','Low Stock')
            GROUP BY dr.drug_name,t.therapeutic_class,fi.stock_status
            ORDER BY CASE fi.stock_status WHEN 'Out of Stock' THEN 1 WHEN 'Critical' THEN 2 ELSE 3 END,dr.drug_name
        """),
        ("🏥 Customer Revenue Report","customer_revenue", f"""
            SELECT c.customer_name, c.customer_type, c.customer_segment,
                   g.country_region AS country, g.city,
                   COUNT(*) AS orders, SUM(f.units_sold) AS units,
                   CAST(SUM(f.net_revenue) AS FLOAT) AS revenue,
                 CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                   ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS avg_margin
            FROM fact_drug_sales f
            JOIN dim_customer c ON f.customer_key=c.customer_key
            LEFT JOIN dim_geography g ON c.geography_key=g.geography_key
            JOIN dim_date d ON f.order_date_key=d.date_key
            WHERE 1=1 {dw}
            GROUP BY c.customer_name,c.customer_type,c.customer_segment,g.country_region,g.city
            ORDER BY revenue DESC
        """),
        ("📅 Monthly Revenue Report","monthly_revenue", f"""
            SELECT d.calendar_year AS year, d.quarter_label AS quarter, d.month_name AS month,
                   COUNT(*) AS orders, SUM(f.units_sold) AS units,
                   CAST(SUM(f.gross_revenue) AS FLOAT) AS gross,
                   CAST(SUM(f.net_revenue)   AS FLOAT) AS net,
                   CAST(SUM(f.gross_profit)  AS FLOAT) AS profit,
                 CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                   ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS margin
            FROM fact_drug_sales f
            JOIN dim_date d ON f.order_date_key=d.date_key
            JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            JOIN dim_customer c ON f.customer_key=c.customer_key
            WHERE 1=1 {dw}
            GROUP BY d.calendar_year,d.quarter_label,d.month_name,d.month_num
            ORDER BY d.calendar_year,d.month_num
        """),
        ("💊 Drug Performance Report","drug_performance", f"""
            SELECT dr.drug_name, dr.dosage_form, dr.dosage_strength,
                   t.therapeutic_class AS class,
                   SUM(f.units_sold) AS units,
                   CAST(SUM(f.net_revenue)  AS FLOAT) AS revenue,
                   CAST(SUM(f.gross_profit) AS FLOAT) AS profit,
                 CAST(CASE WHEN SUM(f.net_revenue)=0 THEN 0
                   ELSE SUM(f.gross_profit)*100.0/SUM(f.net_revenue) END AS FLOAT) AS avg_margin,
                   COUNT(DISTINCT f.customer_key) AS customers
            FROM fact_drug_sales f
            JOIN dim_drug dr ON f.drug_key=dr.drug_key
            JOIN dim_therapeutic_class t ON dr.therapeutic_class_key=t.therapeutic_class_key
            JOIN dim_date d ON f.order_date_key=d.date_key
            WHERE 1=1 {dw}
            GROUP BY dr.drug_name,dr.dosage_form,dr.dosage_strength,t.therapeutic_class
            ORDER BY revenue DESC
        """),
    ]

    for title, key, sql in REPS:
        with st.expander(f"**{title}**"):
            ca,cb = st.columns([1,4])
            with ca:
                if st.button("Generate", key=f"gen_{key}"):
                    st.session_state[f"r_{key}"] = qry(sql)
            if f"r_{key}" in st.session_state:
                df = st.session_state[f"r_{key}"]
                with cb: st.metric("Rows",f"{len(df):,}")
                c1,c2 = st.columns(2)
                with c1:
                    st.download_button("⬇️ CSV", df.to_csv(index=False),
                        f"zentrik_{key}.csv","text/csv",key=f"csv_{key}")
                with c2:
                    st.download_button("⬇️ Excel", to_excel(df),
                        f"zentrik_{key}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"xl_{key}")
                st.dataframe(df,use_container_width=True)


if __name__ == "__main__":
    show()
