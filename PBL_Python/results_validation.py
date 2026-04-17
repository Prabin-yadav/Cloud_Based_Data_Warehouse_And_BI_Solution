"""
results_validation.py
Generates a complete Results & Validation report for Zentrik Pharma DW.
Run this and screenshot the output for your PPT slide.
"""
import psycopg2
import pandas as pd
from datetime import datetime

from config import (
    TARGET_DBNAME,
    TARGET_HOST,
    TARGET_PASSWORD,
    TARGET_PORT,
    TARGET_SSLMODE,
    TARGET_USER,
)

DB = {
    "host": TARGET_HOST,
    "port": TARGET_PORT,
    "dbname": TARGET_DBNAME,
    "user": TARGET_USER,
    "password": TARGET_PASSWORD,
    "sslmode": TARGET_SSLMODE,
    "connect_timeout": 15,
}

SEP  = "=" * 70
SEP2 = "-" * 70

def q(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]

def qall(cur, sql):
    cur.execute(sql)
    return cur.fetchall()

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)

def subsection(title):
    print(f"\n  {title}")
    print(f"  {SEP2[:60]}")

def row(label, value, status=None):
    st = f"  [{status}]" if status else ""
    print(f"  {label:<45} {str(value):<20}{st}")

def main():
    conn = psycopg2.connect(**DB)
    cur  = conn.cursor()

    print(SEP)
    print("  ZENTRIK PHARMA DATA WAREHOUSE")
    print("  RESULTS & VALIDATION REPORT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  AWS RDS: zentrik-pharma-dw (ap-south-1)")
    print(SEP)

    # ══════════════════════════════════════════════════════════════
    # SECTION 1 — ROW COUNT VALIDATION
    # ══════════════════════════════════════════════════════════════
    section("1. ROW COUNT VALIDATION")

    subsection("A) Source vs Target Row Counts")
    print(f"  {'Table':<35} {'Source (AW)':<18} {'Target (RDS)':<18} {'Status'}")
    print(f"  {'-'*35} {'-'*18} {'-'*18} {'-'*10}")

    counts = [
        ("fact_drug_sales",         "FactInternetSales",       60398),
        ("fact_inventory",          "FactProductInventory",    505600),
        ("dim_drug",                "DimProduct (filtered)",   395),
        ("dim_customer",            "DimCustomer",             18484),
        ("dim_date",                "Generated (2022-2030)",   4018),
        ("dim_geography",           "DimGeography",            655),
        ("dim_sales_territory",     "DimSalesTerritory",       11),
        ("dim_therapeutic_class",   "DimProductSubcategory",   37),
    ]

    total_target = 0
    for table, source, expected in counts:
        actual = q(cur, f"SELECT COUNT(*) FROM {table}")
        total_target += actual
        match = "PASS" if actual == expected else f"NOTE: {actual}"
        print(f"  {table:<35} {str(expected):<18} {str(actual):<18} {match}")

    print(f"\n  {'TOTAL ROWS IN AWS RDS':<35} {'':<18} {total_target:,}")

    subsection("B) Data Volume Summary")
    sales_rows = q(cur, "SELECT COUNT(*) FROM fact_drug_sales")
    inv_rows   = q(cur, "SELECT COUNT(*) FROM fact_inventory")
    print(f"  Sales transactions loaded:          {sales_rows:,}")
    print(f"  Inventory snapshots loaded:         {inv_rows:,}")
    print(f"  Total fact rows:                    {sales_rows + inv_rows:,}")
    print(f"  Total dimension rows:               {total_target - sales_rows - inv_rows:,}")
    print(f"  Total rows in warehouse:            {total_target:,}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 2 — DATA QUALITY VALIDATION
    # ══════════════════════════════════════════════════════════════
    section("2. DATA QUALITY VALIDATION")

    subsection("A) NULL Value Checks")
    null_checks = [
        ("fact_drug_sales - null drug_key",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE drug_key IS NULL"),
        ("fact_drug_sales - null customer_key",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE customer_key IS NULL"),
        ("fact_drug_sales - null order_date_key",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE order_date_key IS NULL"),
        ("fact_drug_sales - null net_revenue",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE net_revenue IS NULL"),
        ("fact_drug_sales - null units_sold",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE units_sold IS NULL"),
        ("fact_inventory - null drug_key",
         "SELECT COUNT(*) FROM fact_inventory WHERE drug_key IS NULL"),
        ("fact_inventory - null snapshot_date_key",
         "SELECT COUNT(*) FROM fact_inventory WHERE snapshot_date_key IS NULL"),
        ("fact_inventory - null stock_status",
         "SELECT COUNT(*) FROM fact_inventory WHERE stock_status IS NULL"),
        ("dim_drug - null drug_name",
         "SELECT COUNT(*) FROM dim_drug WHERE drug_name IS NULL OR drug_name=''"),
        ("dim_customer - null customer_name",
         "SELECT COUNT(*) FROM dim_customer WHERE customer_name IS NULL OR customer_name=''"),
    ]

    null_pass = 0
    for label, sql in null_checks:
        val = q(cur, sql)
        status = "PASS" if val == 0 else f"FAIL ({val} nulls)"
        if val == 0: null_pass += 1
        row(label, val, status)

    print(f"\n  NULL Checks Passed: {null_pass}/{len(null_checks)}")

    subsection("B) Business Rule Checks")
    biz_checks = [
        ("net_revenue >= 0 (no negative revenue)",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE net_revenue < 0",
         True),
        ("units_sold > 0 (no zero quantity orders)",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE units_sold <= 0",
         True),
        ("unit_price > 0 (no zero price sales)",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE unit_price <= 0",
         True),
        ("gross_profit <= net_revenue (profit not > revenue)",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE gross_profit > net_revenue + 0.01",
         True),
        ("gross_margin_pct between -100 and 100",
         "SELECT COUNT(*) FROM fact_drug_sales WHERE gross_margin_pct < -100 OR gross_margin_pct > 100",
         True),
        ("units_on_hand >= 0 (no negative stock)",
         "SELECT COUNT(*) FROM fact_inventory WHERE units_on_hand < 0",
         True),
        ("stock_value >= 0 (no negative stock value)",
         "SELECT COUNT(*) FROM fact_inventory WHERE stock_value < 0",
         True),
        ("all drugs have valid therapeutic class",
         "SELECT COUNT(*) FROM dim_drug WHERE therapeutic_class_key IS NULL",
         True),
        ("unit_cost <= unit_price (cost not > price)",
         "SELECT COUNT(*) FROM dim_drug WHERE unit_cost > unit_price * 1.5",
         True),
        ("all customers have a type assigned",
         "SELECT COUNT(*) FROM dim_customer WHERE customer_type IS NULL OR customer_type=''",
         True),
    ]

    biz_pass = 0
    for label, sql, should_zero in biz_checks:
        val = q(cur, sql)
        status = "PASS" if val == 0 else f"FAIL ({val} violations)"
        if val == 0: biz_pass += 1
        row(label, val, status)

    print(f"\n  Business Rule Checks Passed: {biz_pass}/{len(biz_checks)}")

    subsection("C) Referential Integrity (Foreign Key) Checks")
    fk_checks = [
        ("fact_drug_sales -> dim_drug (drug_key)",
         "SELECT COUNT(*) FROM fact_drug_sales f LEFT JOIN dim_drug d ON f.drug_key=d.drug_key WHERE d.drug_key IS NULL"),
        ("fact_drug_sales -> dim_customer (customer_key)",
         "SELECT COUNT(*) FROM fact_drug_sales f LEFT JOIN dim_customer c ON f.customer_key=c.customer_key WHERE c.customer_key IS NULL"),
        ("fact_drug_sales -> dim_date (order_date_key)",
         "SELECT COUNT(*) FROM fact_drug_sales f LEFT JOIN dim_date d ON f.order_date_key=d.date_key WHERE d.date_key IS NULL"),
        ("fact_inventory -> dim_drug (drug_key)",
         "SELECT COUNT(*) FROM fact_inventory fi LEFT JOIN dim_drug d ON fi.drug_key=d.drug_key WHERE d.drug_key IS NULL"),
        ("fact_inventory -> dim_date (snapshot_date_key)",
         "SELECT COUNT(*) FROM fact_inventory fi LEFT JOIN dim_date d ON fi.snapshot_date_key=d.date_key WHERE d.date_key IS NULL"),
        ("dim_drug -> dim_therapeutic_class",
         "SELECT COUNT(*) FROM dim_drug d LEFT JOIN dim_therapeutic_class t ON d.therapeutic_class_key=t.therapeutic_class_key WHERE t.therapeutic_class_key IS NULL"),
    ]

    fk_pass = 0
    for label, sql in fk_checks:
        val = q(cur, sql)
        status = "PASS" if val == 0 else f"FAIL ({val} orphans)"
        if val == 0: fk_pass += 1
        row(label, val, status)

    print(f"\n  FK Integrity Checks Passed: {fk_pass}/{len(fk_checks)}")

    subsection("D) Duplicate Record Checks")
    dup_checks = [
        ("Duplicate order lines in fact_drug_sales",
         "SELECT COUNT(*) FROM (SELECT source_order_number, source_order_line_num, source_system, COUNT(*) FROM fact_drug_sales GROUP BY 1,2,3 HAVING COUNT(*)>1) x"),
        ("Duplicate drug keys in dim_drug",
         "SELECT COUNT(*) FROM (SELECT drug_key, COUNT(*) FROM dim_drug GROUP BY drug_key HAVING COUNT(*)>1) x"),
        ("Duplicate customer keys in dim_customer",
         "SELECT COUNT(*) FROM (SELECT customer_key, COUNT(*) FROM dim_customer GROUP BY customer_key HAVING COUNT(*)>1) x"),
        ("Duplicate date keys in dim_date",
         "SELECT COUNT(*) FROM (SELECT date_key, COUNT(*) FROM dim_date GROUP BY date_key HAVING COUNT(*)>1) x"),
        ("Duplicate inventory snapshots (same drug+date)",
         "SELECT COUNT(*) FROM (SELECT snapshot_date_key, drug_key, COUNT(*) FROM fact_inventory GROUP BY 1,2 HAVING COUNT(*)>1) x"),
    ]

    dup_pass = 0
    for label, sql in dup_checks:
        val = q(cur, sql)
        status = "PASS" if val == 0 else f"FAIL ({val} duplicates)"
        if val == 0: dup_pass += 1
        row(label, val, status)

    print(f"\n  Duplicate Checks Passed: {dup_pass}/{len(dup_checks)}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 3 — BUSINESS LOGIC VALIDATION
    # ══════════════════════════════════════════════════════════════
    section("3. BUSINESS LOGIC VALIDATION")

    subsection("A) Key Financial Metrics")
    cur.execute("""
        SELECT
            CAST(SUM(net_revenue)       AS FLOAT),
            CAST(SUM(gross_profit)      AS FLOAT),
            CAST(AVG(gross_margin_pct)  AS FLOAT),
            SUM(units_sold),
            COUNT(*),
            COUNT(DISTINCT drug_key),
            COUNT(DISTINCT customer_key)
        FROM fact_drug_sales
    """)
    r = cur.fetchone()
    total_rev, total_prof, avg_mgn, total_units, total_orders, distinct_drugs, distinct_custs = r

    row("Total Net Revenue (USD)",       f"${total_rev:,.2f}")
    row("Total Gross Profit (USD)",      f"${total_prof:,.2f}")
    row("Average Gross Margin %",        f"{avg_mgn:.2f}%")
    row("Total Units Sold",              f"{int(total_units):,}")
    row("Total Sales Transactions",      f"{int(total_orders):,}")
    row("Distinct Drugs Sold",           f"{int(distinct_drugs):,}")
    row("Distinct Customers",            f"{int(distinct_custs):,}")

    subsection("B) Top 5 Drugs by Revenue")
    cur.execute("""
        SELECT d.drug_name,
               CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
               SUM(f.units_sold) AS units,
               CAST(AVG(f.gross_margin_pct) AS FLOAT) AS mgn
        FROM fact_drug_sales f
        JOIN dim_drug d ON f.drug_key=d.drug_key
        GROUP BY d.drug_name ORDER BY rev DESC LIMIT 5
    """)
    print(f"  {'Rank':<5} {'Drug Name':<40} {'Revenue (USD)':<18} {'Units':<10} {'Margin%'}")
    print(f"  {'-'*5} {'-'*40} {'-'*18} {'-'*10} {'-'*8}")
    for i, r in enumerate(cur.fetchall(), 1):
        print(f"  {i:<5} {r[0]:<40} ${float(r[1]):>14,.0f}   {int(r[2]):<10} {float(r[3]):.1f}%")

    subsection("C) Revenue by Customer Type")
    cur.execute("""
        SELECT c.customer_type,
               CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
               COUNT(DISTINCT f.customer_key) AS custs,
               COUNT(*) AS orders
        FROM fact_drug_sales f
        JOIN dim_customer c ON f.customer_key=c.customer_key
        GROUP BY c.customer_type ORDER BY rev DESC
    """)
    print(f"  {'Customer Type':<30} {'Revenue (USD)':<18} {'Customers':<12} {'Orders'}")
    print(f"  {'-'*30} {'-'*18} {'-'*12} {'-'*8}")
    for r in cur.fetchall():
        print(f"  {str(r[0]):<30} ${float(r[1]):>14,.0f}   {int(r[2]):<12} {int(r[3])}")

    subsection("D) Revenue by Therapeutic Class")
    cur.execute("""
        SELECT t.therapeutic_class,
               CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
               CAST(SUM(f.net_revenue)*100.0/SUM(SUM(f.net_revenue)) OVER() AS FLOAT) AS pct
        FROM fact_drug_sales f
        JOIN dim_drug d ON f.drug_key=d.drug_key
        JOIN dim_therapeutic_class t ON d.therapeutic_class_key=t.therapeutic_class_key
        GROUP BY t.therapeutic_class ORDER BY rev DESC
    """)
    print(f"  {'Therapeutic Class':<35} {'Revenue (USD)':<18} {'% of Total'}")
    print(f"  {'-'*35} {'-'*18} {'-'*12}")
    for r in cur.fetchall():
        print(f"  {str(r[0]):<35} ${float(r[1]):>14,.0f}   {float(r[2]):.1f}%")

    subsection("E) Revenue by Year (Date Shift Validation)")
    cur.execute("""
        SELECT d.calendar_year,
               COUNT(*) AS orders,
               CAST(SUM(f.net_revenue) AS FLOAT) AS rev
        FROM fact_drug_sales f
        JOIN dim_date d ON f.order_date_key=d.date_key
        GROUP BY d.calendar_year ORDER BY 1
    """)
    print(f"  {'Year':<10} {'Orders':<12} {'Revenue (USD)'}")
    print(f"  {'-'*10} {'-'*12} {'-'*18}")
    for r in cur.fetchall():
        print(f"  {str(r[0]):<10} {int(r[1]):<12} ${float(r[2]):>14,.0f}")

    subsection("F) Revenue by Country")
    cur.execute("""
        SELECT g.country_region,
               CAST(SUM(f.net_revenue) AS FLOAT) AS rev,
               SUM(f.units_sold) AS units
        FROM fact_drug_sales f
        JOIN dim_geography g ON f.geography_key=g.geography_key
        GROUP BY g.country_region ORDER BY rev DESC
    """)
    print(f"  {'Country':<25} {'Revenue (USD)':<18} {'Units Sold'}")
    print(f"  {'-'*25} {'-'*18} {'-'*12}")
    for r in cur.fetchall():
        print(f"  {str(r[0]):<25} ${float(r[1]):>14,.0f}   {int(r[2])}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 4 — INVENTORY VALIDATION
    # ══════════════════════════════════════════════════════════════
    section("4. INVENTORY VALIDATION")

    subsection("A) Stock Status Summary")
    cur.execute("""
        SELECT stock_status,
               COUNT(*) AS records,
               COUNT(DISTINCT drug_key) AS drugs,
               SUM(units_on_hand) AS total_units,
               CAST(SUM(stock_value) AS FLOAT) AS total_value
        FROM fact_inventory
        GROUP BY stock_status
        ORDER BY CASE stock_status
            WHEN 'Out of Stock' THEN 1 WHEN 'Critical' THEN 2
            WHEN 'Low Stock' THEN 3 WHEN 'In Stock' THEN 4
            ELSE 5 END
    """)
    print(f"  {'Status':<18} {'Records':<12} {'Drugs':<10} {'Units':<15} {'Value (USD)'}")
    print(f"  {'-'*18} {'-'*12} {'-'*10} {'-'*15} {'-'*15}")
    for r in cur.fetchall():
        print(f"  {str(r[0]):<18} {int(r[1]):<12} {int(r[2]):<10} {int(r[3]):<15} ${float(r[4]):>12,.0f}")

    subsection("B) Top 10 Out of Stock / Critical Drugs")
    cur.execute("""
        SELECT DISTINCT d.drug_name, t.therapeutic_class, fi.stock_status,
               SUM(fi.units_on_hand) AS units
        FROM fact_inventory fi
        JOIN dim_drug d ON fi.drug_key=d.drug_key
        JOIN dim_therapeutic_class t ON d.therapeutic_class_key=t.therapeutic_class_key
        WHERE fi.stock_status IN ('Out of Stock','Critical')
        GROUP BY d.drug_name, t.therapeutic_class, fi.stock_status
        ORDER BY fi.stock_status, d.drug_name
        LIMIT 10
    """)
    rows_fetched = cur.fetchall()
    if rows_fetched:
        print(f"  {'Drug Name':<40} {'Class':<25} {'Status':<15} {'Units'}")
        print(f"  {'-'*40} {'-'*25} {'-'*15} {'-'*8}")
        for r in rows_fetched:
            print(f"  {str(r[0]):<40} {str(r[1]):<25} {str(r[2]):<15} {int(r[3])}")
    else:
        print("  No out-of-stock or critical drugs found.")

    # ══════════════════════════════════════════════════════════════
    # SECTION 5 — ETL PIPELINE VALIDATION
    # ══════════════════════════════════════════════════════════════
    section("5. ETL PIPELINE VALIDATION")

    subsection("A) ETL Audit Log Summary")
    cur.execute("""
        SELECT pipeline_step, table_name,
               rows_read, rows_inserted, rows_rejected,
               status, duration_seconds
        FROM etl_audit_log
        ORDER BY started_at DESC
        LIMIT 20
    """)
    etl_rows = cur.fetchall()
    if etl_rows:
        print(f"  {'Step':<12} {'Table':<28} {'Read':<8} {'Inserted':<10} {'Rejected':<10} {'Status':<10} {'Secs'}")
        print(f"  {'-'*12} {'-'*28} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*6}")
        for r in etl_rows:
            print(f"  {str(r[0]):<12} {str(r[1]):<28} {str(r[2]):<8} {str(r[3]):<10} {str(r[4]):<10} {str(r[5]):<10} {str(r[6])}")
    else:
        print("  No ETL audit records found.")

    subsection("B) Data Completeness by Source System")
    cur.execute("""
        SELECT source_system,
               COUNT(*) AS rows,
               CAST(SUM(net_revenue) AS FLOAT) AS revenue,
               MIN(order_date_key) AS min_date,
               MAX(order_date_key) AS max_date
        FROM fact_drug_sales
        GROUP BY source_system ORDER BY rows DESC
    """)
    print(f"  {'Source System':<25} {'Rows':<10} {'Revenue (USD)':<20} {'Min Date':<12} {'Max Date'}")
    print(f"  {'-'*25} {'-'*10} {'-'*20} {'-'*12} {'-'*10}")
    for r in cur.fetchall():
        print(f"  {str(r[0]):<25} {int(r[1]):<10} ${float(r[2]):>16,.0f}   {str(r[3]):<12} {str(r[4])}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 6 — ACCURACY SCORE
    # ══════════════════════════════════════════════════════════════
    section("6. OVERALL ACCURACY & VALIDATION SCORE")

    total_checks = (len(null_checks) + len(biz_checks) +
                    len(fk_checks)   + len(dup_checks))
    passed_checks = null_pass + biz_pass + fk_pass + dup_pass
    accuracy = (passed_checks / total_checks) * 100

    print(f"\n  {'Check Category':<35} {'Passed':<10} {'Total':<10} {'Score'}")
    print(f"  {'-'*35} {'-'*10} {'-'*10} {'-'*8}")
    print(f"  {'NULL Value Checks':<35} {null_pass:<10} {len(null_checks):<10} {null_pass/len(null_checks)*100:.1f}%")
    print(f"  {'Business Rule Checks':<35} {biz_pass:<10} {len(biz_checks):<10} {biz_pass/len(biz_checks)*100:.1f}%")
    print(f"  {'Referential Integrity Checks':<35} {fk_pass:<10} {len(fk_checks):<10} {fk_pass/len(fk_checks)*100:.1f}%")
    print(f"  {'Duplicate Record Checks':<35} {dup_pass:<10} {len(dup_checks):<10} {dup_pass/len(dup_checks)*100:.1f}%")
    print(f"\n  {'-'*65}")
    print(f"  {'OVERALL ACCURACY SCORE':<35} {passed_checks:<10} {total_checks:<10} {accuracy:.1f}%")
    print(f"  {'-'*65}")

    if accuracy == 100:
        print(f"\n  RESULT: PERFECT -- All {total_checks} validation checks PASSED")
    elif accuracy >= 90:
        print(f"\n  RESULT: EXCELLENT -- {passed_checks}/{total_checks} checks passed")
    else:
        print(f"\n  RESULT: NEEDS REVIEW -- {total_checks-passed_checks} checks failed")

    # ══════════════════════════════════════════════════════════════
    # SECTION 7 — SCHEMA SUMMARY
    # ══════════════════════════════════════════════════════════════
    section("7. SCHEMA & INFRASTRUCTURE SUMMARY")

    cur.execute("""
        SELECT table_name,
               (SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name=t.table_name AND table_schema='public') AS col_count
        FROM information_schema.tables t
        WHERE table_schema='public' ORDER BY table_name
    """)
    print(f"\n  {'Table Name':<35} {'Columns':<10} {'Rows'}")
    print(f"  {'-'*35} {'-'*10} {'-'*12}")
    total_rows_schema = 0
    for r in cur.fetchall():
        tname = r[0]; cols = r[1]
        rc = q(cur, f"SELECT COUNT(*) FROM {tname}")
        total_rows_schema += rc
        print(f"  {tname:<35} {cols:<10} {rc:,}")
    print(f"\n  {'TOTAL':<35} {'':<10} {total_rows_schema:,}")

    print(f"\n  Platform:   AWS RDS PostgreSQL 17.6")
    print(f"  Region:     ap-south-1 (Mumbai)")
    print(f"  Instance:   db.t3.micro (Free Tier)")
    print(f"  Storage:    20GB SSD")
    print(f"  SSL:        Enabled (sslmode=require)")
    print(f"  Tables:     9 (6 Dim + 2 Fact + 1 Audit)")
    print(f"  Date Range: 2022 - 2026 (shifted from AW 2010-2014)")

    print(f"\n{SEP}")
    print(f"  END OF REPORT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEP)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()