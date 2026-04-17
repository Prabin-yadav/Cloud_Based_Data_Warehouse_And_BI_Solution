import psycopg2
import pandas as pd

from config import (
    TARGET_DBNAME,
    TARGET_HOST,
    TARGET_PASSWORD,
    TARGET_PORT,
    TARGET_SSLMODE,
    TARGET_USER,
)

conn = psycopg2.connect(
    host=TARGET_HOST,
    port=TARGET_PORT,
    dbname=TARGET_DBNAME,
    user=TARGET_USER,
    password=TARGET_PASSWORD,
    sslmode=TARGET_SSLMODE,
)

cur = conn.cursor()

queries = {
    "Total Drug Sales":     "SELECT COUNT(*) FROM fact_drug_sales",
    "Total Inventory Rows": "SELECT COUNT(*) FROM fact_inventory",
    "Total Drugs":          "SELECT COUNT(*) FROM dim_drug",
    "Total Customers":      "SELECT COUNT(*) FROM dim_customer",
    "Avg Gross Margin %":   "SELECT ROUND(AVG(gross_margin_pct)::numeric,2) FROM fact_drug_sales",
    "Top 5 Drugs by Revenue": """
        SELECT d.drug_name, ROUND(SUM(f.net_revenue)::numeric,2) as total_revenue
        FROM fact_drug_sales f
        JOIN dim_drug d ON f.drug_key = d.drug_key
        GROUP BY d.drug_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """,
    "Sales by Customer Type": """
        SELECT c.customer_type, COUNT(*) as orders,
               ROUND(SUM(f.net_revenue)::numeric,2) as revenue
        FROM fact_drug_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_type
        ORDER BY revenue DESC
    """,
    "Stock Status Summary": """
        SELECT stock_status, COUNT(*) as count
        FROM fact_inventory
        GROUP BY stock_status
        ORDER BY count DESC
    """,
    "Revenue by Year": """
        SELECT d.calendar_year, ROUND(SUM(f.net_revenue)::numeric,2) as revenue
        FROM fact_drug_sales f
        JOIN dim_date d ON f.order_date_key = d.date_key
        GROUP BY d.calendar_year
        ORDER BY d.calendar_year
    """,
    "Top 5 Therapeutic Classes": """
        SELECT t.therapeutic_class,
               ROUND(SUM(f.net_revenue)::numeric,2) as revenue
        FROM fact_drug_sales f
        JOIN dim_drug d ON f.drug_key = d.drug_key
        JOIN dim_therapeutic_class t ON d.therapeutic_class_key = t.therapeutic_class_key
        GROUP BY t.therapeutic_class
        ORDER BY revenue DESC
        LIMIT 5
    """
}

print("=" * 60)
print("  ZENTRIK PHARMA DATA WAREHOUSE — LIVE DATA VIEW")
print("=" * 60)

for title, sql in queries.items():
    print(f"\n📊 {title}")
    print("-" * 40)
    cur.execute(sql)
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)
    print(df.to_string(index=False))

cur.close()
conn.close()
print("\n" + "=" * 60)
print("Connection closed.")