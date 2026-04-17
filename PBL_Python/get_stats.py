import psycopg2

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

print("=" * 50)
print("TOP 5 DRUGS BY REVENUE:")
cur.execute("""
    SELECT d.drug_name, CAST(SUM(f.net_revenue) AS FLOAT)
    FROM fact_drug_sales f
    JOIN dim_drug d ON f.drug_key = d.drug_key
    GROUP BY d.drug_name ORDER BY 2 DESC LIMIT 5
""")
for i, row in enumerate(cur.fetchall(), 1):
    print(f"  {i}. {row[0]} --- USD {float(row[1]):,.0f}")

print("\nTOTAL REVENUE:")
cur.execute("SELECT CAST(SUM(net_revenue) AS FLOAT) FROM fact_drug_sales")
print(f"  USD {float(cur.fetchone()[0]):,.0f}")

print("\nAVG GROSS MARGIN:")
cur.execute("SELECT CAST(AVG(gross_margin_pct) AS FLOAT) FROM fact_drug_sales")
print(f"  {float(cur.fetchone()[0]):.2f}%")

print("\nREVENUE BY CUSTOMER TYPE:")
cur.execute("""
    SELECT c.customer_type, CAST(SUM(f.net_revenue) AS FLOAT)
    FROM fact_drug_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_type ORDER BY 2 DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]} --- USD {float(row[1]):,.0f}")

print("\nREVENUE BY THERAPEUTIC CLASS:")
cur.execute("""
    SELECT t.therapeutic_class, CAST(SUM(f.net_revenue) AS FLOAT)
    FROM fact_drug_sales f
    JOIN dim_drug d ON f.drug_key = d.drug_key
    JOIN dim_therapeutic_class t ON d.therapeutic_class_key = t.therapeutic_class_key
    GROUP BY t.therapeutic_class ORDER BY 2 DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]} --- USD {float(row[1]):,.0f}")

print("\nREVENUE BY YEAR:")
cur.execute("""
    SELECT d.calendar_year, CAST(SUM(f.net_revenue) AS FLOAT)
    FROM fact_drug_sales f
    JOIN dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.calendar_year ORDER BY 1
""")
for row in cur.fetchall():
    print(f"  {row[0]} --- USD {float(row[1]):,.0f}")

print("\nROW COUNTS:")
for table in ["fact_drug_sales", "fact_inventory", "dim_drug", "dim_customer", "dim_date", "dim_geography", "dim_therapeutic_class", "dim_sales_territory"]:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"  {table}: {cur.fetchone()[0]:,}")

print("\nSTOCK STATUS SUMMARY:")
cur.execute("""
    SELECT stock_status, COUNT(*)
    FROM fact_inventory
    GROUP BY stock_status ORDER BY 2 DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,}")

print("=" * 50)
cur.close()
conn.close()
print("Done.")