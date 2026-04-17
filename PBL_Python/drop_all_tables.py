"""
drop_all_tables.py
Drops all Zentrik Pharma tables from AWS RDS.
Run this ONCE before re-running the ETL pipeline.
"""
import psycopg2

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
}

TABLES = [
    # Facts first (they have FKs pointing to dims)
    "fact_drug_sales",
    "fact_inventory",
    # Dims
    "dim_drug",
    "dim_customer",
    "dim_date",
    "dim_geography",
    "dim_sales_territory",
    "dim_therapeutic_class",
    # Audit
    "etl_audit_log",
]

def main():
    conn = psycopg2.connect(**DB)
    cur  = conn.cursor()

    print("Dropping all Zentrik Pharma tables...\n")
    for table in TABLES:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"  ✅ Dropped: {table}")
        except Exception as e:
            print(f"  ❌ Error dropping {table}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("\nAll tables dropped. Now run: python create_tables.py")

if __name__ == "__main__":
    confirm = input("Are you sure? This deletes ALL data. Type YES to confirm: ")
    if confirm.strip().upper() == "YES":
        main()
    else:
        print("Cancelled.")