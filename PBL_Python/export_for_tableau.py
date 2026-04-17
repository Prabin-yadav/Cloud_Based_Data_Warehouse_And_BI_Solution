import psycopg2
import pandas as pd
import os

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

os.makedirs("tableau_data", exist_ok=True)

queries = {
    "sales_dashboard": """
        SELECT
            d.full_date,
            d.calendar_year,
            d.month_name,
            d.quarter_label,
            dr.drug_name,
            dr.dosage_form,
            dr.dosage_strength,
            t.therapeutic_class,
            t.therapeutic_subclass,
            c.customer_name,
            c.customer_type,
            c.customer_segment,
            g.city,
            g.country_region,
            g.distribution_zone,
            st.territory_name,
            st.territory_group,
            f.units_sold,
            f.unit_price,
            f.gross_revenue,
            f.discount_amount,
            f.net_revenue,
            f.cost_of_goods,
            f.gross_profit,
            f.gross_margin_pct,
            f.tax_amount,
            f.freight_cost,
            f.source_system
        FROM fact_drug_sales f
        JOIN dim_date d         ON f.order_date_key = d.date_key
        JOIN dim_drug dr        ON f.drug_key = dr.drug_key
        JOIN dim_customer c     ON f.customer_key = c.customer_key
        JOIN dim_therapeutic_class t ON dr.therapeutic_class_key = t.therapeutic_class_key
        LEFT JOIN dim_geography g    ON f.geography_key = g.geography_key
        LEFT JOIN dim_sales_territory st ON f.territory_key = st.territory_key
    """,
    "inventory_dashboard": """
        SELECT
            d.full_date,
            d.calendar_year,
            d.month_name,
            d.quarter_label,
            dr.drug_name,
            dr.dosage_form,
            dr.dosage_strength,
            t.therapeutic_class,
            f.units_on_hand,
            f.units_ordered,
            f.units_dispatched,
            f.safety_stock_level,
            f.reorder_point,
            f.stock_value,
            f.days_of_supply,
            f.stock_status
        FROM fact_inventory f
        JOIN dim_date d    ON f.snapshot_date_key = d.date_key
        JOIN dim_drug dr   ON f.drug_key = dr.drug_key
        JOIN dim_therapeutic_class t ON dr.therapeutic_class_key = t.therapeutic_class_key
    """
}

print("Exporting data for Tableau...")
print("-" * 40)

cur = conn.cursor()

for filename, sql in queries.items():
    print(f"Exporting {filename}...")
    cur.execute(sql)
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)
    
    # Save as Excel
    excel_path = f"tableau_data/{filename}.xlsx"
    df.to_excel(excel_path, index=False)
    
    # Save as CSV backup
    csv_path = f"tableau_data/{filename}.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"  ✓ {len(df):,} rows → {excel_path}")

cur.close()
conn.close()

print("-" * 40)
print("Done! Files saved in tableau_data/ folder")
print("Open Tableau → Connect → Microsoft Excel → select the .xlsx files")