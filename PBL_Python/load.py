"""
load.py  —  Load transformed DataFrames into AWS RDS PostgreSQL
Handles dimension key resolution and upserts.
"""
import psycopg2
import psycopg2.extras
import pandas as pd
import logging
from datetime import datetime
from config import (
    TARGET_HOST, TARGET_PORT, TARGET_DBNAME,
    TARGET_USER, TARGET_PASSWORD, TARGET_SSLMODE,
    BATCH_SIZE, ETL_BATCH_ID
)

logger = logging.getLogger(__name__)

DB = {
    "host": TARGET_HOST, "port": TARGET_PORT,
    "dbname": TARGET_DBNAME, "user": TARGET_USER,
    "password": TARGET_PASSWORD, "sslmode": TARGET_SSLMODE,
    "connect_timeout": 30,
}


def get_conn():
    return psycopg2.connect(**DB)


def _write_audit(cur, batch_id, step, table, read=0, ins=0, upd=0,
                 rej=0, status="success", err=None, t0=None, t1=None):
    dur = (t1 - t0).total_seconds() if t0 and t1 else None
    cur.execute("""
        INSERT INTO etl_audit_log
            (batch_id,pipeline_step,table_name,rows_read,rows_inserted,
             rows_updated,rows_rejected,status,error_message,
             started_at,completed_at,duration_seconds)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (batch_id, step, table, read, ins, upd, rej, status, err, t0, t1, dur))


def _bulk_insert(cur, table, cols, rows, conflict_cols, update_cols=None):
    """Bulk insert using execute_values — 50x faster than executemany."""
    from psycopg2.extras import execute_values

    if not rows:
        return 0, 0

    col_list     = ",".join(cols)
    conflict_str = ",".join(conflict_cols)

    if update_cols:
        update_str = ",".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
        sql = f"""
            INSERT INTO {table} ({col_list}) VALUES %s
            ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
        """
    else:
        sql = f"""
            INSERT INTO {table} ({col_list}) VALUES %s
            ON CONFLICT ({conflict_str}) DO NOTHING
        """

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        execute_values(cur, sql, batch, page_size=BATCH_SIZE)
        inserted += cur.rowcount
        logger.info(f"    {table}: {min(i+BATCH_SIZE, len(rows)):,}/{len(rows):,} rows")

    return inserted, 0


# ── Individual table loaders ──────────────────────────────────────

def load_dim_date(conn, df):
    logger.info("Loading dim_date...")
    t0 = datetime.now()
    rows = [tuple(r) for r in df[[
        "date_key","full_date","calendar_year","calendar_quarter",
        "calendar_month","month_name","month_abbr","month_num",
        "day_of_month","day_of_week","day_name","week_of_year",
        "quarter_num","quarter_label","is_weekend","fiscal_year","fiscal_quarter"
    ]].itertuples(index=False)]

    with conn.cursor() as cur:
        ins, upd = _bulk_insert(cur, "dim_date",
            ["date_key","full_date","calendar_year","calendar_quarter",
             "calendar_month","month_name","month_abbr","month_num",
             "day_of_month","day_of_week","day_name","week_of_year",
             "quarter_num","quarter_label","is_weekend","fiscal_year","fiscal_quarter"],
            rows, ["date_key"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "dim_date",
                     read=len(df), ins=ins, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  dim_date: {ins:,} inserted")
    return ins


def load_dim_geography(conn, df):
    logger.info("Loading dim_geography...")
    t0 = datetime.now()
    rows = [tuple(r) for r in df[[
        "source_geo_key","city","state_province","country_region",
        "postal_code","region_type","distribution_zone"
    ]].itertuples(index=False)]
    with conn.cursor() as cur:
        ins, upd = _bulk_insert(cur, "dim_geography",
            ["source_geo_key","city","state_province","country_region",
             "postal_code","region_type","distribution_zone"],
            rows, ["source_geo_key"],
            update_cols=["city","country_region","distribution_zone"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "dim_geography",
                     read=len(df), ins=ins, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  dim_geography: {ins:,} rows")
    return ins


def load_dim_territory(conn, df):
    logger.info("Loading dim_sales_territory...")
    t0 = datetime.now()
    rows = [tuple(r) for r in df[[
        "source_territory_key","territory_name","territory_country","territory_group"
    ]].itertuples(index=False)]
    with conn.cursor() as cur:
        ins, _ = _bulk_insert(cur, "dim_sales_territory",
            ["source_territory_key","territory_name","territory_country","territory_group"],
            rows, ["source_territory_key"],
            update_cols=["territory_name","territory_country"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "dim_sales_territory",
                     read=len(df), ins=ins, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  dim_sales_territory: {ins:,} rows")
    return ins


def load_dim_therapeutic_class(conn, df):
    logger.info("Loading dim_therapeutic_class...")
    t0 = datetime.now()
    rows = [tuple(r) for r in df[[
        "source_category_key","source_subcategory_key","class_code",
        "therapeutic_class","therapeutic_subclass","regulatory_category","atc_code"
    ]].itertuples(index=False)]
    with conn.cursor() as cur:
        ins, _ = _bulk_insert(cur, "dim_therapeutic_class",
            ["source_category_key","source_subcategory_key","class_code",
             "therapeutic_class","therapeutic_subclass","regulatory_category","atc_code"],
            rows, ["source_subcategory_key"],
            update_cols=["therapeutic_class","therapeutic_subclass"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "dim_therapeutic_class",
                     read=len(df), ins=ins, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  dim_therapeutic_class: {ins:,} rows")
    return ins


def load_dim_drug(conn, df):
    logger.info("Loading dim_drug...")
    t0 = datetime.now()

    # Resolve therapeutic_class_key from DB
    with conn.cursor() as cur:
        cur.execute("SELECT therapeutic_class_key, source_subcategory_key FROM dim_therapeutic_class")
        tc_map = {r[1]: r[0] for r in cur.fetchall()}

    df["therapeutic_class_key"] = df["source_subcat_key"].map(tc_map).fillna(1).astype(int)

    rows = [tuple(r) for r in df[[
        "source_product_key","drug_code","drug_name","dosage_form","dosage_strength",
        "therapeutic_class_key","unit_price","unit_cost","standard_cost",
        "manufacturer","drug_status"
    ]].itertuples(index=False)]

    with conn.cursor() as cur:
        ins, _ = _bulk_insert(cur, "dim_drug",
            ["source_product_key","drug_code","drug_name","dosage_form","dosage_strength",
             "therapeutic_class_key","unit_price","unit_cost","standard_cost",
             "manufacturer","drug_status"],
            rows, ["source_product_key"],
            update_cols=["drug_name","unit_price","unit_cost","drug_status"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "dim_drug",
                     read=len(df), ins=ins, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  dim_drug: {ins:,} rows")
    return ins


def load_dim_customer(conn, df):
    logger.info("Loading dim_customer...")
    t0 = datetime.now()

    # Resolve geography_key
    with conn.cursor() as cur:
        cur.execute("SELECT geography_key, source_geo_key FROM dim_geography")
        geo_map = {r[1]: r[0] for r in cur.fetchall()}

    df["geography_key"] = df["source_geo_key"].map(geo_map)

    rows = [tuple(r) for r in df[[
        "source_customer_key","customer_code","customer_name","customer_type",
        "email","phone","payment_terms","customer_segment","customer_status","geography_key"
    ]].itertuples(index=False)]

    with conn.cursor() as cur:
        ins, _ = _bulk_insert(cur, "dim_customer",
            ["source_customer_key","customer_code","customer_name","customer_type",
             "email","phone","payment_terms","customer_segment","customer_status","geography_key"],
            rows, ["source_customer_key"],
            update_cols=["customer_name","customer_type","customer_segment"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "dim_customer",
                     read=len(df), ins=ins, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  dim_customer: {ins:,} rows")
    return ins


def load_fact_sales(conn, df):
    logger.info("Loading fact_drug_sales...")
    t0 = datetime.now()

    # Resolve surrogate keys
    with conn.cursor() as cur:
        cur.execute("SELECT drug_key, source_product_key FROM dim_drug")
        drug_map = {r[1]: r[0] for r in cur.fetchall()}

        cur.execute("SELECT customer_key, source_customer_key FROM dim_customer")
        cust_map = {r[1]: r[0] for r in cur.fetchall()}

        cur.execute("SELECT territory_key, source_territory_key FROM dim_sales_territory")
        terr_map = {r[1]: r[0] for r in cur.fetchall()}

        cur.execute("SELECT geography_key, source_geo_key FROM dim_geography")
        geo_map  = {r[1]: r[0] for r in cur.fetchall()}

    df["drug_key"]      = df["source_product_key"].map(drug_map)
    df["customer_key"]  = df["source_customer_key"].map(cust_map)
    df["territory_key"] = df["source_territory_key"].map(terr_map)

    # Drop rows where keys couldn't be resolved
    before = len(df)
    df = df.dropna(subset=["drug_key","customer_key"])
    df["drug_key"]     = df["drug_key"].astype(int)
    df["customer_key"] = df["customer_key"].astype(int)
    df["territory_key"]= df["territory_key"].fillna(0).astype(int).replace({0: None})
    skipped = before - len(df)

    rows = [tuple(r) for r in df[[
        "order_date_key","ship_date_key","due_date_key",
        "drug_key","customer_key","territory_key",
        "source_order_number","source_order_line_num","source_system",
        "units_sold","unit_price","unit_price_discount_pct",
        "gross_revenue","discount_amount","net_revenue",
        "cost_of_goods","gross_profit","gross_margin_pct",
        "tax_amount","freight_cost","etl_batch_id"
    ]].itertuples(index=False)]

    with conn.cursor() as cur:
        ins, _ = _bulk_insert(cur, "fact_drug_sales",
            ["order_date_key","ship_date_key","due_date_key",
             "drug_key","customer_key","territory_key",
             "source_order_number","source_order_line_num","source_system",
             "units_sold","unit_price","unit_price_discount_pct",
             "gross_revenue","discount_amount","net_revenue",
             "cost_of_goods","gross_profit","gross_margin_pct",
             "tax_amount","freight_cost","etl_batch_id"],
            rows, ["source_order_number","source_order_line_num","source_system"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "fact_drug_sales",
                     read=len(df)+skipped, ins=ins, rej=skipped, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  fact_drug_sales: {ins:,} inserted, {skipped:,} skipped")
    return ins


def load_fact_inventory(conn, df):
    logger.info("Loading fact_inventory...")
    t0 = datetime.now()

    with conn.cursor() as cur:
        cur.execute("SELECT drug_key, source_product_key FROM dim_drug")
        drug_map = {r[1]: r[0] for r in cur.fetchall()}

    df["drug_key"] = df["source_product_key"].map(drug_map)
    before = len(df)
    df = df.dropna(subset=["drug_key"])
    df["drug_key"] = df["drug_key"].astype(int)
    skipped = before - len(df)

    rows = [tuple(r) for r in df[[
        "snapshot_date_key","drug_key","units_on_hand","units_ordered",
        "units_dispatched","safety_stock_level","reorder_point",
        "stock_value","days_of_supply","stock_status","etl_batch_id"
    ]].itertuples(index=False)]

    with conn.cursor() as cur:
        ins, _ = _bulk_insert(cur, "fact_inventory",
            ["snapshot_date_key","drug_key","units_on_hand","units_ordered",
             "units_dispatched","safety_stock_level","reorder_point",
             "stock_value","days_of_supply","stock_status","etl_batch_id"],
            rows, ["snapshot_date_key","drug_key"],
            update_cols=["units_on_hand","units_ordered","units_dispatched",
                         "stock_value","stock_status"])
        t1 = datetime.now()
        _write_audit(cur, ETL_BATCH_ID, "load", "fact_inventory",
                     read=len(df)+skipped, ins=ins, rej=skipped, t0=t0, t1=t1)
    conn.commit()
    logger.info(f"  fact_inventory: {ins:,} inserted, {skipped:,} skipped")
    return ins


def load_all(transformed):
    """Load all tables in correct order."""
    logger.info("=" * 60)
    logger.info("LOAD PHASE — AWS RDS PostgreSQL")
    logger.info("=" * 60)

    conn = get_conn()
    results = {}

    # Dimensions first (order matters for FK constraints)
    results["dim_date"]              = load_dim_date(conn, transformed["dim_date"])
    results["dim_geography"]         = load_dim_geography(conn, transformed["dim_geography"])
    results["dim_sales_territory"]   = load_dim_territory(conn, transformed["dim_sales_territory"])
    results["dim_therapeutic_class"] = load_dim_therapeutic_class(conn, transformed["dim_therapeutic_class"])
    results["dim_drug"]              = load_dim_drug(conn, transformed["dim_drug"])
    results["dim_customer"]          = load_dim_customer(conn, transformed["dim_customer"])

    # Facts last
    results["fact_drug_sales"]       = load_fact_sales(conn, transformed["fact_drug_sales"])
    results["fact_inventory"]        = load_fact_inventory(conn, transformed["fact_inventory"])

    conn.close()

    logger.info("\nLOAD SUMMARY:")
    total = sum(results.values())
    for k, v in results.items():
        logger.info(f"  {k}: {v:,} rows loaded")
    logger.info(f"  TOTAL LOADED: {total:,} rows")

    return results