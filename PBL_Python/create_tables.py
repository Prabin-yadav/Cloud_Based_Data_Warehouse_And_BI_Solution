"""
create_tables.py
Creates all Zentrik Pharma DW tables on AWS RDS PostgreSQL.
Run after drop_all_tables.py.
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

DDL = """

-- ── DIMENSION: DATE ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key         INTEGER PRIMARY KEY,
    full_date        DATE NOT NULL,
    calendar_year    INTEGER NOT NULL,
    calendar_quarter INTEGER NOT NULL,
    calendar_month   INTEGER NOT NULL,
    month_name       VARCHAR(20),
    month_abbr       VARCHAR(5),
    month_num        INTEGER,
    day_of_month     INTEGER,
    day_of_week      INTEGER,
    day_name         VARCHAR(15),
    week_of_year     INTEGER,
    quarter_num      INTEGER,
    quarter_label    VARCHAR(10),
    is_weekend       BOOLEAN DEFAULT FALSE,
    fiscal_year      INTEGER,
    fiscal_quarter   INTEGER,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- ── DIMENSION: GEOGRAPHY ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key      SERIAL PRIMARY KEY,
    source_geo_key     INTEGER UNIQUE,
    city               VARCHAR(100),
    state_province     VARCHAR(100),
    country_region     VARCHAR(100),
    postal_code        VARCHAR(20),
    region_type        VARCHAR(50),
    distribution_zone  VARCHAR(100),
    created_at         TIMESTAMP DEFAULT NOW()
);

-- ── DIMENSION: SALES TERRITORY ───────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_sales_territory (
    territory_key        SERIAL PRIMARY KEY,
    source_territory_key INTEGER UNIQUE,
    territory_name       VARCHAR(100),
    territory_country    VARCHAR(100),
    territory_group      VARCHAR(100),
    created_at           TIMESTAMP DEFAULT NOW()
);

-- ── DIMENSION: THERAPEUTIC CLASS ─────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_therapeutic_class (
    therapeutic_class_key  SERIAL PRIMARY KEY,
    source_category_key    INTEGER,
    source_subcategory_key INTEGER UNIQUE,
    class_code             VARCHAR(20),
    therapeutic_class      VARCHAR(100) NOT NULL,
    therapeutic_subclass   VARCHAR(100),
    regulatory_category    VARCHAR(100),
    atc_code               VARCHAR(20),
    created_at             TIMESTAMP DEFAULT NOW()
);

-- ── DIMENSION: DRUG ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_drug (
    drug_key               SERIAL PRIMARY KEY,
    source_product_key     INTEGER UNIQUE,
    drug_code              VARCHAR(50),
    drug_name              VARCHAR(200) NOT NULL,
    dosage_form            VARCHAR(50),
    dosage_strength        VARCHAR(50),
    therapeutic_class_key  INTEGER REFERENCES dim_therapeutic_class(therapeutic_class_key),
    unit_price             DECIMAL(18,4),
    unit_cost              DECIMAL(18,4),
    standard_cost          DECIMAL(18,4),
    manufacturer           VARCHAR(200),
    drug_status            VARCHAR(20) DEFAULT 'Active',
    discontinue_date       DATE,
    created_at             TIMESTAMP DEFAULT NOW(),
    updated_at             TIMESTAMP DEFAULT NOW()
);

-- ── DIMENSION: CUSTOMER ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key         SERIAL PRIMARY KEY,
    source_customer_key  INTEGER UNIQUE,
    customer_code        VARCHAR(50),
    customer_name        VARCHAR(200) NOT NULL,
    customer_type        VARCHAR(50),
    email                VARCHAR(200),
    phone                VARCHAR(50),
    payment_terms        VARCHAR(50),
    customer_segment     VARCHAR(50),
    customer_status      VARCHAR(20) DEFAULT 'Active',
    geography_key        INTEGER REFERENCES dim_geography(geography_key),
    created_at           TIMESTAMP DEFAULT NOW(),
    updated_at           TIMESTAMP DEFAULT NOW()
);

-- ── FACT: DRUG SALES ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_drug_sales (
    sales_key                BIGSERIAL PRIMARY KEY,
    order_date_key           INTEGER NOT NULL REFERENCES dim_date(date_key),
    ship_date_key            INTEGER REFERENCES dim_date(date_key),
    due_date_key             INTEGER REFERENCES dim_date(date_key),
    drug_key                 INTEGER NOT NULL REFERENCES dim_drug(drug_key),
    customer_key             INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    geography_key            INTEGER REFERENCES dim_geography(geography_key),
    territory_key            INTEGER REFERENCES dim_sales_territory(territory_key),
    source_order_number      VARCHAR(50) NOT NULL,
    source_order_line_num    INTEGER NOT NULL DEFAULT 1,
    source_system            VARCHAR(50) NOT NULL DEFAULT 'AW_INTERNET',
    units_sold               INTEGER NOT NULL DEFAULT 0,
    unit_price               DECIMAL(18,4),
    unit_price_discount_pct  DECIMAL(8,4) DEFAULT 0,
    gross_revenue            DECIMAL(18,4),
    discount_amount          DECIMAL(18,4) DEFAULT 0,
    net_revenue              DECIMAL(18,4),
    cost_of_goods            DECIMAL(18,4),
    gross_profit             DECIMAL(18,4),
    gross_margin_pct         DECIMAL(8,4),
    tax_amount               DECIMAL(18,4) DEFAULT 0,
    freight_cost             DECIMAL(18,4) DEFAULT 0,
    etl_batch_id             VARCHAR(100),
    etl_loaded_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_order_number, source_order_line_num, source_system)
);

-- ── FACT: INVENTORY ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_inventory (
    inventory_key      BIGSERIAL PRIMARY KEY,
    snapshot_date_key  INTEGER NOT NULL REFERENCES dim_date(date_key),
    drug_key           INTEGER NOT NULL REFERENCES dim_drug(drug_key),
    geography_key      INTEGER REFERENCES dim_geography(geography_key),
    units_on_hand      INTEGER DEFAULT 0,
    units_ordered      INTEGER DEFAULT 0,
    units_dispatched   INTEGER DEFAULT 0,
    safety_stock_level INTEGER DEFAULT 0,
    reorder_point      INTEGER DEFAULT 0,
    stock_value        DECIMAL(18,4) DEFAULT 0,
    days_of_supply     DECIMAL(10,2),
    stock_status       VARCHAR(20),
    etl_batch_id       VARCHAR(100),
    etl_loaded_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (snapshot_date_key, drug_key)
);

-- ── AUDIT LOG ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_audit_log (
    log_id           BIGSERIAL PRIMARY KEY,
    batch_id         VARCHAR(100),
    pipeline_step    VARCHAR(50),
    table_name       VARCHAR(100),
    rows_read        INTEGER DEFAULT 0,
    rows_inserted    INTEGER DEFAULT 0,
    rows_updated     INTEGER DEFAULT 0,
    rows_rejected    INTEGER DEFAULT 0,
    status           VARCHAR(20),
    error_message    TEXT,
    started_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    duration_seconds DECIMAL(10,3),
    created_at       TIMESTAMP DEFAULT NOW()
);

-- ── INDEXES ──────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_sales_order_date   ON fact_drug_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_sales_drug         ON fact_drug_sales(drug_key);
CREATE INDEX IF NOT EXISTS idx_sales_customer     ON fact_drug_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_sales_territory    ON fact_drug_sales(territory_key);
CREATE INDEX IF NOT EXISTS idx_sales_geography    ON fact_drug_sales(geography_key);
CREATE INDEX IF NOT EXISTS idx_inv_date           ON fact_inventory(snapshot_date_key);
CREATE INDEX IF NOT EXISTS idx_inv_drug           ON fact_inventory(drug_key);
CREATE INDEX IF NOT EXISTS idx_drug_name          ON dim_drug(drug_name);
CREATE INDEX IF NOT EXISTS idx_drug_class         ON dim_drug(therapeutic_class_key);
CREATE INDEX IF NOT EXISTS idx_customer_type      ON dim_customer(customer_type);
CREATE INDEX IF NOT EXISTS idx_date_year          ON dim_date(calendar_year);
CREATE INDEX IF NOT EXISTS idx_date_full          ON dim_date(full_date);
"""

def main():
    conn = psycopg2.connect(**DB)
    cur  = conn.cursor()

    print("Creating Zentrik Pharma DW schema...\n")
    try:
        cur.execute(DDL)
        conn.commit()
        print("✅ All tables and indexes created successfully!\n")

        # Verify
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' ORDER BY table_name
        """)
        tables = cur.fetchall()
        print("Tables in zentrik_pharma_dw:")
        for t in tables:
            print(f"  ✅ {t[0]}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")

    cur.close()
    conn.close()
    print("\nNext step: python pipeline.py")

if __name__ == "__main__":
    main()