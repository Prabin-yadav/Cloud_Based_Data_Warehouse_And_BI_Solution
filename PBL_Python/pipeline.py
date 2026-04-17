"""
pipeline.py  —  Zentrik Pharma ETL Main Runner
Usage:
    python pipeline.py              # Full load
    python pipeline.py --validate   # Validate after load
"""
import logging
import sys
import time
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)


def validate(conn_str=None):
    """Run post-load validation checks."""
    import psycopg2
    from config import TARGET_HOST, TARGET_PORT, TARGET_DBNAME, TARGET_USER, TARGET_PASSWORD, TARGET_SSLMODE
    conn = psycopg2.connect(
        host=TARGET_HOST, port=TARGET_PORT, dbname=TARGET_DBNAME,
        user=TARGET_USER, password=TARGET_PASSWORD, sslmode=TARGET_SSLMODE
    )
    cur = conn.cursor()

    logger.info("\n" + "="*60)
    logger.info("POST-LOAD VALIDATION")
    logger.info("="*60)

    checks = {
        "dim_date rows":            "SELECT COUNT(*) FROM dim_date",
        "dim_drug rows":            "SELECT COUNT(*) FROM dim_drug",
        "dim_customer rows":        "SELECT COUNT(*) FROM dim_customer",
        "dim_geography rows":       "SELECT COUNT(*) FROM dim_geography",
        "dim_therapeutic_class":    "SELECT COUNT(*) FROM dim_therapeutic_class",
        "dim_sales_territory":      "SELECT COUNT(*) FROM dim_sales_territory",
        "fact_drug_sales rows":     "SELECT COUNT(*) FROM fact_drug_sales",
        "fact_inventory rows":      "SELECT COUNT(*) FROM fact_inventory",
        "Sales null drug_key":      "SELECT COUNT(*) FROM fact_drug_sales WHERE drug_key IS NULL",
        "Sales negative revenue":   "SELECT COUNT(*) FROM fact_drug_sales WHERE net_revenue < 0",
        "Null FK in inventory":     "SELECT COUNT(*) FROM fact_inventory WHERE drug_key IS NULL",
        "Date range (sales)":       "SELECT MIN(d.full_date)||' to '||MAX(d.full_date) FROM fact_drug_sales f JOIN dim_date d ON f.order_date_key=d.date_key",
        "Total revenue":            "SELECT CAST(SUM(net_revenue) AS FLOAT) FROM fact_drug_sales",
        "Avg gross margin":         "SELECT ROUND(AVG(gross_margin_pct)::numeric,2) FROM fact_drug_sales",
        "Out of stock count":       "SELECT COUNT(*) FROM fact_inventory WHERE stock_status='Out of Stock'",
    }

    all_pass = True
    for name, sql in checks.items():
        cur.execute(sql)
        val = cur.fetchone()[0]
        fail_checks = ["Sales null drug_key","Sales negative revenue","Null FK in inventory"]
        if name in fail_checks and int(val or 0) > 0:
            logger.warning(f"  ❌ FAIL  {name}: {val}")
            all_pass = False
        else:
            logger.info(f"  ✅ PASS  {name}: {val}")

    cur.close()
    conn.close()
    return all_pass


def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("ZENTRIK PHARMA ETL PIPELINE")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Date shift: +12 years (AW 2010-2014 → 2022-2026)")
    logger.info("=" * 60)

    try:
        # STEP 1 — EXTRACT
        logger.info("\n[STEP 1] EXTRACT")
        from extract import extract_all
        t0 = time.time()
        raw = extract_all()
        logger.info(f"Extract completed in {time.time()-t0:.1f}s")

        # STEP 2 — TRANSFORM
        logger.info("\n[STEP 2] TRANSFORM")
        from transform import transform_all
        t0 = time.time()
        transformed = transform_all(raw)
        logger.info(f"Transform completed in {time.time()-t0:.1f}s")

        # STEP 3 — LOAD
        logger.info("\n[STEP 3] LOAD")
        from load import load_all
        t0 = time.time()
        results = load_all(transformed)
        logger.info(f"Load completed in {time.time()-t0:.1f}s")

        # STEP 4 — VALIDATE
        if "--validate" in sys.argv or True:  # Always validate
            logger.info("\n[STEP 4] VALIDATE")
            passed = validate()
            if passed:
                logger.info("✅ All validation checks passed!")
            else:
                logger.warning("⚠️ Some validation checks failed — review above")

        elapsed = time.time() - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"ETL PIPELINE COMPLETE in {elapsed:.1f}s")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"❌ ETL PIPELINE FAILED: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()