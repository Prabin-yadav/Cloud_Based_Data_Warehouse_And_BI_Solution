"""STEP 1 — EXTRACT: Read uploaded file into raw DataFrame."""
import pandas as pd
from pathlib import Path

SALES_COLS = [
    "drug_name","customer_name","customer_type","order_date",
    "units_sold","unit_price","unit_price_discount_pct",
    "total_product_cost","sales_amount","tax_amt","freight",
    "sales_territory_name","country_region","city",
]
INV_COLS = [
    "drug_name","snapshot_date","units_on_hand",
    "units_ordered","units_dispatched","unit_cost",
]

def normalise(df):
    df.columns = (df.columns.str.strip().str.lower()
                    .str.replace(" ","_").str.replace(r"[^a-z0-9_]","",regex=True))
    return df

def extract(filepath):
    p = Path(filepath)
    suffix = p.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(filepath)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file type: {p.suffix}")
    df = normalise(df)
    is_inv = "units_on_hand" in df.columns
    required = INV_COLS if is_inv else SALES_COLS
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    utype = "inventory" if is_inv else "sales"
    return df, utype, {"rows": len(df), "columns": list(df.columns), "type": utype}
