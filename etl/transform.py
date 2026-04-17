"""STEP 2 — TRANSFORM: Clean, validate, enrich data."""
import pandas as pd
import numpy as np

CUST_MAP = {
    "hospital":"Hospital","clinic":"Clinic","pharmacy":"Pharmacy",
    "wholesale":"Wholesale Distributor","distributor":"Wholesale Distributor",
    "retail":"Retail Chain","government":"Government",
}
ZONE_MAP = {
    "united states":"North America Zone","canada":"North America Zone",
    "france":"Europe Zone","germany":"Europe Zone","united kingdom":"Europe Zone",
    "australia":"Asia-Pacific Zone","india":"Asia Zone",
}

def _stock_status(u):
    if u<=0: return "Out of Stock"
    elif u<50: return "Critical"
    elif u<150: return "Low Stock"
    elif u>5000: return "Overstock"
    return "In Stock"

def transform_sales(df):
    removed = {}; warnings = []; df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    n = df["order_date"].isna().sum()
    if n: removed["bad_dates"] = int(n); df = df[df["order_date"].notna()]

    for c in ["units_sold","unit_price","unit_price_discount_pct","total_product_cost","sales_amount","tax_amt","freight"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    n = (df["units_sold"]<=0).sum()
    if n: removed["zero_units"] = int(n); df = df[df["units_sold"]>0]
    n = (df["unit_price"]<=0).sum()
    if n: removed["zero_price"] = int(n); df = df[df["unit_price"]>0]

    df["customer_type"] = df["customer_type"].astype(str).str.strip().str.lower().map(
        lambda x: next((v for k,v in CUST_MAP.items() if k in x), "Hospital"))

    df["gross_revenue"]    = (df["unit_price"]*df["units_sold"]).round(4)
    df["discount_amount"]  = (df["gross_revenue"]*df["unit_price_discount_pct"]/100).round(4)
    df["net_revenue"]      = (df["gross_revenue"]-df["discount_amount"]).round(4)

    # Uploads may provide total_product_cost as either per-unit or line-total.
    # If it looks too small versus line revenue, treat it as per-unit and scale by units.
    cogs_raw = df["total_product_cost"].clip(lower=0)
    cogs_as_total = cogs_raw
    cogs_as_unit = cogs_raw * df["units_sold"]
    looks_like_unit_cost = cogs_as_total < (df["gross_revenue"] * 0.5)
    df["cost_of_goods"] = np.where(looks_like_unit_cost, cogs_as_unit, cogs_as_total).round(4)

    df["gross_profit"]     = (df["net_revenue"]-df["cost_of_goods"]).round(4)
    df["gross_margin_pct"] = np.where(df["net_revenue"]>0,
        (df["gross_profit"]/df["net_revenue"]*100).round(2), 0.0)
    df["tax_amount"]   = df.get("tax_amt",   pd.Series(0, index=df.index)).fillna(0).round(4)
    df["freight_cost"] = df.get("freight",   pd.Series(0, index=df.index)).fillna(0).round(4)
    df["distribution_zone"] = df["country_region"].astype(str).str.lower().map(
        lambda c: ZONE_MAP.get(c, "International Zone"))
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)
    for c in ["drug_name","customer_name","customer_type","sales_territory_name","country_region","city"]:
        if c in df.columns: df[c] = df[c].astype(str).str.strip()
    before = len(df)
    df = df.dropna(subset=["drug_name","customer_name","order_date"])
    n = before-len(df)
    if n: removed["null_critical"] = int(n)
    return df, {"input_rows": len(df)+sum(removed.values()),
                "output_rows": len(df), "removed": removed, "warnings": warnings}

def transform_inventory(df):
    removed = {}; df = df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    n = df["snapshot_date"].isna().sum()
    if n: removed["bad_dates"] = int(n); df = df[df["snapshot_date"].notna()]
    for c in ["units_on_hand","units_ordered","units_dispatched","unit_cost"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    for c in ["units_on_hand","units_ordered","units_dispatched"]:
        df[c] = df[c].clip(lower=0)
    df["stock_status"]  = df["units_on_hand"].apply(lambda u: _stock_status(int(u)))
    df["stock_value"]   = (df["units_on_hand"]*df["unit_cost"]).round(4)
    avg_d = df.groupby("drug_name")["units_dispatched"].transform("mean")
    df["days_of_supply"] = np.where(avg_d>0, (df["units_on_hand"]/avg_d).round(2), None)
    df["date_key"] = df["snapshot_date"].dt.strftime("%Y%m%d").astype(int)
    df["drug_name"] = df["drug_name"].astype(str).str.strip()
    before = len(df)
    df = df.drop_duplicates(subset=["drug_name","date_key"], keep="last")
    n = before-len(df)
    if n: removed["duplicates"] = int(n)
    before = len(df)
    df = df.dropna(subset=["drug_name","snapshot_date"])
    n = before-len(df)
    if n: removed["null_critical"] = int(n)
    return df, {"input_rows": len(df)+sum(removed.values()),
                "output_rows": len(df), "removed": removed, "warnings": []}
