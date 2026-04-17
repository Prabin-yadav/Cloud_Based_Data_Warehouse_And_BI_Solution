"""
transform.py  —  Transform AdventureWorks data to Zentrik Pharma context
Full data cleaning on ALL tables:
- NULL checks on every column
- Duplicate removal
- Data type validation
- Business rule enforcement
- Anomaly detection and logging
"""
import pandas as pd
import numpy as np
import logging
from datetime import date as dt_date
from config import (
    CATEGORY_TO_THERAPEUTIC_CLASS, SUBCATEGORY_TO_SUBCLASS,
    DRUG_NAMES, COLOR_TO_DOSAGE_FORM, SIZE_TO_STRENGTH,
    CUSTOMER_KEY_TO_TYPE, REGULATORY_CATEGORY, ETL_BATCH_ID
)

logger = logging.getLogger(__name__)

TODAY_KEY = int(dt_date.today().strftime("%Y%m%d"))


# ══════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════

def _safe_str(val, default="Unknown"):
    s = str(val).strip()
    return default if s.lower() in ["nan", "none", "null", ""] else s


def _safe_float(val, default=0.0):
    try:
        f = float(val)
        return default if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return default


def _safe_int(val, default=0):
    try:
        return int(float(val))
    except Exception:
        return default


def _log_removal(table, reason, count):
    if count > 0:
        logger.warning(f"  [{table}] Removed {count:,} rows -- {reason}")


def _get_drug_name(product_name, subcategory_name, product_key):
    candidates = DRUG_NAMES.get(subcategory_name,
                 DRUG_NAMES.get("General", ["Zentrik Drug"]))
    idx = int(product_key) % len(candidates)
    return candidates[idx]


def _get_dosage_form(color):
    return COLOR_TO_DOSAGE_FORM.get(str(color), "Tablet")


def _get_strength(size):
    return SIZE_TO_STRENGTH.get(str(size), "250mg")


def _get_customer_info(customer_key):
    k = int(customer_key)
    for (lo, hi), (ctype, seg) in CUSTOMER_KEY_TO_TYPE.items():
        if lo <= k <= hi:
            return ctype, seg
    return "Hospital", "Government"


def _zone(country):
    z = {
        "United States":  "North America Zone",
        "Canada":         "North America Zone",
        "France":         "Europe Zone",
        "Germany":        "Europe Zone",
        "United Kingdom": "Europe Zone",
        "Australia":      "Asia-Pacific Zone",
        "India":          "Asia Zone",
    }
    return z.get(str(country), "International Zone")


def _stock_status(units):
    u = int(units) if pd.notna(units) else 0
    if u <= 0:       return "Out of Stock"
    elif u < 50:     return "Critical"
    elif u < 150:    return "Low Stock"
    elif u > 5000:   return "Overstock"
    return "In Stock"


# ══════════════════════════════════════════════════════════════════
# DIM_DATE
# ══════════════════════════════════════════════════════════════════

def transform_dim_date(df):
    logger.info("Transforming dim_date...")
    removed = {}
    out = pd.DataFrame()

    out["date_key"]         = pd.to_numeric(df["DateKey"], errors="coerce")
    out["full_date"]        = pd.to_datetime(df["FullDateAlternateKey"], errors="coerce").dt.date
    out["calendar_year"]    = pd.to_numeric(df["CalendarYear"], errors="coerce")
    out["calendar_quarter"] = pd.to_numeric(df["CalendarQuarter"], errors="coerce")
    out["calendar_month"]   = pd.to_numeric(df["CalendarMonth"], errors="coerce")
    out["month_name"]       = df["EnglishMonthName"].astype(str).str.strip().replace("nan", "Unknown")
    out["month_abbr"]       = df["MonthAbbr"].astype(str).str.strip().replace("nan", "UNK")
    out["month_num"]        = pd.to_numeric(df["MonthNumberOfYear"], errors="coerce")
    out["day_of_month"]     = pd.to_numeric(df["DayNumberOfMonth"], errors="coerce")
    out["day_of_week"]      = pd.to_numeric(df["DayNumberOfWeek"], errors="coerce")
    out["day_name"]         = df["EnglishDayNameOfWeek"].astype(str).str.strip().replace("nan", "Unknown")
    out["week_of_year"]     = pd.to_numeric(df["WeekNumberOfYear"], errors="coerce")
    out["quarter_num"]      = pd.to_numeric(df["CalendarQuarter"], errors="coerce")
    out["quarter_label"]    = "Q" + df["CalendarQuarter"].astype(str) + " " + df["CalendarYear"].astype(str)
    out["is_weekend"]       = df["IsWeekend"].fillna(False).astype(bool)
    out["fiscal_year"]      = pd.to_numeric(df["FiscalYear"], errors="coerce")
    out["fiscal_quarter"]   = pd.to_numeric(df["FiscalQuarter"], errors="coerce")

    # Remove NULL date_key or full_date
    before = len(out)
    out = out.dropna(subset=["date_key", "full_date"])
    removed["null_date_key_or_full_date"] = before - len(out)

    # Remove invalid years (outside 2000-2035)
    before = len(out)
    out = out[(out["calendar_year"] >= 2000) & (out["calendar_year"] <= 2035)]
    removed["year_out_of_range"] = before - len(out)

    # Remove invalid months (must be 1-12)
    before = len(out)
    out = out[(out["calendar_month"] >= 1) & (out["calendar_month"] <= 12)]
    removed["invalid_month"] = before - len(out)

    # Remove invalid quarters (must be 1-4)
    before = len(out)
    out = out[(out["calendar_quarter"] >= 1) & (out["calendar_quarter"] <= 4)]
    removed["invalid_quarter"] = before - len(out)

    # Fill remaining NULLs with 0
    for col in ["calendar_year", "calendar_quarter", "calendar_month",
                "month_num", "day_of_month", "day_of_week", "week_of_year",
                "quarter_num", "fiscal_year", "fiscal_quarter"]:
        out[col] = out[col].fillna(0).astype(int)

    # Deduplicate on date_key
    before = len(out)
    out = out.drop_duplicates(subset=["date_key"])
    removed["duplicates"] = before - len(out)

    out["date_key"] = out["date_key"].astype(int)

    for reason, count in removed.items():
        _log_removal("dim_date", reason, count)
    logger.info(f"  dim_date: {len(out):,} rows  (removed {sum(removed.values()):,} total)")
    return out


# ══════════════════════════════════════════════════════════════════
# DIM_GEOGRAPHY
# ══════════════════════════════════════════════════════════════════

def transform_dim_geography(df):
    logger.info("Transforming dim_geography...")
    removed = {}
    out = pd.DataFrame()

    out["source_geo_key"]    = pd.to_numeric(df["GeographyKey"], errors="coerce")
    out["city"]              = df["City"].astype(str).str.strip().replace("nan", "Unknown").fillna("Unknown")
    out["state_province"]    = df["StateProvinceName"].astype(str).str.strip().replace("nan", "").fillna("")
    out["country_region"]    = df["CountryRegion"].astype(str).str.strip().replace("nan", "Unknown").fillna("Unknown")
    out["postal_code"]       = df["PostalCode"].astype(str).str.strip().replace("nan", "").fillna("")
    out["region_type"]       = "Urban"
    out["distribution_zone"] = out["country_region"].apply(_zone)

    # Remove NULL geo_key
    before = len(out)
    out = out.dropna(subset=["source_geo_key"])
    removed["null_geo_key"] = before - len(out)

    # Remove rows where country is completely unknown
    before = len(out)
    out = out[out["country_region"] != "Unknown"]
    removed["unknown_country"] = before - len(out)

    # Sanitise text -- remove special characters
    for col in ["city", "state_province"]:
        out[col] = out[col].str.replace(r"[^\w\s\-]", "", regex=True).str.strip()

    # Deduplicate
    before = len(out)
    out = out.drop_duplicates(subset=["source_geo_key"])
    removed["duplicates"] = before - len(out)

    out["source_geo_key"] = out["source_geo_key"].astype(int)

    for reason, count in removed.items():
        _log_removal("dim_geography", reason, count)
    logger.info(f"  dim_geography: {len(out):,} rows  (removed {sum(removed.values()):,} total)")
    return out


# ══════════════════════════════════════════════════════════════════
# DIM_SALES_TERRITORY
# ══════════════════════════════════════════════════════════════════

def transform_dim_territory(df):
    logger.info("Transforming dim_sales_territory...")
    removed = {}
    out = pd.DataFrame()

    out["source_territory_key"] = pd.to_numeric(df["SalesTerritoryKey"], errors="coerce")
    out["territory_name"]       = df["TerritoryName"].astype(str).str.strip()
    out["territory_country"]    = df["TerritoryCountry"].astype(str).str.strip()
    out["territory_group"]      = df["TerritoryGroup"].astype(str).str.strip()

    # Remove NULL keys
    before = len(out)
    out = out.dropna(subset=["source_territory_key"])
    removed["null_territory_key"] = before - len(out)

    # Replace nan strings
    for col in ["territory_name", "territory_country", "territory_group"]:
        out[col] = out[col].replace("nan", "Unknown").replace("", "Unknown")

    # Remove rows where all 3 descriptive fields are unknown
    before = len(out)
    out = out[~((out["territory_name"]    == "Unknown") &
                (out["territory_country"] == "Unknown") &
                (out["territory_group"]   == "Unknown"))]
    removed["all_fields_unknown"] = before - len(out)

    # Deduplicate
    before = len(out)
    out = out.drop_duplicates(subset=["source_territory_key"])
    removed["duplicates"] = before - len(out)

    out["source_territory_key"] = out["source_territory_key"].astype(int)

    for reason, count in removed.items():
        _log_removal("dim_sales_territory", reason, count)
    logger.info(f"  dim_sales_territory: {len(out):,} rows  (removed {sum(removed.values()):,} total)")
    return out


# ══════════════════════════════════════════════════════════════════
# DIM_THERAPEUTIC_CLASS
# ══════════════════════════════════════════════════════════════════

def transform_dim_therapeutic_class(df_cat, df_subcat):
    logger.info("Transforming dim_therapeutic_class...")
    removed = {"null_keys": 0, "blank_subcategory_name": 0, "unknown_category": 0}
    rows = []

    for _, subcat in df_subcat.iterrows():
        sk = pd.to_numeric(subcat.get("ProductSubcategoryKey"), errors="coerce")
        ck = pd.to_numeric(subcat.get("ProductCategoryKey"), errors="coerce")

        # NULL key check
        if pd.isna(sk) or pd.isna(ck):
            removed["null_keys"] += 1
            continue
        sk = int(sk); ck = int(ck)

        # Blank name check
        name = _safe_str(subcat.get("SubcategoryName", ""), "")
        if not name:
            removed["blank_subcategory_name"] += 1
            continue

        # Category lookup
        cat_row  = df_cat[df_cat["ProductCategoryKey"] == ck]
        cat_name = _safe_str(cat_row["CategoryName"].iloc[0], "") if not cat_row.empty else ""
        if not cat_name:
            removed["unknown_category"] += 1
            continue

        tc, code, atc = CATEGORY_TO_THERAPEUTIC_CLASS.get(
            cat_name, ("Pharmaceutical", "Z00", "ATC-Z"))
        sub = SUBCATEGORY_TO_SUBCLASS.get(name, name)
        reg = REGULATORY_CATEGORY.get(tc, "Schedule H")

        rows.append({
            "source_category_key":    ck,
            "source_subcategory_key": sk,
            "class_code":             code,
            "therapeutic_class":      tc,
            "therapeutic_subclass":   sub,
            "regulatory_category":    reg,
            "atc_code":               atc,
        })

    for reason, count in removed.items():
        _log_removal("dim_therapeutic_class", reason, count)

    out = pd.DataFrame(rows).drop_duplicates(subset=["source_subcategory_key"])
    logger.info(f"  dim_therapeutic_class: {len(out):,} rows  (removed {sum(removed.values()):,} total)")
    return out


# ══════════════════════════════════════════════════════════════════
# DIM_DRUG
# ══════════════════════════════════════════════════════════════════

def transform_dim_drug(df_prod, df_subcat, df_cat):
    logger.info("Transforming dim_drug...")

    sub_map = {}
    for _, r in df_subcat.iterrows():
        sk = pd.to_numeric(r.get("ProductSubcategoryKey"), errors="coerce")
        ck = pd.to_numeric(r.get("ProductCategoryKey"), errors="coerce")
        if pd.notna(sk) and pd.notna(ck):
            sub_map[int(sk)] = {
                "sub_name": _safe_str(r.get("SubcategoryName"), "General"),
                "cat_key":  int(ck),
            }

    cat_map = {}
    for _, r in df_cat.iterrows():
        ck = pd.to_numeric(r.get("ProductCategoryKey"), errors="coerce")
        if pd.notna(ck):
            cat_map[int(ck)] = _safe_str(r.get("CategoryName"), "General")

    rows = []
    removed = {
        "null_product_key":   0,
        "null_product_name":  0,
        "zero_or_null_price": 0,
        "negative_price":     0,
    }
    anomaly_fixed = {"anomalous_cost_fixed": 0}

    for _, p in df_prod.iterrows():
        # NULL product key
        pk = pd.to_numeric(p.get("ProductKey"), errors="coerce")
        if pd.isna(pk):
            removed["null_product_key"] += 1
            continue
        pk = int(pk)

        # NULL or blank product name
        pname = _safe_str(p.get("ProductName", ""), "")
        if not pname:
            removed["null_product_name"] += 1
            continue

        # NULL or zero price
        price = pd.to_numeric(p.get("ListPrice"), errors="coerce")
        if pd.isna(price) or price <= 0:
            removed["zero_or_null_price"] += 1
            continue

        # Negative price
        if price < 0:
            removed["negative_price"] += 1
            continue

        price = round(float(price), 4)

        # Cost anomaly check -- cost > 3x price is a data error
        cost = _safe_float(p.get("StandardCost"), 0.0)
        if cost < 0:
            cost = 0.0
        if cost > price * 3:
            anomaly_fixed["anomalous_cost_fixed"] += 1
            cost = round(price * 0.70, 4)  # cap at 70% of price
        else:
            cost = round(cost, 4)

        stdp = _safe_float(p.get("DealerPrice"), price)
        if stdp <= 0:
            stdp = price

        sub_key  = pd.to_numeric(p.get("ProductSubcategoryKey"), errors="coerce")
        sub_info = sub_map.get(int(sub_key)) if pd.notna(sub_key) else None
        sub_name = sub_info["sub_name"] if sub_info else "General"
        cat_key  = sub_info["cat_key"]  if sub_info else 0
        cat_name = cat_map.get(cat_key, "General")

        generic  = _get_drug_name(pname, sub_name, pk)
        form     = _get_dosage_form(p.get("Color", ""))
        strength = _get_strength(p.get("Size", ""))

        # Make drug name unique by including product key suffix
        # This prevents two products mapping to the exact same drug name
        base_name = f"{generic} {strength} {form}".strip()
        if not base_name:
            base_name = f"Drug-{pk:04d}"

        # Check if this base_name was already used — if so, append a batch
        # identifier to make it unique (e.g. "Ramipril 250mg Injection-v2")
        # We track used names using the rows list
        used_names = {r["drug_name"] for r in rows}
        if base_name in used_names:
            # Try adding manufacturer batch code
            batch_codes = ["XR","SR","LA","CR","ER","IR","MR","DR","PR","EC"]
            code = batch_codes[pk % len(batch_codes)]
            drug_name = f"{base_name} ({code})"
            # If still duplicate, append product key
            if drug_name in used_names:
                drug_name = f"{base_name} [{pk}]"
        else:
            drug_name = base_name

        _, _, atc = CATEGORY_TO_THERAPEUTIC_CLASS.get(cat_name, ("", "Z00", "ATC-Z"))
        drug_code = f"{atc[:1]}-{pk:04d}"

        status_val  = str(p.get("Status", "")).strip()
        drug_status = "Discontinued" if status_val == "0" else "Active"

        rows.append({
            "source_product_key": pk,
            "drug_code":          drug_code,
            "drug_name":          drug_name,
            "dosage_form":        form,
            "dosage_strength":    strength,
            "source_subcat_key":  int(sub_key) if pd.notna(sub_key) else None,
            "unit_price":         price,
            "unit_cost":          cost,
            "standard_cost":      round(stdp, 4),
            "manufacturer":       "Zentrik Pharma Ltd",
            "drug_status":        drug_status,
        })

    for reason, count in removed.items():
        _log_removal("dim_drug", reason, count)
    for reason, count in anomaly_fixed.items():
        if count > 0:
            logger.warning(f"  [dim_drug] Fixed {count:,} rows -- {reason}")

    out = pd.DataFrame(rows).drop_duplicates(subset=["source_product_key"])
    logger.info(f"  dim_drug: {len(out):,} rows  (from {len(df_prod):,}, removed {sum(removed.values()):,})")
    return out


# ══════════════════════════════════════════════════════════════════
# DIM_CUSTOMER
# ══════════════════════════════════════════════════════════════════

def transform_dim_customer(df_cust, df_geo):
    logger.info("Transforming dim_customer...")

    geo_map = {}
    for _, r in df_geo.iterrows():
        gk = pd.to_numeric(r.get("source_geo_key"), errors="coerce")
        if pd.notna(gk):
            geo_map[int(gk)] = _safe_str(r.get("country_region"), "Unknown")

    rows = []
    removed  = {"null_customer_key": 0, "null_name": 0}
    fixed    = {"invalid_email_fixed": 0}

    for _, c in df_cust.iterrows():
        # NULL customer key
        ck = pd.to_numeric(c.get("CustomerKey"), errors="coerce")
        if pd.isna(ck):
            removed["null_customer_key"] += 1
            continue
        ck = int(ck)

        # NULL name check
        first = _safe_str(c.get("FirstName", ""), "")
        last  = _safe_str(c.get("LastName", ""), "")
        full  = f"{first} {last}".strip()
        if not full or full == "Unknown Unknown":
            removed["null_name"] += 1
            continue

        ctype, seg = _get_customer_info(ck)

        first_word = full.split()[0]
        if ctype == "Hospital":
            cname = f"{first_word} General Hospital #{ck}"
        elif ctype == "Clinic":
            cname = f"{first_word} Multispeciality Clinic #{ck}"
        elif ctype == "Pharmacy":
            cname = f"{first_word} Pharmacy #{ck}"
        elif ctype == "Wholesale Distributor":
            cname = f"{first_word} Medical Wholesale #{ck}"
        else:
            cname = f"{first_word} Retail Chain #{ck}"

        # Geography key
        gk_val  = pd.to_numeric(c.get("GeographyKey"), errors="coerce")
        geo_key = int(gk_val) if pd.notna(gk_val) else None

        # Email validation
        email = _safe_str(c.get("EmailAddress", ""), "")
        if email and "@" not in email:
            fixed["invalid_email_fixed"] += 1
            email = None
        elif not email or email == "Unknown":
            email = None

        # Phone cleanup
        phone = _safe_str(c.get("Phone", ""), "")
        phone = None if (not phone or phone == "Unknown") else phone

        rows.append({
            "source_customer_key": ck,
            "customer_code":       f"ZCU-{ck:06d}",
            "customer_name":       cname[:200],
            "customer_type":       ctype,
            "email":               email,
            "phone":               phone,
            "payment_terms":       "Net30",
            "customer_segment":    seg,
            "customer_status":     "Active",
            "source_geo_key":      geo_key,
        })

    for reason, count in removed.items():
        _log_removal("dim_customer", reason, count)
    for reason, count in fixed.items():
        if count > 0:
            logger.warning(f"  [dim_customer] Fixed {count:,} rows -- {reason}")

    out = pd.DataFrame(rows).drop_duplicates(subset=["source_customer_key"])
    logger.info(f"  dim_customer: {len(out):,} rows  (from {len(df_cust):,}, removed {sum(removed.values()):,})")
    return out


# ══════════════════════════════════════════════════════════════════
# FACT_DRUG_SALES
# ══════════════════════════════════════════════════════════════════

def transform_fact_sales(df_is, dim_drug, dim_customer, dim_date):
    logger.info("Transforming fact_drug_sales...")

    drug_map    = {int(r["source_product_key"]): i+1
                   for i, (_, r) in enumerate(dim_drug.iterrows())}
    cust_map    = {int(r["source_customer_key"]): i+1
                   for i, (_, r) in enumerate(dim_customer.iterrows())}
    valid_dates = set(dim_date["date_key"].astype(int).tolist())

    rows = []
    removed = {
        "null_order_number":  0,
        "drug_not_found":     0,
        "customer_not_found": 0,
        "date_not_found":     0,
        "future_date":        0,
        "zero_quantity":      0,
        "zero_or_null_price": 0,
        "negative_revenue":   0,
    }
    fixed = {"anomalous_cost_fixed": 0}

    for _, r in df_is.iterrows():
        # NULL order number
        order_num = _safe_str(r.get("SalesOrderNumber", ""), "")
        if not order_num:
            removed["null_order_number"] += 1
            continue

        # FK resolution
        pk = _safe_int(r.get("ProductKey"), -1)
        ck = _safe_int(r.get("CustomerKey"), -1)
        dk = _safe_int(r.get("OrderDateKey"), -1)

        if pk not in drug_map:
            removed["drug_not_found"] += 1
            continue
        if ck not in cust_map:
            removed["customer_not_found"] += 1
            continue
        if dk not in valid_dates:
            removed["date_not_found"] += 1
            continue

        # Future date check
        if dk > TODAY_KEY:
            removed["future_date"] += 1
            continue

        # Quantity must be positive
        qty = _safe_int(r.get("OrderQuantity"), 0)
        if qty <= 0:
            removed["zero_quantity"] += 1
            continue

        # Price must be positive
        price = _safe_float(r.get("UnitPrice"), 0.0)
        if price <= 0:
            removed["zero_or_null_price"] += 1
            continue

        # Revenue must not be negative
        net = _safe_float(r.get("SalesAmount"), 0.0)
        if net < 0:
            removed["negative_revenue"] += 1
            continue

        # Derive all measures
        disc  = _safe_float(r.get("UnitPriceDiscountPct"), 0.0)
        cost  = _safe_float(r.get("TotalProductCost"), 0.0)
        gross = _safe_float(r.get("ExtendedAmount"), price * qty)
        tax   = _safe_float(r.get("TaxAmt"), 0.0)
        frt   = _safe_float(r.get("Freight"), 0.0)

        # Cost anomaly -- cost cannot be more than 2x net revenue
        if cost < 0:
            cost = 0.0
        if net > 0 and cost > net * 2:
            fixed["anomalous_cost_fixed"] += 1
            cost = round(net * 0.60, 4)

        prof     = net - cost
        mgn      = round(prof / net * 100, 4) if net > 0 else 0.0
        disc_pct = max(0.0, min(100.0, disc * 100))

        # Validate ship and due date keys
        sdk = _safe_int(r.get("ShipDateKey"), 0)
        ddk = _safe_int(r.get("DueDateKey"), 0)
        sdk = sdk if sdk in valid_dates else None
        ddk = ddk if ddk in valid_dates else None

        # Territory key
        tk = _safe_int(r.get("SalesTerritoryKey"), 0)
        tk = tk if tk > 0 else None

        rows.append({
            "order_date_key":          dk,
            "ship_date_key":           sdk,
            "due_date_key":            ddk,
            "source_product_key":      pk,
            "source_customer_key":     ck,
            "source_territory_key":    tk,
            "source_order_number":     order_num,
            "source_order_line_num":   _safe_int(r.get("SalesOrderLineNumber"), 1),
            "source_system":           "AW_INTERNET",
            "units_sold":              qty,
            "unit_price":              round(price, 4),
            "unit_price_discount_pct": round(disc_pct, 4),
            "gross_revenue":           round(gross, 4),
            "discount_amount":         round(gross * disc, 4),
            "net_revenue":             round(net, 4),
            "cost_of_goods":           round(cost, 4),
            "gross_profit":            round(prof, 4),
            "gross_margin_pct":        mgn,
            "tax_amount":              round(tax, 4),
            "freight_cost":            round(frt, 4),
            "etl_batch_id":            ETL_BATCH_ID,
        })

    # Deduplicate order lines
    out    = pd.DataFrame(rows)
    before = len(out)
    out    = out.drop_duplicates(
        subset=["source_order_number", "source_order_line_num", "source_system"])
    removed["duplicate_order_lines"] = before - len(out)

    for reason, count in removed.items():
        _log_removal("fact_drug_sales", reason, count)
    for reason, count in fixed.items():
        if count > 0:
            logger.warning(f"  [fact_drug_sales] Fixed {count:,} rows -- {reason}")

    total_removed = sum(removed.values())
    logger.info(f"  fact_drug_sales: {len(out):,} rows  (from {len(df_is):,}, removed {total_removed:,})")
    return out


# ══════════════════════════════════════════════════════════════════
# FACT_INVENTORY
# ══════════════════════════════════════════════════════════════════

def transform_fact_inventory(df_inv, dim_drug, dim_date):
    logger.info("Transforming fact_inventory...")

    drug_map    = {int(r["source_product_key"]): i+1
                   for i, (_, r) in enumerate(dim_drug.iterrows())}
    valid_dates = set(dim_date["date_key"].astype(int).tolist())

    rows = []
    removed = {
        "null_product_key": 0,
        "null_date_key":    0,
        "drug_not_found":   0,
        "date_not_found":   0,
        "future_date":      0,
    }
    fixed = {"negative_units_fixed": 0, "negative_cost_fixed": 0}

    for _, r in df_inv.iterrows():
        pk = pd.to_numeric(r.get("ProductKey"), errors="coerce")
        dk = pd.to_numeric(r.get("DateKey"), errors="coerce")

        # NULL checks
        if pd.isna(pk):
            removed["null_product_key"] += 1
            continue
        if pd.isna(dk):
            removed["null_date_key"] += 1
            continue

        pk = int(pk)
        dk = int(dk)

        # FK checks
        if pk not in drug_map:
            removed["drug_not_found"] += 1
            continue
        if dk not in valid_dates:
            removed["date_not_found"] += 1
            continue

        # Future date check
        if dk > TODAY_KEY:
            removed["future_date"] += 1
            continue

        # Units -- clamp negatives to 0
        units = _safe_int(r.get("UnitsBalance"), 0)
        if units < 0:
            fixed["negative_units_fixed"] += 1
            units = 0

        # Cost -- clamp negatives to 0
        cost = _safe_float(r.get("UnitCost"), 0.0)
        if cost < 0:
            fixed["negative_cost_fixed"] += 1
            cost = 0.0

        stock_val = round(units * cost, 4)

        rows.append({
            "snapshot_date_key":  dk,
            "source_product_key": pk,
            "units_on_hand":      units,
            "units_ordered":      0,
            "units_dispatched":   0,
            "safety_stock_level": 0,
            "reorder_point":      0,
            "stock_value":        stock_val,
            "days_of_supply":     None,
            "stock_status":       _stock_status(units),
            "etl_batch_id":       ETL_BATCH_ID,
        })

    # Deduplicate -- one record per drug per date
    out    = pd.DataFrame(rows)
    before = len(out)
    out    = out.drop_duplicates(
        subset=["snapshot_date_key", "source_product_key"], keep="last")
    removed["duplicate_snapshots"] = before - len(out)

    for reason, count in removed.items():
        _log_removal("fact_inventory", reason, count)
    for reason, count in fixed.items():
        if count > 0:
            logger.warning(f"  [fact_inventory] Fixed {count:,} rows -- {reason}")

    total_removed = sum(removed.values())
    logger.info(f"  fact_inventory: {len(out):,} rows  (from {len(df_inv):,}, removed {total_removed:,})")
    return out


# ══════════════════════════════════════════════════════════════════
# MASTER TRANSFORM FUNCTION
# ══════════════════════════════════════════════════════════════════

def transform_all(raw):
    logger.info("=" * 60)
    logger.info("TRANSFORM PHASE -- Full data cleaning on all tables")
    logger.info("=" * 60)

    t = {}
    t["dim_date"]              = transform_dim_date(raw["dim_date"])
    t["dim_geography"]         = transform_dim_geography(raw["dim_geography"])
    t["dim_sales_territory"]   = transform_dim_territory(raw["dim_sales_territory"])
    t["dim_therapeutic_class"] = transform_dim_therapeutic_class(
                                     raw["dim_product_category"],
                                     raw["dim_product_subcategory"])
    t["dim_drug"]              = transform_dim_drug(
                                     raw["dim_product"],
                                     raw["dim_product_subcategory"],
                                     raw["dim_product_category"])
    t["dim_customer"]          = transform_dim_customer(
                                     raw["dim_customer"],
                                     t["dim_geography"])
    t["fact_drug_sales"]       = transform_fact_sales(
                                     raw["fact_internet_sales"],
                                     t["dim_drug"],
                                     t["dim_customer"],
                                     t["dim_date"])
    t["fact_inventory"]        = transform_fact_inventory(
                                     raw["fact_product_inventory"],
                                     t["dim_drug"],
                                     t["dim_date"])

    logger.info("\nTRANSFORM SUMMARY:")
    logger.info("-" * 45)
    total = 0
    for k, df in t.items():
        logger.info(f"  {k:<30}: {len(df):>8,} rows")
        total += len(df)
    logger.info(f"  {'TOTAL':<30}: {total:>8,} rows")
    logger.info("=" * 60)
    return t