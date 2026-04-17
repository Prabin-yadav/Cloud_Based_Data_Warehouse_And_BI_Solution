"""
extract.py  —  Extract from AdventureWorks SQL Server
Reads all source tables needed for the Zentrik Pharma DW.
"""
import pyodbc
import pandas as pd
import logging
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from config import SOURCE_CONN_STR, DATE_SHIFT_YEARS, DIM_DATE_START, DIM_DATE_END

logger = logging.getLogger(__name__)


def get_connection():
    return pyodbc.connect(SOURCE_CONN_STR)


def _fetch(conn, sql, table_name):
    """Fetch a table using cursor (avoids pandas/pyodbc compatibility issues)."""
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    data = {c: [] for c in cols}
    for row in rows:
        for c, v in zip(cols, row):
            data[c].append(v)
    df = pd.DataFrame(data)
    logger.info(f"  {table_name}: {len(df):,} rows extracted")
    return df


def shift_date_col(df, col):
    """Shift a date column forward by DATE_SHIFT_YEARS years."""
    if col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col], errors="coerce")
    df[col] = df[col].apply(
        lambda d: d + relativedelta(years=DATE_SHIFT_YEARS) if pd.notna(d) else d
    )
    return df


def generate_dim_date():
    """Generate dim_date rows from DIM_DATE_START to DIM_DATE_END."""
    logger.info(f"  Generating dim_date: {DIM_DATE_START} → {DIM_DATE_END}")
    MONTHS = ["","January","February","March","April","May","June",
              "July","August","September","October","November","December"]
    MABBR  = ["","Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]
    DAYS   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    rows = []
    d = date.fromisoformat(DIM_DATE_START)
    end = date.fromisoformat(DIM_DATE_END)
    while d <= end:
        q = (d.month - 1) // 3 + 1
        rows.append({
            "DateKey":        int(d.strftime("%Y%m%d")),
            "FullDateAlternateKey": d,
            "CalendarYear":   d.year,
            "CalendarQuarter": q,
            "CalendarMonth":  d.month,
            "MonthNumberOfYear": d.month,
            "EnglishMonthName": MONTHS[d.month],
            "MonthAbbr":      MABBR[d.month],
            "DayNumberOfMonth": d.day,
            "DayNumberOfWeek": d.weekday() + 1,
            "EnglishDayNameOfWeek": DAYS[d.weekday()],
            "WeekNumberOfYear": int(d.strftime("%W")),
            "CalendarSemester": 1 if d.month <= 6 else 2,
            "FiscalYear":    d.year if d.month >= 4 else d.year - 1,
            "FiscalQuarter": ((d.month - 4) % 12) // 3 + 1,
            "IsWeekend":     d.weekday() >= 5,
        })
        d += timedelta(1)

    df = pd.DataFrame(rows)
    logger.info(f"  dim_date: {len(df):,} rows generated ({DIM_DATE_START} → {DIM_DATE_END})")
    return df


def extract_all():
    """Extract all tables from AdventureWorks and return dict of DataFrames."""
    logger.info("=" * 60)
    logger.info("EXTRACT PHASE — AdventureWorks DW")
    logger.info(f"Date shift: +{DATE_SHIFT_YEARS} years (2010-2014 → 2022-2026)")
    logger.info("=" * 60)

    conn = get_connection()
    data = {}

    # ── dim_date (generated, not from AW) ────────────────────────
    data["dim_date"] = generate_dim_date()

    # ── DimGeography ─────────────────────────────────────────────
    data["dim_geography"] = _fetch(conn, """
        SELECT GeographyKey, City, StateProvinceName,
               EnglishCountryRegionName AS CountryRegion,
               PostalCode, SalesTerritoryKey
        FROM DimGeography
    """, "dim_geography")

    # ── DimSalesTerritory ─────────────────────────────────────────
    data["dim_sales_territory"] = _fetch(conn, """
        SELECT SalesTerritoryKey, SalesTerritoryRegion AS TerritoryName,
               SalesTerritoryCountry AS TerritoryCountry,
               SalesTerritoryGroup AS TerritoryGroup
        FROM DimSalesTerritory
    """, "dim_sales_territory")

    # ── DimProductCategory + DimProductSubcategory ───────────────
    data["dim_product_category"] = _fetch(conn, """
        SELECT ProductCategoryKey, EnglishProductCategoryName AS CategoryName
        FROM DimProductCategory
    """, "dim_product_category")

    data["dim_product_subcategory"] = _fetch(conn, """
        SELECT ProductSubcategoryKey, ProductCategoryKey,
               EnglishProductSubcategoryName AS SubcategoryName
        FROM DimProductSubcategory
    """, "dim_product_subcategory")

    # ── DimProduct ────────────────────────────────────────────────
    data["dim_product"] = _fetch(conn, """
        SELECT p.ProductKey, p.ProductAlternateKey, p.EnglishProductName AS ProductName,
               p.Color, p.Size, p.ListPrice, p.StandardCost, p.DealerPrice,
               p.ProductSubcategoryKey, p.Status,
               p.FinishedGoodsFlag, p.ProductLine
        FROM DimProduct p
        WHERE p.ListPrice IS NOT NULL
    """, "dim_product")

    # ── DimCustomer ───────────────────────────────────────────────
    data["dim_customer"] = _fetch(conn, """
        SELECT CustomerKey, FirstName, LastName,
               CONCAT(FirstName, ' ', LastName) AS FullName,
               EmailAddress, Phone,
               GeographyKey
        FROM DimCustomer
    """, "dim_customer")

    # ── FactInternetSales (with date shift) ───────────────────────
    df_is = _fetch(conn, """
        SELECT SalesOrderNumber, SalesOrderLineNumber,
               ProductKey, CustomerKey,
               SalesTerritoryKey,
               OrderDateKey, ShipDateKey, DueDateKey,
               OrderQuantity, UnitPrice, UnitPriceDiscountPct,
               ExtendedAmount, TotalProductCost,
               SalesAmount, TaxAmt, Freight
        FROM FactInternetSales
    """, "fact_internet_sales")

    # Shift date keys: add DATE_SHIFT_YEARS*10000 to YYYYMMDD keys
    # e.g. 20130322 → 20240322 (shift=11 years)
    for col in ["OrderDateKey", "ShipDateKey", "DueDateKey"]:
        df_is[col] = df_is[col].apply(lambda k: _shift_date_key(k, DATE_SHIFT_YEARS))
    data["fact_internet_sales"] = df_is

    # ── FactProductInventory (with date shift) ────────────────────
    df_inv = _fetch(conn, """
        SELECT ProductKey, DateKey,
               UnitsBalance, UnitCost,
               MovementDate
        FROM FactProductInventory
    """, "fact_product_inventory")

    df_inv["DateKey"] = df_inv["DateKey"].apply(lambda k: _shift_date_key(k, DATE_SHIFT_YEARS))
    df_inv["MovementDate"] = pd.to_datetime(df_inv["MovementDate"], errors="coerce")
    df_inv["MovementDate"] = df_inv["MovementDate"].apply(
        lambda d: d + relativedelta(years=DATE_SHIFT_YEARS) if pd.notna(d) else d
    )
    data["fact_product_inventory"] = df_inv

    conn.close()

    # Summary
    logger.info("\nEXTRACT SUMMARY:")
    total = 0
    for k, df in data.items():
        logger.info(f"  {k}: {len(df):,} rows")
        total += len(df)
    logger.info(f"  TOTAL: {total:,} rows")

    return data


def _shift_date_key(key, years):
    """
    Shift a YYYYMMDD integer key by N years.
    e.g. 20130322 + 11 years = 20240322
    """
    try:
        k = int(key)
        if k <= 0:
            return k
        y = k // 10000
        md = k % 10000
        new_y = y + years
        return new_y * 10000 + md
    except Exception:
        return key