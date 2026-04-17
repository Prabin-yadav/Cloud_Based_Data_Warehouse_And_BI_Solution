import psycopg2
import pandas as pd
import streamlit as st

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


_ROOT = Path(__file__).resolve().parent
if load_dotenv is not None:
    load_dotenv(_ROOT / ".env", override=False)
    load_dotenv(override=False)


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

DB = {
    "host": _env("RDS_HOST") or _env("DB_HOST") or "",
    "port": _env_int("RDS_PORT", 5432),
    "dbname": _env("RDS_DBNAME") or _env("DB_NAME") or _env("DB_DBNAME") or "",
    "user": _env("RDS_USER") or _env("DB_USER") or "",
    "password": _env("RDS_PASSWORD") or _env("DB_PASSWORD") or "",
    "sslmode": _env("RDS_SSLMODE", "require"),
    "connect_timeout": _env_int("RDS_CONNECT_TIMEOUT", 15),
}

@st.cache_resource(show_spinner=False)
def _conn(): 
    return psycopg2.connect(**DB)

def qry(sql, params=None):
    try:
        c = _conn()
        if c.closed:
            st.cache_resource.clear()
            c = _conn()
        return pd.read_sql(sql, c, params=params)
    except Exception as e:
        try:
            c2 = psycopg2.connect(**DB)
            return pd.read_sql(sql, c2, params=params)
        except Exception as e2:
            return pd.DataFrame()

def ping():
    try:
        c = _conn()
        with c.cursor() as cur: cur.execute("SELECT 1")
        return True
    except: return False
