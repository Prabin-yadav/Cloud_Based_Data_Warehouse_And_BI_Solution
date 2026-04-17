import os
from pathlib import Path

import psycopg2

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

ROOT = Path(__file__).resolve().parent
if load_dotenv is not None:
    load_dotenv(ROOT / ".env", override=False)
    load_dotenv(override=False)

conn = psycopg2.connect(
    host=os.getenv("RDS_HOST", ""),
    port=int(os.getenv("RDS_PORT", "5432")),
    dbname=os.getenv("RDS_DBNAME", ""),
    user=os.getenv("RDS_USER", ""),
    password=os.getenv("RDS_PASSWORD", ""),
    sslmode=os.getenv("RDS_SSLMODE", "require"),
)
cur = conn.cursor()
cur.execute("SELECT drug_name, unit_price FROM dim_drug ORDER BY drug_name")
for row in cur.fetchall():
    print(f'{row[0]}  --  USD {row[1]}')
conn.close()