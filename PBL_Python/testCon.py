import psycopg2

from config import (
    TARGET_DBNAME,
    TARGET_HOST,
    TARGET_PASSWORD,
    TARGET_PORT,
    TARGET_SSLMODE,
    TARGET_USER,
)

try:
    conn = psycopg2.connect(
        host=TARGET_HOST,
        port=TARGET_PORT,
        dbname=TARGET_DBNAME,
        user=TARGET_USER,
        password=TARGET_PASSWORD,
        sslmode=TARGET_SSLMODE,
    )
    print('SUCCESS — connected to AWS RDS!')
    conn.close()
except Exception as e:
    print('FAILED — not connected to AWS RDS.')
    print('Error:', e)
