"""STEP 3 — LOAD: Insert clean data into AWS RDS PostgreSQL."""
import psycopg2
import hashlib
from datetime import datetime
from db import DB

def _conn(): return psycopg2.connect(**DB)

def _drug_key(cur, name):
    cur.execute("SELECT drug_key FROM dim_drug WHERE LOWER(drug_name)=LOWER(%s) LIMIT 1",(name,))
    r=cur.fetchone(); return r[0] if r else None

def _ensure_drug(cur, drug_name):
    # Try exact match first.
    dk = _drug_key(cur, drug_name)
    if dk:
        return dk

    # Fallback to first-token fuzzy match when exact name is unavailable.
    token = str(drug_name).strip().split()[0] if str(drug_name).strip() else ""
    if not token:
        return None
    cur.execute(
        "SELECT drug_key FROM dim_drug WHERE LOWER(drug_name) LIKE LOWER(%s) LIMIT 1",
        (f"%{token}%",)
    )
    r = cur.fetchone()
    return r[0] if r else None

def _date_ok(cur, dk):
    cur.execute("SELECT 1 FROM dim_date WHERE date_key=%s",(dk,)); return bool(cur.fetchone())

def _customer_key(cur, name, ctype):
    cur.execute("SELECT customer_key FROM dim_customer WHERE LOWER(customer_name) LIKE LOWER(%s) LIMIT 1",
                (f"%{str(name)[:40]}%",))
    r=cur.fetchone()
    if r: return r[0]
    cur.execute("""INSERT INTO dim_customer
        (source_customer_key,customer_code,customer_name,customer_type,
         payment_terms,customer_segment,customer_status,created_at,updated_at)
        VALUES((SELECT COALESCE(MAX(source_customer_key),99999)+1 FROM dim_customer),
               CONCAT('ZCU-',EXTRACT(EPOCH FROM NOW())::int),%s,%s,'Net30','Standard','Active',NOW(),NOW())
        RETURNING customer_key""",(str(name)[:200],str(ctype)[:50]))
    return cur.fetchone()[0]

def _geo_key(cur, country):
    if not country: return None
    cur.execute("SELECT geography_key FROM dim_geography WHERE LOWER(country_region)=LOWER(%s) LIMIT 1",(str(country),))
    r=cur.fetchone(); return r[0] if r else None

def _terr_key(cur, name):
    if not name: return None
    cur.execute("SELECT territory_key FROM dim_sales_territory WHERE LOWER(territory_name)=LOWER(%s) LIMIT 1",(str(name),))
    r=cur.fetchone(); return r[0] if r else None

def _audit(cur, batch_id, step, table, read=0, ins=0, upd=0, rej=0, status="success", err=None, t0=None, t1=None):
    dur = (t1-t0).total_seconds() if t0 and t1 else None
    cur.execute("""INSERT INTO etl_audit_log
        (batch_id,pipeline_step,table_name,rows_read,rows_inserted,rows_updated,
         rows_rejected,status,error_message,started_at,completed_at,duration_seconds)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (batch_id,step,table,read,ins,upd,rej,status,err,t0,t1,dur))

def load_sales(df, batch_id):
    conn=_conn(); t0=datetime.now(); ins=upd=skip=0; errs=[]
    try:
        with conn.cursor() as cur:
            for _,row in df.iterrows():
                try:
                    dk = _ensure_drug(cur, str(row["drug_name"]))
                    if not dk: skip+=1; errs.append(f"Drug not found: {row['drug_name']}"); continue
                    date_key=int(row["date_key"])
                    if not _date_ok(cur,date_key): skip+=1; errs.append(f"Date not in dim_date: {date_key}"); continue
                    ck = _customer_key(cur, row["customer_name"], row.get("customer_type","Hospital"))
                    if not ck: skip+=1; continue
                    gk = _geo_key(cur, row.get("country_region",""))
                    tk = _terr_key(cur, row.get("sales_territory_name",""))

                    # Deterministic order key prevents duplicate inserts across repeated uploads.
                    dedupe_src = "|".join([
                        str(date_key),
                        str(dk),
                        str(ck),
                        str(int(row["units_sold"])),
                        f"{float(row['unit_price']):.4f}",
                        f"{float(row['net_revenue']):.4f}",
                    ])
                    order_num = "UP-" + hashlib.sha1(dedupe_src.encode("utf-8")).hexdigest()[:20].upper()
                    cur.execute("""
                        INSERT INTO fact_drug_sales (
                            order_date_key,drug_key,customer_key,geography_key,territory_key,
                            source_order_number,source_order_line_num,source_system,
                            units_sold,unit_price,unit_price_discount_pct,gross_revenue,
                            discount_amount,net_revenue,cost_of_goods,gross_profit,
                            gross_margin_pct,tax_amount,freight_cost,etl_batch_id)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (source_order_number,source_order_line_num,source_system) DO NOTHING
                        RETURNING 1
                    """,(date_key,dk,ck,gk,tk,order_num,1,"DAILY_UPLOAD",
                         int(row["units_sold"]),float(row["unit_price"]),
                         float(row.get("unit_price_discount_pct",0)),
                         float(row["gross_revenue"]),float(row["discount_amount"]),
                         float(row["net_revenue"]),float(row["cost_of_goods"]),
                         float(row["gross_profit"]),float(row["gross_margin_pct"]),
                         float(row["tax_amount"]),float(row["freight_cost"]),batch_id))
                    inserted = cur.fetchone()
                    if inserted:
                        ins+=1
                    else:
                        skip+=1
                except Exception as e:
                    skip+=1; errs.append(str(e)[:150]); continue
            t1=datetime.now()
            _audit(cur,batch_id,"load","fact_drug_sales",
                   read=len(df),ins=ins,upd=upd,rej=skip,t0=t0,t1=t1)
            conn.commit()
    except Exception as e:
        conn.rollback(); conn.close(); raise e
    conn.close()
    return {"inserted":ins,"skipped":skip,"errors":errs[:10]}

def load_inventory(df, batch_id):
    conn=_conn(); t0=datetime.now(); ins=upd=skip=0; errs=[]
    try:
        with conn.cursor() as cur:
            for _,row in df.iterrows():
                try:
                    dk = _ensure_drug(cur, str(row["drug_name"]))
                    if not dk: skip+=1; errs.append(f"Drug not found: {row['drug_name']}"); continue
                    date_key=int(row["date_key"])
                    if not _date_ok(cur,date_key): skip+=1; errs.append(f"Date: {date_key}"); continue
                    cur.execute("""
                        INSERT INTO fact_inventory (
                            snapshot_date_key,drug_key,units_on_hand,units_ordered,
                            units_dispatched,safety_stock_level,reorder_point,
                            stock_value,days_of_supply,stock_status,etl_batch_id)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (snapshot_date_key,drug_key) DO UPDATE SET
                            units_on_hand=EXCLUDED.units_on_hand,
                            units_ordered=EXCLUDED.units_ordered,
                            units_dispatched=EXCLUDED.units_dispatched,
                            stock_value=EXCLUDED.stock_value,
                            days_of_supply=EXCLUDED.days_of_supply,
                            stock_status=EXCLUDED.stock_status,
                            etl_batch_id=EXCLUDED.etl_batch_id
                        RETURNING (xmax = 0) AS inserted
                    """,(date_key,dk,
                         int(row["units_on_hand"]),int(row["units_ordered"]),
                         int(row["units_dispatched"]),0,0,
                         float(row["stock_value"]),row.get("days_of_supply"),
                         str(row["stock_status"]),batch_id))
                    was_insert = cur.fetchone()[0]
                    if was_insert:
                        ins+=1
                    else:
                        upd+=1
                except Exception as e:
                    skip+=1; errs.append(str(e)[:150]); continue
            t1=datetime.now()
            _audit(cur,batch_id,"load","fact_inventory",
                   read=len(df),ins=ins,upd=upd,rej=skip,t0=t0,t1=t1)
            conn.commit()
    except Exception as e:
        conn.rollback(); conn.close(); raise e
    conn.close()
    return {"inserted":ins,"updated":upd,"skipped":skip,"errors":errs[:10]}