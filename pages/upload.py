import streamlit as st
import pandas as pd
import sys, os
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
UPLOAD_DIR = ROOT / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)


def run_full_etl(filepath, batch_id):
    """Run all 3 ETL steps: extract → transform → load."""
    from etl.extract   import extract
    from etl.transform import transform_sales, transform_inventory
    from etl.load      import load_sales, load_inventory

    result = {"batch_id": batch_id, "steps": {}}

    # STEP 1 — EXTRACT
    try:
        df_raw, utype, meta = extract(filepath)
        result["steps"]["extract"] = {"status":"success","rows":len(df_raw),
                                       "type":utype,"columns":list(df_raw.columns)}
        result["type"] = utype
    except Exception as e:
        result["steps"]["extract"] = {"status":"failed","error":str(e)}
        result["overall"] = "failed"; return result

    # STEP 2 — TRANSFORM
    try:
        if utype == "sales":
            df_clean, rpt = transform_sales(df_raw)
        else:
            df_clean, rpt = transform_inventory(df_raw)
        result["steps"]["transform"] = {"status":"success","input":rpt["input_rows"],
                                         "output":rpt["output_rows"],"removed":rpt["removed"],
                                         "warnings":rpt["warnings"]}
        result["df_clean"] = df_clean
    except Exception as e:
        result["steps"]["transform"] = {"status":"failed","error":str(e)}
        result["overall"] = "failed"; return result

    # STEP 3 — LOAD
    try:
        if utype == "sales":
            lr = load_sales(df_clean, batch_id)
        else:
            lr = load_inventory(df_clean, batch_id)
        result["steps"]["load"] = {"status":"success","inserted":lr.get("inserted",0),
                                    "updated":lr.get("updated",0),
                                    "skipped":lr.get("skipped",0),"errors":lr.get("errors",[])}
        result["overall"] = "success"
    except Exception as e:
        result["steps"]["load"] = {"status":"failed","error":str(e)}
        result["overall"] = "failed"

    return result


def show():
    st.markdown('<div class="page-title">Upload & ETL Pipeline</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Upload daily files → Extract → Transform → Load into AWS RDS</div>', unsafe_allow_html=True)

    # ETL Flow diagram
    st.markdown("""
    <div style="background:#0d1117;border:1px solid #1e2d45;border-radius:14px;padding:24px;margin-bottom:24px;">
    <div style="font-family:'JetBrains Mono';font-size:11px;color:#38bdf8;letter-spacing:2.5px;margin-bottom:16px;">ETL PIPELINE FLOW</div>
    <div style="display:flex;align-items:center;gap:0;flex-wrap:wrap;">
        <div style="flex:1;min-width:160px;background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px">📁</div>
            <div style="color:#818cf8;font-weight:600;font-size:14px;margin:4px 0">① UPLOAD</div>
            <div style="color:#64748b;font-size:12px">CSV / Excel file with all required columns</div>
        </div>
        <div style="color:#38bdf8;font-size:24px;padding:0 12px">→</div>
        <div style="flex:1;min-width:160px;background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px">📖</div>
            <div style="color:#38bdf8;font-weight:600;font-size:14px;margin:4px 0">② EXTRACT</div>
            <div style="color:#64748b;font-size:12px">Read file, validate columns, detect type</div>
        </div>
        <div style="color:#38bdf8;font-size:24px;padding:0 12px">→</div>
        <div style="flex:1;min-width:160px;background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px">⚗️</div>
            <div style="color:#22c55e;font-weight:600;font-size:14px;margin:4px 0">③ TRANSFORM</div>
            <div style="color:#64748b;font-size:12px">Clean nulls, validate, derive metrics</div>
        </div>
        <div style="color:#38bdf8;font-size:24px;padding:0 12px">→</div>
        <div style="flex:1;min-width:160px;background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px">☁️</div>
            <div style="color:#eab308;font-weight:600;font-size:14px;margin:4px 0">④ LOAD</div>
            <div style="color:#64748b;font-size:12px">Insert into AWS RDS PostgreSQL</div>
        </div>
    </div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["📤 Upload File", "📋 Column Templates"])

    with tab2:
        st.markdown('<div class="sec-hdr">REQUIRED COLUMNS — DOWNLOAD TEMPLATES</div>', unsafe_allow_html=True)

        c1,c2 = st.columns(2)
        with c1:
            st.markdown("#### 💊 Sales Upload Template")
            sales_tmpl = pd.DataFrame({
                "drug_name":               ["Ramipril 250mg Injection","Metoprolol 250mg Injection","Losartan 250mg Tablet","Amlodipine 250mg Cream"],
                "customer_name":           ["City General Hospital","Apollo Multispeciality Clinic","MedPlus Pharmacy Chennai","Fortis Wholesale Mumbai"],
                "customer_type":           ["Hospital","Clinic","Pharmacy","Wholesale Distributor"],
                "order_date":              ["2026-03-22","2026-03-22","2026-03-22","2026-03-22"],
                "units_sold":              [10,5,20,50],
                "unit_price":              [3578.27,2800.00,1499.00,1552.45],
                "unit_price_discount_pct": [0,5,0,2],
                "total_product_cost":      [2171.29,1680.00,899.40,930.00],
                "sales_amount":            [35782.70,13300.00,29980.00,76173.00],
                "tax_amt":                 [286.26,106.40,239.84,609.38],
                "freight":                 [89.46,33.25,74.95,190.43],
                "sales_territory_name":    ["Northwest","Northeast","Southwest","Northwest"],
                "country_region":          ["United States","United States","Australia","United States"],
                "city":                    ["Seattle","Boston","Sydney","New York"],
            })
            st.download_button("⬇️ Download Sales Template",
                sales_tmpl.to_csv(index=False),
                "zentrik_sales_template.csv","text/csv")
            st.dataframe(sales_tmpl,use_container_width=True)

        with c2:
            st.markdown("#### 📦 Inventory Upload Template")
            inv_tmpl = pd.DataFrame({
                "drug_name":        ["Ramipril 250mg Injection","Metoprolol 250mg Injection","Losartan 250mg Tablet","Amlodipine 250mg Cream"],
                "snapshot_date":    ["2026-03-22","2026-03-22","2026-03-22","2026-03-22"],
                "units_on_hand":    [500,0,250,80],
                "units_ordered":    [100,50,0,200],
                "units_dispatched": [30,0,15,25],
                "unit_cost":        [2171.29,1680.00,899.40,930.00],
            })
            st.download_button("⬇️ Download Inventory Template",
                inv_tmpl.to_csv(index=False),
                "zentrik_inventory_template.csv","text/csv")
            st.dataframe(inv_tmpl,use_container_width=True)

    with tab1:
        st.markdown('<div class="sec-hdr">UPLOAD YOUR DAILY FILE</div>', unsafe_allow_html=True)

        uploaded = st.file_uploader("Choose CSV or Excel file",type=["csv","xlsx","xls"],
            help="Must contain all required columns from the template")

        if uploaded:
            # Preview
            if uploaded.name.endswith(".csv"):
                df_preview = pd.read_csv(uploaded)
            else:
                df_preview = pd.read_excel(uploaded)
            uploaded.seek(0)

            st.markdown('<div class="sec-hdr">FILE PREVIEW</div>', unsafe_allow_html=True)
            m1,m2,m3,m4 = st.columns(4)
            m1.metric("Rows",    f"{len(df_preview):,}")
            m2.metric("Columns", str(len(df_preview.columns)))
            m3.metric("File",    uploaded.name.split(".")[0][:15])
            m4.metric("Type",    uploaded.name.split(".")[-1].upper())

            cols_lower = [c.strip().lower().replace(" ","_") for c in df_preview.columns]
            dtype = "📦 Inventory" if "units_on_hand" in cols_lower else "💊 Sales"
            st.info(f"Detected upload type: **{dtype}**")
            st.dataframe(df_preview.head(5),use_container_width=True)

            st.markdown("---")
            col_btn, col_warn = st.columns([1,3])
            with col_btn:
                run = st.button("🚀 Run Full ETL Pipeline", type="primary", use_container_width=True)
            with col_warn:
                st.markdown("""<div style="padding:12px;background:#111827;border:1px solid #1e2d45;border-radius:8px;
                    font-size:13px;color:#94a3b8;">
                    Data will go through <b style="color:#38bdf8">Extract → Transform → Load</b>.
                    Only clean valid rows will be inserted into AWS RDS.
                    Duplicate rows are automatically handled during load.</div>""", unsafe_allow_html=True)

            if run:
                save_path = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uploaded.name}"
                with open(save_path,"wb") as f: f.write(uploaded.getbuffer())
                batch_id = f"UPLOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                prog = st.progress(0,"Starting ETL...")
                prog.progress(5,"① EXTRACT — Reading file...")

                with st.spinner("Running ETL pipeline..."):
                    result = run_full_etl(str(save_path), batch_id)

                prog.progress(100,"✅ Complete!")

                st.markdown('<div class="sec-hdr">PIPELINE RESULTS</div>', unsafe_allow_html=True)

                # Step result cards
                c1,c2,c3 = st.columns(3)
                for col, step, icon, lbl in [
                    (c1,"extract","📖","EXTRACT"),
                    (c2,"transform","⚗️","TRANSFORM"),
                    (c3,"load","☁️","LOAD"),
                ]:
                    s = result["steps"].get(step,{})
                    ok = s.get("status")=="success"
                    col.markdown(f"""
                    <div class="etl-step {'etl-step-ok' if ok else 'etl-step-err'}">
                        <div style="font-size:32px">{icon}</div>
                        <div style="font-family:'JetBrains Mono';font-size:13px;
                            color:{'#22c55e' if ok else '#ef4444'};margin:6px 0">{lbl}</div>
                        <div class="status-pill {'pill-ok' if ok else 'pill-err'}">
                            {'SUCCESS' if ok else 'FAILED'}</div>
                        {"<div style='font-size:11px;color:#64748b;margin-top:8px'>"+str(s.get('error',''))[:60]+"</div>" if not ok else ""}
                    </div>""", unsafe_allow_html=True)

                st.markdown("")

                if result.get("overall")=="success":
                    e_s = result["steps"]["extract"]
                    t_s = result["steps"]["transform"]
                    l_s = result["steps"]["load"]

                    m1,m2,m3,m4,m5 = st.columns(5)
                    m1.metric("Extracted",   f"{e_s.get('rows',0):,}")
                    m2.metric("After Clean", f"{t_s.get('output',0):,}")
                    m3.metric("Removed",     f"{t_s.get('input',0)-t_s.get('output',0):,}")
                    m4.metric("Loaded",      f"{l_s.get('inserted',0):,}")
                    m5.metric("Skipped",     f"{l_s.get('skipped',0):,}")

                    if l_s.get("updated", 0):
                        st.info(f"Updated existing records: **{l_s.get('updated', 0):,}**")

                    if t_s.get("removed"):
                        st.markdown('<div class="sec-hdr">ROWS REMOVED IN TRANSFORM</div>', unsafe_allow_html=True)
                        for k,v in t_s["removed"].items():
                            st.warning(f"**{k.replace('_',' ').title()}** — {v:,} rows removed")

                    if l_s.get("errors"):
                        with st.expander(f"⚠️ {len(l_s['errors'])} load warnings"):
                            for err in l_s["errors"]: st.text(err)

                    if "df_clean" in result:
                        st.markdown('<div class="sec-hdr">TRANSFORMED DATA SAMPLE</div>', unsafe_allow_html=True)
                        st.dataframe(result["df_clean"].head(10),use_container_width=True)

                    st.success(f"✅ **Batch `{batch_id}`** complete — **{l_s.get('inserted',0):,} rows** loaded into AWS RDS")
                else:
                    for step, data in result["steps"].items():
                        if data.get("status")=="failed":
                            st.error(f"❌ **{step.upper()} FAILED**: {data.get('error','Unknown error')}")


if __name__ == "__main__":
    show()
