# Zentrik Pharma Intelligence (Streamlit App + Data Warehouse ETL)

This repository contains **two related but separate pieces**:

1. **Streamlit analytics application** (top-level project) that connects to an **AWS RDS PostgreSQL** data warehouse and provides dashboards, analytics, search, reporting, and an **in-app Upload → ETL** flow.
2. **`PBL_Python/` global ELT pipeline** (standalone) that extracts **AdventureWorks DW (SQL Server)**, transforms it into the Zentrik Pharma **star schema**, and loads it into **AWS RDS PostgreSQL**, including evaluation/validation scripts.

Both parts can share the same `.env` (AWS RDS connection), but **`PBL_Python/` is not required to run the Streamlit app** if your RDS warehouse is already populated.

---

## What the Streamlit app does

Entry point: `app.py`

The app connects to AWS RDS PostgreSQL and provides these pages:

- **Dashboard**: high-level KPIs and summaries
- **Analytics**: deeper trends and breakdowns (with date filtering)
- **Search & Filter**: find drugs/customers/sales quickly
- **Upload & ETL**: upload daily sales/inventory files and run **Extract → Transform → Load** into RDS
- **Reports**: generate exports (CSV/Excel)
- **ETL Monitor**: system health, audit logs, validation checks

The app reads from the warehouse using `db.py` (`qry()` + `ping()`).

### In-app Upload ETL (daily files)

When you upload a file from **Upload & ETL**, the app runs a lightweight ETL located in:

- `etl/extract.py` — reads CSV/Excel, normalizes columns, validates required fields
- `etl/transform.py` — cleans data, derives pharma KPIs (revenue, margin, stock status, etc.)
- `etl/load.py` — inserts into warehouse tables (with basic dedupe logic)

Uploaded files are stored locally in `uploads/` with a timestamped name.

---

## What `PBL_Python/` does (global ELT pipeline)

Folder: `PBL_Python/`

This is the **full ELT pipeline** used for the project’s main dataset:

- **Source**: AdventureWorks DW on **SQL Server** (local/VM)
- **Target**: Zentrik Pharma **star schema** on **AWS RDS PostgreSQL**
- Includes:
  - schema creation scripts
  - end-to-end pipeline runner with logs
  - validation/evaluation scripts (row counts, data quality, performance proof)

Key scripts:

- `PBL_Python/create_tables.py` — creates the star schema tables on AWS RDS
- `PBL_Python/drop_all_tables.py` — drops all warehouse tables (destructive)
- `PBL_Python/pipeline.py` — runs Extract → Transform → Load → Validate (and writes `etl_run_*.log`)
- `PBL_Python/results_validation.py` — prints a full validation report for slides
- `PBL_Python/evaluation_proof.py` — performance/scalability/cost/accuracy proof queries
- `PBL_Python/testCon.py` — quick AWS RDS connectivity test

The pipeline applies a **date shift** (see `PBL_Python/config.py`) so AdventureWorks dates appear in a modern range.

---

## Prerequisites

- **Python 3.10+** recommended
- Network access to your **AWS RDS PostgreSQL** instance
- For the AdventureWorks pipeline:
  - AdventureWorks DW database in **SQL Server**
  - **ODBC Driver 17 for SQL Server** installed (Windows)

---

## Configuration (.env)

This repo uses environment variables for credentials.

- Do **not** commit secrets. `.env` is ignored by git (see `.gitignore`).
- Use `.env.example` as the template.

Minimum required for the app and loaders:

```env
RDS_HOST=...
RDS_PORT=5432
RDS_DBNAME=...
RDS_USER=...
RDS_PASSWORD=...
RDS_SSLMODE=require
RDS_CONNECT_TIMEOUT=15
```

Optional (only needed if you run the AdventureWorks pipeline):

```env
AW_SOURCE_SERVER=.\\SQLEXPRESS
AW_SOURCE_DATABASE=AdventureWorksDW2025
```

---

## How to run the whole system (end-to-end)

### 1) Create & activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2) Install dependencies

Install Streamlit app deps:

```bash
pip install -r requirements.txt
```

If you also want to run the AdventureWorks pipeline, install its extra deps too:

```bash
pip install -r PBL_Python/requirements.txt
```

### 3) Create `.env`

Copy the example and fill your values:

- Copy `.env.example` → `.env`
- Set `RDS_*` variables (and `AW_SOURCE_*` if needed)

### 4) Create the warehouse schema (run once)

This creates all tables + indexes required by **both** the app and the ETL:

```bash
python PBL_Python/create_tables.py
```

(If you need a clean reset, run `python PBL_Python/drop_all_tables.py` first — it deletes all data.)

### 5A) Load the main dataset from AdventureWorks (optional)

Run the ELT pipeline (AdventureWorks → AWS RDS):

```bash
python PBL_Python/pipeline.py
```

This produces log files like `etl_run_YYYYMMDD_HHMMSS.log` **in your current working directory**.

Tip: if you want logs to stay inside `PBL_Python/`, run it from that folder:

```bash
cd PBL_Python
python pipeline.py
```

### 5B) Start the Streamlit application

```bash
streamlit run app.py
```

Open the URL printed by Streamlit.

### 6) Run daily uploads (optional)

In the app, go to **Upload & ETL**:

- Download the provided **Sales** or **Inventory** templates
- Upload your daily file
- Click **Run Full ETL Pipeline** to load it into AWS RDS

---

## Quick smoke tests

- Test AWS RDS connection from the Streamlit-style config:

```bash
python test.py
```

- Test AWS RDS connection for the AdventureWorks pipeline config:

```bash
python PBL_Python/testCon.py
```

---

## Troubleshooting

- **App shows OFFLINE — AWS RDS**: check `RDS_HOST`, `RDS_DBNAME`, `RDS_USER`, `RDS_PASSWORD`, and security group inbound rules.
- **Upload ETL fails with missing columns**: use the templates in the Upload page and ensure column names match.
- **AdventureWorks extract fails**: ensure SQL Server is running, AdventureWorksDW is installed, and **ODBC Driver 17 for SQL Server** is installed.
- **Tables not found**: run `python PBL_Python/create_tables.py` to create the schema.

---

## Repository structure (high level)

- `app.py` — Streamlit app shell + navigation
- `pages/` — Streamlit pages (dashboard, analytics, upload ETL UI, etc.)
- `etl/` — lightweight daily-file ETL used by the Streamlit Upload page
- `db.py` — shared PostgreSQL connection + query helpers
- `PBL_Python/` — standalone AdventureWorks → AWS ELT pipeline + evaluation scripts
- `uploads/` — local store of uploaded files (created automatically)
