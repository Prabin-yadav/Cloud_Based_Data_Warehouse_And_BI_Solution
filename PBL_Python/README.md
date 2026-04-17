# Zentrik Pharma — AdventureWorks → AWS RDS ELT (`PBL_Python/`)

This folder contains the **standalone ELT pipeline** used to convert **AdventureWorks DW (SQL Server)** into the **Zentrik Pharma star schema** and load it into **AWS RDS PostgreSQL**.

It also includes scripts used for **validation** and **evaluation proof** (performance, accuracy, etc.).

---

## Folder contents

```
PBL_Python/
├── config.py               # Source/target config + pharma mapping constants
├── create_tables.py        # Creates the full star schema on AWS RDS
├── drop_all_tables.py      # Drops all warehouse tables (DESTRUCTIVE)
├── pipeline.py             # Main runner: Extract → Transform → Load → Validate
├── extract.py              # AdventureWorks extraction logic
├── transform.py            # AW → pharma star schema transforms
├── load.py                 # Loads into AWS RDS
├── results_validation.py   # Full results + validation report (for slides)
├── evaluation_proof.py     # Evaluation criteria proof queries
├── export_for_tableau.py   # Optional: exports for Tableau
├── view_data.py            # Helpers to view/query loaded data
├── testCon.py              # Quick AWS RDS connectivity check
└── requirements.txt        # Pipeline-specific dependencies
```

---

## Prerequisites

- Python 3.10+
- AdventureWorks DW installed in SQL Server
- Windows: **ODBC Driver 17 for SQL Server** installed
- AWS RDS PostgreSQL reachable from your machine

---

## Configuration

`config.py` reads environment variables from the repo root `.env` (via `python-dotenv`).

Use the repo root `.env.example` as a template, and set at least:

```env
RDS_HOST=...
RDS_PORT=5432
RDS_DBNAME=...
RDS_USER=...
RDS_PASSWORD=...
RDS_SSLMODE=require

AW_SOURCE_SERVER=.\\SQLEXPRESS
AW_SOURCE_DATABASE=AdventureWorksDW2025
```

Notes:

- The SQL Server connection uses **Windows Trusted Connection** by default.
- `DATE_SHIFT_YEARS` is defined in `config.py` to shift AW dates forward.

---

## Run (fresh setup)

From the repo root:

1) Install pipeline dependencies

```bash
pip install -r PBL_Python/requirements.txt
```

2) Create the star schema on AWS RDS (run once)

```bash
python PBL_Python/create_tables.py
```

3) Run the full ELT pipeline

```bash
python PBL_Python/pipeline.py
```

This writes log files like `etl_run_YYYYMMDD_HHMMSS.log` **to your current working directory**.

---

## Reset (optional)

To drop all tables (DESTRUCTIVE):

```bash
python PBL_Python/drop_all_tables.py
```

Then recreate:

```bash
python PBL_Python/create_tables.py
```

---

## Validation & evaluation scripts

- Full validation report:

```bash
python PBL_Python/results_validation.py
```

- Evaluation proof (performance/cost/accuracy checks):

```bash
python PBL_Python/evaluation_proof.py
```

- Quick connectivity test:

```bash
python PBL_Python/testCon.py
```