
A production-grade, incremental data pipeline built on the **Medallion Architecture** (Bronze → Silver → Gold) using Databricks, Delta Lake, and Azure SQL via Lakehouse Federation. Designed to process only new or changed data, maintain full pipeline state across runs, and power BI dashboards through a curated Gold layer.

---

##  Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Pipeline Design](#pipeline-design)
- [Key Concepts](#key-concepts)
- [Orchestration](#orchestration)
- [Setup & Configuration](#setup--configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Alerts](#monitoring--alerts)
- [Tech Stack](#tech-stack)

---

## Overview

Most beginner pipelines perform full reloads on every run. This project demonstrates how real production systems are built:

-  **Incremental loading** — only new or updated records are processed
-  **Idempotent design** — safe to retry; watermarks only advance on success
-  **SCD Type 2** — full history of product dimension changes preserved
-  **Data quality enforcement** — quarantine layer captures bad records without failing the pipeline
-  **Automated orchestration** — Bronze → Silver → Gold chained via Databricks Workflows
-  **Version-controlled** — all notebooks managed through GitHub + Databricks Repos

---

## Architecture
<img width="1920" height="1020" alt="image" src="https://github.com/user-attachments/assets/74dc7d15-591e-4a61-a45d-1ad06d5c2e60" />


```

---

## Project Structure

```
ecommerce-pipeline/
│
├── notebooks/
│   ├── 01_bronze_incremental_ingestion.py   # Raw ingestion via Lakehouse Federation
│   ├── 02_silver_clean_transform.py         # Cleaning, DQ, dedup, MERGE
│   ├── 03_gold_scd2_fact_table.py           # SCD2 dim + fact table + BI views
│   └── 04_workflow_config_monitoring.py     # Workflow JSON + observability helpers
│
└── README.md
```

---

## Data Model

```
products ──────────────────────────────────────┐
  product_id (PK)                              │
  product_name                                 │
  category                                     ▼
  price                             orders ────────────── payments
  updated_at                          order_id (PK)         payment_id (PK)
                                      customer_id           order_id (FK)
                                      product_id (FK) ──►   payment_status
                                      order_status          paid_amount
                                      order_amount          processed_at
                                      created_at / updated_at
```

**Gold Tables**

| Table | Type | Description |
|-------|------|-------------|
| `dim_products` | SCD Type 2 | Full history of product changes (`is_current`, `eff_start_date`, `eff_end_date`) |
| `fact_orders` | Fact | Denormalised orders enriched with product and payment data |
| `vw_revenue_by_category` | View | Revenue aggregated by product category |
| `vw_order_status_summary` | View | Order counts and amounts grouped by status |
| `vw_daily_revenue` | View | Daily revenue trend excluding cancelled orders |

---

## Pipeline Design

###  Bronze — Incremental Ingestion

- Reads source tables via **Lakehouse Federation** (Unity Catalog connection to Azure SQL) — no JDBC connectors or staging databases required
- **Watermark logic**: `WHERE updated_at > last_processed_ts` ensures only new/changed rows are read per run
- **Control table** (`control.pipeline_watermark`) stores per-entity watermark, updated *after* a successful write → guarantees idempotency on retry
- **Append-only** Delta tables — Bronze is a full audit log; records are never deleted or overwritten
- `mergeSchema=true` absorbs source schema drift without code changes
- Entity ingestion order respects FK dependencies: `products → orders → payments`

###  Silver — Clean & Transform

- **Money normalisation**: strips currency symbols, handles European decimal format, coerces empty strings to `NULL` before casting (ANSI-safe)
- **Deduplication**: window function on `(primary_key, watermark_col)` keeps the latest row per entity per batch
- **DQ checks**: each row tagged `_dq_pass / _dq_reason`; passing rows merge into Silver tables, failing rows route to `dq_quarantine`
- **Delta MERGE**: upsert pattern — matched rows update in place, new rows insert; no duplicates regardless of re-runs

###  Gold — Business Layer

- **SCD Type 2** on `dim_products`: detects attribute changes using NULL-safe comparison (`eqNullSafe`); closes old version (`eff_end_date`, `is_current=false`) and inserts new version
- **fact_orders**: left-joins orders ← products ← payments; MERGE on `order_id` handles late-arriving payments (updates `payment_id` when payment arrives after order)
- **OPTIMIZE + ZORDER**: `dim_products` on `(product_id, is_current)`; `fact_orders` on `(order_id, customer_id)` — partition columns excluded from ZORDER per Delta Lake rules

---

## Key Concepts

| Concept | Implementation |
|---------|---------------|
| Incremental loading | Watermark column (`updated_at` / `processed_at`) per entity |
| Idempotency | Watermark advances only after successful write |
| Deduplication | Window function on PK + watermark, keep `row_number = 1` |
| SCD Type 2 | NULL-safe change detection → expire old row, insert new version |
| Data quality | Per-row DQ tags → quarantine bad records without stopping pipeline |
| Schema evolution | `mergeSchema=true` on Bronze writes |
| Task chaining | `dbutils.jobs.taskValues` passes `batch_id` and status between layers |

---

## Orchestration

Pipeline is orchestrated via **Databricks Workflows** with task-level dependencies:

```
[bronze_ingestion]
        │ (on SUCCESS)
        ▼
[silver_transform]
        │ (on SUCCESS)
        ▼
[gold_scd2_fact]
        │ (on SUCCESS)
        ├──► [bi_dashboard_refresh]
        └──► [pipeline_alert]
```

Task values flow across layers:

```
Bronze sets  →  bronze_batch_id, bronze_status, products_count, orders_count, payments_count
Silver reads →  bronze_batch_id    Silver sets  →  silver_batch_id, silver_status
Gold reads   →  silver_batch_id    Gold sets    →  gold_status, fact_orders_count
```

---

## Setup & Configuration

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Azure SQL Database with `products`, `orders`, `payments` tables
- Lakehouse Federation connection configured in Unity Catalog (connection name: `novacart-sql-connection`)

### Configuration (top of each notebook)

```python
CATALOG        = "main"                            # Unity Catalog name
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"
CONTROL_SCHEMA = "control"
FOREIGN_CATALOG = "novacart-sql-connection_catalog"  # Lakehouse Federation catalog
```

### First Run

All schemas and tables are bootstrapped automatically on first execution:
- `control.pipeline_watermark` seeds with epoch (`1900-01-01`) → triggers full initial load
- Bronze, Silver, and Gold tables created with explicit schemas if they don't exist

---

## Running the Pipeline

**Via Databricks Workflow (recommended):**
1. Import notebooks into Databricks Repos (linked to this GitHub repo)
2. Create a Workflow using the JSON in `04_workflow_config_monitoring.py`
3. Trigger a run — layers execute in sequence automatically

**Manual / standalone run:**
- Run each notebook top-to-bottom in order: `01 → 02 → 03`
- On standalone runs, Silver and Gold automatically resolve `BATCH_ID` from the control table / previously written data (no widget required)

---

## Monitoring & Alerts

- **Control table** (`control.pipeline_watermark`) shows per-entity run status, record counts, and last watermark at all times
- **Quarantine table** (`silver.dq_quarantine`) captures every DQ failure with `entity_name`, `primary_key`, `raw_record` JSON, `dq_reason`, and `batch_id` for easy investigation
- **Task values** expose counts and statuses to the Databricks Jobs UI for each layer
- **Alerts** configured on workflow outcome — notifies on `PARTIAL_FAILURE` or `FAILED` status

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Cloud platform | Microsoft Azure |
| Compute & notebooks | Databricks (DBR 14+) |
| Storage format | Delta Lake |
| Source database | Azure SQL Database |
| Source connectivity | Databricks Lakehouse Federation |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Language | Python (PySpark) |
| Version control | GitHub + Databricks Repos |
