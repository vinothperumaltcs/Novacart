#  Novacart — Production-Grade E-Commerce Data Pipeline

A production-grade, incremental data pipeline built on the **Medallion Architecture** (Bronze → Silver → Gold) using **Databricks**, **Delta Lake**, and **Azure SQL** via **Lakehouse Federation**. Designed to process only new or changed data, maintain full pipeline state across runs, and power BI dashboards through a curated Gold layer.

---

##  Architecture

![Novacart — Production-Grade E-Commerce Data Pipeline Architecture](novacart_architecture.png)

---

##  Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Data Model](#data-model)
- [Pipeline Design](#pipeline-design)
- [Key Concepts](#key-concepts)
- [Orchestration](#orchestration)
- [Setup & Configuration](#setup--configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Alerts](#monitoring--alerts)
- [Tech Stack](#tech-stack)

---

##  Overview

Most beginner pipelines perform full reloads on every run. This project demonstrates how real production systems are built:

- **Incremental loading** — only new or updated records are processed each run using watermark logic
- **Idempotent design** — watermarks advance only after a successful write; safe to retry without duplicates
- **SCD Type 2** — full history of product dimension changes preserved with `eff_start_date`, `eff_end_date`, and `is_current` flags
- **Data quality enforcement** — per-row DQ tagging routes bad records to a quarantine table without failing the pipeline
- **Automated orchestration** — Bronze → Silver → Gold chained via Databricks Workflows with task value passing
- **Lakehouse Federation** — direct query of Azure SQL via Unity Catalog; no JDBC connectors or staging databases required
- **Version-controlled** — all notebooks managed through GitHub + Databricks Repos

---

##  Repository Structure

```
Novacart/
│
├── BRONZE.py                # Incremental ingestion via Lakehouse Federation → Delta Bronze
├── SILVER.py                # Cleaning, deduplication, DQ checks, Delta MERGE → Silver
├── GOLD.py                  # SCD Type 2 dim + fact table + BI views → Gold
│
├── DDL.sql                  # Azure SQL source table definitions (products, orders, payments)
├── INITIAL_LOAD.sql         # Seed data for the first full load
├── IncrementalLoad-1.sql    # Simulated incremental changes — batch 1
├── IncrementalLoad-2.sql    # Simulated incremental changes — batch 2
│
└── README.md
```

---

##  Data Model

### Source Tables (Azure SQL)

```
products                          orders                    payments
────────────────────              ──────────────────────    ────────────────────
product_id      PK                order_id        PK        payment_id      PK
product_name                      customer_id               order_id        FK
category                          product_id      FK ──►    payment_status
price                             order_status              paid_amount
updated_at                        order_amount              processed_at
                                  created_at
                                  updated_at
```

### Gold Tables

| Table | Type | Description |
|---|---|---|
| `dim_products` | SCD Type 2 | Full history of product changes — tracks `product_name`, `category`, `price` with `is_current`, `eff_start_date`, `eff_end_date` |
| `fact_orders` | Fact | Denormalised orders enriched with current product and payment data; MERGE handles late-arriving payments |
| `vw_revenue_by_category` | View | Total revenue aggregated by product category |
| `vw_order_status_summary` | View | Order counts and amounts grouped by status |
| `vw_daily_revenue` | View | Daily revenue trend excluding cancelled orders |

---

##  Pipeline Design

###  Bronze — Incremental Ingestion (`BRONZE.py`)

Reads source tables from Azure SQL via Lakehouse Federation and writes raw, immutable data to Delta Bronze tables.

- **Lakehouse Federation** — Unity Catalog connection (`novacart-sql-connection`) queries Azure SQL directly with no JDBC setup
- **Watermark logic** — `WHERE updated_at > last_processed_ts` ensures only new or changed rows are read per run
- **Control table** (`control.pipeline_watermark`) — stores the last processed timestamp per entity; updated **only after** a successful Delta write, guaranteeing idempotency on retries
- **Append-only Delta tables** — Bronze is a full immutable audit log; records are never updated or deleted
- **`mergeSchema=true`** — absorbs upstream schema changes without code modifications
- **FK-ordered ingestion** — entities loaded in dependency order: `products → orders → payments`
- **Audit metadata** — every Bronze row carries `_batch_id`, `_ingestion_ts`, `_source`, `_ingestion_date`

###  Silver — Clean & Transform (`SILVER.py`)

Reads from Bronze Delta tables and produces clean, validated, deduplicated Silver tables via MERGE.

- **Money normalisation** — strips currency symbols (`$`, `€`), handles European decimal format (`,` → `.`), coerces empty strings to `NULL` before casting (ANSI-safe)
- **Deduplication** — window function on `(primary_key, watermark_col)` retains only the latest row per entity per batch
- **DQ tagging** — each row receives `_dq_pass` (Boolean) and `_dq_reason`; passing rows merge into Silver, failing rows route to `silver.dq_quarantine`
- **Delta MERGE (upsert)** — matched rows update in-place; new rows insert; no duplicates regardless of re-runs or retries
- **OPTIMIZE + ZORDER** — improves downstream join performance on frequently filtered columns

###  Gold — Business Layer (`GOLD.py`)

Transforms Silver data into analytics-ready dimensional models and BI views.

- **SCD Type 2 on `dim_products`** — detects attribute changes using NULL-safe comparison (`eqNullSafe`); closes the old version (`eff_end_date = now()`, `is_current = false`) and inserts a new current version
- **`fact_orders`** — left-joins orders ← products ← payments; MERGE on `order_id` handles late-arriving payments by updating `payment_id` and `paid_amount` when the payment record arrives after the order
- **BI views** — three pre-aggregated views for dashboard consumption
- **OPTIMIZE + ZORDER** — `dim_products` on `(product_id, is_current)`; `fact_orders` on `(order_id, customer_id)` — partition columns excluded from ZORDER per Delta Lake constraints

---

##  Key Concepts

| Concept | Implementation |
|---|---|
| Incremental loading | Watermark column (`updated_at` / `processed_at`) per entity; reads only deltas |
| Idempotency | Watermark advances only after successful Delta write — safe to re-run |
| Deduplication | Window function on PK + watermark column; keep `row_number = 1` |
| SCD Type 2 | NULL-safe change detection → expire old row, insert new version with lineage |
| Data quality | Per-row DQ tags → quarantine bad records without stopping the pipeline |
| Schema evolution | `mergeSchema=true` on Bronze writes; no code change needed on source drift |
| Task chaining | `dbutils.jobs.taskValues` passes `batch_id` and row counts between layers |
| Lakehouse Federation | Unity Catalog external connection to Azure SQL — no staging, no JDBC |

---

##  Orchestration

The pipeline is orchestrated via **Databricks Workflows** with explicit task dependencies and task-value passing:

```
[bronze_ingestion]
        │  (on SUCCESS)
        ▼
[silver_transform]
        │  (on SUCCESS)
        ▼
[gold_scd2_fact]
        │  (on SUCCESS)
        ├──► [bi_dashboard_refresh]
        └──► [pipeline_alert]
```

**Task values passed between layers:**

| Layer | Sets | Reads |
|---|---|---|
| Bronze | `bronze_batch_id`, `bronze_status`, `products_count`, `orders_count`, `payments_count` | — |
| Silver | `silver_batch_id`, `silver_status` | `bronze_batch_id` |
| Gold | `gold_status`, `fact_orders_count` | `silver_batch_id` |

---

## ️ Setup & Configuration

### Prerequisites

- Databricks workspace with **Unity Catalog** enabled (DBR 14+)
- Azure SQL Database with `products`, `orders`, `payments` tables (create using `DDL.sql`)
- Lakehouse Federation connection configured in Unity Catalog — connection name: `novacart-sql-connection`

### Step 1 — Create Source Tables

Run `DDL.sql` against your Azure SQL Database to create the three source tables.

### Step 2 — Seed Initial Data

Run `INITIAL_LOAD.sql` to populate the source tables with the initial dataset.

### Step 3 — Configure Notebook Parameters

Set these variables at the top of each notebook:

```python
CATALOG         = "main"                               # Unity Catalog name
BRONZE_SCHEMA   = "bronze"
SILVER_SCHEMA   = "silver"
GOLD_SCHEMA     = "gold"
CONTROL_SCHEMA  = "control"
FOREIGN_CATALOG = "novacart-sql-connection_catalog"    # Lakehouse Federation catalog
```

### Step 4 — Import to Databricks Repos

Link your Databricks workspace to this GitHub repository via **Databricks Repos** for version-controlled notebook execution.

### First Run Behaviour

All schemas and tables are bootstrapped automatically on first execution:

- `control.pipeline_watermark` seeds with epoch timestamp (`1900-01-01`) → triggers a full initial load
- Bronze, Silver, and Gold tables are created with explicit schemas if they don't already exist

---

##  Running the Pipeline

**Via Databricks Workflow (recommended):**

1. Import notebooks into Databricks Repos linked to this GitHub repository
2. Create a Workflow using the JSON configuration in the notebook `GOLD.py` (monitoring section)
3. Trigger a run — layers execute sequentially with automatic dependency management

**Simulating incremental changes:**

After the initial load, run `IncrementalLoad-1.sql` and `IncrementalLoad-2.sql` against Azure SQL to simulate product price changes, new orders, and payment updates. Re-trigger the workflow to see incremental processing, SCD Type 2 versioning, and late-arrival handling in action.

**Manual / standalone run:**

Run each notebook top-to-bottom in order: `BRONZE.py → SILVER.py → GOLD.py`

On standalone runs, Silver and Gold automatically resolve `BATCH_ID` from the control table — no widget configuration required.

---

##  Monitoring & Alerts

- **Control table** (`control.pipeline_watermark`) — shows per-entity run status, record counts, and last watermark timestamp at all times; queryable directly from Databricks SQL
- **Quarantine table** (`silver.dq_quarantine`) — captures every DQ failure with `entity_name`, `primary_key`, `raw_record` (JSON), `dq_reason`, and `batch_id` for easy investigation and replay
- **Task values** — expose row counts and status flags to the Databricks Jobs UI for each layer
- **Workflow alerts** — configured on job outcome; notifies on `PARTIAL_FAILURE` or `FAILED` status

---

##  Tech Stack

| Component | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Compute & notebooks | Azure Databricks (DBR 14+) |
| Storage format | Delta Lake |
| Source database | Azure SQL Database |
| Source connectivity | Databricks Lakehouse Federation |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Language | Python (PySpark) |
| Version control | GitHub + Databricks Repos |

---

##  License

This project is open source and available under the [MIT License](LICENSE).
