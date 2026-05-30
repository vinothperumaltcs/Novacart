# Databricks notebook source
# =============================================================================
# NOTEBOOK  : 01_bronze_incremental_ingestion.py
# LAYER     : Bronze  (Raw Ingestion)
# PURPOSE   : Incrementally ingest products / orders / payments from Azure SQL
#             via Lakehouse Federation → append-only Delta tables
# AUTHOR    : Data Engineering
# VERSION   : 1.0
# =============================================================================
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  INTERVIEW TALKING POINTS                                               │
# │  ► Watermark logic  : only rows WHERE updated_at > last_watermark       │
# │  ► Control table    : tracks per-entity state; updated AFTER success    │
# │  ► Append-only      : Bronze = audit log; never delete/overwrite        │
# │  ► Metadata columns : _ingestion_ts, _batch_id, _source_entity          │
# │  ► Idempotency      : watermark only advances on success → safe retry   │
# │  ► mergeSchema=true : absorbs source schema drift without code changes  │
# └─────────────────────────────────────────────────────────────────────────┘

# COMMAND ----------
# ── 0. IMPORTS & CONFIGURATION ──────────────────────────────────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from datetime import datetime
import uuid


# ── Catalog / Schema / Source names  (parameterised for multi-env promotion)
CATALOG         = "novacart_adb"                            # Unity Catalog name
BRONZE_SCHEMA   = "bronze"
CONTROL_SCHEMA  = "control"
FOREIGN_CATALOG = "novacart-sql-connection_catalog"   # Lakehouse Federation catalog
# NOTE: Catalog name contains hyphens → must be backtick-quoted in Spark SQL identifiers

# ── Source entity definitions  (name → source table path)
# Backticks are required around the catalog name because it contains hyphens,
# which are invalid unquoted characters in Spark SQL identifiers.
SOURCE_ENTITIES = {
    "products" : f"`{FOREIGN_CATALOG}`.dbo.products",
    "orders"   : f"`{FOREIGN_CATALOG}`.dbo.orders",
    "payments" : f"`{FOREIGN_CATALOG}`.dbo.payments",
}

# ── Watermark column per entity  (column tracked for incremental detection)
WATERMARK_COL = {
    "products" : "updated_at",
    "orders"   : "updated_at",
    "payments" : "processed_at",   # payments table uses processed_at
}

# ── Unique batch identifier for this pipeline run
BATCH_ID = str(uuid.uuid4())[:8]
RUN_TS   = datetime.now()

print(f"Batch ID  : {BATCH_ID}")
print(f"Run Time  : {RUN_TS}")

# COMMAND ----------
# ── 1. BOOTSTRAP CONTROL TABLE ───────────────────────────────────────────────
#
# WHY: The control table is the "memory" of the pipeline.
#      It remembers the last successfully processed watermark per entity.
#      On first run it seeds with epoch (1900-01-01) → full initial load.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CONTROL_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark (
        entity_name       STRING        NOT NULL,
        last_processed_ts TIMESTAMP     NOT NULL,
        last_run_ts       TIMESTAMP,
        records_loaded    BIGINT,
        batch_id          STRING,
        pipeline_status   STRING        -- SUCCESS / RUNNING / FAILED
    )
    USING DELTA
    COMMENT 'Tracks incremental watermark per Bronze entity'
""")

# Seed rows for any new entity not yet registered
for entity in SOURCE_ENTITIES:
    spark.sql(f"""
        INSERT INTO {CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark
        SELECT
            '{entity}'                            AS entity_name,
            CAST('1900-01-01T00:00:00' AS TIMESTAMP) AS last_processed_ts,
            current_timestamp()                   AS last_run_ts,
            0                                     AS records_loaded,
            '{BATCH_ID}'                          AS batch_id,
            'SEED'                                AS pipeline_status
        WHERE NOT EXISTS (
            SELECT 1 FROM {CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark
            WHERE entity_name = '{entity}'
        )
    """)
    
    # COMMAND ----------
# ── 2. BOOTSTRAP BRONZE TABLES ───────────────────────────────────────────────
#
# WHY: We create tables upfront with explicit schemas.
#      mergeSchema=true handles new columns from the source automatically.
#      Partitioning by _ingestion_date accelerates downstream Silver reads
#      (only the latest partition needs scanning on incremental runs).


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.products_raw (
        product_id    INT,
        product_name  STRING,
        category      STRING,
        price         STRING,          -- Kept as STRING; cleaning happens in Silver
        updated_at    TIMESTAMP,
        _ingestion_ts TIMESTAMP,
        _batch_id     STRING,
        _source       STRING,
        _ingestion_date DATE           -- Partition key
    )
    USING DELTA
    PARTITIONED BY (_ingestion_date)
    COMMENT 'Raw products from Azure SQL via Lakehouse Federation. Append-only.'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.orders_raw (
        order_id      INT,
        customer_id   INT,
        product_id    INT,
        order_status  STRING,
        order_amount  STRING,          -- Kept as STRING; cleaning happens in Silver
        created_at    TIMESTAMP,
        updated_at    TIMESTAMP,
        _ingestion_ts TIMESTAMP,
        _batch_id     STRING,
        _source       STRING,
        _ingestion_date DATE
    )
    USING DELTA
    PARTITIONED BY (_ingestion_date)
    COMMENT 'Raw orders from Azure SQL via Lakehouse Federation. Append-only.'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.payments_raw (
        payment_id     INT,
        order_id       INT,
        payment_status STRING,
        paid_amount    STRING,         -- Kept as STRING; cleaning happens in Silver
        processed_at   TIMESTAMP,
        _ingestion_ts  TIMESTAMP,
        _batch_id      STRING,
        _source        STRING,
        _ingestion_date DATE
    )
    USING DELTA
    PARTITIONED BY (_ingestion_date)
    COMMENT 'Raw payments from Azure SQL via Lakehouse Federation. Append-only.'
""")

# COMMAND ----------
# ── 3. CORE INGESTION FUNCTION ────────────────────────────────────────────────
#
# INTERVIEW: "Walk me through the incremental ingestion logic."
#
#   a) Read current watermark from control table
#   b) Query foreign catalog table filtered by watermark column > last value
#   c) Attach audit columns (_ingestion_ts, _batch_id, _source, _ingestion_date)
#   d) Append to Bronze Delta table (mergeSchema=true for schema evolution)
#   e) Update control table ONLY on success  → idempotency guaranteed

def ingest_entity(entity_name: str) -> dict:
    """
    Incrementally ingest one entity from Azure SQL into Bronze.

    Returns a result dict with counts and status for observability.
    """
    source_table  = SOURCE_ENTITIES[entity_name]
    bronze_table  = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}_raw"
    wm_col        = WATERMARK_COL[entity_name]
    control_table = f"{CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark"

    print(f"\n{'='*60}")
    print(f"  Ingesting: {entity_name.upper()}")
    print(f"{'='*60}")

    # ── a) Fetch current watermark ───────────────────────────────────────────
    wm_row = (
        spark.table(control_table)
             .filter(F.col("entity_name") == entity_name)
             .select("last_processed_ts")
             .collect()
    )
    last_watermark = wm_row[0]["last_processed_ts"] if wm_row else None
    print(f"  Last watermark : {last_watermark}")

    # ── b) Incremental read from Lakehouse Federation ────────────────────────
    # WHY Lakehouse Federation: no JDBC connector, no staging DB,
    # Unity Catalog governs access, predicates push down to Azure SQL.
    source_df = (
        spark.table(source_table)
             .filter(F.col(wm_col) > F.lit(last_watermark).cast(TimestampType()))
    )

    record_count = source_df.count()
    print(f"  New records    : {record_count}")

    if record_count == 0:
        print("  No new records — skipping write.")
        return {"entity": entity_name, "records": 0, "status": "NO_NEW_DATA"}

    # ── c) Attach ingestion metadata ─────────────────────────────────────────
    enriched_df = (
        source_df
        .withColumn("_ingestion_ts",   F.current_timestamp())
        .withColumn("_batch_id",        F.lit(BATCH_ID))
        .withColumn("_source",          F.lit(source_table))
        .withColumn("_ingestion_date",  F.current_date())
    )

    # ── d) Append to Bronze (never overwrite — Bronze is immutable) ──────────
    (
        enriched_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")   # handles source schema evolution
        .saveAsTable(bronze_table)
    )
    print(f"  Written to     : {bronze_table}")

    # ── e) Compute new high-watermark and update control table ───────────────
    # CRITICAL: watermark update happens AFTER the write succeeds.
    # If the write fails, watermark stays unchanged → next run re-processes
    # the same window → idempotent by design.
    new_watermark = (
        source_df
        .agg(F.max(F.col(wm_col)).alias("max_ts"))
        .collect()[0]["max_ts"]
    )

    spark.sql(f"""
        UPDATE {control_table}
        SET
            last_processed_ts = CAST('{new_watermark}' AS TIMESTAMP),
            last_run_ts       = current_timestamp(),
            records_loaded    = {record_count},
            batch_id          = '{BATCH_ID}',
            pipeline_status   = 'SUCCESS'
        WHERE entity_name = '{entity_name}'
    """)
    print(f"  Watermark → {new_watermark}")

    return {"entity": entity_name, "records": record_count, "status": "SUCCESS"}
    
    # COMMAND ----------
# ── 4. RUN INGESTION FOR ALL ENTITIES ─────────────────────────────────────────
#
# ORDER MATTERS: Products → Orders → Payments  (respects FK dependencies)
# Products must land in Bronze before Orders (product_id FK),
# Orders before Payments (order_id FK).

results = []
ENTITY_ORDER = ["products", "orders", "payments"]

for entity in ENTITY_ORDER:
    try:
        result = ingest_entity(entity)
        results.append(result)
    except Exception as e:
        # Mark failed entity in control table and continue
        # (partial success is logged; workflow alert catches failures)
        spark.sql(f"""
            UPDATE {CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark
            SET pipeline_status = 'FAILED',
                last_run_ts     = current_timestamp(),
                batch_id        = '{BATCH_ID}'
            WHERE entity_name = '{entity}'
        """)
        print(f"  ERROR for {entity}: {e}")
        results.append({"entity": entity, "records": 0, "status": f"FAILED: {e}"})
        
        
   # COMMAND ----------
# ── 5. BRONZE SUMMARY ──────────────────────────────────────────────────────────

print("\n" + "="*60)
print("  BRONZE INGESTION SUMMARY")
print("="*60)
for r in results:
    status_icon = "✅" if r["status"] == "SUCCESS" else ("⏭" if r["status"] == "NO_NEW_DATA" else "❌")
    print(f"  {status_icon}  {r['entity']:<12} | {r['records']:>6} records | {r['status']}")
print("="*60)

# Expose metrics for Databricks Jobs UI and downstream tasks
dbutils.jobs.taskValues.set("bronze_batch_id",     BATCH_ID)
dbutils.jobs.taskValues.set("bronze_status",       "SUCCESS" if all(r["status"] in ("SUCCESS","NO_NEW_DATA") for r in results) else "PARTIAL_FAILURE")
dbutils.jobs.taskValues.set("products_count",      str(next((r["records"] for r in results if r["entity"]=="products"),  0)))
dbutils.jobs.taskValues.set("orders_count",        str(next((r["records"] for r in results if r["entity"]=="orders"),    0)))
dbutils.jobs.taskValues.set("payments_count",      str(next((r["records"] for r in results if r["entity"]=="payments"),  0)))


     