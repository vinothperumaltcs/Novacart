#  IMPORTS & CONFIGURATION 

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from datetime import datetime
import uuid

# Catalog / Schema / Source names  (parameterised for multi-env promotion)  # Unity Catalog name
CATALOG         = "novacart_adb"                           
BRONZE_SCHEMA   = "bronze"
CONTROL_SCHEMA  = "control"
FOREIGN_CATALOG = "novacart-sql-connection_catalog"   # Lakehouse Federation catalog

# NOTE: Catalog name contains hyphens → must be backtick-quoted in Spark SQL identifiers

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


# ── 1. BOOTSTRAP CONTROL TABLE ───────────────────────────────────────────────


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
    

# ── 2. BOOTSTRAP BRONZE TABLES ───────────────────────────────────────────────


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.products_raw (
        product_id    INT,
        product_name  STRING,
        category      STRING,
        price         STRING,          -- Kept as STRING; cleaning happens in Silver
        updated_at    TIMESTAMP,
        ingestion_ts TIMESTAMP,
        batch_id     STRING,
        source       STRING,
       ingestion_date DATE           -- Partition key
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
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
        ingestion_ts TIMESTAMP,
        batch_id     STRING,
        source       STRING,
        ingestion_date DATE
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Raw orders from Azure SQL via Lakehouse Federation. Append-only.'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.payments_raw (
        payment_id     INT,
        order_id       INT,
        payment_status STRING,
        paid_amount    STRING,         -- Kept as STRING; cleaning happens in Silver
        processed_at   TIMESTAMP,
        ingestion_ts  TIMESTAMP,
        batch_id      STRING,
        source        STRING,
        ingestion_date DATE
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Raw payments from Azure SQL via Lakehouse Federation. Append-only.'
""")


# ── 3. CORE INGESTION FUNCTION ────────────────────────────────────────────────


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
        .withColumn("ingestion_ts",   F.current_timestamp())
        .withColumn("batch_id",        F.lit(BATCH_ID))
        .withColumn("source",          F.lit(source_table))
        .withColumn("ingestion_date",  F.current_date())
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
    
 
# ── 4. RUN INGESTION FOR ALL ENTITIES ─────────────────────────────────────────

results = []
ENTITY_ORDER = ["products", "orders", "payments"]

for entity in ENTITY_ORDER:
    try:
        result = ingest_entity(entity)
        results.append(result)
    except Exception as e:
    
        spark.sql(f"""
            UPDATE {CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark
            SET pipeline_status = 'FAILED',
                last_run_ts     = current_timestamp(),
                batch_id        = '{BATCH_ID}'
            WHERE entity_name = '{entity}'
        """)
        print(f"  ERROR for {entity}: {e}")
        results.append({"entity": entity, "records": 0, "status": f"FAILED: {e}"})
        
        

# ── 5. BRONZE SUMMARY ──────────────────────────────────────────────────────────

print("\n" + "="*60)
print("  BRONZE INGESTION SUMMARY")
print("="*60)
for r in results:
    status = "SUCCESS" if r["status"] == "SUCCESS" else ("NO Records Found" if r["status"] == "NO_NEW_DATA" else "FAILED")
    print(f"  {status}  {r['entity']:<12} | {r['records']:>6} records | {r['status']}")
print("="*60)

# Expose metrics for Databricks Jobs UI and downstream tasks
dbutils.jobs.taskValues.set("bronzebatch_id",     BATCH_ID)
dbutils.jobs.taskValues.set("bronze_status",       "SUCCESS" if all(r["status"] in ("SUCCESS","NO_NEW_DATA") for r in results) else "PARTIAL_FAILURE")
dbutils.jobs.taskValues.set("products_count",      str(next((r["records"] for r in results if r["entity"]=="products"),  0)))
dbutils.jobs.taskValues.set("orders_count",        str(next((r["records"] for r in results if r["entity"]=="orders"),    0)))
dbutils.jobs.taskValues.set("payments_count",      str(next((r["records"] for r in results if r["entity"]=="payments"),  0)))


     