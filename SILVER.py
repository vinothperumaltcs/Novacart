
#  IMPORTS & CONFIGURATION 

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime



CATALOG         = "novacart_adb"
BRONZE_SCHEMA   = "bronze"
SILVER_SCHEMA   = "silver"
CONTROL_SCHEMA  = "control"


try:
    BATCH_ID = dbutils.jobs.taskValues.get(taskKey="bronze_ingestion", key="bronzebatch_id")
except Exception:
    # Standalone / manual run: pick the most recently successful batch from control table
    try:
        BATCH_ID = (
            spark.table(f"{CATALOG}.{CONTROL_SCHEMA}.pipeline_watermark")
            .filter(F.col("pipeline_status") == "SUCCESS")   # only completed batches
            .orderBy(F.col("last_run_ts").desc())             # correct column name from Bronze schema
            .limit(1)
            .collect()[0]["batch_id"]
        )
        print(f"  INFO: Standalone run - resolved BATCH_ID from control table: {BATCH_ID}")
    except Exception as e:
        raise RuntimeError(
            f"Could not resolve BATCH_ID: task value unavailable and control table unreadable ({e}). "
            "Ensure Bronze notebook has run successfully before running Silver standalone."
        )

print(f"Processing Batch: {BATCH_ID}")



# ── 1. BOOTSTRAP SILVER TABLES ────────────────────────────────────────────────



spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# ── products (clean) ──────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.products (
        product_id    INT           NOT NULL,
        product_name  STRING,
        category      STRING,                 -- standardised to UPPER
        price         DECIMAL(10,2),          -- cleaned from VARCHAR
        updated_at    TIMESTAMP,
        silver_ts    TIMESTAMP,
        batch_id     STRING,
        CONSTRAINT pk_silver_products PRIMARY KEY (product_id) NOT ENFORCED
    )
    USING DELTA
    COMMENT 'Clean, deduplicated products. One row per product_id.'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact'   = 'true')
""")

# ── orders (clean) ────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.orders (
        order_id      INT           NOT NULL,
        customer_id   INT,
        product_id    INT,
        order_status  STRING,                 -- standardised to UPPER
        order_amount  DECIMAL(10,2),
        created_at    TIMESTAMP,
        updated_at    TIMESTAMP,
        silver_ts    TIMESTAMP,
        batch_id     STRING,
        CONSTRAINT pk_silver_orders PRIMARY KEY (order_id) NOT ENFORCED
    )
    USING DELTA
    COMMENT 'Clean, deduplicated orders. One row per order_id.'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact'   = 'true')
""")

# ── payments (clean) ──────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.payments (
        payment_id     INT          NOT NULL,
        order_id       INT,
        payment_status STRING,               -- standardised to UPPER
        paid_amount    DECIMAL(10,2),
        processed_at   TIMESTAMP,
        silver_ts     TIMESTAMP,
       batch_id      STRING,
        CONSTRAINT pk_silver_payments PRIMARY KEY (payment_id) NOT ENFORCED
    )
    USING DELTA
    COMMENT 'Clean, deduplicated payments. One row per payment_id.'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact'   = 'true')
""")

# ── quarantine table (shared across all entities) ────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.dq_quarantine (
        entity_name    STRING,
        primary_key    STRING,
        raw_record     STRING,               -- JSON-serialised source row
        dq_reason      STRING,
        batch_id       STRING,
        quarantine_ts  TIMESTAMP
    )
    USING DELTA
    COMMENT 'Rows that failed DQ checks. Investigate and reprocess manually.'
""")

# COMMAND ----------
# ── 2. REUSABLE CLEANING UTILITIES ────────────────────────────────────────────


def clean_money_column(col_name: str):
    """
    Normalises a messy money VARCHAR column to DECIMAL(10,2).
    Handles: '$50.00', ' 50.00 ', '50,00', '??', 'N/A', '0.00', '', NULL

    FIX: The original code called .cast("decimal(10,2)") directly on the cleaned
    string. Spark's ANSI cast (default in Databricks Runtime 11+) throws
    CAST_INVALID_INPUT instead of returning NULL when the cleaned string is empty
    (e.g. source value was '' or contained only non-numeric chars like '??').
    Fix: coerce empty string → NULL BEFORE casting, so the DQ check downstream
    correctly catches it as invalid_price_format / invalid_amount_format rather
    than crashing the whole command.
    """
    cleaned = F.regexp_replace(
        F.regexp_replace(
            F.trim(F.col(col_name)),
            r'[^\d.,]', ''        # strip non-numeric chars except . and ,
        ),
        r',(\d{2})$', r'.\1'      # replace trailing ,XX → .XX  (European format)
    )
    return (
        F.when(cleaned == "", F.lit(None))   # empty after stripping → NULL (not cast error)
         .otherwise(cleaned)
         .cast("decimal(10,2)")
    )

def clean_status_column(col_name: str):
    """
    Normalises status strings: trim whitespace + UPPER case.
    Handles: 'shipped', ' PLACED ', 'CANCELLED', ''  → 'SHIPPED','PLACED','CANCELLED', NULL
    """
    return F.when(
        F.trim(F.col(col_name)) == "", F.lit(None).cast("string")
    ).otherwise(
        F.upper(F.trim(F.col(col_name)))
    )

def clean_name_column(col_name: str):
    """
    Normalises product names: trim whitespace.
    Handles: '   Product 15   ', 'PRODUCT-11', 'Prod_9'
    """
    return F.trim(F.col(col_name))
    
    
    # COMMAND ----------
# ── 3. PRODUCTS — SILVER PROCESSING ──────────────────────────────────────────

print("=" * 60)
print("  SILVER: PRODUCTS")
print("=" * 60)

# ── 3a. Read new Bronze records (only this batch's partition for efficiency)
bronze_products = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.products_raw")
    .filter(F.col("batch_id") == BATCH_ID)
)

print(f"  Bronze input rows : {bronze_products.count()}")

# ── 3b. Deduplicate: keep the LATEST updated_at per product_id

window_products = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc())

deduped_products = (
    bronze_products
    .withColumn("rn", F.row_number().over(window_products))
    .filter(F.col("rn") == 1)
    .drop("rn", "ingestion_ts", "source", "ingestion_date")
)

print(f"  After dedup       : {deduped_products.count()}")

# ── 3c. Apply data cleaning
cleaned_products = (
    deduped_products
    .withColumn("product_name", clean_name_column("product_name"))
    .withColumn("category",     F.upper(F.trim(F.col("category"))))   # UPPER + trim
    .withColumn("category",                                            # Fix known typos
        F.when(F.col("category") == "ELECTRNICS",  F.lit("ELECTRONICS"))  # typo fix
         .when(F.col("category") == "LIFESTYLE",   F.lit("LIFESTYLE"))
         .when(F.col("category") == "FITNESS",     F.lit("FITNESS"))
         .otherwise(F.col("category"))
    )
    .withColumn("price_clean",  clean_money_column("price"))
    .withColumn("silver_ts",   F.current_timestamp())
    .withColumn("batch_id",    F.lit(BATCH_ID))
)

# ── 3d. Data Quality: tag each row

VALID_CATEGORIES = ["ELECTRONICS", "LIFESTYLE", "FITNESS", "CLOTHING", "SPORTS", "HOME", "BOOKS"]

dq_products = (
    cleaned_products
    .withColumn("dq_pass",
        F.when(F.col("product_id").isNull(),              F.lit(False))
         .when(F.col("price_clean").isNull(),             F.lit(False))  # unparseable price
         .when(F.col("price_clean") <= 0,                 F.lit(False))  # zero / negative
         .when(F.col("category").isNull(),                F.lit(False))  # null category
         .when(~F.col("category").isin(VALID_CATEGORIES), F.lit(False))  # unknown category
         .otherwise(F.lit(True))
    )
    .withColumn("dq_reason",
        F.when(F.col("product_id").isNull(),              F.lit("null_product_id"))
         .when(F.col("price_clean").isNull(),             F.lit("invalid_price_format"))
         .when(F.col("price_clean") <= 0,                 F.lit("non_positive_price"))
         .when(F.col("category").isNull(),                F.lit("null_category"))
         .when(~F.col("category").isin(VALID_CATEGORIES), F.lit("invalid_category"))
         .otherwise(F.lit(None).cast("string"))
    )
)

good_products      = dq_products.filter(F.col("dq_pass") == True)
quarantine_products = dq_products.filter(F.col("dq_pass") == False)

print(f"  DQ pass           : {good_products.count()}")
print(f"  DQ quarantine     : {quarantine_products.count()}")

# ── 3e. Write quarantine records
if quarantine_products.count() > 0:
   
    qtn_exclude = {"dq_pass", "dq_reason", "price_clean"}
    qtn_cols    = [c for c in quarantine_products.columns if c not in qtn_exclude]
    qtn = (
        quarantine_products
        .withColumn("entity_name",   F.lit("products"))
        .withColumn("primary_key",   F.col("product_id").cast("string"))
        .withColumn("raw_record",    F.to_json(F.struct(*qtn_cols)))
        .withColumn("dq_reason",     F.col("dq_reason"))
        .withColumn("quarantine_ts", F.current_timestamp())
        .select("entity_name","primary_key","raw_record","dq_reason","batch_id","quarantine_ts")
        .withColumnRenamed("batch_id","batch_id")
    )
    qtn.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine"
    )

# ── 3f. Prepare final Silver columns
silver_products_df = (
    good_products
    .withColumn("price", F.col("price_clean"))
    .select("product_id","product_name","category","price",
            "updated_at","silver_ts","batch_id")
)

# ── 3g. MERGE into Silver (upsert — prevents duplicates on reruns)
if spark.catalog.tableExists(f"{CATALOG}.{SILVER_SCHEMA}.products"):
    silver_products_tbl = DeltaTable.forName(spark, f"{CATALOG}.{SILVER_SCHEMA}.products")
    
    (
        silver_products_tbl.alias("tgt")
        .merge(
            silver_products_df.alias("src"),
            "tgt.product_id = src.product_id"                     # match on PK
        )
        .whenMatchedUpdate(
            condition="src.updated_at > tgt.updated_at",          # only update if newer
            set={
                "product_name" : "src.product_name",
                "category"     : "src.category",
                "price"        : "src.price",
                "updated_at"   : "src.updated_at",
                "silver_ts"   : "src.silver_ts",
                "batch_id"    : "src.batch_id",
            }
        )
        .whenNotMatchedInsertAll()                                 # net-new product → INSERT
        .execute()
    )
    print(" MERGE complete for products")
else:
    silver_products_df.write.format("delta").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.products")
    print(" Initial load complete for products")
    

# ── 4. ORDERS — SILVER PROCESSING ────────────────────────────────────────────

print("\n" + "=" * 60)
print("  SILVER: ORDERS")
print("=" * 60)

# ── Valid statuses from the source (normalised to UPPER)
VALID_ORDER_STATUSES = ["PLACED", "SHIPPED", "CANCELLED", "COMPLETE", "PENDING"]

bronze_orders = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw")
    .filter(F.col("batch_id") == BATCH_ID)
)
print(f"  Bronze input rows : {bronze_orders.count()}")

window_orders = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

deduped_orders = (
    bronze_orders
    .withColumn("rn", F.row_number().over(window_orders))
    .filter(F.col("rn") == 1)
    .drop("rn", "ingestion_ts", "source", "ingestion_date")
)

cleaned_orders = (
    deduped_orders
    .withColumn("order_status",  clean_status_column("order_status"))
    .withColumn("order_amount_clean", clean_money_column("order_amount"))
    .withColumn("silver_ts",    F.current_timestamp())
    .withColumn("batch_id",     F.lit(BATCH_ID))
)

dq_orders = (
    cleaned_orders
    .withColumn("dq_pass",
        F.when(F.col("order_id").isNull(),                            F.lit(False))
         .when(F.col("customer_id").isNull(),                         F.lit(False))
         .when(F.col("order_status").isNull(),                        F.lit(False))
         .when(~F.col("order_status").isin(VALID_ORDER_STATUSES),     F.lit(False))
         .when(F.col("order_amount_clean").isNull(),                  F.lit(False))
         .when(F.col("order_amount_clean") < 0,                       F.lit(False))
         .otherwise(F.lit(True))
    )
    .withColumn("dq_reason",
        F.when(F.col("order_id").isNull(),                            F.lit("null_order_id"))
         .when(F.col("customer_id").isNull(),                         F.lit("null_customer_id"))
         .when(F.col("order_status").isNull(),                        F.lit("blank_or_null_status"))
         .when(~F.col("order_status").isin(VALID_ORDER_STATUSES),     F.lit("invalid_order_status"))
         .when(F.col("order_amount_clean").isNull(),                  F.lit("invalid_amount_format"))
         .when(F.col("order_amount_clean") < 0,                       F.lit("negative_amount"))
         .otherwise(F.lit(None).cast("string"))
    )
)

good_orders      = dq_orders.filter(F.col("dq_pass") == True)
quarantine_orders = dq_orders.filter(F.col("dq_pass") == False)

print(f"  DQ pass           : {good_orders.count()}")
print(f"  DQ quarantine     : {quarantine_orders.count()}")

if quarantine_orders.count() > 0:

    qtn_exclude = {"dq_pass", "dq_reason", "order_amount_clean"}
    qtn_cols    = [c for c in quarantine_orders.columns if c not in qtn_exclude]
    qtn = (
        quarantine_orders
        .withColumn("entity_name",   F.lit("orders"))
        .withColumn("primary_key",   F.col("order_id").cast("string"))
        .withColumn("raw_record",    F.to_json(F.struct(*qtn_cols)))
        .withColumn("dq_reason",     F.col("dq_reason"))
        .withColumn("quarantine_ts", F.current_timestamp())
        .select("entity_name","primary_key","raw_record","dq_reason","batch_id","quarantine_ts")
        .withColumnRenamed("batch_id","batch_id")
    )
    qtn.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine"
    )

silver_orders_df = (
    good_orders
    .withColumn("order_amount", F.col("order_amount_clean"))
    .select("order_id","customer_id","product_id","order_status","order_amount",
            "created_at","updated_at","silver_ts","batch_id")
)

if spark.catalog.tableExists(f"{CATALOG}.{SILVER_SCHEMA}.orders"):
    silver_orders_tbl = DeltaTable.forName(spark, f"{CATALOG}.{SILVER_SCHEMA}.orders")
    (
        silver_orders_tbl.alias("tgt")
        .merge(
            silver_orders_df.alias("src"),
            "tgt.order_id = src.order_id"
        )
        .whenMatchedUpdate(
            condition="src.updated_at > tgt.updated_at",
            set={
                "customer_id"  : "src.customer_id",
                "product_id"   : "src.product_id",
                "order_status" : "src.order_status",
                "order_amount" : "src.order_amount",
                "updated_at"   : "src.updated_at",
                "silver_ts"   : "src.silver_ts",
                "batch_id"    : "src.batch_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(" MERGE complete for orders")
else:
    silver_orders_df.write.format("delta").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    print("  Initial load complete for orders")
    

# ── 5. PAYMENTS — SILVER PROCESSING ──────────────────────────────────────────

print("\n" + "=" * 60)
print("  SILVER: PAYMENTS")
print("=" * 60)

VALID_PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]

bronze_payments = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.payments_raw")
    .filter(F.col("batch_id") == BATCH_ID)
)
print(f"  Bronze input rows : {bronze_payments.count()}")

window_payments = Window.partitionBy("payment_id").orderBy(F.col("processed_at").desc())

deduped_payments = (
    bronze_payments
    .withColumn("rn", F.row_number().over(window_payments))
    .filter(F.col("rn") == 1)
    .drop("rn", "ingestion_ts", "source", "ingestion_date")
)

cleaned_payments = (
    deduped_payments
    .withColumn("payment_status",   clean_status_column("payment_status"))
    .withColumn("paid_amount_clean", clean_money_column("paid_amount"))
    .withColumn("silver_ts",        F.current_timestamp())
    .withColumn("batch_id",         F.lit(BATCH_ID))
)

dq_payments = (
    cleaned_payments
    .withColumn("dq_pass",
        F.when(F.col("payment_id").isNull(),                              F.lit(False))
         .when(F.col("order_id").isNull(),                                F.lit(False))
         .when(F.col("payment_status").isNull(),                          F.lit(False))
         .when(~F.col("payment_status").isin(VALID_PAYMENT_STATUSES),     F.lit(False))
         .when(F.col("paid_amount_clean").isNull(),                       F.lit(False))
         .when(F.col("paid_amount_clean") < 0,                            F.lit(False))
         .otherwise(F.lit(True))
    )
    .withColumn("dq_reason",
        F.when(F.col("payment_id").isNull(),                              F.lit("null_payment_id"))
         .when(F.col("order_id").isNull(),                                F.lit("null_order_id"))
         .when(F.col("payment_status").isNull(),                          F.lit("null_payment_status"))
         .when(~F.col("payment_status").isin(VALID_PAYMENT_STATUSES),     F.lit("invalid_payment_status"))
         .when(F.col("paid_amount_clean").isNull(),                       F.lit("invalid_amount_format"))
         .when(F.col("paid_amount_clean") < 0,                            F.lit("negative_amount"))
         .otherwise(F.lit(None).cast("string"))
    )
)

good_payments       = dq_payments.filter(F.col("dq_pass") == True)
quarantine_payments = dq_payments.filter(F.col("dq_pass") == False)

print(f"  DQ pass           : {good_payments.count()}")
print(f"  DQ quarantine     : {quarantine_payments.count()}")

if quarantine_payments.count() > 0:
    qtn_exclude = {"dq_pass", "dq_reason", "paid_amount_clean"}
    qtn_cols    = [c for c in quarantine_payments.columns if c not in qtn_exclude]
    qtn = (
        quarantine_payments
        .withColumn("entity_name",   F.lit("payments"))
        .withColumn("primary_key",   F.col("payment_id").cast("string"))
        .withColumn("raw_record",    F.to_json(F.struct(*qtn_cols)))
        .withColumn("dq_reason",     F.col("dq_reason"))
        .withColumn("quarantine_ts", F.current_timestamp())
        .select("entity_name","primary_key","raw_record","dq_reason","batch_id","quarantine_ts")
        .withColumnRenamed("batch_id","batch_id")
    )
    qtn.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine"
    )

silver_payments_df = (
    good_payments
    .withColumn("paid_amount", F.col("paid_amount_clean"))
    .select("payment_id","order_id","payment_status","paid_amount",
            "processed_at","silver_ts","batch_id")
)

if spark.catalog.tableExists(f"{CATALOG}.{SILVER_SCHEMA}.payments"):
    silver_payments_tbl = DeltaTable.forName(spark, f"{CATALOG}.{SILVER_SCHEMA}.payments")
    (
        silver_payments_tbl.alias("tgt")
        .merge(
            silver_payments_df.alias("src"),
            "tgt.payment_id = src.payment_id"
        )
        .whenMatchedUpdate(
            condition="src.processed_at > tgt.processed_at",
            set={
                "order_id"       : "src.order_id",
                "payment_status" : "src.payment_status",
                "paid_amount"    : "src.paid_amount",
                "processed_at"   : "src.processed_at",
                "silver_ts"     : "src.silver_ts",
                "batch_id"      : "src.batch_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("  MERGE complete for payments")
else:
    silver_payments_df.write.format("delta").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.payments")
    print("  Initial load complete for payments")
    
    
    

# ── 6. SILVER SUMMARY & OPTIMISE ─────────────────────────────────────────────

print("\n" + "="*60)
print("  SILVER PROCESSING SUMMARY")
print("="*60)

for tbl in ["products","orders","payments"]:
    cnt = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{tbl}").count()
    print(f"  silver.{tbl:<12} : {cnt:>6} rows total")

qtn_cnt = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine").count()
print(f"  dq_quarantine     : {qtn_cnt:>6} rows total")
print("="*60)

# ── OPTIMIZE + ZORDER for faster downstream joins ────────────────────────────

spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.products  ZORDER BY (product_id)")
spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.orders    ZORDER BY (order_id, product_id)")
spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.payments  ZORDER BY (payment_id, order_id)")

# Pass metrics to Gold task
dbutils.jobs.taskValues.set("silver_status", "SUCCESS")
dbutils.jobs.taskValues.set("silverbatch_id", BATCH_ID)


# ── 7. DQ REPORT ) ─────────────────────────

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine")
    .groupBy("entity_name","dq_reason")
     .agg(F.count("*").alias("count"))
    .orderBy("entity_name","count", ascending=False)
 )