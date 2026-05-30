# ── 0. IMPORTS & CONFIGURATION ──────────────────────────────────────────────

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

CATALOG        = "novacart_adb"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"

# Sentinel date for "open" SCD2 rows (current records)
SCD2_OPEN_DATE = "9999-12-31"

try:
    BATCH_ID = dbutils.jobs.taskValues.get(taskKey="silver_transform", key="silverbatch_id")
except Exception:
 
    try:
        BATCH_ID = (
            spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")
            .orderBy(F.col("gold_ts").desc())
            .limit(1)
            .collect()[0]["batch_id"]
        )
        print(f"  INFO: Standalone run - resolved BATCH_ID from fact_orders: {BATCH_ID}")
    except Exception:
        try:
            BATCH_ID = (
                spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
                .orderBy(F.col("_silver_ts").desc())
                .limit(1)
                .collect()[0]["batch_id"]
            )
            print(f"  INFO: Standalone run - resolved BATCH_ID from silver.products: {BATCH_ID}")
        except Exception as e:
            raise RuntimeError(
                f"Could not resolve BATCH_ID: task value unavailable and no Silver/Gold data found ({e}). "
                "Ensure Silver notebook has run successfully before running Gold standalone."
            )

print(f"Processing Batch: {BATCH_ID}")



# ── 1. BOOTSTRAP GOLD TABLES ─────────────────────────────────────────────────

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# ── dim_products (SCD Type 2) ─────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.dim_products (
        product_key       BIGINT GENERATED ALWAYS AS IDENTITY,   -- surrogate key
        product_id        INT        NOT NULL,                   -- natural key
        product_name      STRING,
        category          STRING,
        price             DECIMAL(10,2),
        effective_start   DATE       NOT NULL,
        effective_end     DATE       NOT NULL,                   -- 9999-12-31 = current
        is_current        BOOLEAN    NOT NULL,
        gold_ts          TIMESTAMP,
        batch_id         STRING
    )
    USING DELTA
    COMMENT 'SCD Type 2 dimension for products. One row per product per price/category version.'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact'   = 'true')
""")

# ── fact_orders (analytics-ready) ────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.fact_orders (
        order_id          INT,
        customer_id       INT,
        order_date        DATE,
        order_status      STRING,
        order_amount      DECIMAL(10,2),
        product_id        INT,
        product_name      STRING,
        category          STRING,
        product_price     DECIMAL(10,2),   -- price at time of order (point-in-time)
        payment_id        INT,
        payment_status    STRING,
        paid_amount       DECIMAL(10,2),
        gold_ts          TIMESTAMP,
        batch_id         STRING
    )
    USING DELTA
    PARTITIONED BY (order_date)
    COMMENT 'Analytics fact table: orders enriched with product and payment details.'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact'   = 'true')
""")

# ── 2. SCD TYPE 2 — DIM_PRODUCTS ─────────────────────────────────────────────


print("=" * 60)
print("  GOLD: dim_products (SCD Type 2)")
print("=" * 60)

# ── a) Read incoming Silver products ──────────────────────────────────────────
incoming_products = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")

# ── b) Compare with current Gold rows to find CHANGED products ───────────────
gold_current = (
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_products")
    .filter(F.col("is_current") == True)
    .select("product_id","product_name","category","price")
    .withColumnRenamed("product_name", "g_product_name")
    .withColumnRenamed("category",     "g_category")
    .withColumnRenamed("price",        "g_price")
)

changes_df = (
    incoming_products
    .join(gold_current, "product_id", "left")
    .withColumn("is_new",
        F.col("g_price").isNull()    # product_id not yet in Gold → new
    )
    .withColumn("is_changed",
   
        (~F.col("is_new")) & (
            (~F.col("price")       .eqNullSafe(F.col("g_price")))        |
            (~F.col("category")    .eqNullSafe(F.col("g_category")))      |
            (~F.col("product_name").eqNullSafe(F.col("g_product_name")))
        )
    )
    .filter(F.col("is_new") | F.col("is_changed"))
    .drop("g_product_name", "g_category", "g_price", "is_new", "is_changed")
)

change_count = changes_df.count()
print(f"  Changed/new products : {change_count}")

if change_count > 0:

    # ── STEP A: Expire current Gold rows for changed products ─────────────────

    gold_tbl = DeltaTable.forName(spark, f"{CATALOG}.{GOLD_SCHEMA}.dim_products")

    (
        gold_tbl.alias("tgt")
        .merge(
            changes_df.select("product_id").alias("src"),
            "tgt.product_id = src.product_id AND tgt.is_current = true"
        )
        .whenMatchedUpdate(set={
            "is_current"   : F.lit(False),
            "effective_end": F.date_sub(F.current_date(), 1),  # closed yesterday
            "gold_ts"     : F.current_timestamp()
        })
        .execute()
    )
    print(f"  ✅  Step A: expired {change_count} old row(s)")

    # ── STEP B: Insert new current rows for changed/new products ──────────────
    new_rows = (
        changes_df
        .withColumn("effective_start", F.current_date())
        .withColumn("effective_end",   F.to_date(F.lit(SCD2_OPEN_DATE)))
        .withColumn("is_current",      F.lit(True))
        .withColumn("gold_ts",        F.current_timestamp())
        .withColumn("batch_id",       F.lit(BATCH_ID))
        .select("product_id","product_name","category","price",
                "effective_start","effective_end","is_current","gold_ts","batch_id")
    )

    new_rows.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.dim_products"
    )
    print(f"  ✅  Step B: inserted {change_count} new current row(s)")

else:
    print("  ⏭  No product changes detected — dim_products unchanged.")
    
    

# ── 3. VERIFY SCD2 CORRECTNESS ───────────────────────────────────────────────


scd2_stats = spark.sql(f"""
    SELECT
        is_current,
        COUNT(*)          AS row_count,
        MIN(effective_start) AS earliest_start,
        MAX(effective_end)   AS latest_end
    FROM {CATALOG}.{GOLD_SCHEMA}.dim_products
    GROUP BY is_current
""")
display(scd2_stats)


# ── 4. FACT TABLE — fact_orders ───────────────────────────────────────────────


print("\n" + "=" * 60)
print("  GOLD: fact_orders")
print("=" * 60)

silver_orders   = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
silver_payments = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.payments")
dim_products_current = (
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_products")
    .filter(F.col("is_current") == True)
    .select("product_id","product_name","category",
            F.col("price").alias("product_price"))
)


fact_df = (
    silver_orders.alias("o")
    .join(
        silver_payments.alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "left"
    )
    .join(
        dim_products_current.alias("pr"),
        F.col("o.product_id") == F.col("pr.product_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.to_date(F.col("o.created_at")).alias("order_date"),
        F.col("o.order_status"),
        F.col("o.order_amount"),
        F.col("o.product_id"),
        F.col("pr.product_name"),
        F.col("pr.category"),
        F.col("pr.product_price"),
        F.col("p.payment_id"),
        F.col("p.payment_status"),
        F.col("p.paid_amount"),
        F.current_timestamp().alias("gold_ts"),
        F.lit(BATCH_ID).alias("batch_id")
    )
)

fact_count = fact_df.count()
print(f"  Fact rows to write : {fact_count}")


if spark.catalog.tableExists(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders"):
    fact_tbl = DeltaTable.forName(spark, f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")
    (
        fact_tbl.alias("tgt")
        .merge(
            fact_df.alias("src"),
            "tgt.order_id = src.order_id"
        )
        .whenMatchedUpdate(set={
            "order_status"   : "src.order_status",
            "order_amount"   : "src.order_amount",
            "product_name"   : "src.product_name",
            "category"       : "src.category",
            "product_price"  : "src.product_price",
            "payment_id"     : "src.payment_id",
            "payment_status" : "src.payment_status",
            "paid_amount"    : "src.paid_amount",
            "gold_ts"       : "src.gold_ts",
            "batch_id"      : "src.batch_id",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("MERGE complete for fact_orders")
else:
    fact_df.write.format("delta").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")
    print("Initial load complete for fact_orders")
    

# ── 5. BUSINESS AGGREGATIONS (BI-ready views) ─────────────────────────────────


spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.vw_revenue_by_category AS
    SELECT
        category,
        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(order_amount)                   AS gross_revenue,
        AVG(order_amount)                   AS avg_order_value,
        ROUND(SUM(CASE WHEN payment_status = 'SUCCESS'
                       THEN paid_amount ELSE 0 END), 2) AS collected_revenue
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_orders
    WHERE order_status != 'CANCELLED'
    GROUP BY category
""")

# Order status distribution
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.vw_order_status_summary AS
    SELECT
        order_status,
        COUNT(*)           AS order_count,
        SUM(order_amount)  AS total_amount
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_orders
    GROUP BY order_status
""")

# Daily revenue trend
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.vw_daily_revenue AS
    SELECT
        order_date,
        COUNT(DISTINCT order_id)  AS orders,
        SUM(order_amount)         AS revenue
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_orders
    WHERE order_status != 'CANCELLED'
    GROUP BY order_date
    ORDER BY order_date
""")

print(" BI views created/refreshed")

# ── 6. GOLD SUMMARY & OPTIMIZE ────────────────────────────────────────────────

print("\n" + "="*60)
print("  GOLD LAYER SUMMARY")
print("="*60)

dim_cnt  = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_products").count()
curr_cnt = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_products").filter("is_current=true").count()
fact_cnt = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders").count()

print(f"  dim_products   : {dim_cnt:>6} total rows ({curr_cnt} current)")
print(f"  fact_orders    : {fact_cnt:>6} total rows")
print("="*60)

# Optimize Gold tables for BI query performance
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.dim_products ZORDER BY (product_id, is_current)")
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.fact_orders  ZORDER BY (order_id, customer_id)")

# Signal downstream tasks (BI refresh, alerts)
dbutils.jobs.taskValues.set("gold_status",      "SUCCESS")
dbutils.jobs.taskValues.set("fact_orders_count", str(fact_cnt))


# ── 7. SAMPLE QUERIES FOR DEMOS ───────────────────────────────────────────────


display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.vw_revenue_by_category"))
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.vw_order_status_summary"))
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.vw_daily_revenue ORDER BY order_date DESC LIMIT 30"))

display(spark.sql(f"""
    SELECT product_id, product_name, category, price, effective_start, effective_end, is_current
   FROM   {CATALOG}.{GOLD_SCHEMA}.dim_products
   WHERE  product_id = 1001
    ORDER  BY effective_start
 """))
