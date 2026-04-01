#pyright: reportMissingImports=false
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count,when,to_date, round, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
import os

SILVER_PATH = "/workspace/output/silver/ecom_clean"
GOLD_CATEGORY = "/workspace/output/gold/category_kpi"
GOLD_COUNTRY = "/workspace/output/gold/country_kpi"
GOLD_PIVOT = "/workspace/output/gold/country_category_pivot"

for _path in [SILVER_PATH, GOLD_CATEGORY, GOLD_COUNTRY, GOLD_PIVOT]:
    os.makedirs(_path, exist_ok=True)

print(f" Silver : {SILVER_PATH}")
print(f" Gold/cat : {GOLD_CATEGORY}")
print(f" Gold/cntry : {GOLD_COUNTRY}")
print(f" Gold/pivot : {GOLD_PIVOT}")

# 1. Create Spark Session
spark = SparkSession.builder \
 .appName("MyFirst_demo") \
 .config("spark.sql.shuffle.partitions", "8") \
 .master("local[*]") \
 .getOrCreate()

#spark.sparkContext.setLogLevel("WARN")
#print("✅ Spark started:", spark.version)

# # 2. Read CSV
# df = spark.read.csv(
# "/workspace/data/ecom.csv",
# header=True,
# inferSchema=True
# )

df_bronze = spark.read \
 .option("header", True) \
 .option("inferSchema", True) \
 .csv("/workspace/data/ecom.csv")

print("\n--- ACTION 1: printSchema() ---")
print("WHY: See column names & data types before any transformation.")
df_bronze.printSchema()

#df_bronze.show()

print("\n--- ACTION 2: show() ---")
print("WHY: Visually inspect the first 5 rows to understand the data shape.")
df_bronze.show(10, truncate=False)

print("\n--- ACTION 3: count() ---")
print("WHY: Confirm total rows loaded. Compare before/after cleaning.")
print("Total Bronze rows:", df_bronze.count())

print("\n--- NARROW 1: filter() ---")
print("WHY: Remove rows with NULL values in critical columns.")
print(" Null rows cannot be used for groupBy or revenue calculation.")

df_filtered = df_bronze.filter(
 col("customer_id").isNotNull() & # Need customer to track who ordered
 col("product_category").isNotNull() & # Need category to group sales
 col("order_date").isNotNull() & # Need date for time-series analysis
 col("price").isNotNull() & # Need price to calculate revenue
 col("quantity").isNotNull() & # Need quantity to calculate revenue
 col("payment_method").isNotNull() & # Need payment for payment analysis
 col("country").isNotNull() # Need country for geo analysis
)

df_filtered.show()

# Also remove logically invalid rows
df_filtered = df_filtered.filter(
 (col("price") > 0) & (col("quantity") > 0)
)

print("Rows before filter:", df_bronze.count()) # ACTION — 1000
print("Rows after filter:", df_filtered.count()) # ACTION — ~484

print("\n--- NARROW 2: select() ---")
print("WHY: Drop customer_email (PII(Personal Identifiable Info), not needed for analytics).")
print(" Fewer columns = less memory in every downstream operation.")

df_selected = df_filtered.select(
"order_id",
"customer_id",
"product_id",
"product_category",
"order_date", # still a string — will be cast below
"quantity",
"price",
"payment_method",
"country"
)
print("Columns now:", df_selected.columns)
df_selected.show()

print("\n--- NARROW 3: withColumn() ---")
print("WHY: (a) Derive 'revenue' = quantity * price — core business metric.")
print(" (b) Cast order_date string to real DateType for time queries.")

df_typed = df_selected \
 .withColumn(
"order_date",
 to_date(col("order_date"), "dd-MM-yyyy") # string → DateType
 ) \
 .withColumn(
"revenue",
 round(col("quantity") * col("price"), 2) # new column: total order value
 ) \
 .withColumn(
"ingested_at",
 current_timestamp() # audit trail — when was this processed
 )

# ACTION — verify the new columns exist and have correct types
print("\n--- ACTION 4: show() after withColumn ---")
print("WHY: Confirm revenue is computed and order_date is now a date.")
df_typed.select("order_id", "order_date", "quantity", "price", "revenue","ingested_at").show(4, truncate=False)

print("\n--- NARROW 4: withColumnRenamed() ---")
print("WHY: Shorten column names for cleaner downstream code.")
print(" product_category → category, payment_method → payment")

df_renamed = df_typed \
 .withColumnRenamed("product_category", "category") \
 .withColumnRenamed("payment_method", "payment")

print("Renamed columns:", df_renamed.columns)
df_renamed.show(4, truncate=False)

print("\n--- NARROW 5: cast() ---")
print("WHY: quantity arrived as Double (5.0). Cast to Integer (5).")
print(" Explicit types prevent wrong arithmetic in aggregations.")

df_cast = df_renamed \
 .withColumn("quantity", col("quantity").cast(IntegerType())) \
 .withColumn("price", col("price").cast(DoubleType())) \
 .withColumn("revenue", col("revenue").cast(DoubleType()))

df_cast.printSchema() # ACTION — confirm types changed

print("\n--- NARROW 6: drop() ---")
print("WHY: Remove internal audit column before finalising Silver.")
print(" Use drop() when you want to remove 1-2 columns from many.")

df_silver = df_cast.drop("ingested_at", "product_id")
print("Final Silver columns:", df_silver.columns)
df_silver.show(4, truncate=False)

print("\n--- NARROW 7: limit() ---")
print("WHY: During dev, sample a few rows instead of scanning the full table.")
print(" In production, always limit() before collect() for safety.")
df_sample = df_silver.limit(15)
df_sample.show()

print("\n--- ACTION 5: describe() ---")
print("WHY: Profile numeric columns — spot outliers, check value ranges.")
df_silver.describe("price", "quantity", "revenue").show()

print("\n--- WIDE 1: groupBy() + agg() — Revenue by Category ---")
print("WHY: Collapse 484 order rows into 6 category-level KPI rows.")
print(" SHUFFLES data so all rows of the same category meet on one node.")

df_category_gold = df_silver.groupBy("category").agg(
F.sum("revenue").alias("total_revenue"), # total money earned
F.count("order_id").alias("total_orders"), # how many orders
F.round(F.avg("revenue"), 2).alias("avg_order_value"), # typical order size
F.round(F.avg("price"), 2).alias("avg_price"), # typical product price
F.sum("quantity").alias("total_units_sold") # total items sold
)

# ACTION — show result
print("\n--- ACTION 6: show() — Category Gold Table ---")
print("WHY: Verify the aggregation looks correct before writing to storage.")
df_category_gold.orderBy(F.desc("total_revenue")).show(truncate=False)

print("\n--- WIDE 2: groupBy() — Revenue by Country ---")
print("WHY: Understand which country contributes the most revenue.")

df_country_gold = df_silver.groupBy("country").agg(
F.sum("revenue").alias("total_revenue"),
F.count("order_id").alias("total_orders"),
F.round(F.avg("revenue"), 2).alias("avg_order_value")
)

# ACTION — show
df_country_gold.orderBy(F.desc("total_revenue")).show()

print("\n--- WIDE 3: groupBy() — Payment Method Analysis ---")
print("WHY: Find which payment channel is most popular and valuable.")

df_payment_gold = df_silver.groupBy("payment").agg(
F.count("order_id").alias("transactions"),
F.round(F.sum("revenue"), 2).alias("total_revenue"),
F.round(F.avg("price"), 2).alias("avg_price")
).orderBy(F.desc("total_revenue"))

df_payment_gold.show() # ACTION

print("\n--- WIDE 4: join() with F.broadcast() ---")
print("WHY join(): Enrich Silver orders with category labels from a lookup.")
print("WHY broadcast(): Lookup has only 6 rows — replicate to each executor.")
print(" Avoids shuffling all 484 Silver rows across network.")

category_lookup = spark.createDataFrame([
 ("Electronics", "High-Value Tech", "HVT"),
 ("Sports", "Active Lifestyle", "ACT"),
 ("Home", "Home & Living", "HML"),
 ("Clothing", "Fashion & Apparel", "FAS"),
 ("Beauty", "Beauty & Wellness", "BEW"),
 ("Books", "Knowledge & Media", "KNM"),
], ["category", "category_label", "category_code"])

df_enriched = df_silver.join(
F.broadcast(category_lookup), # broadcast = no shuffle of big table
 on="category",
 how="left" # left = keep all Silver rows even if no match
)

# ACTION — verify enrichment
df_enriched.select("order_id", "category", "category_label","category_code", "revenue").orderBy(F.desc("revenue")).show(5)

print("\n--- WIDE 5: distinct() ---")
print("WHY: Remove duplicate rows that may appear from reprocessed source files.")
print(" SHUFFLES all data so identical rows meet on the same partition.")

before = df_bronze.count() # ACTION — count before
df_deduped = df_silver.distinct()
after = df_deduped.count() # ACTION — count after
print(f"Before distinct: {before} | After distinct: {after} | Duplicates: {before - after}")

print("\n--- WIDE 6: orderBy() — Top 5 Highest Revenue Orders ---")
print("WHY: Global sort to surface the highest-value orders.")
print(" EXPENSIVE — use only when you need true global order.")

df_top5 = df_silver \
 .orderBy(F.desc("revenue")) \
 .select("order_id", "customer_id", "category", "country", "revenue")

# ACTION — show top 5
df_top5.show(5, truncate=False)

print("\n--- WIDE 7: repartition() ---")
print("WHY: Co-locate rows by country so groupBy('country') avoids a second shuffle.")
print(" Use when you know your next operation groups by the same key.")

df_repartitioned = df_silver.repartition(6, "country")
print("Default partitions :", df_silver.rdd.getNumPartitions())
print("After repartition(6):", df_repartitioned.rdd.getNumPartitions())

print("\n--- COALESCE (Narrow-ish) ---")
print("WHY: Reduce to 1 partition before writing Gold table.")
print(" coalesce avoids shuffle (unlike repartition) — faster for shrinking.")

df_cat_write = df_country_gold.coalesce(1)
print("Partitions before coalesce:", df_repartitioned.rdd.getNumPartitions())
print("Partitions after coalesce:", df_cat_write.rdd.getNumPartitions())

print("\n--- WIDE 8: Window Functions — rank() within category ---")
print("WHY: Rank each order within its category WITHOUT collapsing rows.")
print(" groupBy collapses to 1 row per group; window keeps all rows.")

window_by_cat = Window.partitionBy("category").orderBy(F.desc("revenue"))

df_ranked = df_silver.withColumn(
"rank_in_category",
F.rank().over(window_by_cat)
)

# ACTION — show top 2 orders per category
print("\n--- ACTION 7: show() — Top 2 orders per category ---")
df_ranked \
 .filter(col("rank_in_category") <= 2) \
 .select("category", "order_id", "country", "revenue", "rank_in_category") \
 .orderBy("category", "rank_in_category") \
 .show(truncate=False)

print("\n--- WIDE 9: union() / unionByName() ---")
print("WHY: Combine orders from different years into a single DataFrame.")
print(" In production this is how you merge monthly or quarterly files.")

df_2023 = df_silver.filter(F.year(col("order_date")) == 2023)
df_2024 = df_silver.filter(F.year(col("order_date")) == 2024)

df_all_years = df_2023.unionByName(df_2024)

print("2023 orders:", df_2023.count()) # ACTION
print("2024 orders:", df_2024.count()) # ACTION
print("Total union:", df_all_years.count()) # ACTION — should equal sum of above

print("\n--- WIDE 10: pivot() — Country × Category Revenue Matrix ---")
print("WHY: Create a cross-tab (country rows, category columns) for BI dashboards.")
print(" Specifying pivot values avoids an extra scan to discover them.")

df_pivot = df_silver.groupBy("country") \
 .pivot("category", ["Electronics", "Sports", "Home", "Clothing", "Beauty", "Books"]) \
 .agg(F.round(F.sum("revenue"), 0))

# ACTION — show the pivot matrix
print("\n--- ACTION 8: show() — Country × Category Pivot ---")
df_pivot.show(truncate=False)

# Write Silver
df_silver.coalesce(4) \
 .write \
 .mode("overwrite") \
 .partitionBy("country") \
 .parquet("output/silver/ecom_clean/")
print("Silver written → output/silver/ecom_clean/ (partitioned by country)")

# Write Gold — Category KPI
df_category_gold.coalesce(1) \
 .write \
 .mode("overwrite") \
 .parquet("output/gold/category_kpi/")
print("Gold written → output/gold/category_kpi/")

# Write Gold — Country KPI
df_country_gold.coalesce(1) \
 .write \
 .mode("overwrite") \
 .parquet("output/gold/country_kpi/")
print("Gold written → output/gold/country_kpi/")

# Write Gold — Pivot as CSV (for BI tools that prefer CSV)
df_pivot.coalesce(1) \
 .write \
 .mode("overwrite") \
 .option("header", True) \
 .csv("output/gold/country_category_pivot/")
print("Pivot written → output/gold/country_category_pivot/ (CSV)")
