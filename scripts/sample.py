from pyspark.sql import SparkSession

# 1 Create Spark Session
spark = SparkSession.builder \
    .appName("CSV_Comma_File") \
    .master("local[*]") \
    .getOrCreate()

# 2 Read comma separated CSV file
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .csv("/workspace/data/ecom.csv")

# 3 Show table data
print("=== Formatted Table ===")
df.show(df.count())

# 4 Show table structure
print("=== Schema ===")
df.printSchema()

# 5 Save formatted output
df.write.mode("overwrite").csv("/workspace/data/output")

print("✅ CSV converted to formatted table and saved.")