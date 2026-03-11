from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Membuat Spark Session
spark = SparkSession.builder \
    .appName("Big Data Praktikum - GroupBy") \
    .getOrCreate()

# Extract
df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)

print("=== DATA AWAL ===")
df.show()

# Transform - Group By Category
grouped_df = df.groupBy("Category").agg(sum("Sales").alias("Total_Sales"))

print("=== TOTAL SALES PER CATEGORY ===")
grouped_df.show()

# Sorting hasil group
sorted_group = grouped_df.orderBy("Total_Sales", ascending=False)

print("=== SORTED TOTAL SALES PER CATEGORY ===")
sorted_group.show()

# Load
sorted_group.write.mode("overwrite").csv("data/processed/grouped_sales", header=True)

spark.stop()
