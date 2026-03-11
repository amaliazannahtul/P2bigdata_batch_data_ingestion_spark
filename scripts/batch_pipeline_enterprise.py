from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as _sum, avg, desc
from pyspark.sql.types import *
import logging
import os
import time

start_time = time.time()

print("ENTERPRISE BATCH PIPELINE STARTED")

spark = SparkSession.builder \
    .appName("EnterpriseBatchPipelineDemo") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", StringType(), True)
])

df = spark.read \
    .schema(schema) \
    .option("header", True) \
    .csv("data/raw/ecommerce_raw.csv")

raw_count = df.count()
print("Total Raw Records:", raw_count)

df_clean = df.dropDuplicates()

df_clean = df_clean.dropna(
    subset=["transaction_id", "customer_id", "price", "quantity"]
)

df_clean = df_clean.filter(
    (col("price") > 0) & (col("quantity") > 0)
)

df_clean = df_clean.withColumn(
    "transaction_date",
    expr("to_date(transaction_date,'yyyy-MM-dd')")
)

df_transformed = df_clean.withColumn(
    "total_amount",
    col("price") * col("quantity")
)

df_curated = df_transformed.groupBy("category") \
    .agg(_sum("total_amount").alias("total_revenue"))

df_top_products = df_transformed.groupBy("product") \
    .agg(_sum("quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(5)

df_avg_transaction = df_transformed.groupBy("customer_id") \
    .agg(avg("total_amount").alias("avg_transaction_value"))

df_top_products.show()
df_curated.show()

df_transformed.write.mode("overwrite").parquet("data/clean/parquet/")

df_curated.write.mode("overwrite").parquet("data/curated/category_revenue/")

df_top_products.write.mode("overwrite").parquet("data/curated/top_products/")

df_avg_transaction.write.mode("overwrite").parquet("data/curated/avg_transaction/")

df_transformed.write.mode("overwrite") \
    .partitionBy("category") \
    .parquet("data/clean/partitioned_by_category/")

spark.stop()

print("PIPELINE COMPLETED SUCCESSFULLY")