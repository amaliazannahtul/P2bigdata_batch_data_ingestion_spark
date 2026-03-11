from pyspark.sql import SparkSession

# Membuat Spark Session
spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .getOrCreate()

# Data contoh
data = [("A", 100), ("B", 200), ("C", 150)]
columns = ["Product", "Sales"]

# Membuat DataFrame
df = spark.createDataFrame(data, columns)

# Menampilkan isi DataFrame
df.show()

# Menutup Spark
spark.stop()
