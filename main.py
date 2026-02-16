from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("SalesETL").getOrCreate()

data = [
    ("2024-01-01", "Phone", 500),
    ("2024-01-01", "Laptop", 1200),
    ("2024-01-02", "Phone", 700),
    ("2024-01-02", "Tablet", 300)
]

df = spark.createDataFrame(data, ["date", "product", "amount"])

result = df.groupBy("product").agg(sum("amount").alias("total_sales"))
result.show()

spark.stop()
