
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, round


spark = (
    SparkSession.builder
    .appName("MarketPulseAnalytics")
    .master("local[*]")
    .getOrCreate()
)


print("Spark version:", spark.version)

stock_data = [
    ("AAPL", 250.00, 255.00),
    ("MSFT", 410.00, 415.00),
    ("NVDA", 180.00, 175.00)
]

columns = ["symbol", "open", "close"]

stock_df = spark.createDataFrame(
    stock_data,
    schema=columns
)

stock_df.show()
stock_df.printSchema()

stock_df = stock_df.withColumn(
    "price_change_percentage",
    round(
        ((col("close") - col("open")) / col("open")) * 100,
        2
    )
)
stock_df.show()


spark.stop()