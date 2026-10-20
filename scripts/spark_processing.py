
from pyspark.sql import SparkSession

# Start Spark
spark = (
    SparkSession.builder
    .appName("MarketPulseAnalytics")
    .master("local[*]")
    .getOrCreate()
)

# Display the version
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

# Stop Spark
spark.stop()