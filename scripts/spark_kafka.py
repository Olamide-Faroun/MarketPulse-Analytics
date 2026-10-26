
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MarketPulseKafkaConsumer")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)


kafka_data = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock_prices_raw")
    .option("startingOffsets", "earliest")
    .load()
)

readable_data = kafka_data.selectExpr(
    "CAST(key AS STRING) AS stock_symbol",
    "CAST(value AS STRING) AS stock_message"
)

query = (
    readable_data.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

try:
    query.awaitTermination()
finally:
    query.stop()
    spark.stop()