
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, LongType)


spark = (SparkSession.builder.appName("MarketPulseKafkaConsumer").master("local[*]").config(
        "spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7").getOrCreate())

stock_schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("data_source", StringType()),
    StructField("market_data_feed", StringType()),
    StructField("ingestion_timestamp_utc", StringType()),
    StructField("pipeline_run_id", StringType()),
    StructField("pipeline_stage", StringType()),
    StructField("is_valid", StringType()),
    StructField("validation_failure_reason", StringType())
])

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)


kafka_data = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "stock_prices_raw")
    .option("startingOffsets", "earliest").load())

readable_data = kafka_data.selectExpr(
    "CAST(key AS STRING) AS stock_symbol",
    "CAST(value AS STRING) AS stock_message"
)

parsed_data = readable_data.withColumn(
    "stock_record",
    from_json(col("stock_message"), stock_schema)
)

stock_data = parsed_data.select(
    "stock_symbol",
    "stock_message",
    "stock_record.*"
)

display_data = stock_data.select(
    "symbol",
    "timestamp",
    "open",
    "close",
    "volume",
    "data_source",
    "pipeline_run_id"
)

query = (
    display_data.writeStream
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