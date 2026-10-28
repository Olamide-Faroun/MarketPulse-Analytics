
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, to_timestamp
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, LongType, BooleanType)
from postgres_writer import write_stock_batch


spark = (SparkSession.builder.appName("MarketPulseKafkaConsumer").master("local[*]").config(
        "spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7").getOrCreate())

spark.conf.set("spark.sql.session.timeZone", "UTC")

stock_schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("data_source", StringType()),
    StructField("market_data_feed", StringType()),
    StructField("ingestion_timestamp_utc", StringType()),
    StructField("pipeline_run_id", StringType()),
    StructField("pipeline_stage", StringType()),
    StructField("is_valid", BooleanType()),
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

complete_stock_data = stock_data.filter(
    col("symbol").isNotNull()
    & col("timestamp").isNotNull()
    & col("open").isNotNull()
    & col("high").isNotNull()
    & col("low").isNotNull()
    & col("close").isNotNull()
    & col("volume").isNotNull()
    & (col("open") > 0)
    & (col("high") >= col("open"))
    & (col("high") >= col("close"))
    & (col("low") <= col("open"))
    & (col("low") <= col("close"))
    & (col("low") > 0)
    & (col("volume") >= 0)
    & (col("high") >= col("low"))
)

complete_stock_data = complete_stock_data.withColumn(
    "market_timestamp_utc",
    to_timestamp(col("timestamp"))
)

complete_stock_data = complete_stock_data.filter(
    col("market_timestamp_utc").isNotNull()
)


processed_stock_data = complete_stock_data.withColumn(
    "price_change_percentage",
    round(
        (
            (col("close") - col("open"))
            / col("open")
        ) * 100,
        2
    )
)

display_data = processed_stock_data.select(
    "symbol",
    "market_timestamp_utc",
    "open",
    "close",
    "volume",
    "price_change_percentage",
    "data_source",
    "pipeline_run_id"
)

query = (
    processed_stock_data.writeStream
    .foreachBatch(write_stock_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/stock_prices_postgres"
    )
    .start()
)

try:
    query.awaitTermination()
finally:
    query.stop()
    spark.stop()