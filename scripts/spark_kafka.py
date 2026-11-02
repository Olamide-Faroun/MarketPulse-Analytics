import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (col, from_json, round, to_timestamp)
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, BooleanType)
from postgres_writer import write_stock_batch
from spark_indicators import (calculate_stock_indicators, save_stock_indicators)
from datetime import timedelta

load_dotenv()

database_password = os.getenv("POSTGRES_PASSWORD")

if not database_password:
    raise ValueError("POSTGRES_PASSWORD is missing")

database_url = "jdbc:postgresql://localhost:5433/marketpulse_db"

database_properties = {
    "user": "marketpulse_user",
    "password": database_password,
    "driver": "org.postgresql.Driver"
}

spark = (SparkSession.builder
    .appName("MarketPulseKafkaConsumer")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
        "org.postgresql:postgresql:42.7.14"
    ).getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Spark version:", spark.version)

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

def refresh_stock_indicators(batch_df, batch_id):

    spark_session = batch_df.sparkSession

    affected_groups = (
        batch_df
        .groupBy("symbol", "market_data_feed")
        .agg(
            F.min("market_timestamp_utc").alias("first_bar"),
            F.max("market_timestamp_utc").alias("last_bar")
        )
        .collect()
    )

    print(
        f"Refreshing {len(affected_groups)} "
        f"stock/feed groups in batch {batch_id}"
    )

    for group in affected_groups:

        symbol = group["symbol"]
        feed = group["market_data_feed"]
        first_bar = group["first_bar"]
        last_bar = group["last_bar"]

        history_start = first_bar - timedelta(minutes=5)
        affected_end = last_bar + timedelta(minutes=5)

        stock_prices = (
            spark_session.read.jdbc(
                url=database_url,
                table="stock_prices",
                properties=database_properties
            )
            .filter(F.col("symbol") == symbol)
            .filter(F.col("market_data_feed") == feed)
            .filter(
                F.col("market_timestamp_utc") >= history_start
            )
            .filter(
                F.col("market_timestamp_utc") <= affected_end
            )
        )

        indicators = calculate_stock_indicators(stock_prices)

        affected_indicators = indicators.filter(
            (F.col("market_timestamp_utc") >= first_bar)
            & (F.col("market_timestamp_utc") <= affected_end)
        )

        save_stock_indicators(
            affected_indicators,
            database_password
        )

        print(
            f"Updated indicators for {symbol} "
            f"({feed}) in batch {batch_id}"
        )

    print(f"Indicators refreshed for batch {batch_id}")



def process_market_batch(batch_df, batch_id):

    batch_df = batch_df.persist()

    try:
        record_count = batch_df.count()

        if record_count == 0:
            print(f"Batch {batch_id} is empty. Skipping.")
            return

        print(
            f"Processing batch {batch_id}: "
            f"{record_count} valid stock records"
        )

        write_stock_batch(batch_df, batch_id)

        refresh_stock_indicators(batch_df, batch_id)

        print(f"Batch {batch_id} completed successfully")

    finally:
        batch_df.unpersist()

kafka_data = (spark.readStream
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
    & (col("is_valid") == True)
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

query = (
    processed_stock_data.writeStream
    .foreachBatch(process_market_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/stock_prices_postgres"
    ).start()
)

try:
    query.awaitTermination()

finally:
    query.stop()
    spark.stop()