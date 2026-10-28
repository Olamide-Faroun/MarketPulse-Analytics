
import os
from datetime import timezone
from uuid import UUID
import psycopg2
from psycopg2.extras import execute_values, register_uuid
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window, functions as F

load_dotenv()
register_uuid()

database_password = os.getenv("POSTGRES_PASSWORD")

if not database_password:
    raise ValueError("POSTGRES_PASSWORD is missing")

spark = (
    SparkSession.builder
    .appName("MarketPulseIndicators")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.14"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

database_url = "jdbc:postgresql://localhost:5433/marketpulse_db"

database_properties = {
    "user": "marketpulse_user",
    "password": database_password,
    "driver": "org.postgresql.Driver"
}


try:
    stock_prices = spark.read.jdbc(
        url=database_url,
        table="stock_prices",
        properties=database_properties
    )

    print("Stock records loaded:", stock_prices.count())

    stock_prices.printSchema()


    stock_order = Window.partitionBy(
        "symbol", "market_data_feed"
    ).orderBy("market_timestamp_utc")


    five_minute_window = Window.partitionBy(
        "symbol", "market_data_feed"
    ).orderBy(
        F.col("market_timestamp_utc").cast("long")
    ).rangeBetween(-299, 0)

    data = stock_prices.withColumn(
        "previous_close",
        F.lag("close").over(stock_order)
    ).withColumn(
        "previous_timestamp",
        F.lag("market_timestamp_utc").over(stock_order)
    )

    seconds_since_previous = (
        F.col("market_timestamp_utc").cast("long")
        - F.col("previous_timestamp").cast("long")
    )


    data = data.withColumn(
        "price_return_percentage",
        F.when(
            (F.col("previous_close") > 0)
            & seconds_since_previous.between(1, 120),
            (
                (F.col("close") - F.col("previous_close"))
                / F.col("previous_close")
            ) * 100
        )
    )


    data = data.withColumn(
        "price_range_percentage",
        (
            (F.col("high") - F.col("low"))
            / F.col("open")
        ) * 100
    )


    data = data.withColumn(
        "observations_in_window",
        F.count("close").over(five_minute_window)
    )


    data = data.withColumn(
        "moving_average_price",
        F.when(
            F.col("observations_in_window") >= 5,
            F.avg("close").over(five_minute_window)
        )
    )


    data = data.withColumn(
        "average_trading_volume",
        F.when(
            F.col("observations_in_window") >= 5,
            F.avg("volume").over(five_minute_window)
        )
    )


    data = data.withColumn(
        "volatility_percentage",
        F.when(
            F.col("observations_in_window") >= 5,
            F.stddev_samp(
                "price_return_percentage"
            ).over(five_minute_window)
        )
    )

    indicators = data.select(
        "symbol",
        "market_timestamp_utc",
        "market_data_feed",
        F.lit(5).alias("calculation_window_minutes"),
        "price_return_percentage",
        "price_range_percentage",
        "moving_average_price",
        "average_trading_volume",
        "volatility_percentage",
        "data_source",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        F.lit("indicator_processing").alias(
            "pipeline_stage"
        ),
        F.current_timestamp().alias(
            "processing_timestamp_utc"
        )
    )

    indicators.show(10, truncate=False)


    indicator_columns = [
        "symbol",
        "market_timestamp_utc",
        "market_data_feed",
        "calculation_window_minutes",
        "price_return_percentage",
        "price_range_percentage",
        "moving_average_price",
        "average_trading_volume",
        "volatility_percentage",
        "data_source",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "pipeline_stage",
        "processing_timestamp_utc"
    ]

    insert_sql = """
        INSERT INTO stock_indicators (
            symbol,
            market_timestamp_utc,
            market_data_feed,
            calculation_window_minutes,
            price_return_percentage,
            price_range_percentage,
            moving_average_price,
            average_trading_volume,
            volatility_percentage,
            data_source,
            ingestion_timestamp_utc,
            pipeline_run_id,
            pipeline_stage,
            processing_timestamp_utc
        )
        VALUES %s
        ON CONFLICT (
            symbol,
            market_timestamp_utc,
            market_data_feed,
            calculation_window_minutes
        )
        DO UPDATE SET
            price_return_percentage = EXCLUDED.price_return_percentage,
            price_range_percentage = EXCLUDED.price_range_percentage,
            moving_average_price = EXCLUDED.moving_average_price,
            average_trading_volume = EXCLUDED.average_trading_volume,
            volatility_percentage = EXCLUDED.volatility_percentage,
            processing_timestamp_utc = EXCLUDED.processing_timestamp_utc
    """

    connection = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="marketpulse_db",
        user="marketpulse_user",
        password=database_password
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                pending = []
                processed_count = 0

                for row in indicators.select(
                    *indicator_columns
                ).toLocalIterator():

                    values = row.asDict()


                    for name in [
                        "market_timestamp_utc",
                        "ingestion_timestamp_utc",
                        "processing_timestamp_utc"
                    ]:
                        if values[name] is not None:
                            values[name] = values[name].replace(
                                tzinfo=timezone.utc
                            )

                    values["pipeline_run_id"] = UUID(
                        values["pipeline_run_id"]
                    )

                    pending.append(
                        tuple(values[name] for name in indicator_columns)
                    )

                    if len(pending) >= 500:
                        execute_values(
                            cursor, insert_sql, pending
                        )
                        processed_count += len(pending)
                        pending.clear()

                if pending:
                    execute_values(
                        cursor, insert_sql, pending
                    )
                    processed_count += len(pending)

        print("Indicator records processed:", processed_count)

    finally:
        connection.close()


finally:
    spark.stop()