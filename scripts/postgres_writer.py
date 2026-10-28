
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


def write_stock_batch(batch_df, batch_id):

    print(f"Processing Spark batch: {batch_id}")

    record_count = batch_df.count()

    print(f"Records received in batch: {record_count}")

    if record_count == 0:
        print("No records to write. Skipping this batch.")
        return

    columns = [
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "price_change_percentage",
        "data_source",
        "market_data_feed",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "pipeline_stage"
    ]

    records = batch_df.select(*columns).toLocalIterator()

    insert_sql = """
        INSERT INTO stock_prices (
            symbol,
            market_timestamp_utc,
            open, high, low, close, volume,
            price_change_percentage,
            data_source,
            market_data_feed,
            ingestion_timestamp_utc,
            pipeline_run_id,
            pipeline_stage,
            processing_timestamp_utc
        )
        VALUES %s
        ON CONFLICT (
            symbol, market_timestamp_utc, market_data_feed
        ) DO NOTHING
    """

    connection = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="marketpulse_db",
        user="marketpulse_user",
        password=os.environ["POSTGRES_PASSWORD"]
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                pending = []

                for row in records:
                    values = tuple(row[field] for field in columns)
                    values += (datetime.now(timezone.utc),)
                    pending.append(values)

                    if len(pending) == 500:
                        execute_values(cursor, insert_sql, pending)
                        pending.clear()

                if pending:
                    execute_values(cursor, insert_sql, pending)

        print(f"Spark batch {batch_id} saved to PostgreSQL")

    finally:
        connection.close()

