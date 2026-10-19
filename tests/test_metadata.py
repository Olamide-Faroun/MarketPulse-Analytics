import pandas as pd

from scripts.metadata import add_ingestion_metadata


def test_metadata_columns_exist():
    stock_data = pd.DataFrame({
        "symbol": ["AAPL"],
        "close": [250.00]
    })

    result = add_ingestion_metadata(stock_data)

    expected_columns = [
        "data_source",
        "market_data_feed",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "pipeline_stage"
    ]

    for column in expected_columns:
        assert column in result.columns