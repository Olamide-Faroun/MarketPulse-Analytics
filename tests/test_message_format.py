import pandas as pd

from scripts.message_format import prepare_kafka_messages


def test_kafka_message_contains_metadata():

    stock_data = pd.DataFrame({
        "symbol": ["AAPL"],
        "timestamp": [pd.Timestamp("2026-10-12T13:30:00Z")],
        "open": [250.00],
        "high": [255.00],
        "low": [248.00],
        "close": [253.00],
        "volume": [12000],
        "data_source": ["Alpaca Markets"],
        "market_data_feed": ["IEX"],
        "ingestion_timestamp_utc": [
            pd.Timestamp("2026-10-20T00:00:00Z")
        ],
        "pipeline_run_id": ["test-run-123"],
        "pipeline_stage": ["historical_ingestion"]
    })

    messages = prepare_kafka_messages(stock_data)

    assert len(messages) == 1

    message = messages[0]

    assert message["symbol"] == "AAPL"
    assert message["data_source"] == "Alpaca Markets"
    assert message["market_data_feed"] == "IEX"
    assert "ingestion_timestamp_utc" in message
    assert "pipeline_run_id" in message
    assert "timestamp" in message