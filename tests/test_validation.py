from scripts.validation import validate_stock_record
from datetime import datetime, timezone


def test_valid_stock_record():

    record = {
        "symbol": "AAPL",
        "timestamp": datetime(2026, 10, 12, 13, 30, tzinfo=timezone.utc),
        "open": 250.00,
        "high": 255.00,
        "low": 248.00,
        "close": 253.00,
        "volume": 12000
    }

    assert validate_stock_record(record) is True

def test_negative_volume():

    record = {
        "symbol": "AAPL",
        "open": 250.00,
        "high": 255.00,
        "low": 248.00,
        "close": 253.00,
        "volume": -12000
    }

    assert validate_stock_record(record) is False

def test_missing_timestamp():
    record = {
        "symbol": "AAPL",
        "open": 250,
        "high": 255,
        "low": 248,
        "close": 253,
        "volume": 1000
    }

    assert validate_stock_record(record) is False