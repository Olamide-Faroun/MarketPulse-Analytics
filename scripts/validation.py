
from datetime import datetime
from math import isfinite
from numbers import Real


def check_stock_record(record):

    required_fields = [
        "symbol", "timestamp", "open",
        "high", "low", "close", "volume"
    ]

    # Check for missing fields
    for field in required_fields:
        if field not in record or record[field] is None:
            return "missing_" + field

    # Check stock symbol
    if not isinstance(record["symbol"], str):
        return "invalid_symbol"

    if not record["symbol"].strip():
        return "invalid_symbol"

    # Check timestamp
    timestamp = record["timestamp"]

    if not isinstance(timestamp, datetime):
        return "invalid_timestamp"

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return "invalid_timestamp"

    # Check numeric values
    for field in ["open", "high", "low", "close", "volume"]:
        value = record[field]

        if isinstance(value, bool) or not isinstance(value, Real):
            return "invalid_" + field

        if not isfinite(value):
            return "invalid_" + field

    # Check prices
    for field in ["open", "high", "low", "close"]:
        if record[field] <= 0:
            return "invalid_" + field

    # Check volume
    if record["volume"] < 0:
        return "negative_volume"

    # Check high price
    if record["high"] < max(
        record["open"], record["low"], record["close"]
    ):
        return "invalid_price_range"

    # Check low price
    if record["low"] > min(
        record["open"], record["high"], record["close"]
    ):
        return "invalid_price_range"

    return None


def validate_stock_record(record):
    return check_stock_record(record) is None


def get_validation_failure_reason(record):
    return check_stock_record(record)
