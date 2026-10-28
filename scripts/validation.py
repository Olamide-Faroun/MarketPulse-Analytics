
from datetime import datetime
from math import isfinite
from numbers import Real


def check_stock_record(record):

    required_fields = [
        "symbol", "timestamp", "open",
        "high", "low", "close", "volume"
    ]


    for field in required_fields:
        if field not in record or record[field] is None:
            return "missing_" + field


    if not isinstance(record["symbol"], str):
        return "invalid_symbol"

    if not record["symbol"].strip():
        return "invalid_symbol"


    timestamp = record["timestamp"]

    if not isinstance(timestamp, datetime):
        return "invalid_timestamp"

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return "invalid_timestamp"


    for field in ["open", "high", "low", "close", "volume"]:
        value = record[field]

        if isinstance(value, bool) or not isinstance(value, Real):
            return "invalid_" + field

        if not isfinite(value):
            return "invalid_" + field


    for field in ["open", "high", "low", "close"]:
        if record[field] <= 0:
            return "invalid_" + field


    if record["volume"] < 0:
        return "negative_volume"


    if record["high"] < max(
        record["open"], record["low"], record["close"]
    ):
        return "invalid_price_range"


    if record["low"] > min(
        record["open"], record["high"], record["close"]
    ):
        return "invalid_price_range"

    return None


def validate_stock_record(record):
    return check_stock_record(record) is None


def get_validation_failure_reason(record):
    return check_stock_record(record)
