def validate_stock_record(record):
        required_fields = [
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

        for field in required_fields:
            if field not in record:
                return False

        if not record["symbol"]:
            return False

        if record["open"] <= 0:
            return False

        if record["high"] <= 0:
            return False

        if record["low"] <= 0:
            return False

        if record["close"] <= 0:
            return False

        if record["volume"] < 0:
            return False

        if max(record["open"], record["close"], record["low"]) > (record["high"]):
            return False

        if min(record["open"], record["close"], record["high"]) < (record["low"]):
            return False

        return True

def get_validation_failure_reason(record):

    required_fields = [
        "symbol", "timestamp", "open",
        "high", "low", "close", "volume"
    ]

    for field in required_fields:
        if field not in record or record[field] is None:
            return f"missing_{field}"

    if record["volume"] < 0:
        return "negative_volume"

    if min(
        record["open"],
        record["high"],
        record["low"],
        record["close"]
    ) <= 0:
        return "invalid_price"

    if record["high"] < max(
        record["open"],
        record["low"],
        record["close"]
    ):
        return "invalid_price_range"

    if record["low"] > min(
        record["open"],
        record["high"],
        record["close"]
    ):
        return "invalid_price_range"

    return None