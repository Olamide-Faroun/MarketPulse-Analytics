import math
from datetime import datetime, timedelta, timezone
import pytest
from pyspark.sql import SparkSession
from scripts.spark_indicators import calculate_stock_indicators

@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder
        .master("local[2]")
        .appName("MarketPulseIndicatorTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def make_bars(spark, closes, gap_at=None):
    start = datetime(2026, 10, 12, 13, 30, tzinfo=timezone.utc)
    rows = []

    for index, close in enumerate(closes):
        minute = index + (1 if gap_at is not None
                          and index >= gap_at else 0)
        rows.append({
            "symbol": "AAPL",
            "market_data_feed": "iex",
            "market_timestamp_utc": start + timedelta(minutes=minute),
            "open": float(close),
            "high": float(close) + 1,
            "low": float(close) - 1,
            "close": float(close),
            "volume": 100 + index,
            "data_source": "test",
            "ingestion_timestamp_utc": start,
            "pipeline_run_id": "00000000-0000-0000-0000-000000000001",
        })

    return spark.createDataFrame(rows)


def test_price_return_and_range(spark):
    df = make_bars(spark, [100, 102])
    results = calculate_stock_indicators(df).orderBy(
        "market_timestamp_utc"
    ).collect()

    assert results[0].price_return_percentage is None
    assert math.isclose(
        float(results[1].price_return_percentage), 2.0,
        abs_tol=1e-6
    )
    assert math.isclose(
        float(results[0].price_range_percentage), 2.0,
        abs_tol=1e-6
    )


def test_five_minute_average_and_volume(spark):
    df = make_bars(spark, [100, 101, 102, 103, 104])
    results = calculate_stock_indicators(df).orderBy(
        "market_timestamp_utc"
    ).collect()

    assert results[3].moving_average_price is None
    assert math.isclose(
        float(results[4].moving_average_price), 102.0,
        abs_tol=1e-6
    )
    assert math.isclose(
        float(results[4].average_trading_volume), 102.0,
        abs_tol=1e-6
    )


def test_volatility_requires_five_valid_returns(spark):
    df = make_bars(spark, [100, 101, 102, 103, 104, 105])
    results = calculate_stock_indicators(df).orderBy(
        "market_timestamp_utc"
    ).collect()

    assert all(
        row.volatility_percentage is None
        for row in results[:5]
    )
    assert results[5].volatility_percentage is not None


def test_missing_minute_invalidates_return_and_volatility(spark):
    df = make_bars(
        spark,
        [100, 101, 102, 103, 104, 105],
        gap_at=3
    )
    results = calculate_stock_indicators(df).orderBy(
        "market_timestamp_utc"
    ).collect()

    assert results[3].price_return_percentage is None
    assert results[-1].volatility_percentage is None
