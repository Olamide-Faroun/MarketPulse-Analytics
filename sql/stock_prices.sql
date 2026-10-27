
CREATE TABLE IF NOT EXISTS stock_prices (

    symbol VARCHAR(20) NOT NULL,
    market_timestamp_utc TIMESTAMPTZ NOT NULL,

    open NUMERIC(18,6) NOT NULL,
    high NUMERIC(18,6) NOT NULL,
    low NUMERIC(18,6) NOT NULL,
    close NUMERIC(18,6) NOT NULL,
    volume BIGINT NOT NULL,

    price_change_percentage NUMERIC(12,4),

    data_source VARCHAR(100) NOT NULL,
    market_data_feed VARCHAR(20) NOT NULL,
    ingestion_timestamp_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id UUID NOT NULL,
    pipeline_stage VARCHAR(50) NOT NULL,

    processing_timestamp_utc TIMESTAMPTZ,

    PRIMARY KEY (
        symbol,
        market_timestamp_utc,
        market_data_feed
    ),

    CONSTRAINT valid_stock_prices CHECK (
        open > 0
        AND high >= open
        AND high >= close
        AND low <= open
        AND low <= close
        AND low > 0
        AND volume >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_timestamp
ON stock_prices (market_timestamp_utc DESC);