
CREATE TABLE IF NOT EXISTS stock_indicators (

    symbol VARCHAR(20) NOT NULL,
    market_timestamp_utc TIMESTAMPTZ NOT NULL,
    market_data_feed VARCHAR(20) NOT NULL,

    calculation_window_minutes INTEGER NOT NULL,

    moving_average_price NUMERIC(18,6),
    volatility_percentage NUMERIC(12,6),

    data_source VARCHAR(100) NOT NULL,
    ingestion_timestamp_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id UUID NOT NULL,
    pipeline_stage VARCHAR(50) NOT NULL,
    processing_timestamp_utc TIMESTAMPTZ,

    PRIMARY KEY (
        symbol,
        market_timestamp_utc,
        market_data_feed,
        calculation_window_minutes
    ),

    CONSTRAINT valid_calculation_window
        CHECK (calculation_window_minutes > 0),

    CONSTRAINT valid_moving_average
        CHECK (
            moving_average_price IS NULL
            OR moving_average_price > 0
        ),

    CONSTRAINT valid_volatility
        CHECK (
            volatility_percentage IS NULL
            OR volatility_percentage >= 0
        )
);
