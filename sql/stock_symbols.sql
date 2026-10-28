
CREATE TABLE IF NOT EXISTS stock_symbols (

    symbol VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(150) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),

    created_timestamp_utc TIMESTAMPTZ
        DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO stock_symbols (symbol, company_name)
VALUES
    ('AAPL', 'Apple'),
    ('MSFT', 'Microsoft'),
    ('NVDA', 'Nvidia'),
    ('GOOGL', 'Alphabet'),
    ('AMZN', 'Amazon'),
    ('META', 'Meta Platforms'),
    ('TSLA', 'Tesla'),
    ('JPM', 'JPMorgan Chase'),
    ('WMT', 'Walmart'),
    ('XOM', 'Exxon Mobil')
ON CONFLICT (symbol) DO NOTHING;