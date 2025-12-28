CREATE TABLE IF NOT EXISTS metrics (
    symbol               TEXT NOT NULL,

    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,

    trade_count          INTEGER NOT NULL,
    total_volume         DOUBLE PRECISION NOT NULL,

    avg_price            DOUBLE PRECISION,
    min_price            DOUBLE PRECISION,
    max_price            DOUBLE PRECISION,
    price_stddev         DOUBLE PRECISION,

    vwap                 DOUBLE PRECISION,

    buy_volume           DOUBLE PRECISION,
    sell_volume          DOUBLE PRECISION,
    buy_sell_ratio       DOUBLE PRECISION,

    trade_intensity      DOUBLE PRECISION,
    liquidity_proxy      DOUBLE PRECISION,

    avg_exchange_latency_ms  DOUBLE PRECISION,
    avg_source_latency_ms    DOUBLE PRECISION,
    avg_end_to_end_latency_ms DOUBLE PRECISION,

    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_agg_metrics_window
ON metrics (window_start);
