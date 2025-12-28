CREATE TABLE IF NOT EXISTS raw_trades (
    symbol              TEXT NOT NULL,
    trade_id            BIGINT NOT NULL,

    price               DOUBLE PRECISION NOT NULL,
    quantity            DOUBLE PRECISION NOT NULL,
    is_buyer_maker      BOOLEAN NOT NULL,

    event_time           TIMESTAMPTZ NOT NULL,
    trade_time           TIMESTAMPTZ NOT NULL,
    ingestion_time       TIMESTAMPTZ NOT NULL,
    spark_time           TIMESTAMPTZ NOT NULL,
    
    exchange_latency_ms  DOUBLE PRECISION,
    source_latency_ms    DOUBLE PRECISION,
    end_to_end_latency_ms DOUBLE PRECISION,

    PRIMARY KEY (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_event_time
ON raw_trades (event_time);