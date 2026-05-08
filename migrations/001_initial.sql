-- migrations/001_initial.sql
-- Creates the ohlcv table with WAL + DEDUP for idempotent ingestion.
-- Run by scripts/init_db.py via the QuestDB HTTP API on first startup.
--
-- ticker and orderbook are intentionally omitted — they are auto-created
-- by QuestDB ILP on the first write from the Celery worker.

CREATE TABLE IF NOT EXISTS ohlcv (
    timestamp   TIMESTAMP,
    exchange    SYMBOL,
    symbol      SYMBOL,
    timeframe   SYMBOL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      DOUBLE
) timestamp(timestamp)
PARTITION BY DAY WAL
DEDUP UPSERT KEYS(timestamp, exchange, symbol, timeframe);
