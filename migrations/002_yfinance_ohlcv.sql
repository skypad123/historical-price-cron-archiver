-- migrations/002_yfinance_ohlcv.sql
-- Creates the yfinance_ohlcv table with WAL + DEDUP for idempotent ingestion.
-- Run by scripts/init_db.py via the QuestDB HTTP API on first startup.

CREATE TABLE IF NOT EXISTS yfinance_ohlcv (
    timestamp   TIMESTAMP,
    ticker      SYMBOL,
    interval    SYMBOL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      DOUBLE
) timestamp(timestamp)
PARTITION BY DAY WAL
DEDUP UPSERT KEYS(timestamp, ticker, interval);
