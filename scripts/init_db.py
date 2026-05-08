#!/usr/bin/env python
"""One-shot script to initialise QuestDB schema on first startup.

Creates the ``ohlcv`` table explicitly with WAL + DEDUP so that duplicate
(timestamp, exchange, symbol, timeframe) rows are silently dropped on
re-ingestion.

``ticker`` and ``orderbook`` are auto-created by QuestDB ILP on first write
and do not need explicit creation.

Safe to re-run — uses IF NOT EXISTS.
"""

from __future__ import annotations

import logging
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OHLCV_DDL = """
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
""".strip()


def get_http_url() -> str:
    host = os.environ.get("QUESTDB_HOST", "localhost")
    port = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
    return f"http://{host}:{port}"


def wait_for_questdb(retries: int = 20, delay: float = 3.0) -> None:
    """Block until QuestDB HTTP API is ready."""
    url = get_http_url() + "/exec"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params={"query": "SELECT 1"}, timeout=5)
            if resp.status_code == 200:
                logger.info("QuestDB ready on attempt %d", attempt)
                return
        except requests.ConnectionError:
            pass
        logger.warning("QuestDB not ready (attempt %d/%d)", attempt, retries)
        if attempt == retries:
            raise RuntimeError("QuestDB did not become ready in time")
        time.sleep(delay)


def run_ddl(sql: str) -> None:
    url = get_http_url() + "/exec"
    resp = requests.get(url, params={"query": sql}, timeout=10)
    resp.raise_for_status()
    result = resp.json()
    if "error" in result:
        raise RuntimeError(f"DDL error: {result['error']}")
    logger.info("DDL executed successfully")


if __name__ == "__main__":
    wait_for_questdb()
    logger.info("Creating ohlcv table (WAL + DEDUP)...")
    run_ddl(OHLCV_DDL)
    logger.info("Schema initialisation complete")
