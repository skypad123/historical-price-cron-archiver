#!/usr/bin/env python
"""One-shot backfill script: fetch full history from Yahoo Finance → QuestDB.

Usage
-----
    # Fetch all tickers defined in config/yfinance.yaml using configured defaults
    python -m scripts.archive_yfinance

    # Override period / interval at runtime
    python -m scripts.archive_yfinance --period 1y --interval 1d

    # Fetch a specific date range
    python -m scripts.archive_yfinance --start 2020-01-01 --end 2024-12-31

    # Target a specific ticker (overrides config list)
    python -m scripts.archive_yfinance --ticker 0P00006O9U.SI

The script is idempotent: QuestDB's WAL DEDUP silently drops rows whose
(timestamp, ticker, interval) key already exists.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv
from questdb.ingress import TimestampNanos

load_dotenv()

# Allow running as `python -m scripts.archive_yfinance` from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.connection import get_sender  # noqa: E402
from src.utils.yfinance_client import fetch_ohlcv  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(os.environ.get("YFINANCE_CONFIG_PATH", "config/yfinance.yaml"))


# ─── Config ──────────────────────────────────────────────────────────────────


def load_yfinance_config() -> dict:
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)


# ─── QuestDB helpers ─────────────────────────────────────────────────────────


def get_http_url() -> str:
    host = os.environ.get("QUESTDB_HOST", "localhost")
    port = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
    return f"http://{host}:{port}"


def wait_for_questdb(retries: int = 20, delay: float = 3.0) -> None:
    url = get_http_url() + "/exec"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params={"query": "SELECT 1"}, timeout=5)
            if resp.status_code == 200:
                logger.info("QuestDB ready (attempt %d)", attempt)
                return
        except requests.ConnectionError:
            pass
        logger.warning("QuestDB not ready (attempt %d/%d)", attempt, retries)
        if attempt == retries:
            raise RuntimeError("QuestDB did not become ready in time")
        time.sleep(delay)


def ensure_table() -> None:
    """Create yfinance_ohlcv table if it doesn't exist."""
    ddl = """
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
    DEDUP UPSERT KEYS(timestamp, ticker, interval)
    """.strip()
    url = get_http_url() + "/exec"
    resp = requests.get(url, params={"query": ddl}, timeout=15)
    resp.raise_for_status()
    result = resp.json()
    if "error" in result:
        raise RuntimeError(f"DDL error: {result['error']}")
    logger.info("yfinance_ohlcv table ready")


# ─── Core archiver ───────────────────────────────────────────────────────────


def archive_ticker(
    ticker: str,
    interval: str,
    period: str | None,
    start: str | None,
    end: str | None,
) -> int:
    """Fetch and store all OHLCV rows for *ticker*. Returns row count written."""
    logger.info(
        "Fetching %s  interval=%s  period=%s  start=%s  end=%s",
        ticker,
        interval,
        period,
        start,
        end,
    )
    rows = fetch_ohlcv(ticker, interval=interval, period=period, start=start, end=end)

    if not rows:
        logger.warning("No data returned for %s — skipping", ticker)
        return 0

    logger.info("Writing %d rows for %s to QuestDB …", len(rows), ticker)

    written = 0
    # Batch writes in chunks to avoid oversized ILP payloads
    chunk_size = 5_000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        with get_sender() as sender:
            for dt, open_, high, low, close, volume in chunk:
                sender.row(
                    "yfinance_ohlcv",
                    symbols={"ticker": ticker, "interval": interval},
                    columns={
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume,
                    },
                    at=TimestampNanos.from_datetime(dt),
                )
            sender.flush()
        written += len(chunk)
        logger.info("  … %d / %d rows flushed", written, len(rows))

    logger.info("Done: %s — %d rows written", ticker, written)
    return written


# ─── CLI ─────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Yahoo Finance OHLCV history into QuestDB"
    )
    parser.add_argument(
        "--ticker",
        help="Single ticker to archive (overrides config tickers list)",
    )
    parser.add_argument(
        "--period",
        help="yfinance period string (e.g. max, 5y, 1y). Overrides config.",
    )
    parser.add_argument(
        "--interval",
        help="yfinance interval string (e.g. 1d, 1wk). Overrides config.",
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD (overrides period)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (used with --start)")
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Skip QuestDB readiness check (useful when DB is already known to be up)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_yfinance_config()
    settings = cfg.get("settings", {})

    interval = args.interval or settings.get("interval", "1d")
    # If explicit date range given, period is ignored
    if args.start:
        period = None
        start = args.start
        end = args.end
    else:
        period = args.period or settings.get("period", "max")
        start = None
        end = None

    tickers: list[str] = [args.ticker] if args.ticker else cfg.get("tickers", [])
    if not tickers:
        logger.error("No tickers configured. Add tickers to config/yfinance.yaml or use --ticker")
        sys.exit(1)

    if not args.no_wait:
        wait_for_questdb()

    ensure_table()

    total = 0
    for ticker in tickers:
        try:
            total += archive_ticker(ticker, interval, period, start, end)
        except Exception as exc:
            logger.error("Failed to archive %s: %s", ticker, exc, exc_info=True)

    logger.info("Backfill complete — %d total rows written across %d ticker(s)", total, len(tickers))


if __name__ == "__main__":
    main()
