"""yfinance OHLCV archive task.

``dispatch_yfinance_ohlcv`` is called by Beat once per day (00:05 UTC).
It spawns one ``fetch_and_store_yfinance_ohlcv`` sub-task per ticker.

Writes via QuestDB ILP HTTP.  The yfinance_ohlcv table is pre-created by
init_db.py (migration 002) with WAL + DEDUP enabled so duplicate
(timestamp, ticker, interval) rows are silently dropped.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml
from questdb.ingress import TimestampNanos

from src.celery_app import app
from src.db.connection import get_sender
from src.utils.alerting import record_failure
from src.utils.yfinance_client import fetch_ohlcv

logger = logging.getLogger(__name__)

TASK_NAME = "yfinance_ohlcv"
CONFIG_PATH = Path(os.environ.get("YFINANCE_CONFIG_PATH", "config/yfinance.yaml"))


def _load_config() -> dict:
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)


# ─── Dispatch (fan-out) ───────────────────────────────────────────────────────


@app.task(name="src.tasks.yfinance_ohlcv.dispatch_yfinance_ohlcv", ignore_result=True)
def dispatch_yfinance_ohlcv() -> None:
    """Spawn one fetch task per configured ticker."""
    cfg = _load_config()
    settings = cfg.get("settings", {})
    interval = settings.get("interval", "1d")
    for ticker in cfg.get("tickers", []):
        fetch_and_store_yfinance_ohlcv.delay(ticker, interval)


# ─── Per-ticker task ──────────────────────────────────────────────────────────


@app.task(
    name="src.tasks.yfinance_ohlcv.fetch_and_store_yfinance_ohlcv",
    bind=True,
    max_retries=0,  # retries handled internally via tenacity inside fetch_ohlcv
    ignore_result=True,
)
def fetch_and_store_yfinance_ohlcv(self, ticker: str, interval: str = "1d") -> None:
    """Fetch the latest daily candle(s) for *ticker* and persist to QuestDB."""
    # Fetch only the last 5 days so we pick up the most recent closed candle
    # without re-ingesting the entire history on every run.
    try:
        rows = fetch_ohlcv(ticker, interval=interval, period="5d")
    except Exception as exc:
        logger.error("yfinance fetch failed – %s: %s", ticker, exc)
        record_failure(TASK_NAME, "yfinance", ticker, str(exc))
        return

    if not rows:
        logger.warning("No yfinance data returned for %s", ticker)
        return

    try:
        with get_sender() as sender:
            for dt, open_, high, low, close, volume in rows:
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
        logger.info("yfinance_ohlcv: wrote %d rows for %s", len(rows), ticker)
    except Exception as exc:
        logger.error("yfinance_ohlcv DB insert failed – %s: %s", ticker, exc)
        record_failure(TASK_NAME, "yfinance", ticker, str(exc))
