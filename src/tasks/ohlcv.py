"""OHLCV archive task.

``dispatch_ohlcv`` is called by Beat every minute.
It spawns one ``fetch_and_store_ohlcv`` sub-task per (exchange, symbol) pair.

Writes via QuestDB ILP HTTP.  The ohlcv table is pre-created by init_db.py
with WAL + DEDUP enabled so duplicate (timestamp, exchange, symbol, timeframe)
rows are silently dropped.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from questdb.ingress import TimestampNanos

from src.celery_app import app
from src.db.connection import get_sender
from src.utils.alerting import record_failure
from src.utils.ccxt_client import get_exchange, safe_fetch
from src.utils.config_loader import get_settings, iter_exchange_symbols
from src.utils.retry import with_retry

logger = logging.getLogger(__name__)

TASK_NAME = "ohlcv"


# ─── Dispatch (fan-out) ───────────────────────────────────────────────────────


@app.task(name="src.tasks.ohlcv.dispatch_ohlcv", ignore_result=True)
def dispatch_ohlcv() -> None:
    """Spawn one fetch task per (exchange, symbol) pair."""
    for exchange_id, symbol in iter_exchange_symbols():
        fetch_and_store_ohlcv.delay(exchange_id, symbol)


# ─── Per-pair task ────────────────────────────────────────────────────────────


@app.task(
    name="src.tasks.ohlcv.fetch_and_store_ohlcv",
    bind=True,
    max_retries=0,  # retries handled internally via tenacity
    ignore_result=True,
)
def fetch_and_store_ohlcv(self, exchange_id: str, symbol: str) -> None:
    """Fetch the latest 1-m OHLCV candle and persist it."""
    settings = get_settings()
    timeframe = settings.get("ohlcv_timeframe", "1m")

    @with_retry
    def _fetch():
        exchange = get_exchange(exchange_id)
        return safe_fetch(exchange, "fetch_ohlcv", symbol, timeframe, limit=2)

    try:
        candles = _fetch()
    except Exception as exc:
        logger.error("OHLCV fetch failed after retries – %s %s: %s", exchange_id, symbol, exc)
        record_failure(TASK_NAME, exchange_id, symbol, str(exc))
        return

    if not candles:
        logger.warning("No OHLCV data returned for %s %s", exchange_id, symbol)
        return

    # Use the most-recently-closed candle (index -2); index -1 is still forming.
    candle = candles[-2] if len(candles) >= 2 else candles[-1]
    ts_ms, open_, high, low, close, volume = candle

    try:
        with get_sender() as sender:
            sender.row(
                "ohlcv",
                symbols={
                    "exchange": exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                },
                columns={
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume),
                },
                at=TimestampNanos.from_datetime(
                    datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                ),
            )
            sender.flush()
    except Exception as exc:
        logger.error("OHLCV DB insert failed – %s %s: %s", exchange_id, symbol, exc)
        record_failure(TASK_NAME, exchange_id, symbol, str(exc))
