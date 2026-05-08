"""Ticker archive task.

``dispatch_ticker`` is called by Beat every minute.
It spawns one ``fetch_and_store_ticker`` sub-task per (exchange, symbol) pair.

Writes via QuestDB ILP HTTP.  The ticker table is auto-created on first write.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from questdb.ingress import TimestampNanos

from src.celery_app import app
from src.db.connection import get_sender
from src.utils.alerting import record_failure
from src.utils.ccxt_client import get_exchange, safe_fetch
from src.utils.config_loader import iter_exchange_symbols
from src.utils.retry import with_retry

logger = logging.getLogger(__name__)

TASK_NAME = "ticker"


@app.task(name="src.tasks.ticker.dispatch_ticker", ignore_result=True)
def dispatch_ticker() -> None:
    for exchange_id, symbol in iter_exchange_symbols():
        fetch_and_store_ticker.delay(exchange_id, symbol)


@app.task(
    name="src.tasks.ticker.fetch_and_store_ticker",
    bind=True,
    max_retries=0,
    ignore_result=True,
)
def fetch_and_store_ticker(self, exchange_id: str, symbol: str) -> None:
    @with_retry
    def _fetch() -> dict[str, Any]:
        exchange = get_exchange(exchange_id)
        return safe_fetch(exchange, "fetch_ticker", symbol)

    try:
        data = _fetch()
    except Exception as exc:
        logger.error("Ticker fetch failed after retries – %s %s: %s", exchange_id, symbol, exc)
        record_failure(TASK_NAME, exchange_id, symbol, str(exc))
        return

    ts = data.get("timestamp")
    time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)

    # Build columns dict — skip None values so ILP doesn't write nulls
    columns: dict[str, float] = {}
    for dest, src in [
        ("bid", "bid"),
        ("ask", "ask"),
        ("last", "last"),
        ("bid_volume", "bidVolume"),
        ("ask_volume", "askVolume"),
        ("base_volume", "baseVolume"),
        ("quote_volume", "quoteVolume"),
        ("vwap", "vwap"),
        ("change", "change"),
        ("percentage", "percentage"),
    ]:
        val = data.get(src)
        if val is not None:
            columns[dest] = float(val)

    try:
        with get_sender() as sender:
            sender.row(
                "ticker",
                symbols={
                    "exchange": exchange_id,
                    "symbol": symbol,
                },
                columns=columns,
                at=TimestampNanos.from_datetime(time),
            )
            sender.flush()
    except Exception as exc:
        logger.error("Ticker DB insert failed – %s %s: %s", exchange_id, symbol, exc)
        record_failure(TASK_NAME, exchange_id, symbol, str(exc))
