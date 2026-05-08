"""Orderbook archive task.

``dispatch_orderbook`` is called by Beat every minute.
It spawns one ``fetch_and_store_orderbook`` sub-task per (exchange, symbol) pair.

Writes via QuestDB ILP HTTP.  The orderbook table is auto-created on first write.

Schema note
-----------
QuestDB has no JSONB type.  Bids and asks are serialised to compact JSON
strings and stored as VARCHAR columns:

    bids  VARCHAR  -- "[[price, amount], ...]"
    asks  VARCHAR  -- "[[price, amount], ...]"
"""

from __future__ import annotations

import json
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

TASK_NAME = "orderbook"


@app.task(name="src.tasks.orderbook.dispatch_orderbook", ignore_result=True)
def dispatch_orderbook() -> None:
    for exchange_id, symbol in iter_exchange_symbols():
        fetch_and_store_orderbook.delay(exchange_id, symbol)


@app.task(
    name="src.tasks.orderbook.fetch_and_store_orderbook",
    bind=True,
    max_retries=0,
    ignore_result=True,
)
def fetch_and_store_orderbook(self, exchange_id: str, symbol: str) -> None:
    settings = get_settings()
    depth = settings.get("orderbook_depth", 20)

    @with_retry
    def _fetch() -> dict:
        exchange = get_exchange(exchange_id)
        return safe_fetch(exchange, "fetch_order_book", symbol, depth)

    try:
        data = _fetch()
    except Exception as exc:
        logger.error("Orderbook fetch failed after retries – %s %s: %s", exchange_id, symbol, exc)
        record_failure(TASK_NAME, exchange_id, symbol, str(exc))
        return

    ts = data.get("timestamp")
    time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)

    bids_json = json.dumps(data.get("bids", []))
    asks_json = json.dumps(data.get("asks", []))

    try:
        with get_sender() as sender:
            sender.row(
                "orderbook",
                symbols={
                    "exchange": exchange_id,
                    "symbol": symbol,
                },
                columns={
                    "bids": bids_json,
                    "asks": asks_json,
                    "depth": int(depth),
                },
                at=TimestampNanos.from_datetime(time),
            )
            sender.flush()
    except Exception as exc:
        logger.error("Orderbook DB insert failed – %s %s: %s", exchange_id, symbol, exc)
        record_failure(TASK_NAME, exchange_id, symbol, str(exc))
