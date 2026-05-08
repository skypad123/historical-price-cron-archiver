"""CCXT exchange factory.

Exchanges are cached per-process (one instance per exchange id) to reuse
HTTP sessions across tasks.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

import ccxt

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def get_exchange(exchange_id: str) -> ccxt.Exchange:
    """Return a cached CCXT exchange instance for *exchange_id*.

    The instance is created with sane defaults:
    - ``enableRateLimit=True``  – respects exchange rate limits automatically
    - ``timeout=10_000``        – 10 s HTTP timeout
    """
    exchange_class: type[ccxt.Exchange] = getattr(ccxt, exchange_id)
    exchange: ccxt.Exchange = exchange_class(
        {
            "enableRateLimit": True,
            "timeout": 10_000,
        }
    )
    logger.info("Created CCXT exchange instance: %s", exchange_id)
    return exchange


def load_markets_if_needed(exchange: ccxt.Exchange) -> None:
    """Lazy-load markets once per exchange instance."""
    if not exchange.markets:
        exchange.load_markets()


def safe_fetch(exchange: ccxt.Exchange, method: str, *args: Any, **kwargs: Any) -> Any:
    """Call *method* on *exchange*, loading markets first if required."""
    load_markets_if_needed(exchange)
    fn = getattr(exchange, method)
    return fn(*args, **kwargs)
