"""Thin yfinance wrapper with retry logic.

Provides ``fetch_ohlcv`` which returns a list of
``(timestamp_utc, open, high, low, close, volume)`` tuples.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from src.utils.retry import with_retry

logger = logging.getLogger(__name__)


def fetch_ohlcv(
    ticker: str,
    interval: str = "1d",
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[tuple[datetime, float, float, float, float, float]]:
    """Fetch OHLCV rows from Yahoo Finance.

    Provide either *period* (e.g. ``"max"``, ``"1y"``) or *start*/*end* date
    strings (``"YYYY-MM-DD"``).  Returns a list of
    ``(timestamp_utc, open, high, low, close, volume)`` tuples sorted oldest
    first.  Returns an empty list if no data is available.
    """

    @with_retry
    def _download() -> Any:
        kwargs: dict[str, Any] = {
            "tickers": ticker,
            "interval": interval,
            "auto_adjust": True,
            "progress": False,
        }
        if period:
            kwargs["period"] = period
        else:
            if start:
                kwargs["start"] = start
            if end:
                kwargs["end"] = end
        return yf.download(**kwargs)

    df = _download()

    if df is None or df.empty:
        logger.warning("yfinance returned no data for %s (interval=%s)", ticker, interval)
        return []

    # yfinance may return a MultiIndex when downloading a single ticker with
    # auto_adjust=True; flatten to a simple column set.
    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    rows: list[tuple[datetime, float, float, float, float, float]] = []
    for ts, row in df.iterrows():
        # Ensure timezone-aware UTC timestamp
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            dt = ts.to_pydatetime().astimezone(timezone.utc)
        else:
            dt = ts.to_pydatetime().replace(tzinfo=timezone.utc)

        rows.append(
            (
                dt,
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                float(row.get("Volume", 0.0)),
            )
        )

    return rows
