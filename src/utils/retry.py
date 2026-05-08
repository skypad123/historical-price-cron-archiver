"""Retry logic using Tenacity.

Strategy
--------
- Up to 5 attempts with exponential backoff (1 s → 2 s → 4 s → 8 s → 16 s).
- Any exception triggers a retry.
- After all retries are exhausted the exception propagates so the task can
  record the failure and (eventually) trigger an alert.
"""

from __future__ import annotations

import logging
from typing import Callable, TypeVar

from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Re-export RetryError so callers can `from src.utils.retry import RetryError`
__all__ = ["with_retry", "RetryError"]

_RETRY_DECORATOR = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)


def with_retry(fn: Callable[..., T]) -> Callable[..., T]:
    """Decorator that wraps *fn* with the standard retry policy."""
    return _RETRY_DECORATOR(fn)
