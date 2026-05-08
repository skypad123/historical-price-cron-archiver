"""Celery application + Beat schedule.

Beat dispatches one task-group per minute that fans out to:
  - fetch_ohlcv    × each (exchange, symbol)
  - fetch_ticker   × each (exchange, symbol)
  - fetch_orderbook × each (exchange, symbol)

The task signatures are simple strings so Celery can route them without
importing the heavy CCXT/DB stack at beat start-up.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

load_dotenv()  # load .env when running outside Docker

logger = logging.getLogger(__name__)

BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

app = Celery(
    "archiver",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=[
        "src.tasks.ohlcv",
        "src.tasks.ticker",
        "src.tasks.orderbook",
    ],
)

app.conf.update(
    # Serialisation
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # Reliability
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Result expiry (keep results 1 h)
    result_expires=3600,
)

# ─── Beat schedule ────────────────────────────────────────────────────────────
# Runs every minute.  The task itself fans out over all configured pairs.
app.conf.beat_schedule = {
    "archive-ohlcv-every-minute": {
        "task": "src.tasks.ohlcv.dispatch_ohlcv",
        "schedule": timedelta(minutes=1),
        "options": {"expires": 55},  # drop if not picked up within 55 s
    },
    "archive-ticker-every-minute": {
        "task": "src.tasks.ticker.dispatch_ticker",
        "schedule": timedelta(minutes=1),
        "options": {"expires": 55},
    },
    "archive-orderbook-every-minute": {
        "task": "src.tasks.orderbook.dispatch_orderbook",
        "schedule": timedelta(minutes=1),
        "options": {"expires": 55},
    },
}
