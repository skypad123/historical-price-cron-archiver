"""QuestDB ILP Sender factory.

Each task call gets a fresh Sender context manager — correct for Celery's
multiprocess worker model where sharing a persistent connection across
forked processes is unsafe.

The Sender uses HTTP ILP (port 9000) which auto-creates tables on first
write and needs no schema pre-declaration (except ohlcv which is created
explicitly by init_db.py to enable dedup).
"""

from __future__ import annotations

import os

from questdb.ingress import Sender


def get_sender() -> Sender:
    """Return a configured QuestDB ILP Sender (not yet entered).

    Usage::

        with get_sender() as sender:
            sender.row(...)
            sender.flush()
    """
    host = os.environ.get("QUESTDB_HOST", "localhost")
    port = int(os.environ.get("QUESTDB_ILP_PORT", "9000"))
    conf = f"http::addr={host}:{port};"
    return Sender.from_conf(conf)
