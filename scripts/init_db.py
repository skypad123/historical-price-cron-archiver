#!/usr/bin/env python
"""One-shot script to initialise QuestDB schema on first startup.

Applies every ``migrations/*.sql`` file in alphabetical order. All migration
files must be idempotent (e.g. use ``IF NOT EXISTS``) — they are re-executed
on every startup without tracking which have already run.

``ticker`` and ``orderbook`` are auto-created by QuestDB ILP on first write
and do not need explicit migration files.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def get_http_url() -> str:
    host = os.environ.get("QUESTDB_HOST", "localhost")
    port = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
    return f"http://{host}:{port}"


def wait_for_questdb(retries: int = 20, delay: float = 3.0) -> None:
    """Block until QuestDB HTTP API is ready."""
    url = get_http_url() + "/exec"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params={"query": "SELECT 1"}, timeout=5)
            if resp.status_code == 200:
                logger.info("QuestDB ready on attempt %d", attempt)
                return
        except requests.ConnectionError:
            pass
        logger.warning("QuestDB not ready (attempt %d/%d)", attempt, retries)
        if attempt == retries:
            raise RuntimeError("QuestDB did not become ready in time")
        time.sleep(delay)


def run_ddl(sql: str) -> None:
    url = get_http_url() + "/exec"
    resp = requests.get(url, params={"query": sql}, timeout=10)
    resp.raise_for_status()
    result = resp.json()
    if "error" in result:
        raise RuntimeError(f"DDL error: {result['error']}")


def run_migrations() -> None:
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        logger.warning("No migration files found in %s", MIGRATIONS_DIR)
        return
    for path in migration_files:
        logger.info("Applying migration: %s", path.name)
        run_ddl(path.read_text())
        logger.info("Applied: %s", path.name)


if __name__ == "__main__":
    wait_for_questdb()
    run_migrations()
    logger.info("Schema initialisation complete")
