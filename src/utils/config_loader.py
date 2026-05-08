"""Config loader – reads config/symbols.yaml."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config/symbols.yaml"))


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)


def iter_exchange_symbols() -> list[tuple[str, str]]:
    """Return a flat list of (exchange_id, symbol) pairs from config."""
    cfg = load_config()
    pairs: list[tuple[str, str]] = []
    for exchange_id, exchange_cfg in cfg.get("exchanges", {}).items():
        for symbol in exchange_cfg.get("symbols", []):
            pairs.append((exchange_id, symbol))
    return pairs


def get_settings() -> dict[str, Any]:
    return load_config().get("settings", {})
