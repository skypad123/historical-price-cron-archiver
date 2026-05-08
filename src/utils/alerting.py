"""Failure alerting via a structured log file.

How it works
------------
Every time a task fails, ``record_failure()`` appends a line to the failure
log file in the format:

    FAILURE|<iso_timestamp>|<task_name>|<exchange>|<symbol>|<error>

When an alert email is sent, an ALERTED line is appended:

    ALERTED|<iso_timestamp>|<task_name>|<exchange>|<symbol>

To decide whether to alert, the log is scanned in reverse for the given
(task_name, exchange, symbol) triple.  Consecutive FAILURE lines since the
last ALERTED line are counted; if the count reaches ALERT_FAILURE_THRESHOLD
an email is dispatched and an ALERTED line is written.
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

THRESHOLD = int(os.environ.get("ALERT_FAILURE_THRESHOLD", "3"))
ALERT_ENABLED = os.environ.get("ALERT_EMAIL_ENABLED", "true").lower() == "true"
LOG_PATH = Path(os.environ.get("FAILURE_LOG_PATH", "logs/failures.log"))


# ─── Log helpers ─────────────────────────────────────────────────────────────


def _append(line: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def _count_consecutive_failures(task_name: str, exchange: str, symbol: str) -> int:
    """Count unalerted consecutive failures for this (task, exchange, symbol)."""
    if not LOG_PATH.exists():
        return 0

    key = f"|{task_name}|{exchange}|{symbol}|"
    alerted_key = f"|{task_name}|{exchange}|{symbol}"
    count = 0

    lines = LOG_PATH.read_text().splitlines()
    for line in reversed(lines):
        if alerted_key not in line:
            continue
        if line.startswith("ALERTED") and line.endswith(alerted_key.rstrip("|")):
            # Hit a previous alert — streak resets here
            break
        if line.startswith("FAILURE") and key in line:
            count += 1

    return count


# ─── Public API ───────────────────────────────────────────────────────────────


def record_failure(
    task_name: str,
    exchange: str,
    symbol: str,
    error_message: Optional[str] = None,
) -> None:
    """Log a failure and send an alert if the threshold is reached."""
    now = datetime.now(timezone.utc).isoformat()
    safe_error = (error_message or "").replace("|", "/").replace("\n", " ")
    _append(f"FAILURE|{now}|{task_name}|{exchange}|{symbol}|{safe_error}")

    try:
        count = _count_consecutive_failures(task_name, exchange, symbol)
        if count >= THRESHOLD:
            _send_alert(task_name, exchange, symbol, error_message, count)
            now2 = datetime.now(timezone.utc).isoformat()
            _append(f"ALERTED|{now2}|{task_name}|{exchange}|{symbol}")
    except Exception:
        logger.exception("Failed to evaluate alert for %s/%s/%s", task_name, exchange, symbol)


# ─── Email ────────────────────────────────────────────────────────────────────


def _send_alert(
    task_name: str,
    exchange: str,
    symbol: str,
    last_error: Optional[str],
    failure_count: int,
) -> None:
    if not ALERT_ENABLED:
        logger.warning(
            "Alert suppressed (disabled): %s failures for %s %s/%s",
            failure_count,
            task_name,
            exchange,
            symbol,
        )
        return

    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    from_addr = os.environ.get("ALERT_FROM", smtp_user)
    to_addr = os.environ.get("ALERT_TO", smtp_user)

    subject = (
        f"[Archiver Alert] {failure_count} consecutive failures – "
        f"{task_name} | {exchange} | {symbol}"
    )
    body = (
        f"Task: {task_name}\n"
        f"Exchange: {exchange}\n"
        f"Symbol: {symbol}\n"
        f"Consecutive failures: {failure_count}\n"
        f"Last error:\n{last_error or 'N/A'}\n\n"
        f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
    )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_addr, [to_addr], msg.as_string())
        logger.info("Alert email sent for %s %s/%s", task_name, exchange, symbol)
    except Exception:
        logger.exception("Failed to send alert email for %s %s/%s", task_name, exchange, symbol)
