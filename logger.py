"""
Logging — Step 2 of the production upgrade
===========================================
Structured logging with loguru.

Usage anywhere in the project:
    from logger import get_logger
    logger = get_logger("my_module")
    logger.info("Something happened")
    logger.error("Something went wrong", exc_info=True)

Log levels: TRACE < DEBUG < INFO < SUCCESS < WARNING < ERROR < CRITICAL
"""

import os
import sys
from pathlib import Path

from loguru import logger as _loguru_logger

# ── Config ────────────────────────────────────────────────────────────────────

LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR     = Path(os.getenv("LOG_DIR", "logs"))
LOG_FILE    = LOG_DIR / "agent.log"
LOG_ROTATION = "10 MB"   # rotate when file reaches 10 MB
LOG_RETENTION = "14 days"

# ── Bootstrap (runs once on import) ──────────────────────────────────────────

def _setup() -> None:
    # Remove default loguru sink
    _loguru_logger.remove()

    # Console sink — coloured, human-readable
    _loguru_logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[name]}</cyan> | "
            "{message}"
        ),
        colorize=True,
    )

    # File sink — JSON-friendly, rotation + retention
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _loguru_logger.add(
        str(LOG_FILE),
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[name]} | {message}",
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        encoding="utf-8",
    )

_setup()


# ── Public factory ────────────────────────────────────────────────────────────

def get_logger(name: str):
    """
    Return a loguru logger pre-bound with a `name` context field.

    Example:
        logger = get_logger("agent1")
        logger.info("LLM1 started")
        # → 2026-03-05 12:00:00 | INFO     | agent1 | LLM1 started
    """
    return _loguru_logger.bind(name=name)


# ── Convenience: a default logger for one-off scripts ─────────────────────────
log = get_logger("app")