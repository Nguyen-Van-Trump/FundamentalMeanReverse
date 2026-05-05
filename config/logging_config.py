from __future__ import annotations

import logging
from datetime import date
from pathlib import Path


LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_CATEGORIES = {"data_fetch", "scan", "order"}


def get_log_file(category: str) -> Path:
    if category not in LOG_CATEGORIES:
        raise ValueError(
            f"invalid log category {category!r}; expected one of {sorted(LOG_CATEGORIES)}"
        )
    return LOG_DIR / f"{category}_{date.today().isoformat()}.log"


def get_logger(name: str, category: str) -> logging.Logger:
    """Return a project logger writing to a category-specific daily file."""
    logger = logging.getLogger(name)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = get_log_file(category)
    log_path = str(log_file.resolve())

    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == log_path:
            return logger

    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))

    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
