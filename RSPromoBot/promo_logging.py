from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(logs_dir: str | Path) -> logging.Logger:
    log_path = Path(logs_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("rspromobot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path / "rspromobot.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
