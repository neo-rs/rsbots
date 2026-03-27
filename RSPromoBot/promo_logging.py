from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


class ExplainableLog:
    """Sectioned, operator-friendly logging for critical decision flows."""

    def __init__(self, logger: logging.Logger, show_technical: bool = False) -> None:
        self._logger = logger
        self._show_technical = show_technical

    def flow(
        self,
        *,
        name: str,
        message_info: list[str],
        eli5_bottom_line: str,
        eli5_points: list[str],
        human_summary: str,
        human_yes: list[str],
        human_no: list[str],
        decision_tag: str,
        destination: str,
        destination_id: str,
        failure_hints: list[str] | None = None,
        dry_send_preview: str | None = None,
        technical_trace: dict[str, Any] | None = None,
        level: int = logging.INFO,
    ) -> None:
        lines: list[str] = [
            "==============================================================================",
            name,
            "==============================================================================",
            "",
            "1) MESSAGE INFO",
            *[f"- {item}" for item in message_info],
            "",
            "2) ELI5 SUMMARY",
            f"Bottom line: {eli5_bottom_line}",
            *[f"- {item}" for item in eli5_points],
            "",
            "3) HUMAN DECISION SUMMARY",
            f"- Summary: {human_summary}",
        ]

        if human_yes:
            lines.extend(["- Winning conditions:", *[f"  - {item}" for item in human_yes]])
        if human_no:
            lines.extend(["- Rejected conditions:", *[f"  - {item}" for item in human_no]])

        lines.extend(
            [
                "",
                "4) DESTINATION DECISION",
                f"- Decision tag: {decision_tag}",
                f"- Destination: {destination}",
                f"- Destination ID: {destination_id}",
            ]
        )

        if failure_hints:
            lines.extend(["", "5) FAILURE HINTS", *[f"- {hint}" for hint in failure_hints]])
        if dry_send_preview:
            lines.extend(["", "6) DRY SEND PREVIEW", f"- {dry_send_preview}"])
        if self._show_technical and technical_trace is not None:
            lines.extend(["", "7) RAW FLAGS / TRACE", f"- {technical_trace!r}"])

        self._logger.log(level, "\n".join(lines))


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
    logger.propagate = False

    # Discord gateway RESUMED messages are high-noise for operators.
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)

    return logger
