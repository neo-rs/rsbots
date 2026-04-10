"""Layered logging for catalog_nav_bot (Explainable Logging SOP)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional


class ExplainableLog:
    """ELI5 + human decision + optional JSON trace; one owner for this bot's explainable output."""

    def __init__(self, log: logging.Logger, *, trace_enabled: bool, log_skip_traffic: bool = False) -> None:
        self._log = log
        self._trace_enabled = trace_enabled
        self._log_skip_traffic = log_skip_traffic

    def section(self, title: str) -> None:
        self._log.info("==============================================================================")
        self._log.info("CATALOG_NAV_BOT / %s", title)
        self._log.info("==============================================================================")

    def eli5(self, summary: str, bullets: Optional[List[str]] = None) -> None:
        self._log.info("Bottom line: %s", summary)
        if bullets:
            for line in bullets:
                self._log.info("  - %s", line)

    def human(
        self,
        summary: str,
        *,
        yes: Optional[List[str]] = None,
        no: Optional[List[str]] = None,
        notes: Optional[List[str]] = None,
    ) -> None:
        self._log.info("Human summary: %s", summary)
        if yes:
            self._log.info("  Matched / yes:")
            for line in yes:
                self._log.info("    + %s", line)
        if no:
            self._log.info("  Rejected / no:")
            for line in no:
                self._log.info("    - %s", line)
        if notes:
            self._log.info("  Notes:")
            for line in notes:
                self._log.info("    * %s", line)

    def trace(self, data: Dict[str, Any]) -> None:
        if not self._trace_enabled:
            return
        self._log.debug("trace %s", json.dumps(data, ensure_ascii=False, default=str))

    def route(self, decision: str, *, destination: str, detail: str, simulated: bool = False) -> None:
        mode = "DRY SEND PREVIEW" if simulated else "LIVE"
        self._log.info("Route [%s]: %s → %s (%s)", mode, decision, destination, detail)

    def failure(self, hints: List[str]) -> None:
        self._log.info("Failure hints:")
        for h in hints:
            self._log.info("  - %s", h)

    def debug_skip(self, reason: str, **fields: Any) -> None:
        if not self._log_skip_traffic:
            return
        self._log.debug("skip: %s %s", reason, json.dumps(fields, default=str))
