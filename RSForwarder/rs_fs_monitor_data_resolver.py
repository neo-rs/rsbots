from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


def _clean_id(value: str) -> str:
    s = (value or "").strip().strip("`").strip()
    # Keep only alnum plus a few separators, lowercase
    return "".join([c for c in s if c.isalnum() or c in {"-", "_"}]).strip().lower()


def _safe_str(d: object, key: str) -> str:
    if not isinstance(d, dict):
        return ""
    return str(d.get(key) or "").strip()


def _store_to_channel_key(store: str) -> str:
    s = (store or "").strip().lower()
    if not s:
        return ""
    if "amazon" in s:
        return "amazon-monitor"
    if "walmart" in s:
        return "walmart-monitor"
    if "target" in s:
        return "target-monitor"
    if "bestbuy" in s or "best buy" in s:
        return "bestbuy-monitor"
    if "homedepot" in s or "home depot" in s:
        return "homedepot-monitor"
    if "lowes" in s:
        return "lowes-monitor"
    if "gamestop" in s:
        return "gamestop-monitor"
    if "costco" in s:
        return "costco-monitor"
    if "topps" in s:
        return "topps-monitor"
    if "pokemon center" in s or "pokemon-center" in s:
        return "pokemon-center"
    if "sam" in s and "club" in s:
        return "samsclub-monitor"
    if "walgreens" in s:
        return "walgreens"
    # Fallback: best-effort; many monitor files use normalized store key already.
    return re.sub(r"\s+", "-", s).replace("_", "-")


@dataclass(frozen=True)
class MonitorResolved:
    title: str
    url: str
    source: str
    last_seen_timestamp: str


class RsFsMonitorDataResolver:
    """
    Resolve (store, sku) into (title, url) using local RSForwarder/monitor_data/*.json snapshots.

    Canonical behavior:
    - Prefer explicit/derived product_id value match (ASIN/SKU/PID/TCIN/UPC/...)
    - Fall back to title-key match only if needed
    """

    def __init__(self, monitor_data_dir: Path) -> None:
        self._dir = monitor_data_dir
        self._index_by_channel: Dict[str, Dict[str, MonitorResolved]] = {}

    def _load_channel_index(self, channel_key: str) -> Dict[str, MonitorResolved]:
        ck = (channel_key or "").strip()
        if not ck:
            return {}
        if ck in self._index_by_channel:
            return self._index_by_channel[ck]

        p = self._dir / f"{ck}.json"
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            self._index_by_channel[ck] = {}
            return {}

        items_by_key = raw.get("items_by_key") if isinstance(raw, dict) else None
        if not isinstance(items_by_key, dict):
            self._index_by_channel[ck] = {}
            return {}

        idx: Dict[str, MonitorResolved] = {}

        for _item_key, item in items_by_key.items():
            if not isinstance(item, dict):
                continue
            latest = item.get("latest")
            if not isinstance(latest, dict):
                continue
            extracted = latest.get("extracted") if isinstance(latest.get("extracted"), dict) else {}
            human = extracted.get("human") if isinstance(extracted.get("human"), dict) else {}

            title = _safe_str(human, "title") or _safe_str(extracted, "title")
            url = _safe_str(human, "url") or _safe_str(extracted, "primary_url")
            last_seen = str(item.get("last_seen_timestamp") or latest.get("timestamp") or "").strip()

            if not (title or url):
                continue

            product_id = extracted.get("product_id") if isinstance(extracted, dict) else None
            if isinstance(product_id, dict):
                kind = str(product_id.get("kind") or "").strip().lower()
                val = _clean_id(str(product_id.get("value") or ""))
                if kind in {"field", "derived"} and val:
                    # Use id value as primary lookup key
                    idx[val] = MonitorResolved(
                        title=title,
                        url=url,
                        source=f"monitor_data:{ck}",
                        last_seen_timestamp=last_seen,
                    )
                    continue

            # Title fallback key
            title_key = _clean_id(str(item.get("title_key") or title))
            if title_key:
                idx[f"title:{title_key}"] = MonitorResolved(
                    title=title,
                    url=url,
                    source=f"monitor_data:{ck}",
                    last_seen_timestamp=last_seen,
                )

        self._index_by_channel[ck] = idx
        return idx

    def resolve(self, *, store: str, sku: str, monitor_tag: str = "") -> Optional[MonitorResolved]:
        sku_clean = _clean_id(sku)
        if not sku_clean:
            return None

        # Prefer monitor tag (most precise mapping to the correct monitor file)
        ck = ""
        mt = (monitor_tag or "").strip().lower()
        if mt:
            # tag already looks like "amazon-monitor" / "pokemon-center" etc.
            ck = mt
        if not ck:
            ck = _store_to_channel_key(store)
        if not ck:
            return None

        idx = self._load_channel_index(ck)
        hit = idx.get(sku_clean)
        if hit:
            return hit

        # If sku is numeric with punctuation, try digits-only fallback (some feeds include extra chars)
        sku_digits = "".join([c for c in sku_clean if c.isdigit()])
        if sku_digits and sku_digits != sku_clean:
            hit2 = idx.get(sku_digits)
            if hit2:
                return hit2

        # Title fallback (only if we have no id hit)
        # This is intentionally conservative: it requires an exact normalized title key match.
        title_key = _clean_id(sku)  # sometimes feed sku field contains label-ish token
        if title_key:
            hit3 = idx.get(f"title:{title_key}")
            if hit3:
                return hit3

        return None

    def explain_resolve(self, *, store: str, sku: str, monitor_tag: str = "") -> Tuple[Optional[MonitorResolved], str, str]:
        """
        Resolve like `resolve()`, but also return:
        - reason: machine-readable short reason for misses
        - channel_key: which monitor_data file we tried (if any)
        """
        sku_clean = _clean_id(sku)
        if not sku_clean:
            return None, "miss:empty_sku", ""

        ck = ""
        mt = (monitor_tag or "").strip().lower()
        if mt:
            ck = mt
        if not ck:
            ck = _store_to_channel_key(store)
        if not ck:
            return None, "miss:no_channel_key", ""

        p = self._dir / f"{ck}.json"
        if not p.is_file():
            return None, "miss:monitor_file_missing", ck

        idx = self._load_channel_index(ck)
        if not idx:
            return None, "miss:monitor_index_empty", ck

        hit = idx.get(sku_clean)
        if hit:
            return hit, "hit:id", ck

        sku_digits = "".join([c for c in sku_clean if c.isdigit()])
        if sku_digits and sku_digits != sku_clean:
            hit2 = idx.get(sku_digits)
            if hit2:
                return hit2, "hit:digits", ck

        title_key = _clean_id(sku)
        if title_key:
            hit3 = idx.get(f"title:{title_key}")
            if hit3:
                return hit3, "hit:title", ck

        return None, "miss:no_match", ck

    def build_resolved_by_key(
        self,
        *,
        store: str,
        sku: str,
        monitor_tag: str = "",
    ) -> Optional[Dict[str, str]]:
        """
        Return dict compatible with rs_forwarder_bot._rsfs_write_current_list(resolved_by_key):
          { "title": ..., "url": ..., "affiliate_url": "", "source": "monitor_data" }
        """
        hit = self.resolve(store=store, sku=sku, monitor_tag=monitor_tag)
        if not hit:
            return None
        u = (hit.url or "").strip()
        t = (hit.title or "").strip()
        if not u:
            return None
        return {"title": t, "url": u, "affiliate_url": "", "source": "monitor_data"}

