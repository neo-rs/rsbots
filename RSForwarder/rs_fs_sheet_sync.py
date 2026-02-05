from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote_plus

import requests


def _cfg_bool(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    v = (cfg or {}).get(key)
    if isinstance(v, bool):
        return v
    if v is None:
        v = os.getenv(key.upper(), "")
    s = str(v or "").strip().lower()
    if not s:
        return default
    return s in {"1", "true", "yes", "y", "on"}


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    v = str((cfg or {}).get(key) or "").strip()
    if v:
        return v
    return str(os.getenv(key.upper(), "") or "").strip() or default


def _cfg_int(cfg: Dict[str, Any], key: str) -> Optional[int]:
    v = (cfg or {}).get(key)
    if isinstance(v, int):
        return v
    s = str(v or "").strip()
    if not s:
        s = str(os.getenv(key.upper(), "") or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def build_store_link(store: str, sku: str) -> str:
    """
    Build a non-affiliate store link mirroring the spreadsheet formula behavior.
    """
    s = (store or "").strip().lower()
    b = (sku or "").strip()
    if not (s and b):
        return ""
    if b.startswith("http://") or b.startswith("https://"):
        return b

    if "amazon" in s:
        return f"https://www.amazon.com/dp/{b}"
    if "walmart" in s:
        return f"https://www.walmart.com/ip/{b}"
    if "target" in s:
        return f"https://www.target.com/p/-/A-{b}"
    if "homedepot" in s or "home depot" in s:
        return f"https://www.homedepot.com/p/{b}"
    if "gamestop" in s:
        return f"https://www.gamestop.com/product/{b}"
    if "costco" in s:
        return f"https://www.costco.com/.product.{b}.html"
    if "bestbuy" in s or "best buy" in s:
        return f"https://www.bestbuy.com/site/searchpage.jsp?st={quote_plus(b)}"
    if "topps" in s:
        return f"https://www.topps.com/catalogsearch/result/?q={quote_plus(b)}"
    if "hotopic" in s or "hot topic" in s:
        return f"https://www.hottopic.com/product/{b}.html"
    if "mattel" in s:
        return f"https://creations.mattel.com/products/{b}"
    if "barnes" in s:
        return f"https://www.barnesandnoble.com/w/{quote_plus(b)}"
    if "shopify" in s:
        return f"https://www.shopify.com/products/{quote_plus(b)}"
    if "sam" in s and "club" in s:
        # mirror formula
        if b[:1].upper() == "P":
            return f"https://www.samsclub.com/ip/{b}"
        return f"https://www.samsclub.com/p/{b}"
    return ""


def _strip_title_suffix(title: str) -> str:
    import html as _html

    t = _html.unescape((title or "").strip())
    if not t:
        return ""
    # Common store suffixes
    for sep in (" | ", " - ", " — ", " – "):
        # Keep the left-most segment if suffix looks like a site name
        parts = t.split(sep)
        if len(parts) >= 2:
            right = (parts[-1] or "").strip().lower()
            if right in {
                "walmart.com",
                "walmart",
                "target",
                "gamestop",
                "costco",
                "best buy",
                "bestbuy.com",
                "the home depot",
                "homedepot.com",
                "topps",
                "hot topic",
                "barnes & noble",
                "barnesandnoble.com",
                "amazon.com",
                "amazon",
            }:
                return (sep.join(parts[:-1]) or "").strip()
    return t


async def _fetch_html(url: str, *, timeout_s: float = 12.0) -> Tuple[Optional[str], Optional[str]]:
    u = (url or "").strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return None, "invalid url"

    ua = (os.getenv("RS_SHEET_SCRAPE_UA", "") or "").strip() or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    def _do() -> Tuple[Optional[str], Optional[str]]:
        try:
            r = requests.get(u, headers=headers, timeout=float(timeout_s), allow_redirects=True)
        except Exception as e:
            return None, str(e)
        try:
            ct = (r.headers.get("content-type") or "").lower()
        except Exception:
            ct = ""
        if int(getattr(r, "status_code", 0) or 0) >= 400:
            return None, f"http {getattr(r, 'status_code', '')}"
        # Even if ct isn't HTML, some sites mislabel; we still try to parse title.
        text = getattr(r, "text", None)
        if not isinstance(text, str) or not text.strip():
            return None, "empty body"
        return text, None

    return await asyncio.to_thread(_do)


def _extract_title_from_html(html: str, *, store: str = "") -> str:
    h = html or ""
    if not h:
        return ""

    # Prefer OG title
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', h, re.IGNORECASE)
    if m:
        return _strip_title_suffix(m.group(1))

    # Amazon: #productTitle is often present even when title tag is noisy
    if "amazon" in (store or "").lower():
        m2 = re.search(r'id=["\']productTitle["\'][^>]*>\s*([^<]+)\s*<', h, re.IGNORECASE)
        if m2:
            return _strip_title_suffix(m2.group(1))

    # Fallback: <title>
    m3 = re.search(r"<title[^>]*>(.*?)</title>", h, re.IGNORECASE | re.DOTALL)
    if m3:
        t = re.sub(r"\s+", " ", m3.group(1) or "").strip()
        return _strip_title_suffix(t)

    return ""


async def fetch_product_title(store: str, sku: str) -> Tuple[str, Optional[str]]:
    url = build_store_link(store, sku)
    if not url:
        return "", "no store url"
    html, err = await _fetch_html(url)
    if err or not html:
        return "", err
    title = _extract_title_from_html(html, store=store)
    t0 = (title or "").strip()
    t0_l = t0.lower()
    # Common anti-bot / interstitial titles (especially Walmart)
    if t0_l in {"robot or human?", "robot or human"} or "robot or human" in t0_l:
        return "", "blocked (anti-bot)"
    if "access denied" in t0_l or "verify you are a human" in t0_l:
        return "", "blocked (anti-bot)"
    # Generic/non-product titles (treat as missing to avoid writing junk like "Amazon.com" or "Target")
    if t0_l in {"amazon.com", "amazon", "target", "walmart", "best buy", "bestbuy", "costco", "gamestop"}:
        return "", "title not found"
    return title, None if title else "title not found"


@dataclass(frozen=True)
class RsFsPreviewEntry:
    store: str
    sku: str
    url: str
    title: str
    error: str
    source: str = ""
    monitor_url: str = ""
    affiliate_url: str = ""


def _try_parse_service_account_json(raw: str) -> Optional[dict]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_service_account_info(cfg: Dict[str, Any]) -> Optional[dict]:
    # 1) dict already
    v = (cfg or {}).get("google_service_account_json")
    if isinstance(v, dict):
        return v

    # 2) inline JSON string
    if isinstance(v, str):
        parsed = _try_parse_service_account_json(v)
        if parsed:
            return parsed

    # 3) explicit file path
    p = _cfg_str(cfg, "google_service_account_file", "")
    if p:
        try:
            path = Path(p)
            if not path.is_absolute():
                # allow relative to repo root
                repo_root = Path(__file__).resolve().parents[1]
                path = (repo_root / path).resolve()
            if path.exists():
                parsed = _try_parse_service_account_json(path.read_text(encoding="utf-8", errors="replace"))
                if parsed:
                    return parsed
        except Exception:
            pass

    # 4) env
    env = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "") or "").strip()
    if env:
        parsed = _try_parse_service_account_json(env)
        if parsed:
            return parsed
    return None


def _build_sheets_service(service_account_info: dict):
    # Lazy import so RSForwarder can run without these deps when feature disabled.
    # Also silence noisy upstream Python EOL warnings in journald; this bot does not control system Python upgrades.
    try:
        import warnings

        warnings.filterwarnings(
            "ignore",
            category=FutureWarning,
            module=r"google\.api_core\._python_version_support",
        )
    except Exception:
        pass
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


@dataclass
class RsFsSheetConfig:
    enabled: bool
    spreadsheet_id: str
    tab_name: str
    tab_gid: Optional[int]
    dedupe_cache_ttl_s: int


def _resolve_sheet_cfg(cfg: Dict[str, Any]) -> RsFsSheetConfig:
    enabled = _cfg_bool(cfg, "rs_fs_sheet_enabled", False)
    spreadsheet_id = _cfg_str(cfg, "rs_fs_sheet_spreadsheet_id", "")
    tab_name = _cfg_str(cfg, "rs_fs_sheet_tab_name", "")
    tab_gid = _cfg_int(cfg, "rs_fs_sheet_tab_gid")
    try:
        ttl = int((cfg or {}).get("rs_fs_sheet_dedupe_cache_ttl_s") or 300)
    except Exception:
        ttl = 300
    ttl = max(30, min(ttl, 3600))
    return RsFsSheetConfig(
        enabled=enabled,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        tab_gid=tab_gid,
        dedupe_cache_ttl_s=ttl,
    )


class RsFsSheetSync:
    """
    Append rows to the RS - FS List Google Sheet.

    Writes:
      A: STORE
      B: SKU-UPC
      C: Product Title
      G: affliated link
      H: monitor url link
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self._sheet_cfg = _resolve_sheet_cfg(self.cfg)
        self._service = None
        self._tab_name_cache: Optional[str] = None
        self._dedupe_skus: Set[str] = set()
        self._dedupe_last_fetch_ts: float = 0.0
        self._last_service_error: str = ""
        # IMPORTANT: googleapiclient/httplib2 are not reliably thread-safe across concurrent calls.
        # RSForwarder can process multiple Zephyr chunks rapidly; serialize all Sheets API usage.
        self._api_lock: asyncio.Lock = asyncio.Lock()
        # History tab cache (store+sku -> cached fields)
        self._history_cache: Dict[str, Dict[str, str]] = {}
        self._history_cache_ts: float = 0.0

    def refresh_config(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg or {}
        self._sheet_cfg = _resolve_sheet_cfg(self.cfg)
        # Keep service cached; credentials rarely change.
        self._tab_name_cache = None
        self._dedupe_skus = set()
        self._dedupe_last_fetch_ts = 0.0
        self._last_service_error = ""
        self._history_cache = {}
        self._history_cache_ts = 0.0

    def _current_tab_title(self) -> str:
        return _cfg_str(self.cfg, "rs_fs_current_tab_name", "Full-Send-Current-List")

    def _history_tab_title(self) -> str:
        return _cfg_str(self.cfg, "rs_fs_history_tab_name", "Full-Send-History")

    @staticmethod
    def _utc_now_iso() -> str:
        try:
            return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        except Exception:
            return ""

    async def _ensure_sheet_tab(self, title: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        """
        Ensure a tab exists, returning (sheetId, tab_title, error).
        """
        t = str(title or "").strip()
        if not t:
            return None, None, "missing tab title"
        if not self._sheet_cfg.spreadsheet_id:
            return None, None, "missing spreadsheet id"
        service = self._get_service()
        if not service:
            return None, None, self.last_service_error() or "missing google service / deps"

        async with self._api_lock:
            def _do_get() -> Dict[str, Any]:
                return (
                    service.spreadsheets()
                    .get(
                        spreadsheetId=self._sheet_cfg.spreadsheet_id,
                        fields="sheets(properties(sheetId,title))",
                    )
                    .execute()
                )

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception as e:
                return None, None, f"spreadsheets.get failed: {e}"

            sheets = resp.get("sheets") if isinstance(resp, dict) else None
            if isinstance(sheets, list):
                for sh in sheets:
                    props = sh.get("properties") if isinstance(sh, dict) else None
                    if not isinstance(props, dict):
                        continue
                    title2 = str(props.get("title") or "").strip()
                    if title2 == t:
                        try:
                            sid = int(props.get("sheetId") or 0)
                        except Exception:
                            sid = 0
                        return (sid if sid > 0 else None), title2, None

            # Not found -> create
            def _do_add() -> Dict[str, Any]:
                return (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=self._sheet_cfg.spreadsheet_id,
                        body={
                            "requests": [
                                {
                                    "addSheet": {
                                        "properties": {
                                            "title": t,
                                            "gridProperties": {"frozenRowCount": 1},
                                        }
                                    }
                                }
                            ]
                        },
                    )
                    .execute()
                )

            try:
                resp2 = await asyncio.to_thread(_do_add)
            except Exception as e:
                return None, None, f"addSheet failed: {e}"
            try:
                replies = resp2.get("replies") if isinstance(resp2, dict) else None
                if isinstance(replies, list) and replies:
                    props = replies[0].get("addSheet", {}).get("properties", {})
                    sid2 = int(props.get("sheetId") or 0)
                    title2 = str(props.get("title") or t).strip()
                    return (sid2 if sid2 > 0 else None), title2, None
            except Exception:
                pass
            return None, t, None

    async def write_current_list_mirror(self, rows: Sequence[Sequence[str]]) -> Tuple[bool, str, int]:
        """
        Mirror-write the `Full-Send-Current-List` tab.
        Expects rows without headers; this method writes headers + rows.
        """
        headers = [
            "Release ID",
            "Store",
            "SKU/Label",
            "Monitor Tag",
            "Category",
            "Channel ID",
            "Resolved Title",
            "Resolved URL",
            "Affiliate URL",
            "Status",
            "Remove Command",
            "Last Seen (UTC)",
        ]
        tab_title = self._current_tab_title()
        sheet_id, tab, err = await self._ensure_sheet_tab(tab_title)
        if err:
            return False, err, 0
        if not tab:
            return False, "missing current tab title", 0
        service = self._get_service()
        if not service:
            return False, self.last_service_error() or "missing google service / deps", 0

        values = [headers] + [[str(c or "") for c in r] for r in (rows or [])]
        rng = f"'{tab}'!A1:L{max(1, len(values))}"

        async with self._api_lock:
            def _do_clear() -> None:
                service.spreadsheets().values().clear(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=f"'{tab}'!A:Z",
                    body={},
                ).execute()

            def _do_update() -> None:
                service.spreadsheets().values().update(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=rng,
                    valueInputOption="USER_ENTERED",
                    body={"values": values},
                ).execute()

            try:
                await asyncio.to_thread(_do_clear)
                await asyncio.to_thread(_do_update)
            except Exception as e:
                return False, f"write current list failed: {e}", 0
        return True, "ok", max(0, len(values) - 1)

    async def fetch_current_list_rows(self) -> List[List[str]]:
        """
        Fetch all rows from the Current List tab (excluding header).
        Returns list of rows, each row is a list of column values.
        """
        tab_title = self._current_tab_title()
        _sheet_id, tab, err = await self._ensure_sheet_tab(tab_title)
        if err or not tab:
            return []
        service = self._get_service()
        if not service:
            return []

        rng = f"'{tab}'!A:L"

        async with self._api_lock:
            def _do_get() -> Dict[str, Any]:
                return service.spreadsheets().values().get(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=rng,
                ).execute()

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception:
                return []

        values = resp.get("values") if isinstance(resp, dict) else None
        rows: List[List[str]] = []
        if isinstance(values, list):
            # Skip header row (row 1)
            for i, row in enumerate(values):
                if i == 0:
                    continue
                # Convert to list of strings, pad to 12 columns if needed
                row_str = [str(c or "").strip() for c in row]
                while len(row_str) < 12:
                    row_str.append("")
                # Skip completely empty rows (no Release ID, Store, or SKU)
                if not (row_str[0] or row_str[1] or row_str[2]):
                    continue
                rows.append(row_str)
        return rows

    async def fetch_live_list_rows(self) -> List[List[str]]:
        """
        Fetch all rows from the Live List tab (excluding header).
        Returns list of rows, each row is a list of column values.
        Live List columns: Store, SKU-UPC, Product Title, STORE LINK, Comps, Category, affiliated link, monitor url link
        """
        tab = await self._resolve_tab_name()
        if not tab:
            return []
        service = self._get_service()
        if not service:
            return []

        rng = f"'{tab}'!A:H"

        async with self._api_lock:
            def _do_get() -> Dict[str, Any]:
                return service.spreadsheets().values().get(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=rng,
                ).execute()

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception:
                return []

        values = resp.get("values") if isinstance(resp, dict) else None
        rows: List[List[str]] = []
        if isinstance(values, list):
            # Skip header row (row 1)
            for i, row in enumerate(values):
                if i == 0:
                    continue
                # Convert to list of strings, pad to 8 columns if needed
                row_str = [str(c or "").strip() for c in row]
                while len(row_str) < 8:
                    row_str.append("")
                # Skip completely empty rows (no Store or SKU)
                if not (row_str[0] or row_str[1]):
                    continue
                rows.append(row_str)
        return rows

    async def fetch_history_cache(self, *, force: bool = False) -> Dict[str, Dict[str, str]]:
        """
        Return mapping key -> record where key is `store_lower|sku_lower`.
        Cached in-memory with TTL.
        """
        try:
            ttl = int((self.cfg or {}).get("rs_fs_history_cache_ttl_s") or 900)
        except Exception:
            ttl = 900
        ttl = max(60, min(ttl, 12 * 3600))

        now = time.time()
        if (not force) and self._history_cache and (now - float(self._history_cache_ts or 0.0)) < float(ttl):
            return dict(self._history_cache)

        tab_title = self._history_tab_title()
        _sheet_id, tab, err = await self._ensure_sheet_tab(tab_title)
        if err:
            return {}
        if not tab:
            return {}
        service = self._get_service()
        if not service:
            return {}

        rng = f"'{tab}'!A:I"

        async with self._api_lock:
            def _do_get() -> Dict[str, Any]:
                return service.spreadsheets().values().get(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=rng,
                ).execute()

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception:
                return {}

        values = resp.get("values") if isinstance(resp, dict) else None
        out: Dict[str, Dict[str, str]] = {}
        if isinstance(values, list):
            for i, row in enumerate(values, start=1):
                if i == 1:
                    continue
                if not isinstance(row, list) or len(row) < 2:
                    continue
                store = str(row[0] or "").strip()
                sku = str(row[1] or "").strip()
                if not (store and sku):
                    continue
                key = f"{store.lower()}|{sku.lower()}"
                out[key] = {
                    "store": store,
                    "sku": sku,
                    "title": str(row[2] or "").strip() if len(row) > 2 else "",
                    "url": str(row[3] or "").strip() if len(row) > 3 else "",
                    "affiliate_url": str(row[4] or "").strip() if len(row) > 4 else "",
                    "first_seen": str(row[5] or "").strip() if len(row) > 5 else "",
                    "last_seen": str(row[6] or "").strip() if len(row) > 6 else "",
                    "last_release_id": str(row[7] or "").strip() if len(row) > 7 else "",
                    "source": str(row[8] or "").strip() if len(row) > 8 else "",
                    "row_index_1_based": str(i),
                }

        self._history_cache = dict(out)
        self._history_cache_ts = now
        return dict(out)

    async def upsert_history_rows(self, rows: Sequence[Sequence[str]]) -> Tuple[bool, str, int, int]:
        """
        Upsert into `Full-Send-History`.\n
        Expected row columns (A:I):\n
          Store, SKU, Title, URL, Affiliate URL, First Seen (UTC), Last Seen (UTC), Last Release ID, Source\n
        Returns (ok, msg, added, updated).
        """
        tab_title = self._history_tab_title()
        _sheet_id, tab, err = await self._ensure_sheet_tab(tab_title)
        if err:
            return False, err, 0, 0
        if not tab:
            return False, "missing history tab title", 0, 0
        service = self._get_service()
        if not service:
            return False, self.last_service_error() or "missing google service / deps", 0, 0

        # Ensure header exists via mirror write if the sheet is empty.
        headers = [
            "Store",
            "SKU",
            "Title",
            "URL",
            "Affiliate URL",
            "First Seen (UTC)",
            "Last Seen (UTC)",
            "Last Release ID",
            "Source",
        ]

        # Load existing map (forces refresh)
        existing = await self.fetch_history_cache(force=True)

        to_update: List[Tuple[int, List[str]]] = []
        to_add: List[List[str]] = []

        now_iso = self._utc_now_iso()
        for r in (rows or []):
            row = [str(c or "") for c in r]
            store = (row[0] if len(row) > 0 else "").strip()
            sku = (row[1] if len(row) > 1 else "").strip()
            if not (store and sku):
                continue
            key = f"{store.lower()}|{sku.lower()}"
            if key in existing:
                ex = existing.get(key) or {}
                # Preserve First Seen if present
                if len(row) < 6:
                    row = row + ([""] * (6 - len(row)))
                if not (row[5] or "").strip():
                    row[5] = str(ex.get("first_seen") or "").strip() or now_iso
                if len(row) < 7:
                    row = row + ([""] * (7 - len(row)))
                if not (row[6] or "").strip():
                    row[6] = now_iso
                try:
                    ri = int(str(ex.get("row_index_1_based") or "0").strip() or "0")
                except Exception:
                    ri = 0
                if ri >= 2:
                    to_update.append((ri, row[:9] + ([""] * max(0, 9 - len(row)))))
            else:
                # Fill first/last seen defaults
                row2 = row[:9] + ([""] * max(0, 9 - len(row)))
                if not row2[5].strip():
                    row2[5] = now_iso
                if not row2[6].strip():
                    row2[6] = now_iso
                to_add.append(row2)

        async with self._api_lock:
            # Ensure header if needed (if empty or only headers)
            if not existing:
                try:
                    service.spreadsheets().values().update(
                        spreadsheetId=self._sheet_cfg.spreadsheet_id,
                        range=f"'{tab}'!A1:I1",
                        valueInputOption="USER_ENTERED",
                        body={"values": [headers]},
                    ).execute()
                except Exception:
                    pass

            updated = 0
            if to_update:
                data = []
                for row_i, row_vals in to_update:
                    data.append({"range": f"'{tab}'!A{row_i}:I{row_i}", "values": [row_vals[:9]]})

                def _do_update() -> None:
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=self._sheet_cfg.spreadsheet_id,
                        body={"valueInputOption": "USER_ENTERED", "data": data},
                    ).execute()

                try:
                    await asyncio.to_thread(_do_update)
                    updated = len(to_update)
                except Exception as e:
                    return False, f"history update failed: {e}", 0, 0

            added = 0
            if to_add:
                def _do_add() -> None:
                    service.spreadsheets().values().append(
                        spreadsheetId=self._sheet_cfg.spreadsheet_id,
                        range=f"'{tab}'!A:I",
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={"values": to_add},
                    ).execute()

                try:
                    await asyncio.to_thread(_do_add)
                    added = len(to_add)
                except Exception as e:
                    return False, f"history append failed: {e}", 0, updated

        # Refresh cache
        try:
            await self.fetch_history_cache(force=True)
        except Exception:
            pass
        return True, "ok", int(added), int(updated)

    def enabled(self) -> bool:
        return bool(self._sheet_cfg.enabled)

    def _get_service(self):
        if self._service is not None:
            return self._service
        info = _load_service_account_info(self.cfg)
        if not info:
            self._last_service_error = "missing google service account json/file"
            return None
        try:
            try:
                self._service = _build_sheets_service(info)
            except ImportError as e:
                self._service = None
                self._last_service_error = f"missing google libs: {e}"
                return None
            self._last_service_error = ""
        except Exception as e:
            self._last_service_error = f"failed to initialize google sheets client: {e}"
            self._service = None
        return self._service

    def last_service_error(self) -> str:
        return (self._last_service_error or "").strip()

    async def preflight(self) -> Tuple[bool, str, Optional[str], int]:
        """
        Non-mutating check: validate credentials + access to spreadsheet/tab and count existing SKUs.
        Returns: (ok, message, tab_name, existing_sku_count)
        """
        async with self._api_lock:
            if not self.enabled():
                return False, "disabled", None, 0
            if not self._sheet_cfg.spreadsheet_id:
                return False, "missing spreadsheet id", None, 0
            service = self._get_service()
            if not service:
                err = self.last_service_error() or "missing google service / deps"
                return False, err, None, 0
            tab = await self._resolve_tab_name()
            if not tab:
                return False, "missing tab name/gid", None, 0
            try:
                await self._fetch_existing_skus_if_needed()
            except Exception as e:
                return False, f"failed to read existing SKUs: {e}", tab, 0
            return True, "ok", tab, len(self._dedupe_skus or set())

    async def filter_new_pairs(self, pairs: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """
        Filter (store, sku) pairs down to only SKUs not already present in the sheet.
        If sheet isn't enabled or we can't read the sheet, returns the input pairs unchanged.
        """
        async with self._api_lock:
            if not self.enabled():
                return list(pairs or [])
            try:
                await self._fetch_existing_skus_if_needed()
            except Exception:
                return list(pairs or [])
            out: List[Tuple[str, str]] = []
            for st, sk in (pairs or []):
                sku = str(sk or "").strip().lower()
                if sku and sku in (self._dedupe_skus or set()):
                    continue
                out.append((st, sk))
            return out

    async def _resolve_tab_name(self) -> Optional[str]:
        if self._tab_name_cache:
            return self._tab_name_cache
        if self._sheet_cfg.tab_name:
            self._tab_name_cache = self._sheet_cfg.tab_name
            return self._tab_name_cache

        service = self._get_service()
        if not service:
            return None
        if not (self._sheet_cfg.spreadsheet_id and self._sheet_cfg.tab_gid):
            return None

        def _do() -> Optional[str]:
            resp = (
                service.spreadsheets()
                .get(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    fields="sheets(properties(sheetId,title))",
                )
                .execute()
            )
            sheets = resp.get("sheets") if isinstance(resp, dict) else None
            if not isinstance(sheets, list):
                return None
            for sh in sheets:
                props = sh.get("properties") if isinstance(sh, dict) else None
                if not isinstance(props, dict):
                    continue
                if int(props.get("sheetId") or 0) == int(self._sheet_cfg.tab_gid or 0):
                    title = str(props.get("title") or "").strip()
                    return title or None
            return None

        name = await asyncio.to_thread(_do)
        self._tab_name_cache = name
        return name

    async def _fetch_existing_skus_if_needed(self) -> None:
        if not self.enabled():
            return

        now = time.time()
        if self._dedupe_skus and (now - self._dedupe_last_fetch_ts) < float(self._sheet_cfg.dedupe_cache_ttl_s):
            return

        tab = await self._resolve_tab_name()
        service = self._get_service()
        if not (service and tab and self._sheet_cfg.spreadsheet_id):
            return

        rng = f"'{tab}'!B:B"

        def _do() -> Set[str]:
            resp = service.spreadsheets().values().get(spreadsheetId=self._sheet_cfg.spreadsheet_id, range=rng).execute()
            values = resp.get("values") if isinstance(resp, dict) else None
            out: Set[str] = set()
            if isinstance(values, list):
                for row in values:
                    if not isinstance(row, list) or not row:
                        continue
                    sku = str(row[0] or "").strip()
                    if sku and sku.lower() != "sku-upc":
                        out.add(sku.lower())
            return out

        self._dedupe_skus = await asyncio.to_thread(_do)
        self._dedupe_last_fetch_ts = now

    async def _fetch_existing_sku_row_map(self) -> Dict[str, int]:
        """
        Return mapping: sku_lower -> 1-based row index in the sheet (based on column B).
        Header row ("SKU-UPC") and blanks are ignored.
        """
        if not self.enabled():
            return {}
        tab = await self._resolve_tab_name()
        service = self._get_service()
        if not (service and tab and self._sheet_cfg.spreadsheet_id):
            return {}

        rng = f"'{tab}'!B:B"

        def _do() -> Dict[str, int]:
            resp = service.spreadsheets().values().get(spreadsheetId=self._sheet_cfg.spreadsheet_id, range=rng).execute()
            values = resp.get("values") if isinstance(resp, dict) else None
            out: Dict[str, int] = {}
            if isinstance(values, list):
                for i, row in enumerate(values, start=1):
                    if not isinstance(row, list) or not row:
                        continue
                    sku = str(row[0] or "").strip()
                    if not sku:
                        continue
                    if sku.lower() == "sku-upc":
                        continue
                    out[sku.lower()] = i
            return out

        return await asyncio.to_thread(_do)

    async def fetch_sheet_abc_map(self) -> Dict[str, Dict[str, str]]:
        """
        Fetch the public sheet tab A:C (store, sku, title) into a dict:
          sku_lower -> {store, sku, title}
        """
        if not self.enabled():
            return {}
        tab = await self._resolve_tab_name()
        service = self._get_service()
        if not (service and tab and self._sheet_cfg.spreadsheet_id):
            return {}

        rng = f"'{tab}'!A:C"

        async with self._api_lock:
            def _do_get() -> Dict[str, Any]:
                return service.spreadsheets().values().get(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=rng,
                ).execute()

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception:
                return {}

        values = resp.get("values") if isinstance(resp, dict) else None
        out: Dict[str, Dict[str, str]] = {}
        if isinstance(values, list):
            for i, row in enumerate(values, start=1):
                if i == 1:
                    continue
                if not isinstance(row, list) or len(row) < 2:
                    continue
                store = str(row[0] or "").strip()
                sku = str(row[1] or "").strip()
                title = str(row[2] or "").strip() if len(row) > 2 else ""
                if not sku:
                    continue
                if sku.lower() == "sku-upc":
                    continue
                out[sku.lower()] = {"store": store, "sku": sku, "title": title}
        return out

    async def _delete_rows_by_indices(self, row_indices_1_based: Sequence[int]) -> int:
        """
        Delete entire rows by 1-based indices (data rows). Returns deleted row count.
        Requires tab_gid (sheetId).
        """
        if not self.enabled():
            return 0
        if not row_indices_1_based:
            return 0
        service = self._get_service()
        if not service:
            return 0
        try:
            sheet_id = int(self._sheet_cfg.tab_gid or 0)
        except Exception:
            sheet_id = 0
        if not sheet_id:
            return 0

        rows = sorted({int(r) for r in row_indices_1_based if int(r) >= 2})
        if not rows:
            return 0

        # Merge contiguous runs, then delete bottom-up (so indices remain valid).
        runs: List[Tuple[int, int]] = []
        start = rows[0]
        prev = rows[0]
        for r in rows[1:]:
            if r == prev + 1:
                prev = r
                continue
            runs.append((start, prev))
            start = r
            prev = r
        runs.append((start, prev))
        runs = sorted(runs, key=lambda t: t[0], reverse=True)

        reqs = []
        for a, b in runs:
            # 0-based, endIndex exclusive
            reqs.append(
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": int(a) - 1,
                            "endIndex": int(b),
                        }
                    }
                }
            )

        def _do() -> None:
            service.spreadsheets().batchUpdate(
                spreadsheetId=self._sheet_cfg.spreadsheet_id,
                body={"requests": reqs},
            ).execute()

        try:
            await asyncio.to_thread(_do)
        except Exception:
            return 0

        deleted = sum((b - a + 1) for a, b in runs)
        return int(deleted)

    async def sync_rows_mirror(self, rows: Sequence[Sequence[str]]) -> Tuple[bool, str, int, int, int]:
        """
        Mirror-mode sync:
        - Update A/B/C + G/H for existing SKUs (by column B match)
        - Append missing SKUs
        - Delete rows for SKUs no longer present in `rows`

        Returns: (ok, message, added_count, updated_count, deleted_count)
        """
        if not self.enabled():
            return False, "disabled", 0, 0, 0
        if not self._sheet_cfg.spreadsheet_id:
            return False, "missing spreadsheet id", 0, 0, 0
        service = self._get_service()
        if not service:
            err = self.last_service_error() or "missing google service account / deps"
            return False, f"google sheets client not ready: {err}", 0, 0, 0
        tab = await self._resolve_tab_name()
        if not tab:
            return False, "missing tab name/gid", 0, 0, 0

        # Current sheet rows keyed by SKU (column B)
        async with self._api_lock:
            existing = await self._fetch_existing_sku_row_map()

        desired_keys: Set[str] = set()
        to_update: List[Tuple[int, List[str], List[str]]] = []
        to_add: List[List[str]] = []

        for r in (rows or []):
            row = [str(c or "") for c in r]
            store = (row[0] if len(row) > 0 else "").strip()
            sku_raw = (row[1] if len(row) > 1 else "").strip()
            title = (row[2] if len(row) > 2 else "").strip()
            aff = (row[3] if len(row) > 3 else "").strip()
            mon = (row[4] if len(row) > 4 else "").strip()
            if not (store and sku_raw):
                continue
            key = sku_raw.lower()
            desired_keys.add(key)
            abc = [store, sku_raw, title]
            gh = [aff, mon]
            if key in existing:
                to_update.append((int(existing[key]), abc, gh))
            else:
                # append_rows expects [A,B,C, G, H]
                to_add.append([store, sku_raw, title, aff, mon])

        # Update existing rows in batch (A:C and G:H), without touching D/E/F.
        updated_count = 0
        if to_update:
            data = []
            for row_i, abc, gh in to_update:
                if row_i < 2:
                    continue
                data.append({"range": f"'{tab}'!A{row_i}:C{row_i}", "values": [abc]})
                data.append({"range": f"'{tab}'!G{row_i}:H{row_i}", "values": [gh]})

            def _do_update() -> None:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    body={"valueInputOption": "USER_ENTERED", "data": data},
                ).execute()

            try:
                async with self._api_lock:
                    await asyncio.to_thread(_do_update)
                updated_count = len(to_update)
            except Exception as e:
                return False, f"update failed: {e}", 0, 0, 0

        # Append new rows
        ok_add, msg_add, added_count = await self.append_rows(to_add)
        if not ok_add:
            return False, msg_add, 0, updated_count, 0

        # Delete stale SKUs (rows present in sheet but not in desired list)
        stale_rows = [row_i for (sku, row_i) in (existing or {}).items() if sku not in desired_keys]
        async with self._api_lock:
            deleted_count = await self._delete_rows_by_indices(stale_rows)

        # Reset dedupe cache so subsequent runs see fresh sheet state.
        self._dedupe_skus = set()
        self._dedupe_last_fetch_ts = 0.0
        return True, "ok", int(added_count), int(updated_count), int(deleted_count)

    async def upsert_rows(self, rows: Sequence[Sequence[str]]) -> Tuple[bool, str, int, int]:
        """
        Upsert rows by SKU (column B):
          - Update A/B/C and G/H for existing SKUs
          - Append missing SKUs

        Does NOT delete any rows.

        Returns: (ok, message, added_count, updated_count)
        """
        if not self.enabled():
            return False, "disabled", 0, 0
        if not rows:
            return True, "no rows", 0, 0
        if not self._sheet_cfg.spreadsheet_id:
            return False, "missing spreadsheet id", 0, 0
        service = self._get_service()
        if not service:
            err = self.last_service_error() or "missing google service account / deps"
            return False, f"google sheets client not ready: {err}", 0, 0
        tab = await self._resolve_tab_name()
        if not tab:
            return False, "missing tab name/gid", 0, 0

        async with self._api_lock:
            existing = await self._fetch_existing_sku_row_map()

        to_update: List[Tuple[int, List[str], List[str]]] = []
        to_add: List[List[str]] = []

        for r in (rows or []):
            row = [str(c or "") for c in r]
            store = (row[0] if len(row) > 0 else "").strip()
            sku_raw = (row[1] if len(row) > 1 else "").strip()
            title = (row[2] if len(row) > 2 else "").strip()
            aff = (row[3] if len(row) > 3 else "").strip()
            mon = (row[4] if len(row) > 4 else "").strip()
            if not (store and sku_raw):
                continue
            key = sku_raw.lower()
            abc = [store, sku_raw, title]
            gh = [aff, mon]
            if key in existing:
                to_update.append((int(existing[key]), abc, gh))
            else:
                to_add.append([store, sku_raw, title, aff, mon])

        updated_count = 0
        if to_update:
            data = []
            for row_i, abc, gh in to_update:
                if row_i < 2:
                    continue
                data.append({"range": f"'{tab}'!A{row_i}:C{row_i}", "values": [abc]})
                data.append({"range": f"'{tab}'!G{row_i}:H{row_i}", "values": [gh]})

            def _do_update() -> None:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    body={"valueInputOption": "USER_ENTERED", "data": data},
                ).execute()

            try:
                async with self._api_lock:
                    await asyncio.to_thread(_do_update)
                updated_count = len(to_update)
            except Exception as e:
                return False, f"update failed: {e}", 0, 0

        ok_add, msg_add, added_count = await self.append_rows(to_add)
        if not ok_add:
            return False, msg_add, 0, int(updated_count)

        # Reset dedupe cache so subsequent runs see fresh sheet state.
        self._dedupe_skus = set()
        self._dedupe_last_fetch_ts = 0.0
        return True, "ok", int(added_count), int(updated_count)

    async def append_rows(self, rows: Sequence[Sequence[str]]) -> Tuple[bool, str, int]:
        """
        Append rows. Returns (ok, message, added_count).
        """
        async with self._api_lock:
            if not self.enabled():
                return False, "disabled", 0
            if not rows:
                return True, "no rows", 0
            if not self._sheet_cfg.spreadsheet_id:
                return False, "missing spreadsheet id", 0

            service = self._get_service()
            if not service:
                err = self.last_service_error() or "missing google service account / deps"
                return False, f"google sheets client not ready: {err}", 0

            tab = await self._resolve_tab_name()
            if not tab:
                return False, "missing tab name/gid", 0

            # Dedupe by SKU (column B)
            await self._fetch_existing_skus_if_needed()
            new_rows: List[List[str]] = []
            new_rows_gh: List[List[str]] = []
            for r in rows:
                row = [str(c or "") for c in r]
                sku = (row[1] if len(row) > 1 else "").strip().lower()
                if sku and sku in self._dedupe_skus:
                    continue
                # A/B/C are required; G/H are optional (row[3], row[4])
                abc = row[:3] + ([""] * max(0, 3 - len(row)))
                aff = (row[3] if len(row) > 3 else "").strip()
                mon = (row[4] if len(row) > 4 else "").strip()
                new_rows.append(abc)
                new_rows_gh.append([aff, mon])

            if not new_rows:
                return True, "all rows already exist", 0

            rng = f"'{tab}'!A:C"

            def _do() -> dict:
                return service.spreadsheets().values().append(
                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                    range=rng,
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": new_rows},
                ).execute()

            try:
                resp = await asyncio.to_thread(_do)
            except Exception as e:
                return False, f"append failed: {e}", 0

            # Try to copy down formulas/validation/formatting for D/E/I/J/K from a template row,
            # so newly inserted rows behave like manual sheet entry (STORE LINK, dropdowns, etc.).
            try:
                sheet_id = int(self._sheet_cfg.tab_gid or 0)
            except Exception:
                sheet_id = 0
            try:
                updated_range = ""
                if isinstance(resp, dict):
                    updates = resp.get("updates") if isinstance(resp.get("updates"), dict) else {}
                    updated_range = str((updates or {}).get("updatedRange") or "").strip()
                m = re.search(r"!A(\d+):C(\d+)", updated_range)
                if sheet_id and m:
                    start_row = int(m.group(1))
                    end_row = int(m.group(2))
                    if end_row >= start_row:
                        # Template row:
                        # - Prefer the row immediately above (most common: last populated row)
                        # - If we just inserted into row 2 (empty sheet case), try the row immediately below.
                        template_row = start_row - 1
                        if template_row < 2:
                            template_row = end_row + 1

                        # Lazy import for Sheets batchUpdate request shapes (same deps).
                        service = self._get_service()
                        if service and template_row >= 2:
                            # 0-based indices for GridRange
                            src_row_start = template_row - 1
                            src_row_end = template_row
                            dst_row_start = start_row - 1
                            dst_row_end = end_row

                            # Column indices (0-based): A=0 ... K=10
                            # We copy:
                            # - Formulas: D, I, J, K
                            # - Validation + formatting: D:K
                            reqs = [
                                {
                                    "copyPaste": {
                                        "source": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": src_row_start,
                                            "endRowIndex": src_row_end,
                                            "startColumnIndex": 3,
                                            "endColumnIndex": 4,
                                        },
                                        "destination": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": dst_row_start,
                                            "endRowIndex": dst_row_end,
                                            "startColumnIndex": 3,
                                            "endColumnIndex": 4,
                                        },
                                        "pasteType": "PASTE_FORMULA",
                                    }
                                },
                                {
                                    "copyPaste": {
                                        "source": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": src_row_start,
                                            "endRowIndex": src_row_end,
                                            "startColumnIndex": 8,
                                            "endColumnIndex": 9,
                                        },
                                        "destination": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": dst_row_start,
                                            "endRowIndex": dst_row_end,
                                            "startColumnIndex": 8,
                                            "endColumnIndex": 9,
                                        },
                                        "pasteType": "PASTE_FORMULA",
                                    }
                                },
                                {
                                    "copyPaste": {
                                        "source": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": src_row_start,
                                            "endRowIndex": src_row_end,
                                            "startColumnIndex": 9,
                                            "endColumnIndex": 10,
                                        },
                                        "destination": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": dst_row_start,
                                            "endRowIndex": dst_row_end,
                                            "startColumnIndex": 9,
                                            "endColumnIndex": 10,
                                        },
                                        "pasteType": "PASTE_FORMULA",
                                    }
                                },
                                {
                                    "copyPaste": {
                                        "source": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": src_row_start,
                                            "endRowIndex": src_row_end,
                                            "startColumnIndex": 10,
                                            "endColumnIndex": 11,
                                        },
                                        "destination": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": dst_row_start,
                                            "endRowIndex": dst_row_end,
                                            "startColumnIndex": 10,
                                            "endColumnIndex": 11,
                                        },
                                        "pasteType": "PASTE_FORMULA",
                                    }
                                },
                                # Data validation + formatting for D:K (no values)
                                {
                                    "copyPaste": {
                                        "source": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": src_row_start,
                                            "endRowIndex": src_row_end,
                                            "startColumnIndex": 3,
                                            "endColumnIndex": 11,
                                        },
                                        "destination": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": dst_row_start,
                                            "endRowIndex": dst_row_end,
                                            "startColumnIndex": 3,
                                            "endColumnIndex": 11,
                                        },
                                        "pasteType": "PASTE_DATA_VALIDATION",
                                    }
                                },
                                {
                                    "copyPaste": {
                                        "source": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": src_row_start,
                                            "endRowIndex": src_row_end,
                                            "startColumnIndex": 3,
                                            "endColumnIndex": 11,
                                        },
                                        "destination": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": dst_row_start,
                                            "endRowIndex": dst_row_end,
                                            "startColumnIndex": 3,
                                            "endColumnIndex": 11,
                                        },
                                        "pasteType": "PASTE_FORMAT",
                                    }
                                },
                            ]

                            def _do_copy() -> None:
                                service.spreadsheets().batchUpdate(
                                    spreadsheetId=self._sheet_cfg.spreadsheet_id,
                                    body={"requests": reqs},
                                ).execute()

                            await asyncio.to_thread(_do_copy)
            except Exception:
                # Never fail the append just because formatting copy didn't work.
                pass

            # Best-effort: populate G/H for the appended rows without touching D/E/F (avoids breaking arrayformulas).
            try:
                updated_range = ""
                if isinstance(resp, dict):
                    updates = resp.get("updates") if isinstance(resp.get("updates"), dict) else {}
                    updated_range = str((updates or {}).get("updatedRange") or "").strip()
                m = re.search(r"!A(\d+):C(\d+)", updated_range)
                if m:
                    start_row = int(m.group(1))
                    end_row = int(m.group(2))
                    if start_row > 0 and end_row >= start_row and (end_row - start_row + 1) == len(new_rows_gh):
                        rng_gh = f"'{tab}'!G{start_row}:H{end_row}"

                        def _do_gh() -> None:
                            service.spreadsheets().values().update(
                                spreadsheetId=self._sheet_cfg.spreadsheet_id,
                                range=rng_gh,
                                valueInputOption="USER_ENTERED",
                                body={"values": new_rows_gh},
                            ).execute()

                        await asyncio.to_thread(_do_gh)
            except Exception:
                # Don't fail the whole append if G/H write fails (sheet can be fixed manually).
                pass

            for r in new_rows:
                sku = (r[1] or "").strip().lower()
                if sku:
                    self._dedupe_skus.add(sku)
            return True, "ok", len(new_rows)


async def build_rows_with_titles(pairs: Iterable[Tuple[str, str]], cfg: Dict[str, Any]) -> List[List[str]]:
    """
    Build A/B/C rows for (store, sku) pairs, fetching titles best-effort.
    """
    entries = await build_preview_entries(pairs, cfg)
    return [[e.store, e.sku, e.title] for e in entries]


async def build_preview_entries(
    pairs: Iterable[Tuple[str, str]],
    cfg: Dict[str, Any],
    *,
    on_progress: Optional[Callable[[int, int, int, RsFsPreviewEntry], Optional[Awaitable[None]]]] = None,
) -> List[RsFsPreviewEntry]:
    """
    Build preview entries (store, sku, title, url) for (store, sku) pairs, fetching titles best-effort.
    """
    unique: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for store, sku in pairs:
        st = (store or "").strip()
        sk = (sku or "").strip()
        if not (st and sk):
            continue
        k = (st.lower(), sk.lower())
        if k in seen:
            continue
        seen.add(k)
        unique.append((st, sk))

    if not unique:
        return []

    # Concurrency limit to avoid hammering stores
    try:
        conc = int((cfg or {}).get("rs_fs_title_fetch_concurrency") or 3)
    except Exception:
        conc = 3
    conc = max(1, min(conc, 5))
    sem = asyncio.Semaphore(conc)

    async def _one(st: str, sk: str) -> RsFsPreviewEntry:
        url = build_store_link(st, sk)
        title = ""
        err = ""
        async with sem:
            if url:
                title, err2 = await fetch_product_title(st, sk)
                err = err2 or ""
            else:
                err = "no store url"
        # IMPORTANT: never write the URL into the title column.
        # If title extraction fails, keep title blank and rely on monitor/manual resolution.
        if not (title or "").strip():
            title = ""
        return RsFsPreviewEntry(
            store=st,
            sku=sk,
            url=url,
            title=title,
            error=err,
            source="website",
            monitor_url=url,
            affiliate_url="",
        )

    async def _one_indexed(i: int, st: str, sk: str) -> Tuple[int, RsFsPreviewEntry]:
        return i, await _one(st, sk)

    total = len(unique)
    results: List[Optional[RsFsPreviewEntry]] = [None] * total
    errors = 0

    tasks: List[asyncio.Task] = []
    for i, (st, sk) in enumerate(unique):
        tasks.append(asyncio.create_task(_one_indexed(i, st, sk)))

    done = 0
    for fut in asyncio.as_completed(tasks):
        i, entry = await fut
        if isinstance(i, int) and 0 <= i < total:
            results[i] = entry
        done += 1
        if (entry.error or "").strip():
            errors += 1
        if on_progress:
            try:
                maybe = on_progress(done, total, errors, entry)
                if asyncio.iscoroutine(maybe):
                    await maybe  # type: ignore[misc]
            except Exception:
                pass

    out: List[RsFsPreviewEntry] = []
    for r in results:
        if isinstance(r, RsFsPreviewEntry):
            out.append(r)
    return out

