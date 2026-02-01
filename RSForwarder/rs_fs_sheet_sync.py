from __future__ import annotations

import asyncio
import json
import os
import re
import time
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
    return title, None if title else "title not found"


@dataclass(frozen=True)
class RsFsPreviewEntry:
    store: str
    sku: str
    url: str
    title: str
    error: str
    source: str = ""


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
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self._sheet_cfg = _resolve_sheet_cfg(self.cfg)
        self._service = None
        self._tab_name_cache: Optional[str] = None
        self._dedupe_skus: Set[str] = set()
        self._dedupe_last_fetch_ts: float = 0.0
        self._last_service_error: str = ""

    def refresh_config(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg or {}
        self._sheet_cfg = _resolve_sheet_cfg(self.cfg)
        # Keep service cached; credentials rarely change.
        self._tab_name_cache = None
        self._dedupe_skus = set()
        self._dedupe_last_fetch_ts = 0.0
        self._last_service_error = ""

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
                    fields="sheets.properties(sheetId,title)",
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

    async def append_rows(self, rows: Sequence[Sequence[str]]) -> Tuple[bool, str, int]:
        """
        Append rows. Returns (ok, message, added_count).
        """
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
        for r in rows:
            row = [str(c or "") for c in r]
            sku = (row[1] if len(row) > 1 else "").strip().lower()
            if sku and sku in self._dedupe_skus:
                continue
            new_rows.append(row[:3] + ([""] * max(0, 3 - len(row))))

        if not new_rows:
            return True, "all rows already exist", 0

        rng = f"'{tab}'!A:C"

        def _do() -> None:
            service.spreadsheets().values().append(
                spreadsheetId=self._sheet_cfg.spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": new_rows},
            ).execute()

        try:
            await asyncio.to_thread(_do)
        except Exception as e:
            return False, f"append failed: {e}", 0

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
        if not (title or "").strip():
            title = url or ""
        return RsFsPreviewEntry(store=st, sku=sk, url=url, title=title, error=err, source="website")

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

