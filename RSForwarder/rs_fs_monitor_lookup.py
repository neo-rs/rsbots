from __future__ import annotations

import re
from typing import Optional

from RSForwarder.rs_fs_sheet_sync import RsFsPreviewEntry


def clean_sku_text(value: str) -> str:
    s = (value or "").strip().strip("`").strip()
    out = "".join([c for c in s if c.isalnum() or c in {"-", "_"}]).strip().lower()
    return out


def first_url_in_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    m = re.search(r"(https?://[^\s<>()]+)", t)
    return (m.group(1) or "").strip() if m else ""


def _iter_embed_fields(embed) -> list:
    try:
        fields = getattr(embed, "fields", None)
        if isinstance(fields, list):
            return fields
    except Exception:
        pass
    return []


def find_entry_in_embed(store: str, sku: str, embed, *, source_tag: str) -> Optional[RsFsPreviewEntry]:
    """
    Inspect one embed object (duck-typed) and return a preview entry if it matches sku.
    """
    target = clean_sku_text(sku)
    if not target:
        return None

    fields = _iter_embed_fields(embed)

    # Prefer explicit SKU/TCIN/ASIN/UPC fields
    for f in fields:
        name = str(getattr(f, "name", "") or "").strip().lower()
        val = str(getattr(f, "value", "") or "").strip()
        if not val:
            continue
        if ("sku" in name) or ("tcin" in name) or ("asin" in name) or ("upc" in name):
            cand = clean_sku_text(val)
            if cand == target:
                title = str(getattr(embed, "title", "") or "").strip()
                url = str(getattr(embed, "url", "") or "").strip()
                if not url:
                    # Try pull first URL from any field values
                    for f2 in fields:
                        url = first_url_in_text(str(getattr(f2, "value", "") or ""))
                        if url:
                            break
                if not url:
                    url = first_url_in_text(str(getattr(embed, "description", "") or ""))
                if not title:
                    title = url or ""
                return RsFsPreviewEntry(
                    store=store,
                    sku=sku,
                    url=url,
                    title=title,
                    error="",
                    source=source_tag,
                )

    # Fallback: match anywhere in title/description/fields blob
    blob = " ".join(
        [
            str(getattr(embed, "title", "") or ""),
            str(getattr(embed, "description", "") or ""),
            " ".join([str(getattr(f, "name", "") or "") + " " + str(getattr(f, "value", "") or "") for f in fields]),
        ]
    )
    if target and target in clean_sku_text(blob):
        title = str(getattr(embed, "title", "") or "").strip()
        url = str(getattr(embed, "url", "") or "").strip()
        if not url:
            url = first_url_in_text(blob)
        if not title:
            title = url or ""
        return RsFsPreviewEntry(
            store=store,
            sku=sku,
            url=url,
            title=title,
            error="",
            source=source_tag,
        )

    return None

