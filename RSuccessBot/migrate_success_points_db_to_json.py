#!/usr/bin/env python3
"""
One-time migration: success_points.db (sqlite) -> success_points.json (new format).

Safety rules:
- Never overwrites an existing success_points.json
- Leaves the DB in place (optionally renamed with .migrated suffix)
- Prints only counts / paths (no user tokens or secrets)
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "success_points.db"
JSON_PATH = BASE / "success_points.json"


def _rows(conn: sqlite3.Connection, sql: str) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def main() -> int:
    if JSON_PATH.exists():
        print(f"[migrate] SKIP: {JSON_PATH} already exists")
        return 0
    if not DB_PATH.exists():
        print(f"[migrate] SKIP: {DB_PATH} not found")
        return 0

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        tables = {r[0] for r in _rows(conn, "SELECT name FROM sqlite_master WHERE type='table'")}
        needed = {"points", "image_hashes", "point_movements"}
        missing = sorted(needed - tables)
        if missing:
            print(f"[migrate] ERROR: DB missing tables: {missing}")
            return 2

        points: Dict[str, Any] = {}
        for r in _rows(conn, "SELECT user_id, points, last_updated FROM points"):
            user_id = str(r["user_id"])
            points[user_id] = {
                "points": int(r["points"] or 0),
                "last_updated": r["last_updated"] or datetime.now(timezone.utc).isoformat(),
            }

        image_hashes: Dict[str, Any] = {}
        for r in _rows(conn, "SELECT hash, user_id, created_at FROM image_hashes"):
            h = str(r["hash"])
            image_hashes[h] = {
                "user_id": int(r["user_id"]),
                "created_at": r["created_at"] or datetime.now(timezone.utc).isoformat(),
            }

        movements: List[Dict[str, Any]] = []
        for r in _rows(
            conn,
            "SELECT user_id, change_amount, old_balance, new_balance, reason, admin_user_id, created_at "
            "FROM point_movements ORDER BY id ASC",
        ):
            movements.append(
                {
                    "user_id": int(r["user_id"]),
                    "change_amount": int(r["change_amount"] or 0),
                    "old_balance": int(r["old_balance"] or 0),
                    "new_balance": int(r["new_balance"] or 0),
                    "reason": r["reason"] or "",
                    "admin_user_id": r["admin_user_id"],
                    "created_at": r["created_at"] or datetime.now(timezone.utc).isoformat(),
                }
            )

        out = {
            "points": points,
            "image_hashes": image_hashes,
            "point_movements": movements,
            "migrated_at": datetime.now(timezone.utc).isoformat(),
            "migrated_from": "success_points.db",
        }

        with open(JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)

        print(f"[migrate] Wrote: {JSON_PATH}")
        print(f"[migrate] points users: {len(points)}")
        print(f"[migrate] image_hashes: {len(image_hashes)}")
        print(f"[migrate] point_movements: {len(movements)}")

        # Keep DB but rename to mark it migrated (best-effort)
        try:
            migrated = DB_PATH.with_suffix(".db.migrated")
            if not migrated.exists():
                DB_PATH.rename(migrated)
                print(f"[migrate] Renamed DB -> {migrated.name}")
        except Exception:
            pass

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())


