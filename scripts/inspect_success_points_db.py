from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple


DB_PATH = Path("Oraclserver-files/RSuccessBot/success_points.db")


def rows(conn: sqlite3.Connection, sql: str, args: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    cur = conn.cursor()
    cur.execute(sql, args)
    return cur.fetchall()


def table_info(conn: sqlite3.Connection, table: str) -> List[Tuple[Any, ...]]:
    return rows(conn, f"PRAGMA table_info({table})")


def pick_time_column(cols: List[str]) -> str | None:
    candidates = [
        "updated_at",
        "created_at",
        "timestamp",
        "time",
        "date",
        "last_updated",
        "last_update",
    ]
    lower = {c.lower(): c for c in cols}
    for k in candidates:
        if k in lower:
            return lower[k]
    return None


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    print(f"DB: {DB_PATH} ({DB_PATH.stat().st_size} bytes)")
    conn = sqlite3.connect(str(DB_PATH))
    try:
        tables = [t[0] for t in rows(conn, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        print("tables:", tables)
        print()

        # Scan tables for likely "points" / "movement" data
        summaries: List[Dict[str, Any]] = []
        for t in tables:
            cols = [c[1] for c in table_info(conn, t)]
            tcol = pick_time_column(cols)
            summaries.append({"table": t, "cols": cols, "time_col": tcol})

        for s in summaries:
            t = s["table"]
            cols = s["cols"]
            tcol = s["time_col"]
            print(f"[{t}]")
            print("  columns:", ", ".join(cols))
            try:
                count = rows(conn, f"SELECT COUNT(*) FROM {t}")[0][0]
            except Exception as e:
                print("  count: <error>", e)
                print()
                continue
            print("  rows:", count)
            if tcol:
                try:
                    last = rows(conn, f"SELECT {tcol} FROM {t} ORDER BY {tcol} DESC LIMIT 1")
                    if last:
                        print(f"  latest {tcol}:", last[0][0])
                except Exception:
                    pass
            print()

        # Heuristic: pick a "movement/events" table if present, else a "points" table
        preferred = None
        for name in tables:
            ln = name.lower()
            if "movement" in ln or "event" in ln or "history" in ln or "log" in ln:
                preferred = name
                break
        if not preferred:
            for name in tables:
                if "point" in name.lower():
                    preferred = name
                    break

        if preferred:
            cols = [c[1] for c in table_info(conn, preferred)]
            tcol = pick_time_column(cols)
            print(f"=== Last 25 rows from '{preferred}' (most recent first) ===")
            if tcol:
                q = f"SELECT * FROM {preferred} ORDER BY {tcol} DESC LIMIT 25"
            else:
                # fallback
                q = f"SELECT * FROM {preferred} LIMIT 25"
            last_rows = rows(conn, q)
            print("columns:", cols)
            for r in last_rows:
                print(r)
            print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()


