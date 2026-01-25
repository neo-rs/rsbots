"""
Kill processes on Windows by matching their command line.

Why this exists:
- Closing a terminal window can leave a Python bot still running.
- This script finds ONLY processes whose CommandLine contains your pattern(s),
  and terminates them.

Safe defaults:
- Dry-run by default (prints matches).
- Requires --yes to actually kill.

Examples:
  # List any running instore bot process:
  .\.venv\Scripts\python.exe scripts\kill_by_cmdline_windows.py --pattern instore_auto_mirror_bot.py

  # Kill it (and its child processes):
  .\.venv\Scripts\python.exe scripts\kill_by_cmdline_windows.py --pattern instore_auto_mirror_bot.py --tree --yes
"""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class Proc:
    pid: int
    ppid: int
    name: str
    cmdline: str


def _ps_json(command: str) -> object:
    """
    Run PowerShell and return parsed JSON.
    """
    ps = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        command,
    ]
    r = subprocess.run(ps, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or "").strip() or f"PowerShell failed (code={r.returncode})")
    import json

    out = (r.stdout or "").strip()
    if not out:
        return []
    return json.loads(out)


def list_processes() -> List[Proc]:
    # Use CIM/WMI to get command lines (tasklist doesn't include them).
    # Emit JSON so parsing is stable.
    data = _ps_json(
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ParentProcessId,Name,CommandLine | "
        "ConvertTo-Json -Depth 2"
    )
    rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
    out: List[Proc] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            pid = int(row.get("ProcessId") or 0)
            ppid = int(row.get("ParentProcessId") or 0)
        except Exception:
            continue
        name = str(row.get("Name") or "").strip()
        cmd = str(row.get("CommandLine") or "").strip()
        if pid > 0:
            out.append(Proc(pid=pid, ppid=ppid, name=name, cmdline=cmd))
    return out


def _descendants(root_pids: Iterable[int], procs: List[Proc]) -> Set[int]:
    children: Dict[int, List[int]] = {}
    for p in procs:
        children.setdefault(p.ppid, []).append(p.pid)
    seen: Set[int] = set()
    stack: List[int] = [int(x) for x in root_pids if int(x) > 0]
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for c in children.get(pid, []):
            if c not in seen:
                stack.append(c)
    return seen


def _ancestor_chain(pid: int, procs: List[Proc]) -> Set[int]:
    """
    Return pid + its parent chain (best-effort) using the snapshot from Win32_Process.
    """
    by_pid: Dict[int, Proc] = {p.pid: p for p in procs}
    seen: Set[int] = set()
    cur = int(pid)
    while cur > 0 and cur not in seen:
        seen.add(cur)
        parent = by_pid.get(cur)
        if not parent:
            break
        cur = int(parent.ppid)
    return seen


def find_matches(patterns: List[str], procs: List[Proc]) -> List[Proc]:
    pats = [(p or "").strip().lower() for p in patterns if (p or "").strip()]
    if not pats:
        return []
    matches: List[Proc] = []
    for p in procs:
        hay = (p.cmdline or "").lower()
        if not hay:
            continue
        if any(pt in hay for pt in pats):
            matches.append(p)
    return matches


def kill_pids(pids: List[int], force: bool) -> Tuple[int, List[str]]:
    if not pids:
        return 0, []
    # Stop-Process is more reliable than taskkill for PowerShell-hosted sessions.
    ids = ",".join(str(int(x)) for x in sorted(set(int(x) for x in pids if int(x) > 0)))
    cmd = f"Stop-Process -Id {ids} {'-Force' if force else ''} -ErrorAction Continue; $LASTEXITCODE"
    ps = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        cmd,
    ]
    r = subprocess.run(ps, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30)
    # Stop-Process doesn't always set exit codes reliably; treat stderr as error signals but continue.
    errs = [ln for ln in (r.stderr or "").splitlines() if ln.strip()]
    return r.returncode, errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--pattern",
        action="append",
        default=[],
        help="Substring to match against process CommandLine. Can be repeated.",
    )
    ap.add_argument("--tree", action="store_true", help="Also kill child processes of matches.")
    ap.add_argument("--force", action="store_true", help="Force kill (Stop-Process -Force).")
    ap.add_argument("--yes", action="store_true", help="Actually kill. Otherwise, dry-run listing only.")
    ap.add_argument(
        "--verify",
        action="store_true",
        help="After killing, re-scan and print any remaining matches.",
    )
    args = ap.parse_args()

    procs = list_processes()
    matches = find_matches(args.pattern, procs)
    if not matches:
        print("No matching processes found.")
        return 0

    # Safety: don't kill ourselves (or the terminal that launched us).
    self_pid = os.getpid()
    parent_chain = _ancestor_chain(self_pid, procs)
    self_markers = {"kill_by_cmdline_windows.py"}

    print(f"Matched {len(matches)} process(es):")
    for p in sorted(matches, key=lambda x: x.pid):
        cmd_short = (p.cmdline or "")[:220].replace("\r", " ").replace("\n", " ")
        excluded = ""
        if p.pid in parent_chain or any(m in (p.cmdline or "").lower() for m in self_markers):
            excluded = " [excluded: self/launcher]"
        print(f"- pid={p.pid} ppid={p.ppid} name={p.name}{excluded} cmd={cmd_short}")

    kill_list = [
        p.pid
        for p in matches
        if p.pid not in parent_chain and not any(m in (p.cmdline or "").lower() for m in self_markers)
    ]
    if args.tree:
        all_pids = _descendants(kill_list, procs)
        kill_list = sorted(all_pids)
        print(f"\nIncluding child processes: total {len(kill_list)} pid(s): {', '.join(map(str, kill_list[:40]))}{' ...' if len(kill_list) > 40 else ''}")

    if not kill_list:
        print("\nNothing left to kill after excluding the current script and its launcher.")
        return 0

    if not args.yes:
        print("\nDry-run only. Re-run with --yes to terminate these processes.")
        return 0

    code, errs = kill_pids(kill_list, force=bool(args.force))
    if errs:
        print("\nWarnings/errors from Stop-Process:")
        for ln in errs[:30]:
            print(f"- {ln}")
    if args.verify:
        procs2 = list_processes()
        matches2 = find_matches(args.pattern, procs2)
        # apply same self/launcher exclusions
        parent_chain2 = _ancestor_chain(os.getpid(), procs2)
        matches2 = [
            p
            for p in matches2
            if p.pid not in parent_chain2 and not any(m in (p.cmdline or "").lower() for m in self_markers)
        ]
        if matches2:
            print("\nVerify: still running:")
            for p in sorted(matches2, key=lambda x: x.pid):
                cmd_short = (p.cmdline or "")[:220].replace("\r", " ").replace("\n", " ")
                print(f"- pid={p.pid} ppid={p.ppid} name={p.name} cmd={cmd_short}")
        else:
            print("\nVerify: no remaining matches.")

    print("\nDone.")
    return 0 if code == 0 else code


if __name__ == "__main__":
    raise SystemExit(main())

