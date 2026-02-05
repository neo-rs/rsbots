from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]


def iter_files(root: Path) -> Iterable[Tuple[str, Path]]:
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(root).as_posix()
        # ignore noisy/runtime
        if "/__pycache__/" in f"/{rel}/":
            continue
        if rel.startswith("logs/"):
            continue
        if rel.endswith((".db", ".sqlite", ".sqlite3", ".pyc")):
            continue
        yield rel, p


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compare(name: str, local_root: Path, remote_root: Path) -> None:
    l: Dict[str, Path] = {rel: p for rel, p in iter_files(local_root)}
    r: Dict[str, Path] = {rel: p for rel, p in iter_files(remote_root)}

    only_l = sorted(set(l) - set(r))
    only_r = sorted(set(r) - set(l))
    common = sorted(set(l) & set(r))

    diff: List[str] = []
    for rel in common:
        # avoid printing potentially sensitive config contents; compare size only
        if rel.endswith(("config.json", "config.secrets.json", ".env")):
            if l[rel].stat().st_size != r[rel].stat().st_size:
                diff.append(rel)
            continue
        if sha256(l[rel]) != sha256(r[rel]):
            diff.append(rel)

    print(f"=== {name} ===")
    print(f"local:  {local_root}")
    print(f"remote: {remote_root}")
    print(f"only local files:  {len(only_l)}")
    print(f"only remote files: {len(only_r)}")
    print(f"different files:   {len(diff)}")
    if only_r[:25]:
        print("remote-only (first 25):")
        for x in only_r[:25]:
            print("  -", x)
    if only_l[:25]:
        print("local-only (first 25):")
        for x in only_l[:25]:
            print("  -", x)
    if diff[:40]:
        print("different (first 40):")
        for x in diff[:40]:
            print("  -", x)
    print()


def find_artifacts(remote_folder: Path) -> List[str]:
    hits: List[str] = []
    for p in remote_folder.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(remote_folder).as_posix()
        if rel.lower().startswith("jacobing/desktop/"):
            hits.append(rel)
    return hits


def main() -> None:
    # Find latest snapshot directory
    snapshot_dir = REPO_ROOT / "Oraclserver-files"
    if snapshot_dir.exists():
        snapshots = sorted([d for d in snapshot_dir.iterdir() if d.is_dir() and d.name.startswith("server_full_snapshot_")], 
                          key=lambda x: x.name, reverse=True)
        if snapshots:
            latest_snapshot = snapshots[0]
            print(f"Using snapshot: {latest_snapshot.name}\n")
            compare("RSOnboarding", REPO_ROOT / "RSOnboarding", latest_snapshot / "RSOnboarding")
            compare("RSuccessBot", REPO_ROOT / "RSuccessBot", latest_snapshot / "RSuccessBot")
        else:
            # Fallback to old structure
            compare("RSOnboarding", REPO_ROOT / "RSOnboarding", REPO_ROOT / "Oraclserver-files" / "RSOnboarding")
            compare("RSuccessBot", REPO_ROOT / "RSuccessBot", REPO_ROOT / "Oraclserver-files" / "RSuccessBot")
    else:
        print("Oraclserver-files directory not found")

    artifacts = []
    for folder in ("RSAdminBot", "RSOnboarding", "RSuccessBot"):
        remote = REPO_ROOT / "Oraclserver-files" / folder
        if remote.exists():
            for rel in find_artifacts(remote):
                artifacts.append(f"{folder}: {rel}")

    if artifacts:
        print("=== Server snapshot artifact paths detected (likely wrong upload paths) ===")
        for line in artifacts[:50]:
            print("-", line)
        if len(artifacts) > 50:
            print(f"(and {len(artifacts) - 50} more)")


if __name__ == "__main__":
    main()


