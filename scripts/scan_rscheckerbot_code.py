from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


RS_DIR = Path(__file__).resolve().parents[1] / "RSCheckerbot"


@dataclass(frozen=True)
class FnRef:
    file: Path
    qualname: str
    name: str
    lineno: int
    end_lineno: int
    kind: str  # "def" | "async def"
    body_hash: str


def _iter_py_files(root: Path) -> list[Path]:
    files = [p for p in root.glob("*.py") if p.is_file()]
    return sorted(files, key=lambda p: p.name.lower())


def _hash_node(node: ast.AST) -> str:
    # Stable-ish representation across runs (no lineno/col offsets).
    dumped = ast.dump(node, include_attributes=False)
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()[:16]


class _FnCollector(ast.NodeVisitor):
    def __init__(self, file: Path) -> None:
        self.file = file
        self.stack: list[str] = []  # class/function nesting
        self.fns: list[FnRef] = []

    def _push(self, name: str) -> None:
        self.stack.append(name)

    def _pop(self) -> None:
        if self.stack:
            self.stack.pop()

    def _qual(self, name: str) -> str:
        if not self.stack:
            return name
        return ".".join(self.stack + [name])

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self._push(node.name)
        self.generic_visit(node)
        self._pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        q = self._qual(node.name)
        h = _hash_node(node)
        self.fns.append(
            FnRef(
                file=self.file,
                qualname=q,
                name=node.name,
                lineno=int(getattr(node, "lineno", 0) or 0),
                end_lineno=int(getattr(node, "end_lineno", getattr(node, "lineno", 0)) or 0),
                kind="def",
                body_hash=h,
            )
        )
        self._push(node.name)
        self.generic_visit(node)
        self._pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        q = self._qual(node.name)
        h = _hash_node(node)
        self.fns.append(
            FnRef(
                file=self.file,
                qualname=q,
                name=node.name,
                lineno=int(getattr(node, "lineno", 0) or 0),
                end_lineno=int(getattr(node, "end_lineno", getattr(node, "lineno", 0)) or 0),
                kind="async def",
                body_hash=h,
            )
        )
        self._push(node.name)
        self.generic_visit(node)
        self._pop()


def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


def _count_occurrences(haystack: str, needle: str) -> int:
    # Very simple heuristic: count raw substring occurrences.
    if not needle:
        return 0
    return haystack.count(needle)


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _scan_functions(py_files: Iterable[Path]) -> list[FnRef]:
    out: list[FnRef] = []
    for p in py_files:
        src = _read_text(p)
        try:
            tree = ast.parse(src, filename=str(p))
        except SyntaxError as e:
            print(f"[PARSE ERROR] {p.name}: {e}")
            continue
        c = _FnCollector(p)
        c.visit(tree)
        out.extend(c.fns)
    return out


def main() -> int:
    if not RS_DIR.is_dir():
        print(f"ERROR: RSCheckerbot folder not found: {RS_DIR}")
        return 2

    py_files = _iter_py_files(RS_DIR)
    _print_header("RSCheckerbot code scan (duplicates + stagnant heuristics)")
    print(f"Folder: {RS_DIR}")
    print(f"Python files: {len(py_files)}")

    # Load all sources once for heuristics
    sources = {p: _read_text(p) for p in py_files}
    all_text = "\n".join(sources.values())

    fns = _scan_functions(py_files)
    print(f"Functions found: {len(fns)}")

    # 1) Duplicate qualified names within the same file (real bug).
    _print_header("1) Duplicate definitions (same file + same qualified name)")
    dupes: list[tuple[Path, str, list[FnRef]]] = []
    by_file_qual: dict[tuple[Path, str], list[FnRef]] = {}
    for fn in fns:
        by_file_qual.setdefault((fn.file, fn.qualname), []).append(fn)
    for (file, qual), refs in sorted(by_file_qual.items(), key=lambda x: (x[0][0].name.lower(), x[0][1])):
        if len(refs) > 1:
            dupes.append((file, qual, refs))
    if not dupes:
        print("OK: none found")
    else:
        for file, qual, refs in dupes:
            spans = ", ".join(f"L{r.lineno}-L{r.end_lineno}" for r in refs)
            print(f"- {file.name}: {qual} -> {len(refs)} defs ({spans})")

    # 2) Identical bodies across files (candidate duplication).
    _print_header("2) Identical function bodies (by AST hash) across files")
    by_hash: dict[str, list[FnRef]] = {}
    for fn in fns:
        by_hash.setdefault(fn.body_hash, []).append(fn)
    groups = [refs for refs in by_hash.values() if len(refs) > 1]
    groups.sort(key=lambda refs: (-len(refs), refs[0].body_hash))
    if not groups:
        print("OK: none found")
    else:
        shown = 0
        for refs in groups:
            # Skip trivial very-small groups for readability? Keep first 20.
            shown += 1
            print(f"- hash {refs[0].body_hash} -> {len(refs)} occurrences")
            for r in refs[:10]:
                print(f"  - {r.file.name}:{r.qualname} ({r.kind} L{r.lineno})")
            if len(refs) > 10:
                print(f"  ... +{len(refs) - 10} more")
            if shown >= 20:
                print("  (truncated)")
                break

    # 3) Heuristic: maybe-unused module-level functions (definition appears once across RSCheckerbot).
    _print_header("3) Heuristic: maybe-unused module-level functions (name appears once)")
    module_level = [fn for fn in fns if "." not in fn.qualname]
    suspects = []
    for fn in module_level:
        # Count occurrences of "name(" to reduce false hits from words.
        needle = f"{fn.name}("
        c = _count_occurrences(all_text, needle)
        if c <= 1:
            suspects.append((fn, c))
    suspects.sort(key=lambda t: (t[1], t[0].file.name.lower(), t[0].name))
    if not suspects:
        print("OK: none found")
    else:
        for fn, c in suspects[:60]:
            print(f"- {fn.file.name}:{fn.name} ({fn.kind} L{fn.lineno}) -> occurrences of '{fn.name}(' = {c}")
        if len(suspects) > 60:
            print(f"(truncated; {len(suspects)} total suspects)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

