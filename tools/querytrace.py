#!/usr/bin/env python3
"""
querytrace — for each function in each persistence backend that runs SQL,
print the SQL statements grouped by function name across sqlite / postgres / mysql.

Usage:
    python3 tools/querytrace.py [fn_name ...]
    (no args = every function with DML / CTE queries)
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BACKENDS = [
    ("sqlite",   REPO_ROOT / "src/persistence/persistence_sqlite.rs"),
    ("postgres", REPO_ROOT / "src/persistence/persistence_postgres.rs"),
    ("mysql",    REPO_ROOT / "src/persistence/persistence_mysql.rs"),
]

# rusqlite and sqlx call patterns whose first argument is a SQL string
CALL_RE = re.compile(r"""(?x)
      \.execute_batch \s* \(
    | \.execute       \s* \(
    | \.prepare       \s* \(
    | \.query_row     \s* \(
    | sqlx::query_as  \s* :: \s* < [^>]* > \s* \(
    | sqlx::query     \s* \(
""")

# Only report DML and CTEs (skip DDL / PRAGMA from schema setup)
SQL_LEAD = re.compile(
    r"^\s*(SELECT|INSERT|UPDATE|DELETE|WITH)\b",
    re.IGNORECASE,
)


# ── Rust source helpers ──────────────────────────────────────────────────────

def _skip_str(s: str, i: int) -> int:
    """Advance past a regular double-quoted string; i points at the opening '\"'."""
    i += 1
    while i < len(s):
        if s[i] == "\\":
            i += 2
        elif s[i] == '"':
            return i + 1
        else:
            i += 1
    return i


def _skip_raw(s: str, i: int) -> int:
    """Advance past a raw string r#"..."#; i points at 'r'."""
    i += 1
    n = 0
    while i < len(s) and s[i] == "#":
        n += 1
        i += 1
    if i >= len(s) or s[i] != '"':
        return i
    i += 1
    marker = '"' + "#" * n
    end = s.find(marker, i)
    return end + len(marker) if end != -1 else len(s)


def _brace_end(s: str, start: int) -> int:
    """Return the index just past the '}' matching s[start] == '{'."""
    depth = 0
    i = start
    while i < len(s):
        if s[i:i+2] == "//":
            nl = s.find("\n", i)
            i = nl + 1 if nl != -1 else len(s)
        elif s[i:i+2] == "/*":
            end = s.find("*/", i + 2)
            i = end + 2 if end != -1 else len(s)
        elif s[i] == '"':
            i = _skip_str(s, i)
        elif s[i] == "r" and i + 1 < len(s) and s[i+1] in "#\"":
            i = _skip_raw(s, i)
        elif s[i] == "{":
            depth += 1
            i += 1
        elif s[i] == "}":
            depth -= 1
            i += 1
            if depth == 0:
                return i
        else:
            i += 1
    return i


def parse_functions(src: str) -> list[tuple[str, str, int]]:
    """
    Return [(fn_name, body_text, body_start), ...] for every `fn` in src.
    body_text spans from the opening '{' to the closing '}'.
    """
    results = []
    for m in re.finditer(r"\bfn\s+(\w+)\b", src):
        j = m.end()
        depth_paren = 0
        while j < len(src):
            if src[j:j+2] == "//":
                nl = src.find("\n", j)
                j = nl + 1 if nl != -1 else len(src)
                continue
            if src[j:j+2] == "/*":
                end = src.find("*/", j + 2)
                j = end + 2 if end != -1 else len(src)
                continue
            if src[j] == '"':
                j = _skip_str(src, j)
                continue
            if src[j] == "r" and j + 1 < len(src) and src[j+1] in "#\"":
                j = _skip_raw(src, j)
                continue
            if src[j] == "(":
                depth_paren += 1
            elif src[j] == ")":
                depth_paren -= 1
            elif src[j] == ";" and depth_paren == 0:
                break  # declaration without a body
            elif src[j] == "{" and depth_paren == 0:
                end = _brace_end(src, j)
                results.append((m.group(1), src[j:end], j))
                break
            j += 1
    return results


# ── SQL string extraction ────────────────────────────────────────────────────

def _read_string(s: str, i: int) -> tuple[str, int] | None:
    """
    Extract the first Rust string literal at or after position i.
    Handles regular strings, raw strings, & prefix, and &format!(...) / format!(...).
    Returns (content, end_pos) or None if no string found.
    """
    while i < len(s) and s[i] in " \t\n\r":
        i += 1
    if i >= len(s):
        return None

    # Strip &format!( or format!( wrapper to get at the SQL template
    for pfx in ("&format!", "format!"):
        if s[i:i+len(pfx)] == pfx:
            i += len(pfx)
            while i < len(s) and s[i] in " \t\n\r(":
                i += 1
            break

    # Strip & reference
    if i < len(s) and s[i] == "&":
        i += 1
        while i < len(s) and s[i] in " \t\n\r":
            i += 1

    if i >= len(s):
        return None

    # Raw string r"..." or r#"..."#
    if s[i] == "r" and i + 1 < len(s) and s[i+1] in "#\"":
        i += 1
        n = 0
        while i < len(s) and s[i] == "#":
            n += 1
            i += 1
        if i >= len(s) or s[i] != '"':
            return None
        i += 1
        marker = '"' + "#" * n
        end = s.find(marker, i)
        if end == -1:
            return None
        return s[i:end], end + len(marker)

    # Regular double-quoted string
    if s[i] != '"':
        return None
    i += 1
    parts: list[str] = []
    ESC = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\"}
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            parts.append(ESC.get(s[i+1], s[i+1]))
            i += 2
        elif c == '"':
            return "".join(parts), i + 1
        else:
            parts.append(c)
            i += 1
    return None


def _line_offsets(src: str) -> list[int]:
    """Character offset of the start of each line (0-indexed list, line N starts at offsets[N])."""
    offsets = [0]
    for i, c in enumerate(src):
        if c == "\n":
            offsets.append(i + 1)
    return offsets


def _line_no(offsets: list[int], pos: int) -> int:
    """1-based line number for character position pos."""
    lo, hi = 0, len(offsets) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if offsets[mid] <= pos:
            lo = mid
        else:
            hi = mid - 1
    return lo + 1


_SQL_COMMENT = re.compile(r"--[^\n]*")


def _normalize_sql(raw: str) -> str:
    """Strip SQL line comments, then collapse whitespace to a single line."""
    return " ".join(_SQL_COMMENT.sub("", raw).split())


def scan_queries(body: str, body_start: int, offsets: list[int]) -> list[tuple[str, int]]:
    """Return [(normalized_sql, line_no), ...] for every DML/CTE call in body."""
    results = []
    for m in CALL_RE.finditer(body):
        r = _read_string(body, m.end())
        if r is None:
            continue
        sql = _normalize_sql(r[0])
        if not SQL_LEAD.match(sql):
            continue
        line = _line_no(offsets, body_start + m.start())
        results.append((sql, line))
    return results


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    filters = set(sys.argv[1:])

    # {fn_name: {backend: [(sql, line_no), ...]}}
    data: dict[str, dict[str, list[tuple[str, int]]]] = defaultdict(dict)
    fn_order: list[str] = []
    seen_fns: set[str] = set()

    for backend, path in BACKENDS:
        if not path.exists():
            continue
        src = path.read_text()
        offsets = _line_offsets(src)
        for fn_name, body, body_start in parse_functions(src):
            if filters and fn_name not in filters:
                continue
            queries = scan_queries(body, body_start, offsets)
            if not queries:
                continue
            if fn_name not in seen_fns:
                fn_order.append(fn_name)
                seen_fns.add(fn_name)
            bq = data[fn_name].setdefault(backend, [])
            seen_sql = {q for q, _ in bq}
            for q, ln in queries:
                if q not in seen_sql:
                    bq.append((q, ln))
                    seen_sql.add(q)

    if not data:
        print("(no SQL queries found)")
        return

    for idx, fn_name in enumerate(fn_order):
        if idx:
            print()
        print(f"=== {fn_name} ===")
        for backend, path in BACKENDS:
            if backend not in data[fn_name]:
                continue
            rel = path.relative_to(REPO_ROOT)
            print(f"  -- {backend} --")
            for i, (sql, ln) in enumerate(data[fn_name][backend], 1):
                print(f"    [{i}] {sql}")
                print(f"        {rel}:{ln}")


if __name__ == "__main__":
    main()
