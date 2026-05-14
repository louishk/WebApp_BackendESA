"""Regression guard: nobody outside common/db.py should be calling
create_engine(get_database_url(...)).

The canonical way to obtain a SQLAlchemy engine or session for any of the
project's Postgres databases is through `common.db`. This test scans the
codebase and fails if a new call site reintroduces the pattern.

Allowlist:
- common/db.py                 — the canonical module itself
- migrations/                  — one-shot scripts run by hand
- scheduler/                   — being decommissioned
- venv/, __pycache__/, build artifacts
"""

from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # repo root
SCAN_ROOTS = [
    REPO_ROOT / "backend" / "python",
    REPO_ROOT / "mcp_esa",
]
ALLOWED_PATHS = {
    "backend/python/common/db.py",
}
ALLOWED_PREFIXES = (
    "backend/python/migrations/",
    "backend/python/scheduler/",   # being decommissioned
    "backend/python/scripts/",     # one-shot CLI probes/reports
    "backend/python/tests/",
    "backend/python/test_",
)
PATTERN = re.compile(
    r"create_engine\s*\(\s*get_database_url\s*\(",
    re.MULTILINE,
)


def _is_allowed(rel: str) -> bool:
    if rel in ALLOWED_PATHS:
        return True
    if any(rel.startswith(p) for p in ALLOWED_PREFIXES):
        return True
    return False


def _iter_py_files():
    for root in SCAN_ROOTS:
        for path in root.rglob("*.py"):
            parts = set(path.parts)
            if "venv" in parts or "__pycache__" in parts or ".pytest_cache" in parts:
                continue
            yield path


def test_no_ad_hoc_create_engine_with_get_database_url():
    offenders = []
    for path in _iter_py_files():
        rel = str(path.relative_to(REPO_ROOT))
        if _is_allowed(rel):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for m in PATTERN.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            offenders.append(f"{rel}:{line_no}")

    assert not offenders, (
        "Ad-hoc engine creation found — use common.db.get_engine() / "
        "get_session() / session_scope() instead:\n  - "
        + "\n  - ".join(offenders)
    )
