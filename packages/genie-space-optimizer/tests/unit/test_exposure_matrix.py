"""Pin the write-to-read exposure matrix to the live MV DDL (Prompt 14).

Twice on this branch a "wire to existing endpoints" instruction assumed a read
the writing side never exposed — the space-scoped proposals read (Prompt 11) and
the created-objects/lift read (Prompt 13). Both were found by a PLAN phase, which
is one prompt too late. ``docs/design/mv-advisor-exposure-matrix.md`` records, for
every MV column, whether a route SERVES it, whether it is DELIBERATELY INTERNAL
(with a reason), or whether it is a GAP. This test walks the authoritative column
lists in ``optimization/ddl.py`` and fails if a column is missing from the matrix
or carries no classification — so a new column added without classifying its
exposure fails here, which is the moment that class of defect is actually caught.

The DDL is the source of truth, not the matrix: this walks the ``CREATE TABLE``
bodies (via the same regex ``test_mv_state`` uses) plus ``ADDITIVE_COLUMN_MIGRATIONS``.
Scope is "MV columns wherever they live" — the three ``genie_opt_mv_*`` tables plus
the single MV column on ``genie_opt_runs`` (``run_kind``, MV-D23).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from genie_space_optimizer.common.config import (
    TABLE_MV_CANDIDATES,
    TABLE_MV_CONSENTS,
    TABLE_MV_CREATED_OBJECTS,
    TABLE_RUNS,
)
from genie_space_optimizer.optimization.ddl import (
    _ALL_DDL,
    ADDITIVE_COLUMN_MIGRATIONS,
)

# tests/unit/<file> -> parents[4] is the repo root (matches test_gap_report_counts).
REPO_ROOT = Path(__file__).resolve().parents[4]
MATRIX = REPO_ROOT / "docs" / "design" / "mv-advisor-exposure-matrix.md"

_LEGEND = {"SERVED", "DELIBERATELY INTERNAL", "GAP"}

# Same walker test_mv_state.py uses for the fake's INSERT-column check.
_DDL_COLUMN_RE = re.compile(
    r"^\s{4}(\w+)\s+(?:STRING|INT|DOUBLE|BOOLEAN|TIMESTAMP)\b", re.MULTILINE,
)

# The three MV tables, in full. run_kind is the only MV column outside them.
_MV_TABLES = (TABLE_MV_CANDIDATES, TABLE_MV_CONSENTS, TABLE_MV_CREATED_OBJECTS)
_EXTRA_MV_COLUMNS = ((TABLE_RUNS, "run_kind"),)


def _ddl_columns(table: str) -> list[str]:
    columns = _DDL_COLUMN_RE.findall(_ALL_DDL[table])
    columns.extend(col for tbl, col, _decl in ADDITIVE_COLUMN_MIGRATIONS if tbl == table)
    return columns


def _mv_column_universe() -> set[tuple[str, str]]:
    universe: set[tuple[str, str]] = set()
    for table in _MV_TABLES:
        for col in _ddl_columns(table):
            universe.add((table, col))
    universe.update(_EXTRA_MV_COLUMNS)
    return universe


def _parse_matrix() -> dict[tuple[str, str], str]:
    """Return ``{(table, column): exposure}`` from the markdown matrix tables.

    Only rows whose first cell is one of the tables we walk are collected, so
    the legend table (first cell ``Token``) and prose are ignored.
    """
    tables = {t for t, _ in _mv_column_universe()}
    rows: dict[tuple[str, str], str] = {}
    for line in MATRIX.read_text(encoding="utf-8").splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        table, column, exposure = cells[0], cells[1], cells[2]
        if table not in tables:
            continue
        rows[(table, column)] = exposure
    return rows


@pytest.fixture(scope="module")
def matrix() -> dict[tuple[str, str], str]:
    if not MATRIX.is_file():
        pytest.fail(f"{MATRIX} is missing — the exposure matrix is the pinned artifact")
    parsed = _parse_matrix()
    # Positive control: a renamed/empty matrix must fail loudly, not vacuously
    # pass because nothing parsed.
    assert parsed, f"parsed zero MV rows from {MATRIX} — the table shape changed"
    return parsed


def test_every_mv_column_is_classified(matrix: dict[tuple[str, str], str]) -> None:
    missing: list[str] = []
    empty: list[str] = []
    for table, column in sorted(_mv_column_universe()):
        exposure = matrix.get((table, column))
        if exposure is None:
            missing.append(f"{table}.{column}")
        elif exposure not in _LEGEND:
            empty.append(f"{table}.{column} -> {exposure!r}")
    assert not missing, (
        "MV columns absent from docs/design/mv-advisor-exposure-matrix.md "
        "(classify each SERVED / DELIBERATELY INTERNAL / GAP):\n  "
        + "\n  ".join(missing)
    )
    assert not empty, (
        "MV columns with a non-legend classification (must be one of "
        f"{sorted(_LEGEND)}):\n  " + "\n  ".join(empty)
    )


def test_matrix_has_no_row_for_a_dropped_column(matrix: dict[tuple[str, str], str]) -> None:
    """The reverse rot: a matrix row for a column the DDL no longer declares."""
    universe = _mv_column_universe()
    stale = sorted(
        f"{table}.{column}"
        for (table, column) in matrix
        if (table, column) not in universe
    )
    assert not stale, (
        "exposure matrix rows for columns not in the live DDL (renamed or "
        "removed?):\n  " + "\n  ".join(stale)
    )
