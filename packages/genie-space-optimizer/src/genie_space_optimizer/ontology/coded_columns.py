"""Bounded coded-column detection for L5 Page mining (MV-D63).

This is the **pure** half of the coded-column batch feed: a deterministic,
metadata-only prefilter + candidate selection + ``ColumnSignal`` assembly. The
job reader (``jobs/run_ontology_materialize.py`` ``SparkSystemTableReader.
coded_column_signals``) does the warehouse I/O — it builds the profiling
statements by REUSING ``optimization.wide_schema_profile`` builders
(``approx_count_distinct`` + ``_value_list_item``) and executes them as the job
``run_as`` identity (MV-D50) — and calls :func:`coded_column_signals` here with an
injected per-candidate ``profiler``. Factoring the decision logic into the wheel
keeps it importable + unit-testable without a cluster (the notebook reader runs
on import), exactly as ``schema_signals`` does for the structural signals.

Design discipline (spec §3.1, honoring MV-D43 / D49 / D57):

  * **Bounded cost.** The prefilter runs on metadata only (no ``SELECT``); at most
    ``max_columns`` candidates/run are ever profiled, in deterministic sorted-FQN
    order; the value list is truncated to :data:`MAX_DISTINCT_VALUES`.
  * **Degrade, never block (MV-D43).** ``warehouse_id == ""`` ⇒ ``[]`` (same posture
    as measures); ANY failure at any step ⇒ ``[]`` — the run still succeeds.
  * **Reuse, do not fork.** The profiling SQL is built by ``wide_schema_profile``
    (reader side); this module invents no comparator and adds no DDL/column —
    ``distinct_values`` / ``governed`` ride the existing ``ColumnSignal`` (no new
    table, MV-D49).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from genie_space_optimizer.ontology.pages import _TAXONOMY_MAX_CARDINALITY, ColumnSignal

# ── Config defaults (MV-D57; the job threads overrides, in-code defaults here) ──
# Hard per-run cap on how many candidates are value-profiled.
CODED_COLUMN_MAX_COLUMNS = 300
# A column is a [Taxonomy] candidate only up to this many distinct values — aligned
# with the miner's own ceiling (a coded list, not free text). Detectors re-apply the
# same ``pages._TAXONOMY_MAX_CARDINALITY`` ceiling, so a higher config would emit
# signals the detectors then drop; keep the two aligned.
CODED_COLUMN_MAX_CARDINALITY = _TAXONOMY_MAX_CARDINALITY
# Value-list ceiling collected per column (the miner shows ~12; a small headroom).
MAX_DISTINCT_VALUES = 24

# Coded-eligible column types: STRING/CHAR/small-INT (spec §3.1). Free-text STRING is
# still admitted by type — the name/comment heuristic is what separates a coded STRING
# from prose, and the cardinality gate drops anything that profiles wide.
_CODED_TYPES = frozenset({"string", "char", "varchar", "tinyint", "smallint", "int", "integer"})

# Name looks coded: one of these as a suffix (``order_type``) or the whole word
# (``code``) — ``_code|_status|_type|_category|_flag|_cd|_ind`` (spec §3.1).
_CODED_NAME_RE = re.compile(r"(?:^|_)(?:code|status|type|category|flag|cd|ind)$", re.IGNORECASE)


def _dtype_base(value: Any) -> str:
    """The base type name (``varchar(10)`` → ``varchar``) — mirrors
    ``wide_schema_profile._dtype_base`` so the type test reads identically."""
    return str(value or "").casefold().split("(", 1)[0].strip()


def _enum_like_comment(comment: str) -> bool:
    """Whether a COMMENT reads as an enum/code list: the phrases ``one of`` /
    ``values:`` / ``codes``, or a comma list of ≥3 short tokens (spec §3.1)."""
    c = (comment or "").strip().lower()
    if not c:
        return False
    if "one of" in c or "values:" in c or "codes" in c:
        return True
    parts = [p.strip() for p in c.split(",") if p.strip()]
    return len(parts) >= 3


def is_coded_candidate(name: str, data_type: str, comment: str = "") -> bool:
    """The metadata-only prefilter (spec §3.1 pass 1): type is STRING/CHAR/small-INT
    AND it looks coded (name matches the coded pattern OR the comment reads enum-like).
    Purely deterministic — no warehouse read."""
    if not name:
        return False
    if _dtype_base(data_type) not in _CODED_TYPES:
        return False
    return bool(_CODED_NAME_RE.search(str(name)) or _enum_like_comment(comment))


@dataclass(frozen=True)
class CandidateColumn:
    """A prefilter-passing coded-column candidate (before profiling)."""

    table_fqn: str                      # catalog.schema.table
    column: str
    comment: str = ""
    data_type: str = ""

    @property
    def ref(self) -> str:
        return f"{self.table_fqn}.{self.column}"

    @property
    def domain_id(self) -> str:
        """Schema-home provenance (``catalog.schema``) — never a page key; the Page's
        real home is source-majority attachment (``pages._resolve_home_domain``)."""
        return ".".join(self.table_fqn.split(".")[:2])


def select_candidates(
    col_rows: Sequence[Mapping[str, Any]], *, max_columns: int = CODED_COLUMN_MAX_COLUMNS,
) -> list[CandidateColumn]:
    """Prefilter + de-dupe + deterministic cap (spec §3.1 pass 1 → pass 2 gating).

    ``col_rows`` are ``information_schema.columns`` rows (``table_catalog`` /
    ``table_schema`` / ``table_name`` / ``column_name`` / ``data_type`` / ``comment``).
    Keeps prefilter-passing columns, sorts by FQN (deterministic order), and returns at
    most ``max_columns`` — the hard per-run cap before any ``SELECT`` is issued."""
    seen: set[str] = set()
    cands: list[CandidateColumn] = []
    for r in col_rows:
        cat, sch, tbl = r.get("table_catalog"), r.get("table_schema"), r.get("table_name")
        col = r.get("column_name")
        if not (cat and sch and tbl and col):
            continue
        dtype = str(r.get("data_type") or "")
        comment = str(r.get("comment") or "")
        if not is_coded_candidate(str(col), dtype, comment):
            continue
        cand = CandidateColumn(table_fqn=f"{cat}.{sch}.{tbl}", column=str(col), comment=comment, data_type=dtype)
        if cand.ref in seen:
            continue
        seen.add(cand.ref)
        cands.append(cand)
    cands.sort(key=lambda c: c.ref)
    return cands[:max_columns]


# A profiler: candidate → ``(distinct_count, values)``. ``distinct_count`` is None when
# the column could not be profiled (dropped). May raise — the caller degrades to [].
Profiler = Callable[[CandidateColumn], "tuple[int | None, Sequence[str]]"]


def build_column_signals(
    candidates: Sequence[CandidateColumn],
    *,
    profiler: Profiler,
    governed_refs: Sequence[str] = (),
    max_cardinality: int = CODED_COLUMN_MAX_CARDINALITY,
    max_values: int = MAX_DISTINCT_VALUES,
) -> list[ColumnSignal]:
    """Profile each candidate (pass 2) and assemble ``ColumnSignal``s. Keeps a column
    iff its profiled distinct count is within ``max_cardinality`` (a code list, not free
    text); collects up to ``max_values`` distinct values (sorted → deterministic).
    ``governed`` is True when the column ref is in ``governed_refs`` (a governed tag or a
    CHECK-constraint enum, computed by the reader). The ``profiler`` may raise; this
    function lets it propagate so :func:`coded_column_signals` can degrade the whole read
    to ``[]`` (spec §3.1: any failure ⇒ [])."""
    governed = {str(g) for g in governed_refs}
    out: list[ColumnSignal] = []
    for cand in candidates:
        distinct, values = profiler(cand)
        if distinct is None or distinct > max_cardinality:
            continue
        vals = tuple(sorted({str(v) for v in (values or [])}))[:max_values]
        out.append(ColumnSignal(
            table_fqn=cand.table_fqn, column=cand.column, comment=cand.comment,
            distinct_values=vals, governed=(cand.ref in governed),
            agent_fqns=(), domain_id=cand.domain_id,
        ))
    return out


def coded_column_signals(
    col_rows: Sequence[Mapping[str, Any]],
    *,
    warehouse_id: str,
    profiler: Profiler,
    governed_refs: Sequence[str] = (),
    max_columns: int = CODED_COLUMN_MAX_COLUMNS,
    max_cardinality: int = CODED_COLUMN_MAX_CARDINALITY,
) -> list[ColumnSignal]:
    """The bounded two-pass coded-column read (MV-D63), factored pure for offline test.

    ``warehouse_id == ""`` ⇒ ``[]`` — no profiling without a warehouse (same posture as
    measures). Otherwise prefilter + cap (pass 1), then profile the survivors (pass 2)
    via the injected ``profiler``. ANY failure at any step degrades to ``[]`` (MV-D43) —
    the run never fails on a coded-column read."""
    if not warehouse_id:
        return []
    try:
        candidates = select_candidates(col_rows, max_columns=max_columns)
        return build_column_signals(
            candidates, profiler=profiler, governed_refs=governed_refs, max_cardinality=max_cardinality,
        )
    except Exception:  # noqa: BLE001 — degrade to [], never fail the run
        return []


__all__ = [
    "CODED_COLUMN_MAX_CARDINALITY",
    "CODED_COLUMN_MAX_COLUMNS",
    "MAX_DISTINCT_VALUES",
    "CandidateColumn",
    "build_column_signals",
    "coded_column_signals",
    "is_coded_candidate",
    "select_candidates",
]
