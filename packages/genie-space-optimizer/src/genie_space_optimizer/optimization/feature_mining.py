"""Reactive SQL feature mining (Task 6 of the lever-loop plan).

The retail run kept missing failures like Q011's missing
``GROUP BY YEAR(date_key_2)`` and Q009's wrong measure column because
the typed AST diff was dead code: ``afs.compute_ast_diff`` existed but
no caller populated ``cluster["_sql_pairs_for_ast_diff"]``. This
module fills that gap.

Public surface:

* ``mine_sql_features(sql) -> SqlFeatures``: deterministic, sqlglot-
  based identifier projection. Never carries raw SQL text.
* ``compute_diff(genie, ground_truth) -> SqlDiff``: typed delta with a
  ``primary_kind`` of ``DiffKind`` and a ``candidate_levers`` ordering.
* ``reactive_patches_from_diff(diff, *, table_id, metadata_snapshot,
  leakage_oracle) -> list[dict]``: typed patch templates the
  strategist may pick from. Honors the dedup contract.
* ``apply_dedup_contract(candidates, metadata_snapshot, leakage_oracle)
  -> list[dict]``: filters candidates against existing artifacts
  (Semantic Consistency Rule 6).

Helpers (``_is_metric_view_measure`` etc.) are pure functions over the
metadata snapshot — no LLM, no remote calls. The module is leaf-level
(no imports from ``harness``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# ── Typed dataclasses ────────────────────────────────────────


class DiffKind(str, Enum):
    """Primary kind of structural difference between Genie and GT SQL.

    The ``candidate_levers`` returned by ``compute_diff`` follows from
    this label. Test cases pin the routing so future contributors do
    not silently widen the lever set.
    """

    MEASURE_SWAP = "measure_swap"
    MISSING_GROUPBY_COL = "missing_groupby_col"
    EXTRA_GROUPBY_COL = "extra_groupby_col"
    MISSING_FILTER = "missing_filter"
    EXTRA_FILTER = "extra_filter"
    TVF_REIMPLEMENTATION = "tvf_reimplementation"
    WRONG_AGGREGATION_FUNCTION = "wrong_aggregation_function"
    MISSING_JOIN_SPEC = "missing_join_spec"
    WRONG_JOIN_TYPE = "wrong_join_type"
    COLUMN_DISAMBIGUATION = "column_disambiguation"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SqlFeatures:
    """Identifier-only projection of a SQL statement.

    Every field is a tuple of strings or string-tuples. Raw SQL text
    is not retained — the dedup contract and leakage firewall both
    rely on this projection NEVER carrying user-visible benchmark
    text into prompts.
    """

    select_cols: tuple[str, ...] = ()
    group_by_cols: tuple[str, ...] = ()
    filter_cols: tuple[str, ...] = ()
    measures: tuple[str, ...] = ()
    aggregation_funcs: tuple[str, ...] = ()
    join_specs: tuple[tuple[str, str, str], ...] = ()  # (left, kind, right)
    tvf_calls: tuple[str, ...] = ()


@dataclass(frozen=True)
class DiffSet:
    """One side of a ``SqlDiff`` (missing in genie or extra in genie)."""

    select_cols: tuple[str, ...] = ()
    group_by_cols: tuple[str, ...] = ()
    filter_cols: tuple[str, ...] = ()
    measures: tuple[str, ...] = ()
    aggregation_funcs: tuple[str, ...] = ()


@dataclass(frozen=True)
class SqlDiff:
    primary_kind: DiffKind
    missing_in_genie: DiffSet = field(default_factory=DiffSet)
    extra_in_genie: DiffSet = field(default_factory=DiffSet)
    candidate_levers: tuple[int, ...] = ()


# ── mine_sql_features ────────────────────────────────────────


_EMPTY_FEATURES = SqlFeatures()


def mine_sql_features(sql: str) -> SqlFeatures:
    """Parse ``sql`` with sqlglot and return a typed feature record.

    Returns an empty ``SqlFeatures`` on parse failure (the diff
    classifier handles that as ``DiffKind.UNKNOWN``). Never raises.
    """
    if not isinstance(sql, str) or not sql.strip():
        return _EMPTY_FEATURES
    try:
        import sqlglot
        from sqlglot import exp as _exp  # noqa: N812
    except Exception:
        logger.debug("sqlglot unavailable; mining returns empty features")
        return _EMPTY_FEATURES

    try:
        parsed = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return _EMPTY_FEATURES
    if parsed is None:
        return _EMPTY_FEATURES

    select_cols: list[str] = []
    try:
        for col in parsed.find_all(_exp.Column):
            name = getattr(col, "alias_or_name", None) or getattr(col, "name", None)
            if name:
                select_cols.append(str(name))
    except Exception:
        pass

    group_by_cols: list[str] = []
    try:
        for grp in parsed.find_all(_exp.Group):
            for expr in (grp.expressions or []):
                try:
                    group_by_cols.append(expr.sql(dialect="databricks"))
                except Exception:
                    name = getattr(expr, "name", None) or str(expr)
                    if name:
                        group_by_cols.append(str(name))
    except Exception:
        pass

    filter_cols: list[str] = []
    try:
        for where in parsed.find_all(_exp.Where):
            for col in where.find_all(_exp.Column):
                name = getattr(col, "name", None)
                if name:
                    filter_cols.append(str(name))
    except Exception:
        pass

    aggregation_funcs: list[str] = []
    measures: list[str] = []
    try:
        for agg in parsed.find_all(_exp.AggFunc):
            try:
                fn_name = agg.sql_name()
                if fn_name:
                    aggregation_funcs.append(str(fn_name).upper())
            except Exception:
                pass
            this = getattr(agg, "this", None)
            if this is not None:
                try:
                    measures.append(this.sql(dialect="databricks"))
                except Exception:
                    name = getattr(this, "name", None) or str(this)
                    if name:
                        measures.append(str(name))
    except Exception:
        pass

    # Catch databricks ``MEASURE(...)`` which sqlglot may classify as a
    # generic Anonymous function rather than an AggFunc. We do this in
    # a second pass so the AggFunc loop above stays canonical.
    try:
        for fn in parsed.find_all(_exp.Anonymous):
            fn_name = getattr(fn, "this", None) or getattr(fn, "name", None)
            if not fn_name:
                continue
            fn_name_str = str(fn_name).upper()
            if fn_name_str == "MEASURE":
                aggregation_funcs.append("MEASURE")
                args = list(getattr(fn, "expressions", None) or [])
                if args:
                    try:
                        measures.append(args[0].sql(dialect="databricks"))
                    except Exception:
                        pass
    except Exception:
        pass

    join_specs: list[tuple[str, str, str]] = []
    try:
        for j in parsed.find_all(_exp.Join):
            kind_raw = (j.args.get("kind") or "INNER")
            kind = (
                kind_raw.upper() if isinstance(kind_raw, str)
                else str(kind_raw).upper() if kind_raw else "INNER"
            )
            this = j.this
            on = j.args.get("on")
            left = ""
            right = ""
            if this is not None:
                try:
                    right = this.sql(dialect="databricks")
                except Exception:
                    right = str(getattr(this, "name", "") or "")
            if on is not None:
                try:
                    left = on.sql(dialect="databricks")
                except Exception:
                    left = ""
            join_specs.append((left, kind, right))
    except Exception:
        pass

    tvf_calls: list[str] = []
    try:
        # Anonymous function calls that are NOT MEASURE/aggregation.
        for fn in parsed.find_all(_exp.Anonymous):
            fn_name = getattr(fn, "this", None) or getattr(fn, "name", None)
            if not fn_name:
                continue
            fn_name_str = str(fn_name).lower()
            if fn_name_str.upper() == "MEASURE":
                continue
            tvf_calls.append(fn_name_str)
    except Exception:
        pass

    return SqlFeatures(
        select_cols=tuple(select_cols),
        group_by_cols=tuple(group_by_cols),
        filter_cols=tuple(filter_cols),
        measures=tuple(measures),
        aggregation_funcs=tuple(aggregation_funcs),
        join_specs=tuple(join_specs),
        tvf_calls=tuple(tvf_calls),
    )


# ── compute_diff ────────────────────────────────────────────


def _diff(genie: tuple[str, ...], gt: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return ``(missing_in_genie, extra_in_genie)`` set diff."""
    g, t = set(genie), set(gt)
    return tuple(sorted(t - g)), tuple(sorted(g - t))


def compute_diff(*, genie: SqlFeatures, ground_truth: SqlFeatures) -> SqlDiff:
    """Classify the structural delta between ``genie`` and ``ground_truth``.

    The branching follows the routing in ``lever-loop-pipeline.md``:
    measure swaps go to Lever 1 (column metadata); missing GROUP BY
    columns go to Lever 4 (example SQL) plus Lever 1 (column hint);
    speculative filters go to Lever 3 (instruction); etc.
    """
    missing_gb, extra_gb = _diff(genie.group_by_cols, ground_truth.group_by_cols)
    missing_f, extra_f = _diff(genie.filter_cols, ground_truth.filter_cols)
    missing_m, extra_m = _diff(genie.measures, ground_truth.measures)
    missing_a, extra_a = _diff(genie.aggregation_funcs, ground_truth.aggregation_funcs)

    if missing_m and extra_m:
        kind = DiffKind.MEASURE_SWAP
        levers: tuple[int, ...] = (1,)
    elif missing_gb and not extra_gb:
        kind = DiffKind.MISSING_GROUPBY_COL
        levers = (4, 1)
    elif extra_gb and not missing_gb:
        kind = DiffKind.EXTRA_GROUPBY_COL
        levers = (3,)
    elif missing_f and not extra_f:
        kind = DiffKind.MISSING_FILTER
        levers = (3, 4)
    elif extra_f and not missing_f:
        kind = DiffKind.EXTRA_FILTER
        levers = (3,)
    elif missing_a or extra_a:
        kind = DiffKind.WRONG_AGGREGATION_FUNCTION
        levers = (1, 4)
    else:
        kind = DiffKind.UNKNOWN
        levers = ()

    return SqlDiff(
        primary_kind=kind,
        missing_in_genie=DiffSet(
            select_cols=tuple(),
            group_by_cols=missing_gb,
            filter_cols=missing_f,
            measures=missing_m,
            aggregation_funcs=missing_a,
        ),
        extra_in_genie=DiffSet(
            select_cols=tuple(),
            group_by_cols=extra_gb,
            filter_cols=extra_f,
            measures=extra_m,
            aggregation_funcs=extra_a,
        ),
        candidate_levers=levers,
    )


# ── Helper predicates for the dedup contract ─────────────────


def _iter_metric_view_measures(metadata_snapshot: dict) -> Iterable[tuple[str, str]]:
    """Yield ``(metric_view_name, measure_name)`` pairs from the snapshot.

    Tolerates the snapshot's flexible shape — metric views may live
    under ``metric_views`` (top-level) or under
    ``data_sources.metric_views``.
    """
    for key in ("metric_views", "metric_view"):
        mvs = metadata_snapshot.get(key) or []
        if isinstance(mvs, list):
            for mv in mvs:
                if not isinstance(mv, dict):
                    continue
                name = str(mv.get("name") or mv.get("identifier") or "")
                for m in mv.get("measures") or []:
                    if isinstance(m, dict):
                        mn = str(m.get("name") or "")
                        if mn:
                            yield (name, mn)
                    elif isinstance(m, str):
                        yield (name, m)
    ds = metadata_snapshot.get("data_sources") or {}
    if isinstance(ds, dict):
        for mv in ds.get("metric_views") or []:
            if not isinstance(mv, dict):
                continue
            name = str(mv.get("name") or mv.get("identifier") or "")
            for m in mv.get("measures") or []:
                if isinstance(m, dict):
                    mn = str(m.get("name") or "")
                    if mn:
                        yield (name, mn)


def _is_metric_view_measure(measure: str | None, metadata_snapshot: dict) -> bool:
    """True iff the (case-insensitive) measure name is declared in any
    metric view's ``measures`` list."""
    if not measure:
        return False
    target = measure.lower().strip()
    for _mv_name, mn in _iter_metric_view_measures(metadata_snapshot):
        if mn.lower().strip() == target:
            return True
    return False


def _owning_metric_view(measure: str | None, metadata_snapshot: dict) -> str | None:
    """Return the metric view that owns ``measure``, or ``None``."""
    if not measure:
        return None
    target = measure.lower().strip()
    for mv_name, mn in _iter_metric_view_measures(metadata_snapshot):
        if mn.lower().strip() == target:
            return mv_name or None
    return None


def _existing_snippet_names(metadata_snapshot: dict) -> set[str]:
    out: set[str] = set()
    inst = metadata_snapshot.get("instructions") or {}
    if isinstance(inst, dict):
        snippets = inst.get("sql_snippets") or []
        if isinstance(snippets, list):
            for s in snippets:
                if isinstance(s, dict):
                    name = s.get("name")
                    if name:
                        out.add(str(name).lower().strip())
    return out


def _snippet_name_collides(name: str | None, metadata_snapshot: dict) -> bool:
    if not name:
        return False
    return name.lower().strip() in _existing_snippet_names(metadata_snapshot)


def _existing_join_specs(metadata_snapshot: dict) -> list[tuple[str, str, frozenset[str]]]:
    """Return ``(left, right, on_columns)`` tuples for existing join_specs."""
    out: list[tuple[str, str, frozenset[str]]] = []
    inst = metadata_snapshot.get("instructions") or {}
    specs = inst.get("join_specs") or []
    if not isinstance(specs, list):
        return out
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        left_obj = spec.get("left", {})
        right_obj = spec.get("right", {})
        if isinstance(left_obj, dict):
            left = str(left_obj.get("identifier") or left_obj.get("name") or "").lower()
        else:
            left = str(left_obj or "").lower()
        if isinstance(right_obj, dict):
            right = str(right_obj.get("identifier") or right_obj.get("name") or "").lower()
        else:
            right = str(right_obj or "").lower()
        on = spec.get("on") or spec.get("on_columns") or []
        on_set = frozenset(str(c).lower() for c in on) if isinstance(on, list) else frozenset()
        if left and right:
            out.append((left, right, on_set))
    return out


def _join_spec_already_exists(candidate: dict, metadata_snapshot: dict) -> bool:
    left = str(
        candidate.get("left_table")
        or candidate.get("left")
        or "",
    ).lower().strip()
    right = str(
        candidate.get("right_table")
        or candidate.get("right")
        or "",
    ).lower().strip()
    on = candidate.get("on_columns") or candidate.get("on") or []
    on_set = frozenset(str(c).lower() for c in on) if isinstance(on, list) else frozenset()
    if not left or not right:
        return False
    for ex_left, ex_right, ex_on in _existing_join_specs(metadata_snapshot):
        # Compare unordered table pairs since join direction is
        # immaterial for dedup.
        same_pair = {ex_left, ex_right} == {left, right}
        if not same_pair:
            continue
        # If the candidate did not specify on_columns, the table pair
        # match alone is enough.
        if not on_set:
            return True
        if not ex_on:
            return True
        if on_set == ex_on:
            return True
    return False


def _existing_column_description(
    column: str, metadata_snapshot: dict,
) -> str | None:
    """Find the first description for ``column`` across tables and metric views."""
    target = column.lower().strip()
    for tbl in metadata_snapshot.get("tables") or []:
        if not isinstance(tbl, dict):
            continue
        for col in tbl.get("columns") or []:
            if not isinstance(col, dict):
                continue
            if str(col.get("name") or "").lower().strip() == target:
                desc = col.get("description")
                if isinstance(desc, str) and desc:
                    return desc
    # Metric view measure descriptions
    for mv in (metadata_snapshot.get("metric_views") or []):
        if not isinstance(mv, dict):
            continue
        for m in mv.get("measures") or []:
            if isinstance(m, dict) and str(m.get("name") or "").lower().strip() == target:
                desc = m.get("description")
                if isinstance(desc, str) and desc:
                    return desc
    return None


def _description_already_conveys(
    candidate: dict, metadata_snapshot: dict, *, jaccard_threshold: float = 0.6,
) -> bool:
    """True when the candidate's proposed description is already
    substantively present.

    Two checks:
      1. Substring containment (case-insensitive) — fast and exact.
      2. Token-set Jaccard ≥ ``jaccard_threshold`` — catches phrasings
         that differ in word order or filler words.
    """
    column = str(candidate.get("column") or "").strip()
    proposed = str(
        candidate.get("proposed_description")
        or candidate.get("description")
        or "",
    ).strip()
    if not column or not proposed:
        return False
    existing = _existing_column_description(column, metadata_snapshot)
    if not existing:
        return False
    proposed_l = proposed.lower()
    existing_l = existing.lower()
    if proposed_l in existing_l:
        return True

    def _tokens(s: str) -> set[str]:
        return {t for t in s.lower().replace(",", " ").replace(".", " ").split() if t}

    a, b = _tokens(proposed), _tokens(existing)
    if not a or not b:
        return False
    inter = a & b
    union = a | b
    return (len(inter) / len(union)) >= jaccard_threshold


# ── reactive_patches_from_diff + apply_dedup_contract ────────


def reactive_patches_from_diff(
    diff: SqlDiff,
    *,
    table_id: str,
    metadata_snapshot: dict | None = None,
    leakage_oracle: Any | None = None,
) -> list[dict]:
    """Map a typed diff to candidate patch templates.

    The strategist picks among these; it does not author target
    identifiers for these rows. Honors the dedup contract — see
    Semantic Consistency Rule 6.
    """
    snap = metadata_snapshot or {}
    out: list[dict] = []

    if diff.primary_kind is DiffKind.MEASURE_SWAP:
        for measure in diff.missing_in_genie.measures:
            measure_name = _short_identifier(measure)
            if _is_metric_view_measure(measure_name, snap):
                out.append({
                    "type": "update_column_description",
                    "column": measure_name,
                    "lever": 1,
                    "source_diff_kind": diff.primary_kind.value,
                    "dedup_route": "metric_view_measure_enrich",
                })
                continue
            out.append({
                "type": "update_column_description",
                "column": measure_name,
                "lever": 1,
                "source_diff_kind": diff.primary_kind.value,
            })

    elif diff.primary_kind is DiffKind.MISSING_GROUPBY_COL:
        for col in diff.missing_in_genie.group_by_cols:
            col_name = _short_identifier(col)
            out.append({
                "type": "add_example_sql",
                "target": col_name,
                "lever": 4,
                "source_diff_kind": diff.primary_kind.value,
            })
            out.append({
                "type": "update_column_description",
                "column": col_name,
                "lever": 1,
                "source_diff_kind": diff.primary_kind.value,
            })

    elif diff.primary_kind is DiffKind.EXTRA_GROUPBY_COL:
        for col in diff.extra_in_genie.group_by_cols:
            out.append({
                "type": "add_instruction",
                "target": _short_identifier(col),
                "lever": 3,
                "source_diff_kind": diff.primary_kind.value,
            })

    elif diff.primary_kind is DiffKind.EXTRA_FILTER:
        for fcol in diff.extra_in_genie.filter_cols:
            out.append({
                "type": "add_instruction",
                "target": fcol,
                "instruction_section": "QUERY CONSTRUCTION",
                "lever": 3,
                "source_diff_kind": diff.primary_kind.value,
            })

    elif diff.primary_kind is DiffKind.MISSING_FILTER:
        for fcol in diff.missing_in_genie.filter_cols:
            out.append({
                "type": "add_instruction",
                "target": fcol,
                "lever": 3,
                "source_diff_kind": diff.primary_kind.value,
            })
            out.append({
                "type": "add_example_sql",
                "target": fcol,
                "lever": 4,
                "source_diff_kind": diff.primary_kind.value,
            })

    elif diff.primary_kind is DiffKind.WRONG_AGGREGATION_FUNCTION:
        # Combined missing/extra aggregation funcs — emit a description
        # enrichment plus an example SQL candidate.
        for measure in (diff.missing_in_genie.measures or diff.extra_in_genie.measures):
            out.append({
                "type": "update_column_description",
                "column": _short_identifier(measure),
                "lever": 1,
                "source_diff_kind": diff.primary_kind.value,
            })

    return apply_dedup_contract(out, snap, leakage_oracle)


def apply_dedup_contract(
    candidates: list[dict],
    metadata_snapshot: dict,
    leakage_oracle: Any | None,
) -> list[dict]:
    """Filter candidates against existing space artifacts.

    Implements Semantic Consistency Rule 6 sub-clauses 1–6. Each
    dropped candidate is annotated with ``dedup_dropped_reason``.
    Candidates rewritten as enrichments carry ``dedup_route`` instead.
    """
    snap = metadata_snapshot or {}
    kept: list[dict] = []

    for c in candidates:
        ctype = c.get("type")

        if ctype == "add_sql_snippet_expression":
            # 6.1: if the snippet's metric is a metric-view measure,
            # rewrite as a metric-view description enrichment.
            metric = c.get("metric") or c.get("column")
            if _is_metric_view_measure(metric, snap):
                kept.append({
                    "type": "update_column_description",
                    "column": str(metric),
                    "lever": 1,
                    "source_diff_kind": c.get("source_diff_kind"),
                    "dedup_route": "metric_view_measure_enrich",
                })
                continue
            # 6.2: snippet name collision.
            if _snippet_name_collides(
                c.get("snippet_name") or c.get("target"), snap,
            ):
                c["dedup_dropped_reason"] = "snippet_already_exists"
                continue

        elif ctype == "add_join_spec":
            # 6.3
            if _join_spec_already_exists(c, snap):
                c["dedup_dropped_reason"] = "join_spec_already_exists"
                continue

        elif ctype == "add_example_sql":
            # 6.5 + 6.6
            if leakage_oracle is not None:
                sql = c.get("sql", "")
                question = c.get("question", "")
                try:
                    if sql and leakage_oracle.contains_sql(sql):
                        c["dedup_dropped_reason"] = "example_sql_already_exists"
                        continue
                except Exception:
                    logger.debug(
                        "leakage_oracle.contains_sql raised; treating as miss",
                        exc_info=True,
                    )
                try:
                    if question and leakage_oracle.contains_question(
                        question, threshold=0.85,
                    ):
                        c["dedup_dropped_reason"] = "example_question_already_exists"
                        continue
                except Exception:
                    logger.debug(
                        "leakage_oracle.contains_question raised; treating as miss",
                        exc_info=True,
                    )

        elif ctype == "update_column_description":
            # 6.4
            if _description_already_conveys(c, snap):
                c["dedup_dropped_reason"] = "description_already_present"
                continue

        kept.append(c)

    return kept


# ── small helpers ────────────────────────────────────────────


# ── Task 9: proactive mining ────────────────────────────────────


# Per-table budgets for proactive enrichment patches. These caps are
# conservative on purpose: proactive mining is opt-in, and a small
# enrichment budget per table is enough to lift the floor without
# overwriting hand-tuned descriptions.
PROACTIVE_PATCH_BUDGET: dict[str, int] = {
    "column_description": 5,
    "join_spec": 3,
    "sql_snippet": 3,
    "example_sql": 3,
}


@dataclass(frozen=True)
class CorpusProfile:
    """Frequency-weighted aggregate of features across passing rows.

    Counters are public so callers can write them straight to Delta;
    consumers are responsible for not mutating the counters in place.
    """

    measure_dim_pairs: dict[tuple[str, str], int] = field(default_factory=dict)
    join_clauses: dict[tuple[str, str, str], int] = field(default_factory=dict)
    tvf_calls: dict[str, int] = field(default_factory=dict)
    aggregation_funcs: dict[str, int] = field(default_factory=dict)


def is_eligible_passing_row(
    row: dict, gt_corrections: dict[str, str] | None = None,
) -> bool:
    """True iff ``row`` is safe to mine as a passing-corpus example.

    Eligibility rules (Task 1 + Task 9 contract):
      * ``result_correctness == "yes"`` OR ``arbiter == "both_correct"``
        — a passing row whose GT and Genie agreed.
      * ``arbiter == "genie_correct"`` AND the corpus reviewer has
        marked the GT-correction queue entry ``accepted_corpus_fix``.
        This is the closed loop: a corpus-defective row only enters
        proactive mining once the GT has actually been fixed.

    Anything else (failures, pending corpus reviews, etc.) is excluded.
    Pure: never raises.
    """
    gt_corrections = gt_corrections or {}
    rc = str(row.get("feedback/result_correctness/value") or "").lower()
    arb = str(row.get("feedback/arbiter/value") or "").lower()
    qid = str(row.get("inputs.question_id") or "")
    if rc in {"yes", "true", "1", "1.0"} or arb == "both_correct":
        return True
    if arb == "genie_correct" and gt_corrections.get(qid) == "accepted_corpus_fix":
        return True
    return False


def aggregate_corpus_profile(
    rows: list[dict],
    *,
    gt_corrections: dict[str, str] | None = None,
) -> CorpusProfile:
    """Walk eligible passing rows and build a typed corpus profile.

    Mining source per row, in priority order:
      1. ``inputs.expected_sql`` (the GT) — preferred because a GT
         row was once curated even if it was synthetic.
      2. ``outputs.predictions.sql`` (Genie's output) — used when GT
         is missing (some rows store only Genie SQL).
    Mining failures (parse errors etc.) are skipped silently.
    """
    pairs: dict[tuple[str, str], int] = {}
    joins: dict[tuple[str, str, str], int] = {}
    tvfs: dict[str, int] = {}
    aggs: dict[str, int] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if not is_eligible_passing_row(row, gt_corrections):
            continue
        sql = (
            row.get("inputs.expected_sql")
            or row.get("outputs.predictions.sql")
            or row.get("expected_sql")
            or row.get("generated_sql")
            or ""
        )
        if not sql:
            continue
        try:
            feat = mine_sql_features(sql)
        except Exception:
            continue
        for m in feat.measures:
            for d in feat.group_by_cols:
                key = (m, d)
                pairs[key] = pairs.get(key, 0) + 1
        for j in feat.join_specs:
            joins[j] = joins.get(j, 0) + 1
        for t in feat.tvf_calls:
            tvfs[t] = tvfs.get(t, 0) + 1
        for a in feat.aggregation_funcs:
            aggs[a] = aggs.get(a, 0) + 1
    return CorpusProfile(
        measure_dim_pairs=pairs,
        join_clauses=joins,
        tvf_calls=tvfs,
        aggregation_funcs=aggs,
    )


def _wraps_metric_view_measure(
    tvf: str | None, metadata_snapshot: dict,
) -> bool:
    """Best-effort heuristic: does ``tvf`` wrap a metric-view measure?

    The proactive mining path observes a TVF call frequency. If the
    snapshot's metric_view definitions reference the TVF in any
    measure expression / definition, treat the TVF as wrapping a
    metric-view measure and route enrichment to the measure rather
    than emitting a duplicate snippet.
    """
    if not tvf:
        return False
    target = tvf.lower().strip()
    for mv_name, _measure in _iter_metric_view_measures(metadata_snapshot):
        # If the metric view declares the TVF anywhere in its measure
        # expressions, treat as a wrap. Schemas vary, so we accept
        # any string field that contains the TVF name.
        for mv in metadata_snapshot.get("metric_views") or []:
            if not isinstance(mv, dict):
                continue
            if str(mv.get("name") or "").lower() != mv_name.lower():
                continue
            for m in mv.get("measures") or []:
                if not isinstance(m, dict):
                    continue
                for fld in ("expression", "definition", "sql", "formula"):
                    val = m.get(fld)
                    if isinstance(val, str) and target in val.lower():
                        return True
    return False


def _wrapped_measure_name(
    tvf: str | None, metadata_snapshot: dict,
) -> str | None:
    """Return the metric-view measure name a ``tvf`` wraps, or None."""
    if not tvf:
        return None
    target = tvf.lower().strip()
    for mv in metadata_snapshot.get("metric_views") or []:
        if not isinstance(mv, dict):
            continue
        for m in mv.get("measures") or []:
            if not isinstance(m, dict):
                continue
            mn = str(m.get("name") or "")
            if not mn:
                continue
            for fld in ("expression", "definition", "sql", "formula"):
                val = m.get(fld)
                if isinstance(val, str) and target in val.lower():
                    return mn
    return None


def synthesize_proactive_patches(
    profile: CorpusProfile,
    *,
    table_id: str,
    metadata_snapshot: dict | None = None,
    leakage_oracle: Any | None = None,
    budget: dict[str, int] | None = None,
) -> list[dict]:
    """Generate bounded proactive enrichment patches honoring the
    Semantic Consistency Rule 6 dedup contract.

    Routing decisions:
      * Measure-dim co-occurrence → ``update_column_description`` on
        the measure. If the measure lives in a metric view, the
        description is targeted at that metric view's measure column
        (Rule 6.1) rather than the base table.
      * Frequent join clauses → ``add_join_spec`` unless an
        equivalent ``instructions.join_specs`` entry already exists
        (Rule 6.3).
      * Frequent TVF calls → ``add_sql_snippet_expression`` unless
        the TVF wraps a metric-view measure (route to enrichment,
        Rule 6.1) or its name collides with an existing snippet
        (Rule 6.2).

    Returns the kept patch list. Dropped candidates carry
    ``dedup_dropped_reason`` so the harness can emit
    ``proposal_grounding`` audit rows.
    """
    snap = metadata_snapshot or {}
    bud = {**PROACTIVE_PATCH_BUDGET, **(budget or {})}
    candidates: list[dict] = []

    # Measure-dim co-occurrence — route to owning metric view when present.
    pairs_sorted = sorted(
        profile.measure_dim_pairs.items(),
        key=lambda kv: (-kv[1], kv[0]),
    )
    for (measure, dim), freq in pairs_sorted[: bud["column_description"]]:
        target_table = (
            _owning_metric_view(measure, snap) or table_id
        )
        candidates.append({
            "type": "update_column_description",
            "column": measure,
            "table_id": target_table,
            "source_signal": f"co_occurs_with:{dim}",
            "frequency": freq,
        })

    # Join clauses — drop when an equivalent join_spec already exists.
    joins_sorted = sorted(
        profile.join_clauses.items(),
        key=lambda kv: (-kv[1], kv[0]),
    )
    for spec, freq in joins_sorted[: bud["join_spec"]]:
        left, kind, right = spec
        candidates.append({
            "type": "add_join_spec",
            "left_table": left,
            "right_table": right,
            "join_kind": kind,
            "source_signal": "frequent_join",
            "frequency": freq,
            "table_id": table_id,
        })

    # TVF invocations — route metric-view-wrapping TVFs to measure
    # enrichment; everything else to a snippet.
    tvfs_sorted = sorted(
        profile.tvf_calls.items(), key=lambda kv: (-kv[1], kv[0]),
    )
    for tvf, freq in tvfs_sorted[: bud["sql_snippet"]]:
        if _wraps_metric_view_measure(tvf, snap):
            wrapped = _wrapped_measure_name(tvf, snap) or tvf
            candidates.append({
                "type": "update_column_description",
                "column": wrapped,
                "lever": 1,
                "source_signal": f"frequent_tvf:{tvf}",
                "frequency": freq,
                "dedup_route": "metric_view_measure_enrich",
            })
            continue
        candidates.append({
            "type": "add_sql_snippet_expression",
            "snippet_name": tvf,
            "source_signal": "frequent_tvf",
            "frequency": freq,
            "table_id": table_id,
        })

    # Run the dedup contract (snippet collisions, join_spec collisions,
    # description-already-conveys, etc.). Reuses the same helper as
    # the reactive path so behavior is consistent across the two
    # mining phases.
    return apply_dedup_contract(candidates, snap, leakage_oracle)


def run_proactive_feature_mining(
    *,
    eval_rows: list[dict],
    metadata_snapshot: dict,
    table_ids: list[str] | None = None,
    gt_corrections: dict[str, str] | None = None,
    leakage_oracle: Any | None = None,
    budget: dict[str, int] | None = None,
) -> dict[str, Any]:
    """End-to-end proactive feature-mining helper.

    Aggregates a corpus profile from eligible passing rows, then
    synthesizes enrichment patches per table id (when ``table_ids``
    supplied) or once for the union (when not). Returns a dict the
    harness can persist via state writers:

    ::

        {
          "profile": {"measure_dim_pairs": {...}, "join_clauses": {...}, ...},
          "eligible_row_count": int,
          "patches": [<patch dict>, ...],
        }

    Pure: never raises. Empty inputs return an empty result.
    """
    profile = aggregate_corpus_profile(eval_rows, gt_corrections=gt_corrections)
    eligible_count = sum(
        1 for r in (eval_rows or [])
        if isinstance(r, dict) and is_eligible_passing_row(r, gt_corrections)
    )
    profile_blob: dict[str, Any] = {
        # Counters serialized as lists of (key, count) so JSON survives
        # the tuple keys.
        "measure_dim_pairs": [
            list(k) + [v] for k, v in profile.measure_dim_pairs.items()
        ],
        "join_clauses": [
            list(k) + [v] for k, v in profile.join_clauses.items()
        ],
        "tvf_calls": profile.tvf_calls,
        "aggregation_funcs": profile.aggregation_funcs,
    }

    patches: list[dict] = []
    targets = list(table_ids or []) or [""]
    for tid in targets:
        for p in synthesize_proactive_patches(
            profile,
            table_id=tid,
            metadata_snapshot=metadata_snapshot,
            leakage_oracle=leakage_oracle,
            budget=budget,
        ):
            # Tag the source so persistence rows are queryable
            # alongside reactive proposals.
            p.setdefault("phase", "proactive")
            patches.append(p)

    return {
        "profile": profile_blob,
        "eligible_row_count": eligible_count,
        "patches": patches,
    }


def _short_identifier(raw: str) -> str:
    """Strip surrounding parentheses/whitespace and return the
    deepest dotted identifier component.

    Example: ``"YEAR(date_key_2)"`` → ``"date_key_2"``;
    ``"mv.col"`` → ``"col"``; already-bare ``"col"`` → ``"col"``.
    """
    if not raw:
        return ""
    s = str(raw).strip()
    # Strip outermost function wrapper if present.
    if "(" in s and s.endswith(")"):
        # ``YEAR(date_key_2)`` -> ``date_key_2``
        inner = s[s.find("(") + 1 : -1].strip()
        if inner:
            s = inner
    # Use the last dotted component (``mv.col`` → ``col``).
    if "." in s:
        s = s.rsplit(".", 1)[-1]
    return s.strip("` \"'")
