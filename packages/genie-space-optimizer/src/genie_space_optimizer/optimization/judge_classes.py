"""Judge signal classes and root-cause voting weights.

Each MLflow evaluation scorer (judge) falls into one of a small set of
signal classes based on what it actually measures:

``SQL_SHAPE``
    Structural SQL correctness — whether the generated query has the
    right tables, joins, filters, aggregations, and columns. These
    judges vote with full weight when selecting a cluster's dominant
    root cause because a disagreement here is almost always about the
    shape of the SQL, which the lever system can fix directly.

``ROUTING``
    Which asset class (table / MV / TVF) the generator chose. Useful
    but only mid-weight because an asset-routing failure is often a
    consequence of description or instruction gaps rather than a
    structural SQL failure on its own.

``NL_TEXT``
    Quality of the natural-language response text. Does not evaluate
    the SQL. Given near-zero weight for root-cause selection so it
    cannot dominate a cluster whose SQL-shape judges have already
    diagnosed a structural issue (see Q004 regression in the
    lever-loop-router-and-resilience plan for the motivating case).

``META``
    Informational judges used for evaluation hygiene
    (``expected_response`` etc.) that do not contribute actionable
    diagnostic signal. Weight zero.

``INFRA``
    Execution-level gates (``syntax_validity``). These either let the
    row proceed to further judges or hard-fail the row before any
    content judgement can be made. Weight zero for root-cause
    selection — they don't tell us *what* to fix, only that something
    prevented evaluation.

The weights in :data:`JUDGE_WEIGHT_FOR_ROOT_CAUSE` are used by the
weighted dominant-root-cause selection in
``cluster_failures`` (Phase B2 of the router-and-resilience plan). Any
judge absent from the map defaults to a mid-weight 0.5 so new scorers
don't accidentally get full weight before an explicit class has been
assigned.
"""

from __future__ import annotations

from enum import Enum


class SignalClass(str, Enum):
    """Taxonomy of what a given judge measures."""

    SQL_SHAPE = "sql_shape"
    ROUTING = "routing"
    NL_TEXT = "nl_text"
    META = "meta"
    INFRA = "infra"


# Keep in sync with the judges registered in ``optimization/scorers``.
JUDGE_TO_SIGNAL_CLASS: dict[str, SignalClass] = {
    "result_correctness":   SignalClass.SQL_SHAPE,
    "schema_accuracy":      SignalClass.SQL_SHAPE,
    "completeness":         SignalClass.SQL_SHAPE,
    "semantic_equivalence": SignalClass.SQL_SHAPE,
    "logical_accuracy":     SignalClass.SQL_SHAPE,
    "asset_routing":        SignalClass.ROUTING,
    "response_quality":     SignalClass.NL_TEXT,
    "expected_response":    SignalClass.META,
    "syntax_validity":      SignalClass.INFRA,
    "previous_sql":         SignalClass.META,
    "repeatability":        SignalClass.META,
}


# Weights used when a judge's vote contributes toward selecting a
# cluster's dominant root cause. SQL-shape judges carry the signal we
# actually want to route on; routing is a mid-weight tiebreaker;
# everything else is near-zero so it can't override structural
# diagnoses. Unknown judges default to 0.5 (see
# ``judge_weight_for_root_cause``).
JUDGE_WEIGHT_FOR_ROOT_CAUSE: dict[str, float] = {
    "result_correctness":   1.0,
    "schema_accuracy":      1.0,
    "completeness":         1.0,
    "semantic_equivalence": 1.0,
    "logical_accuracy":     1.0,
    "asset_routing":        0.5,
    "response_quality":     0.1,
    "expected_response":    0.0,
    "syntax_validity":      0.0,
    "previous_sql":         0.0,
    "repeatability":        0.0,
}

_DEFAULT_UNKNOWN_JUDGE_WEIGHT: float = 0.5


def judge_signal_class(judge: str) -> SignalClass:
    """Return the :class:`SignalClass` for *judge*, defaulting to ``NL_TEXT``
    when the judge is unknown. ``NL_TEXT`` is a conservative default — a
    new, unclassified judge should not accidentally get SQL-shape
    authority over the router.
    """
    return JUDGE_TO_SIGNAL_CLASS.get(judge, SignalClass.NL_TEXT)


def judge_weight_for_root_cause(judge: str) -> float:
    """Return the root-cause voting weight for *judge*.

    Unknown judges return ``0.5`` so they contribute but don't dominate.
    """
    return JUDGE_WEIGHT_FOR_ROOT_CAUSE.get(judge, _DEFAULT_UNKNOWN_JUDGE_WEIGHT)


def aggregate_cluster_signal_class(judges: list[str]) -> str:
    """Summarise a cluster's overall signal class from its failing judges.

    Returns one of ``"sql_shape"``, ``"routing"``, ``"nl_text"``,
    ``"meta"``, ``"infra"``, or ``"mixed"``. Used by the observability
    logs in Phase E1.
    """
    if not judges:
        return "mixed"
    classes = {judge_signal_class(j) for j in judges}
    if len(classes) == 1:
        return next(iter(classes)).value
    return "mixed"
