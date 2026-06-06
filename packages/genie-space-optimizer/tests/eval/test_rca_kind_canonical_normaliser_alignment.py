"""Trial 26 W26.4 — RCA-kind canonical normaliser alignment.

The merge gate for the canonicaliser. The dataset below is a curated
``(raw_label, expected_canonical_key)`` corpus mined from production
test fixtures (``tests/replay/anchors/fixtures/``,
``tests/integration/postmortem_replay/fixtures/``,
``tests/fixtures/trial19_postmortem/``, ``tests/replay/test_l6_*``)
plus the labels documented in the Trial 26 plan and the runid_analysis
postmortem bundles.

Two categories of label are tested:

  * ``RESOLVE`` — the label MUST canonicalise to the named canonical
    key. A regression on any of these is a merge blocker (the
    canonicaliser broke a real production case).
  * ``UNKNOWN`` — the label is OUT OF DOMAIN today; the canonicaliser
    MUST return ``unknown_kind``. These pin the negative-criterion
    bar so the canonicaliser cannot silently grow false positives on
    labels that have no kit-map representation. A regression here
    means a new keyword/alias accidentally matched a label that does
    not actually map to that kit.

The acceptance bar is the same one the tracker named: ``>= 95%`` exact
match across the corpus. The dataset is intentionally embedded in the
test file (rather than an external JSONL) so the corpus and the
expected outcomes evolve together in git history.

The test runs the canonicaliser in deterministic-tier mode only (no
``w``). The LLM tier is exercised in unit tests with mocked
responses; live LLM alignment is owed by W26.4's tier-4 wire-up and is
gated on the ``GSO_TRIAL26_W26_4_LIVE_ALIGNMENT`` env flag so the
merge gate stays offline.
"""
from __future__ import annotations

from typing import Literal

import pytest

# ---------------------------------------------------------------------
# Curated corpus.
#
# Sourced from:
#   * tests/replay/test_l6_anchor_applyability_gs_009.py
#   * tests/replay/test_l6_anchor_applyability_gs_024.py
#   * tests/replay/test_l6_anchor_applyability_gs_004.py
#   * tests/replay/anchors/fixtures/gs_009_full_trajectory.json
#   * tests/replay/anchors/fixtures/gs_021_baseline.json
#   * tests/integration/postmortem_replay/fixtures/run_b_452249357578743.json
#   * tests/fixtures/trial19_postmortem/airline_634185464201993.json
#   * tests/unit/test_plan11_stage1_marker_blame_set_source_field.py
#   * tests/unit/test_diagnose_pr_a_instrumentation.py
#   * tests/unit/test_diagnose_seed_backfill.py
#   * tests/unit/test_cluster_plan11_primary_blame_backfill.py
#   * tests/unit/state_machine/test_question_state_advance.py
#   * Trial 26 plan: "Top-N cardinality collapse via spurious RANK()=1 filter"
#
# Each row is ``(raw_label, expected_canonical_key, kind)`` where
# ``kind`` is "RESOLVE" or "UNKNOWN" so failures are reported with the
# right diagnostic.
# ---------------------------------------------------------------------
_CORPUS: tuple[tuple[str, str, Literal["RESOLVE", "UNKNOWN"]], ...] = (
    # ── RESOLVE — tier 1 (exact canonical) ─────────────────────────────
    ("top_n_cardinality_collapse", "top_n_cardinality_collapse", "RESOLVE"),
    ("extra_defensive_filter", "extra_defensive_filter", "RESOLVE"),
    ("wrong_aggregation", "wrong_aggregation", "RESOLVE"),
    ("wrong_column", "wrong_column", "RESOLVE"),
    ("join_semantics_wrong", "join_semantics_wrong", "RESOLVE"),
    ("column_disambiguation", "column_disambiguation", "RESOLVE"),
    ("time_grain_wrong", "time_grain_wrong", "RESOLVE"),
    ("table_routing_wrong", "table_routing_wrong", "RESOLVE"),
    ("value_mapping_missing", "value_mapping_missing", "RESOLVE"),
    # ── RESOLVE — tier 2 (alias) ───────────────────────────────────────
    ("top_n_collapse", "top_n_cardinality_collapse", "RESOLVE"),
    ("plural_top_n_collapse", "top_n_cardinality_collapse", "RESOLVE"),
    ("defensive_filter", "extra_defensive_filter", "RESOLVE"),
    ("col_disambig", "column_disambiguation", "RESOLVE"),
    # ── RESOLVE — tier 3 (keyword on production English labels) ────────
    (
        "Top-N cardinality collapse via spurious RANK()=1 filter",
        "top_n_cardinality_collapse",
        "RESOLVE",
    ),
    (
        "top-N collapsed (RANK without LIMIT)",
        "top_n_cardinality_collapse",
        "RESOLVE",
    ),
    ("top-N collapsed", "top_n_cardinality_collapse", "RESOLVE"),
    (
        "wrong_aggregation (quantity vs revenue, COUNT vs COUNT DISTINCT)",
        "wrong_aggregation",
        "RESOLVE",
    ),
    ("wrong_join_spec", "join_semantics_wrong", "RESOLVE"),
    # ── UNKNOWN — labels without canonical mapping today ───────────────
    #
    # These intentionally fail every tier — they reflect RCA kinds the
    # kit map does not currently cover. The canonicaliser MUST return
    # the ``unknown_kind`` sentinel so the kit gate falls through to
    # legacy single-lever-allowed behaviour rather than silently
    # mis-routing to the wrong kit.
    ("missing_filter", "unknown_kind", "UNKNOWN"),
    ("missing customer_segment filter", "unknown_kind", "UNKNOWN"),
    ("filter_predicate_missing", "unknown_kind", "UNKNOWN"),
    ("missing_metadata", "unknown_kind", "UNKNOWN"),
    ("row_count_mismatch", "unknown_kind", "UNKNOWN"),
    ("tvf_parameter_error", "unknown_kind", "UNKNOWN"),
    ("cannot ground", "unknown_kind", "UNKNOWN"),
    ("hallucinated columns", "unknown_kind", "UNKNOWN"),
)


# Minimum exact-match rate across the entire corpus. The tracker
# committed to ``>= 95%`` as the merge gate; we set the threshold here
# so a future corpus expansion can dilute the count without dropping
# the bar.
ALIGNMENT_THRESHOLD = 0.95


@pytest.fixture(autouse=True)
def _clear_canonicaliser_cache(monkeypatch):
    """Each test run starts with a fresh cache and a default-ON sub-flag.
    Tests in this module never need an LLM client; the deterministic
    tiers (1-3) carry the entire merge gate.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization import rca_kind_canonical

    rca_kind_canonical._reset_cache_for_tests()
    yield
    rca_kind_canonical._reset_cache_for_tests()


def test_alignment_threshold_met():
    """The deterministic tiers must canonicalise the corpus at the
    tracker-pinned threshold (>= 95%). Both RESOLVE and UNKNOWN
    rows are weighted equally — getting an UNKNOWN row wrong (false
    positive) is just as bad as missing a RESOLVE row.
    """
    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    misses: list[tuple[str, str, str, str]] = []
    for raw, expected, kind in _CORPUS:
        got = canonicalise_rca_kind(raw).canonical_key
        if got != expected:
            misses.append((raw, expected, got, kind))

    total = len(_CORPUS)
    hits = total - len(misses)
    accuracy = hits / total

    assert accuracy >= ALIGNMENT_THRESHOLD, (
        f"Trial 26 W26.4 alignment regression: {hits}/{total} = "
        f"{accuracy:.0%} (threshold {ALIGNMENT_THRESHOLD:.0%}). "
        f"Misses:\n"
        + "\n".join(
            f"  [{k}] {raw!r:60s} expected={exp!r:35s} got={got!r}"
            for raw, exp, got, k in misses
        )
    )


def test_zero_resolve_regressions():
    """RESOLVE rows are the strict floor — every label sourced from a
    real production fixture must canonicalise correctly. A miss here
    means the canonicaliser regressed on a known-good case.
    """
    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    misses: list[tuple[str, str, str]] = []
    for raw, expected, kind in _CORPUS:
        if kind != "RESOLVE":
            continue
        got = canonicalise_rca_kind(raw).canonical_key
        if got != expected:
            misses.append((raw, expected, got))

    assert not misses, (
        "Trial 26 W26.4 RESOLVE regression — every label below was "
        "sourced from a real production fixture and must canonicalise:\n"
        + "\n".join(
            f"  {raw!r:60s} expected={exp!r:35s} got={got!r}"
            for raw, exp, got in misses
        )
    )


def test_zero_unknown_false_positives():
    """UNKNOWN rows are the negative criterion — a label that is
    genuinely out of domain must NOT be silently mapped onto a kit
    (which would mis-route the kit gate). False positives here are a
    higher-severity bug than a missed RESOLVE because they
    actively break the optimizer rather than fail safely.
    """
    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    fp: list[tuple[str, str]] = []
    for raw, _expected, kind in _CORPUS:
        if kind != "UNKNOWN":
            continue
        got = canonicalise_rca_kind(raw).canonical_key
        if got != "unknown_kind":
            fp.append((raw, got))

    assert not fp, (
        "Trial 26 W26.4 UNKNOWN false-positive regression — these "
        "labels have no kit-map representation and the canonicaliser "
        "must return ``unknown_kind`` so the kit gate falls through "
        "safely:\n"
        + "\n".join(f"  {raw!r:60s} -> {got!r}" for raw, got in fp)
    )


@pytest.mark.parametrize(
    "raw,expected,kind",
    _CORPUS,
    ids=[f"{k}::{r[:50]}" for r, _e, k in _CORPUS],
)
def test_per_row(raw, expected, kind):
    """One row per parametrised case — gives per-row visibility in
    pytest output so a single regression is named individually.
    """
    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    got = canonicalise_rca_kind(raw).canonical_key
    assert got == expected, (
        f"[{kind}] {raw!r} expected {expected!r}, got {got!r}"
    )
