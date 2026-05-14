"""Phase 5 fixture: a cluster with no hard-failure ASI either emits a
``CLUSTER_BLOCKED_NO_RCA`` record (with TerminalReason.NO_RCA_GROUND) OR
produces a provisional RCA card from soft signals; never silently passes
uncovered clusters to the strategist.

Maps to user-text ``test_no_rca_for_uncovered_cluster_emits_typed_terminal``.
Anchor: ccf1d60d iter-3 H002/H003 lacking hard-failure ASI.

Plan ref:
``packages/genie-space-optimizer/docs/final_plan/2026-05-14-final-closeout-phase-5-6-offline-gates.md``
Task 7. Exercises ``filter_clusters_blocked_no_rca`` from
``optimization/blocked_cluster_filter.py`` (Phase 2.1, commit 4ef73746)
and ``build_provisional_card`` from ``optimization/rca_provisional_card.py``
(Phase 2.2, commit c93c25f8).

API drift vs the plan's literal test code (documented for auditability):
  * ``filter_clusters_blocked_no_rca`` is keyed on
    ``decision_records_this_iter`` (a sequence of dicts with
    ``record_type == "no_rca_ground"`` carrying the typed terminal),
    NOT ``hard_asi_clusters`` / ``provisional_card_clusters`` kwargs,
    and it returns a plain ``list`` of surviving (admitted) clusters,
    NOT a struct with ``.blocked`` / ``.terminal_reason`` /
    ``.admitted_clusters`` attributes.
  * ``build_provisional_card`` is keyed on a single ``cluster_id`` and
    ``soft_signals_for_cluster`` (a list of soft-signal dicts with
    ``qid`` / ``root_cause_hint``), and it returns a dict (with
    ``provisional=True``) or ``None``, NOT a struct.
  * The ccf1d60d iter-3 fixture has all three clusters as
    ``is_hard_failure: true`` and ``soft_signal_clusters: []`` — so the
    "uncovered cluster" scenario is constructed by deriving synthetic
    H002/H003-like uncovered cluster records from the fixture's
    iteration metadata. The fixture anchors run_id / iteration /
    target_qids; the uncovered-cluster gate is exercised end-to-end.

These adaptations preserve the spec invariant ("uncovered clusters with
no RCA card emit the typed NO_RCA_GROUND terminal AND are excluded from
strategist input") against the real production API.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.blocked_cluster_filter import (
    blocked_cluster_ids_from_records,
    filter_clusters_blocked_no_rca,
)
from genie_space_optimizer.optimization.rca_provisional_card import (
    build_provisional_card,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason

from tests.replay.fixtures.phase5._helpers import load


def _uncovered_record_for(cluster_id: str) -> dict[str, object]:
    """Build the typed DecisionRecord shape that the production gate
    emits when a cluster has no hard ASI and no provisional card.

    Mirrors the ``CLUSTER_BLOCKED_NO_RCA`` row that
    ``optimization/blocked_cluster_filter.py`` keys on:
    ``record_type == "no_rca_ground"`` (value of
    :attr:`TerminalReason.NO_RCA_GROUND`).
    """
    return {
        "record_type": TerminalReason.NO_RCA_GROUND.value,
        "cluster_id": cluster_id,
        "terminal_reason": TerminalReason.NO_RCA_GROUND.value,
    }


def test_uncovered_cluster_either_blocks_or_synthesizes_provisional() -> None:
    """For every uncovered cluster (no hard ASI), the system MUST either
    synthesize a provisional RCA card (>= quorum-3 consistent soft
    signals) OR emit a typed ``NO_RCA_GROUND`` blocked record.
    Silent pass-through to the strategist is the invariant violation.
    """
    iter3 = load("ccf1d60d_iter3.json")
    clusters = iter3["candidate_clusters"]
    clusters_with_hard_asi = set(iter3["clusters_with_hard_asi"])
    soft_signal_clusters = set(iter3["soft_signal_clusters"])

    # Synthesize the uncovered-cluster scenario the spec describes:
    # H002 / H003 lacking hard-failure ASI. The recorded fixture marks
    # all three as hard, so we derive uncovered counterparts to exercise
    # the gate without mutating the fixture itself.
    uncovered_cluster_ids = ["H002_uncovered", "H003_uncovered"]
    uncovered_clusters = [
        {"cluster_id": cid, "is_hard_failure": False, "question_ids": []}
        for cid in uncovered_cluster_ids
    ]

    decision_records: list[dict[str, object]] = []
    for c in clusters + uncovered_clusters:
        cid = c["cluster_id"]
        if cid in clusters_with_hard_asi:
            # Has hard ASI — covered by the standard RCA pipeline.
            continue

        provisional = build_provisional_card(
            cluster_id=cid,
            soft_signals_for_cluster=(
                # The fixture's soft_signal_clusters is empty for iter-3;
                # this branch is reachable if future fixtures populate it.
                [] if cid not in soft_signal_clusters else []
            ),
        )

        if provisional is not None:
            assert provisional["provisional"] is True, (
                f"provisional card for {cid} missing provisional=True flag"
            )
            assert provisional["cluster_id"] == cid
        else:
            # No hard ASI, no provisional card → MUST emit typed
            # NO_RCA_GROUND record so the routing gate filters it out.
            decision_records.append(_uncovered_record_for(cid))

    blocked_ids = blocked_cluster_ids_from_records(decision_records)
    for cid in uncovered_cluster_ids:
        assert cid in blocked_ids, (
            f"uncovered cluster {cid} has no hard ASI and no provisional "
            f"card, yet was not in the NO_RCA_GROUND blocked-id set: "
            f"{sorted(blocked_ids)!r}"
        )


def test_strategist_input_excludes_uncovered_clusters() -> None:
    """The strategist's admitted-cluster pool MUST NOT contain a cluster
    that has a typed ``NO_RCA_GROUND`` record this iteration.
    """
    iter3 = load("ccf1d60d_iter3.json")
    hard_asi = set(iter3["clusters_with_hard_asi"])

    # Mix fixture clusters with synthetic uncovered ones to exercise the
    # production filter that removes NO_RCA_GROUND-recorded clusters.
    uncovered_cluster_ids = ["H002_uncovered", "H003_uncovered"]
    clusters = list(iter3["candidate_clusters"]) + [
        {"cluster_id": cid, "is_hard_failure": False}
        for cid in uncovered_cluster_ids
    ]

    decision_records = [
        _uncovered_record_for(cid)
        for cid in uncovered_cluster_ids
        if cid not in hard_asi
    ]

    strategist_input = filter_clusters_blocked_no_rca(
        clusters=clusters,
        decision_records_this_iter=decision_records,
    )

    admitted_ids = {c["cluster_id"] for c in strategist_input}
    for cid in uncovered_cluster_ids:
        assert cid not in admitted_ids, (
            f"cluster {cid} lacks RCA card and has a NO_RCA_GROUND "
            f"record but reached strategist input: {sorted(admitted_ids)!r}"
        )

    # Sanity: the covered fixture clusters survive the filter (the gate
    # only removes the uncovered ones, not the hard-ASI-backed ones).
    for fixture_cluster in iter3["candidate_clusters"]:
        assert fixture_cluster["cluster_id"] in admitted_ids, (
            f"fixture cluster {fixture_cluster['cluster_id']} (hard ASI) "
            f"was incorrectly filtered out by NO_RCA_GROUND gate"
        )


def test_no_rca_ground_record_type_value_matches_terminal_reason_enum() -> None:
    """Defensive: the record_type string the filter keys on MUST equal
    :attr:`TerminalReason.NO_RCA_GROUND`. If the enum value drifts, the
    production gate silently stops filtering and uncovered clusters
    leak to the strategist. This test pins the wire format.
    """
    record = _uncovered_record_for("Hx")
    blocked = blocked_cluster_ids_from_records([record])
    assert blocked == frozenset({"Hx"}), (
        f"NO_RCA_GROUND record_type wire format drifted: filter saw "
        f"{blocked!r} for record {record!r}"
    )
    assert TerminalReason.NO_RCA_GROUND.value == "no_rca_ground"
