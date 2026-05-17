"""Phase 6.1 — the live forbidden-set producer must consume
terminal_signature from reflection entries, not only the legacy
root_cause / source_cluster_signatures axes.

Regression evidence: Run A (59a173d3) and Run B (ab65fefe)
repeatedly re-ran the same AG shape across iter 2..5 because the
live producer (_compute_forbidden_ag_set_pair) ignored
terminal_signature even though Phase 2 populated it on every
non-accepted reflection entry.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _ag_collision_key_pair,
    _collision_pair_matches,
    _compute_forbidden_ag_set_pair,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
)


def _make_reflection_with_terminal_signature(
    *,
    root_cause: str,
    lever_set: tuple[int, ...],
    target_qids: tuple[str, ...],
    terminal_reason: str = "proposal_generation_empty",
) -> dict:
    sig = TerminalSignature(
        root_cause=root_cause,
        blame_set_norm=(),
        lever_set=frozenset(lever_set),
        target_qids=frozenset(target_qids),
        terminal_reason=terminal_reason,
    )
    return {
        "accepted": False,
        "rollback_class": "no_action",
        "rollback_reason": "proposal_generation_empty",
        "root_cause": root_cause,
        "blame_set": (),
        "lever_set": list(lever_set),
        "source_cluster_ids": list(target_qids),
        "source_cluster_signatures": [],
        "terminal_signature": sig,
    }


def test_pair_includes_terminal_signature_axis_when_flag_on(monkeypatch):
    monkeypatch.setenv(
        "GSO_FORBIDDEN_SET_TERMINAL_SIGNATURE_AXIS_ENABLED", "1",
    )
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_TERMINAL_SIGNATURE_RETIRE", "1")
    refl = [_make_reflection_with_terminal_signature(
        root_cause="plural_top_n_collapse",
        lever_set=(5,),
        target_qids=("gs_007", "gs_013"),
    )]
    pair = _compute_forbidden_ag_set_pair(refl)
    assert len(pair.by_terminal_signature) == 1


def test_pair_excludes_terminal_signature_axis_when_flag_off(monkeypatch):
    monkeypatch.setenv(
        "GSO_FORBIDDEN_SET_TERMINAL_SIGNATURE_AXIS_ENABLED", "0",
    )
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    refl = [_make_reflection_with_terminal_signature(
        root_cause="plural_top_n_collapse",
        lever_set=(5,),
        target_qids=("gs_007", "gs_013"),
    )]
    pair = _compute_forbidden_ag_set_pair(refl)
    assert pair.by_terminal_signature == frozenset()


def test_collision_pair_matches_on_terminal_signature_axis(monkeypatch):
    monkeypatch.setenv(
        "GSO_FORBIDDEN_SET_TERMINAL_SIGNATURE_AXIS_ENABLED", "1",
    )
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_TERMINAL_SIGNATURE_RETIRE", "1")
    refl = [_make_reflection_with_terminal_signature(
        root_cause="plural_top_n_collapse",
        lever_set=(5,),
        target_qids=("gs_007", "gs_013"),
    )]
    forbidden_pair = _compute_forbidden_ag_set_pair(refl)
    # Phase 0.1 (2026-05-17) — production AGs carry the qids in
    # ``affected_questions``; ``source_cluster_ids`` holds cluster
    # labels like ``H001``.
    candidate_ag = {
        "source_cluster_ids": ["H001"],
        "affected_questions": ["gs_007", "gs_013"],
        "source_cluster_signatures": [],
    }
    candidate_pair = _ag_collision_key_pair(
        candidate_ag,
        ag_root_cause="plural_top_n_collapse",
        ag_blame_set=(),
        lever_keys=["5"],
    )
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is True


def test_collision_pair_matches_blocks_renamed_root_cause_with_same_signature(
    monkeypatch,
):
    """Reviewer's 'AG-id-based vs semantic' point. The forbidden-set
    must block a candidate that re-routes through a different
    root_cause label but produces the same terminal signature."""
    monkeypatch.setenv(
        "GSO_FORBIDDEN_SET_TERMINAL_SIGNATURE_AXIS_ENABLED", "1",
    )
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_TERMINAL_SIGNATURE_RETIRE", "1")
    refl = [_make_reflection_with_terminal_signature(
        root_cause="plural_top_n_collapse",
        lever_set=(5,),
        target_qids=("gs_007",),
    )]
    forbidden_pair = _compute_forbidden_ag_set_pair(refl)
    # Phase 0.1 (2026-05-17) — production AGs carry the qids in
    # ``affected_questions``; ``source_cluster_ids`` holds cluster
    # labels like ``H001``. The candidate key now reads
    # ``affected_questions``, so the fixture must follow.
    candidate_ag_renamed = {
        "source_cluster_ids": ["H001"],
        "affected_questions": ["gs_007"],
        "source_cluster_signatures": [],
    }
    candidate_pair = _ag_collision_key_pair(
        candidate_ag_renamed,
        ag_root_cause="ambiguous_question",  # renamed
        ag_blame_set=(),
        lever_keys=["5"],
    )
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is True
