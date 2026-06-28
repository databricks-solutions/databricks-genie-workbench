"""Cycle 9 — Patch-Acceptance Reliability unit tests.

Combines the per-workstream test files described in the plan
(W1 force-L6 fallback, W2 plateau open-hard guard, W3 narrow-
replacement builder, W4 DOA fingerprint buffer, W5 double-plateau
prefix). Anchored on run 11110002 and 2afb0be2 596465849524605.
"""
from __future__ import annotations

import pytest


# ── W1 — Cycle 7 N3 force-L6 reads affected_questions ────────────────


def test_w1_flag_default_on() -> None:
    from genie_space_optimizer.common.config import (
        force_l6_reads_affected_questions_enabled,
    )
    assert force_l6_reads_affected_questions_enabled() is True


def test_w1_flag_off_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_FORCE_L6_READS_AFFECTED_QUESTIONS", "0")
    from genie_space_optimizer.common.config import (
        force_l6_reads_affected_questions_enabled,
    )
    assert force_l6_reads_affected_questions_enabled() is False


# ── W2 — plateau open-hard guard ─────────────────────────────────────


def test_w2_open_hard_returns_progress_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The N3 anchor case from run 11110002: gs_009/gs_013/gs_024
    are still hard, no SQL delta, no overlapping AG, not in
    quarantine, no regression debt — resolver must return
    PROGRESS_PENDING_OPEN_HARD with should_continue=True."""
    monkeypatch.delenv("GSO_PLATEAU_REQUIRES_ZERO_OPEN_HARD", raising=False)
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalStatus,
        resolve_terminal_on_plateau,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"gs_009", "gs_013", "gs_024"},
        regression_debt_qids=set(),
        sql_delta_qids=None,
        pending_diagnostic_ags=None,
    )
    assert decision.status is RcaTerminalStatus.PROGRESS_PENDING_OPEN_HARD
    assert decision.should_continue is True
    assert "gs_009" in decision.reason


def test_w2_clean_plateau_when_hard_set_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_PLATEAU_REQUIRES_ZERO_OPEN_HARD", raising=False)
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalStatus,
        resolve_terminal_on_plateau,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids=set(),
        regression_debt_qids=set(),
    )
    assert decision.status is RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES
    assert decision.should_continue is False


def test_w2_flag_off_preserves_legacy_clean_plateau(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_PLATEAU_REQUIRES_ZERO_OPEN_HARD", "0")
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalStatus,
        resolve_terminal_on_plateau,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"gs_009"},
        regression_debt_qids=set(),
    )
    assert decision.status is RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES
    assert decision.should_continue is False


# ── W3 — Lever-6 narrow-replacement builder ──────────────────────────


def _l6_patch(**overrides) -> dict:
    base = {
        "proposal_id": "P_L6_001",
        "patch_type": "add_sql_snippet_filter",
        "target_table": "tkt_doc",
        "where_predicate": "outbound_route_total_segments = 1",
        "qid_predicate_column": "query_id",
    }
    base.update(overrides)
    return base


def test_w3_narrow_replacement_scopes_predicate_to_target_qids() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )
    out = build_narrow_l6_replacement(
        original_patch=_l6_patch(),
        ag_target_qids=("gs_009",),
        root_cause="missing_filter",
    )
    assert out is not None
    assert out["patch_type"] == "add_sql_snippet_filter"
    assert out["target_table"] == "tkt_doc"
    assert "gs_009" in out["where_predicate"]
    assert out["proposal_id"].endswith("#NARROW")
    assert out["derived_from"] == "P_L6_001"
    assert out["narrow_target_qids"] == ("gs_009",)
    assert out["_cycle_9_narrow_replacement"] is True


def test_w3_narrow_replacement_returns_none_for_non_l6_patch() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )
    bad = _l6_patch(patch_type="update_column_description")
    assert build_narrow_l6_replacement(
        original_patch=bad,
        ag_target_qids=("gs_009",),
        root_cause="missing_filter",
    ) is None


def test_w3_narrow_replacement_returns_none_for_empty_qids() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )
    assert build_narrow_l6_replacement(
        original_patch=_l6_patch(),
        ag_target_qids=(),
        root_cause="missing_filter",
    ) is None


def test_w3_narrow_replacement_returns_none_when_already_scoped() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )
    already = _l6_patch(
        where_predicate=(
            "(outbound_route_total_segments = 1) "
            "AND (query_id IN ('gs_009'))"
        ),
    )
    assert build_narrow_l6_replacement(
        original_patch=already,
        ag_target_qids=("gs_009",),
        root_cause="missing_filter",
    ) is None


# ── W4 — DOA fingerprint buffer ──────────────────────────────────────


def _doa_patch(**o) -> dict:
    base = {
        "patch_type": "add_sql_snippet_filter",
        "target_table": "tkt_doc",
        "target_column": "outbound_route_total_segments",
        "section_set": frozenset({"where"}),
        "parent_proposal_id": "P_PARENT",
        "content_fingerprint": "fp:abc",
        "value": "1",
    }
    base.update(o)
    return base


def test_w4_add_then_contains_returns_true_for_same_signature() -> None:
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )
    buf = DoaFingerprintBuffer()
    buf.add(ag_id="AG1", patch=_doa_patch())
    assert buf.contains(ag_id="AG1", patch=_doa_patch()) is True


def test_w4_contains_returns_false_for_different_ag() -> None:
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )
    buf = DoaFingerprintBuffer()
    buf.add(ag_id="AG1", patch=_doa_patch())
    assert buf.contains(ag_id="AG2", patch=_doa_patch()) is False


def test_w4_add_is_idempotent() -> None:
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )
    buf = DoaFingerprintBuffer()
    buf.add(ag_id="AG1", patch=_doa_patch())
    buf.add(ag_id="AG1", patch=_doa_patch())
    assert len(buf.signatures_for("AG1")) == 1


def test_w4_different_value_yields_different_signature() -> None:
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )
    buf = DoaFingerprintBuffer()
    buf.add(ag_id="AG1", patch=_doa_patch(value="1"))
    assert buf.contains(
        ag_id="AG1", patch=_doa_patch(value="999"),
    ) is False


# ── W5 — double-plateau prefix fix ───────────────────────────────────


def test_w5_already_prefixed_status_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_PLATEAU_REASON_NO_DOUBLE_PREFIX", raising=False)
    from genie_space_optimizer.optimization.harness import (
        _resolve_lever_loop_exit_reason,
    )
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalDecision,
        RcaTerminalStatus,
    )

    decision = RcaTerminalDecision(
        status=RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES,
        should_continue=False,
        reason="...",
    )
    out = _resolve_lever_loop_exit_reason(decision, divergence_label=None)
    assert out == "plateau_no_open_failures"
    assert "plateau_plateau" not in out


def test_w5_unprefixed_status_still_gets_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_PLATEAU_REASON_NO_DOUBLE_PREFIX", raising=False)
    from genie_space_optimizer.optimization.harness import (
        _resolve_lever_loop_exit_reason,
    )
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalDecision,
        RcaTerminalStatus,
    )

    decision = RcaTerminalDecision(
        status=RcaTerminalStatus.CONVERGED,
        should_continue=False,
        reason="...",
    )
    out = _resolve_lever_loop_exit_reason(decision, divergence_label=None)
    assert out == "plateau_converged"


def test_w5_flag_off_preserves_legacy_double_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_PLATEAU_REASON_NO_DOUBLE_PREFIX", "0")
    from genie_space_optimizer.optimization.harness import (
        _resolve_lever_loop_exit_reason,
    )
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalDecision,
        RcaTerminalStatus,
    )

    decision = RcaTerminalDecision(
        status=RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES,
        should_continue=False,
        reason="...",
    )
    out = _resolve_lever_loop_exit_reason(decision, divergence_label=None)
    assert out == "plateau_plateau_no_open_failures"  # legacy bug pinned


# ── W3 wiring — narrow-replacement loop helper at blast-radius sites ─


def _hcrf_drop(patch: dict) -> dict:
    """Build a synthetic _blast_dropped entry as the harness builds them."""
    return {
        "proposal_id": str(patch.get("proposal_id") or "?"),
        "patch_type": str(patch.get("patch_type") or "?"),
        "reason": "high_collateral_risk_flagged",
        "passing_dependents_outside_target": [],
        "target": "",
        "original_patch": patch,
    }


def _other_drop(patch: dict) -> dict:
    drop = _hcrf_drop(patch)
    drop["reason"] = "non_semantic_collateral_warning"
    return drop


def test_w3_wiring_appends_narrow_replacement_when_l6_hcrf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_ON_HCRF", raising=False)
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization import proposal_grounding as _pg

    monkeypatch.setattr(
        _pg, "patch_blast_radius_is_safe",
        lambda *a, **k: {"safe": True, "reason": "narrow_scope_safe"},
    )
    patch = {
        "proposal_id": "P_L6_001",
        "patch_type": "add_sql_snippet_filter",
        "target_table": "tkt_doc",
        "where_predicate": "outbound_route_total_segments = 1",
        "qid_predicate_column": "query_id",
    }
    survivors = _harness._run_narrow_l6_replacement_loop(
        blast_dropped=[_hcrf_drop(patch)],
        blast_target_qids=("gs_009",),
        ag_root_cause="missing_filter",
    )
    assert len(survivors) == 1
    assert survivors[0]["patch_type"] == "add_sql_snippet_filter"
    assert "gs_009" in survivors[0]["where_predicate"]


def test_w3_wiring_skips_non_l6_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_ON_HCRF", raising=False)
    from genie_space_optimizer.optimization import harness as _harness

    patch = {
        "proposal_id": "P_DESC_001",
        "patch_type": "update_column_description",
        "where_predicate": "x = 1",
    }
    survivors = _harness._run_narrow_l6_replacement_loop(
        blast_dropped=[_hcrf_drop(patch)],
        blast_target_qids=("gs_009",),
        ag_root_cause="missing_filter",
    )
    assert survivors == []


def test_w3_wiring_skips_non_hcrf_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_ON_HCRF", raising=False)
    from genie_space_optimizer.optimization import harness as _harness

    patch = {
        "proposal_id": "P_L6_001",
        "patch_type": "add_sql_snippet_filter",
        "where_predicate": "x = 1",
        "qid_predicate_column": "query_id",
    }
    survivors = _harness._run_narrow_l6_replacement_loop(
        blast_dropped=[_other_drop(patch)],
        blast_target_qids=("gs_009",),
        ag_root_cause="missing_filter",
    )
    assert survivors == []


def test_w3_wiring_returns_empty_when_flag_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_ON_HCRF", "0")
    from genie_space_optimizer.optimization import harness as _harness

    patch = {
        "proposal_id": "P_L6_001",
        "patch_type": "add_sql_snippet_filter",
        "where_predicate": "x = 1",
        "qid_predicate_column": "query_id",
    }
    survivors = _harness._run_narrow_l6_replacement_loop(
        blast_dropped=[_hcrf_drop(patch)],
        blast_target_qids=("gs_009",),
        ag_root_cause="missing_filter",
    )
    assert survivors == []


# ── W4 wiring — capture target_still_hard rollbacks into DOA buffer ──


class _DoaDecisionStub:
    """Minimal stand-in for ControlPlaneAcceptance — only the two
    attributes the helper reads via getattr."""

    def __init__(self, *, accepted: bool, target_still_hard_qids):
        self.accepted = accepted
        self.target_still_hard_qids = tuple(target_still_hard_qids or ())


def _w4_patch(*, ptype: str, table: str, column: str, body: str) -> dict:
    return {
        "patch_type": ptype,
        "target_table": table,
        "target_column": column,
        "new_text": body,
    }


def test_w4_capture_skips_when_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    decision = _DoaDecisionStub(accepted=True, target_still_hard_qids=("gs_009",))
    n = _harness._capture_doa_fingerprints_on_rollback(
        buffer=buf,
        decision=decision,
        ag_id="ag_1",
        applied_patches=[_w4_patch(
            ptype="add_sql_snippet_filter", table="t", column="c", body="x",
        )],
    )
    assert n == 0
    assert buf.signatures_for("ag_1") == ()


def test_w4_capture_skips_when_target_still_hard_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    decision = _DoaDecisionStub(accepted=False, target_still_hard_qids=())
    n = _harness._capture_doa_fingerprints_on_rollback(
        buffer=buf,
        decision=decision,
        ag_id="ag_1",
        applied_patches=[_w4_patch(
            ptype="add_sql_snippet_filter", table="t", column="c", body="x",
        )],
    )
    assert n == 0
    assert buf.signatures_for("ag_1") == ()


def test_w4_capture_fires_on_rollback_with_target_still_hard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    decision = _DoaDecisionStub(
        accepted=False, target_still_hard_qids=("gs_009", "gs_013"),
    )
    patches = [
        _w4_patch(
            ptype="add_sql_snippet_filter", table="t", column="c1", body="b1",
        ),
        _w4_patch(
            ptype="update_instruction_section", table="", column="", body="b2",
        ),
    ]
    n = _harness._capture_doa_fingerprints_on_rollback(
        buffer=buf,
        decision=decision,
        ag_id="ag_42",
        applied_patches=patches,
    )
    assert n == 2
    sigs = buf.signatures_for("ag_42")
    assert len(sigs) == 2
    # Buffer is keyed per-AG — different AG sees no signatures.
    assert buf.signatures_for("ag_other") == ()


def test_w4_capture_returns_zero_when_flag_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", "0")
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    decision = _DoaDecisionStub(
        accepted=False, target_still_hard_qids=("gs_009",),
    )
    n = _harness._capture_doa_fingerprints_on_rollback(
        buffer=buf,
        decision=decision,
        ag_id="ag_1",
        applied_patches=[_w4_patch(
            ptype="add_sql_snippet_filter", table="t", column="c", body="x",
        )],
    )
    assert n == 0
    assert buf.signatures_for("ag_1") == ()


def test_w4_capture_no_op_when_buffer_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import harness as _harness

    decision = _DoaDecisionStub(
        accepted=False, target_still_hard_qids=("gs_009",),
    )
    n = _harness._capture_doa_fingerprints_on_rollback(
        buffer=None,
        decision=decision,
        ag_id="ag_1",
        applied_patches=[_w4_patch(
            ptype="add_sql_snippet_filter", table="t", column="c", body="x",
        )],
    )
    assert n == 0


# ── W4 wiring — strategist-preprocessing prune helper ───────────────


def test_w4_prune_drops_buffered_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import optimizer as _optimizer
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    blocked = _w4_patch(
        ptype="add_sql_snippet_filter", table="t", column="c", body="x",
    )
    buf.add(ag_id="ag_1", patch=blocked)
    out = _optimizer._prune_doa_fingerprints(
        [blocked], buffer=buf, ag_id="ag_1",
    )
    assert out == []


def test_w4_prune_keeps_unmatched_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import optimizer as _optimizer
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    blocked = _w4_patch(
        ptype="add_sql_snippet_filter", table="t", column="c1", body="x",
    )
    fresh = _w4_patch(
        ptype="add_sql_snippet_filter", table="t", column="c2", body="x",
    )
    buf.add(ag_id="ag_1", patch=blocked)
    out = _optimizer._prune_doa_fingerprints(
        [fresh], buffer=buf, ag_id="ag_1",
    )
    assert len(out) == 1
    assert out[0] is fresh


def test_w4_prune_passes_through_when_buffer_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import optimizer as _optimizer

    candidates = [
        _w4_patch(
            ptype="add_sql_snippet_filter", table="t", column="c", body="x",
        ),
    ]
    out = _optimizer._prune_doa_fingerprints(
        candidates, buffer=None, ag_id="ag_1",
    )
    assert out == candidates


def test_w4_prune_passes_through_when_flag_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", "0")
    from genie_space_optimizer.optimization import optimizer as _optimizer
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    blocked = _w4_patch(
        ptype="add_sql_snippet_filter", table="t", column="c", body="x",
    )
    buf.add(ag_id="ag_1", patch=blocked)
    # Flag off — even though the signature is buffered, prune is a no-op.
    out = _optimizer._prune_doa_fingerprints(
        [blocked], buffer=buf, ag_id="ag_1",
    )
    assert len(out) == 1


def test_w4_prune_buffer_per_ag_isolation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Buffered signatures for ag_1 must NOT prune candidates for ag_2."""
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL", raising=False)
    from genie_space_optimizer.optimization import optimizer as _optimizer
    from genie_space_optimizer.optimization.reflection_retry import (
        DoaFingerprintBuffer,
    )

    buf = DoaFingerprintBuffer()
    patch = _w4_patch(
        ptype="add_sql_snippet_filter", table="t", column="c", body="x",
    )
    buf.add(ag_id="ag_1", patch=patch)
    # Same patch shape, but the prune is keyed against ag_2 — should NOT drop.
    out = _optimizer._prune_doa_fingerprints(
        [patch], buffer=buf, ag_id="ag_2",
    )
    assert len(out) == 1
