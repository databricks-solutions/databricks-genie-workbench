"""Plan 12 PR 4 deferred harness wire-in tests.

Three scenarios:

  1. Flag OFF → legacy path unchanged (byte-stable replay). The
     narrow_skipped_no_original_patch_type typed-decline record + marker
     are emitted via iter_inputs; NO GSO_PATCH_OUTCOME_V1 marker fires.
  2. Flag ON, ``w`` supplied, LLM returns a narrowed proposal → emit one
     GSO_PATCH_OUTCOME_V1 with ``outcome_kind=blast_radius_rejected``,
     ``narrow_replacement_attempted=True``, ``narrow_outcome=narrowed``.
     The legacy typed-decline record is still emitted for byte-stable
     replay (defense in depth — downstream consumers that read the
     decision-record schema keep working).
  3. Flag ON, ``w`` supplied, LLM declines (returns None) → emit one
     GSO_PATCH_OUTCOME_V1 with ``narrow_outcome=exhausted``. Legacy
     emission still fires.
"""
from unittest.mock import patch


def _diag_dict():
    return {
        "applicable": False,
        "reason": "narrow_skipped_no_original_patch_type",
        "original_patch_type": "",
    }


def _drop_dict():
    return {
        "reason": "high_collateral_risk_flagged",
        "original_patch": {
            "intent_id": "intent_021",
            "patch_type": "add_sql_snippet_filter",
            "patch_body": {
                "name": "mtd_filter",
                "sql_expression": "order_date >= DATE_TRUNC('month', CURRENT_DATE)",
            },
            "target_qids": ["gs_021"],
            "causal_target": "catalog.schema.orders.order_date",
            "cluster_id": "H001",
            "ag_id": "AG1",
        },
        "patch_type": "add_sql_snippet_filter",
        "proposal_id": "P001",
        "collateral_qids": ["gs_003"],
    }


def test_flag_off_preserves_legacy_decline(monkeypatch):
    """Flag OFF — _attempt_llm_narrow_replacement_and_emit_outcome
    must be a no-op even if invoked. Returns False (no LLM attempt
    made) and emits nothing."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "0")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_dict(),
        diag=_diag_dict(),
        ag_target_qids=("gs_021",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=object(),  # would be used if flag were on
    )
    assert attempted is False


def test_flag_on_no_w_skips_attempt(monkeypatch):
    """Flag ON but ``w`` is None — the helper bails out before any LLM
    call. Returns False; no GSO_PATCH_OUTCOME_V1 fires. The legacy
    path can still emit its typed-decline record (the wrapping
    function in the wire-in remains responsible for that)."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_dict(),
        diag=_diag_dict(),
        ag_target_qids=("gs_021",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=None,
    )
    assert attempted is False


def test_flag_on_llm_succeeds_emits_narrowed_outcome(capsys, monkeypatch):
    """Flag ON + w supplied + LLM returns a narrowed proposal →
    GSO_PATCH_OUTCOME_V1 with narrow_outcome=narrowed."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )
    from genie_space_optimizer.optimization.stages import (
        narrow_replacement as _narrow_mod,
    )

    reset_patch_outcome_emitter()

    def _fake_narrow_loop(patch, **kwargs):
        from dataclasses import replace
        return replace(
            patch,
            patch_body={
                **patch.patch_body,
                "name": patch.patch_body.get("name", "x") + "_scoped",
            },
        )

    monkeypatch.setattr(
        _narrow_mod, "narrow_replacement_with_llm", _fake_narrow_loop,
    )

    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_dict(),
        diag=_diag_dict(),
        ag_target_qids=("gs_021",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=object(),
    )

    assert attempted is True
    out = capsys.readouterr().out
    assert "GSO_PATCH_OUTCOME_V1" in out
    assert '"outcome_kind":"blast_radius_rejected"' in out
    assert '"narrow_replacement_attempted":true' in out
    assert '"narrow_outcome":"narrowed"' in out


def test_flag_on_llm_declines_emits_exhausted_outcome(capsys, monkeypatch):
    """Flag ON + w supplied + LLM returns None (declined / exhausted) →
    GSO_PATCH_OUTCOME_V1 with narrow_outcome=exhausted."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )
    from genie_space_optimizer.optimization.stages import (
        narrow_replacement as _narrow_mod,
    )

    reset_patch_outcome_emitter()

    monkeypatch.setattr(
        _narrow_mod,
        "narrow_replacement_with_llm",
        lambda patch, **kwargs: None,
    )

    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_dict(),
        diag=_diag_dict(),
        ag_target_qids=("gs_021",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=object(),
    )

    assert attempted is True
    out = capsys.readouterr().out
    assert "GSO_PATCH_OUTCOME_V1" in out
    assert '"narrow_outcome":"exhausted"' in out


def test_flag_on_skips_when_drop_has_no_intent_id(capsys, monkeypatch):
    """The helper requires an intent_id (the outcome marker is keyed on
    it via I22). When the legacy drop dict carries no intent_id, the
    helper bails out without emitting — the legacy typed-decline path
    fills the gap."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    drop = _drop_dict()
    drop["original_patch"]["intent_id"] = ""  # missing

    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=drop,
        diag=_diag_dict(),
        ag_target_qids=("gs_021",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=object(),
    )

    assert attempted is False
    assert "GSO_PATCH_OUTCOME_V1" not in capsys.readouterr().out


def test_flag_on_skips_when_reason_not_narrow_skipped(monkeypatch):
    """The helper only activates on the
    ``narrow_skipped_no_original_patch_type`` reason. Other
    inapplicability reasons (e.g. ``narrow_unsupported_patch_type``)
    fall through to the legacy emission unchanged."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    diag = _diag_dict()
    diag["reason"] = "narrow_unsupported_patch_type"

    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_dict(),
        diag=diag,
        ag_target_qids=("gs_021",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=object(),
    )

    assert attempted is False
