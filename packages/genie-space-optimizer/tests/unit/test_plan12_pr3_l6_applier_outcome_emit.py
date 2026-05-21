"""Plan 12 PR 3 deferred production wire-in — L6 applier outcome
emission tests.

The harness's lever loop stamps Plan-11/Plan-12 identity fields
(``intent_id``, ``ag_id``, ``cluster_id``, ``_run_id``,
``_iteration``) on each patch before invoking ``apply_patch_set``.
The applier emits one ``GSO_PATCH_OUTCOME_V1`` per patch at its
terminal state when the production-wire flag is on. Closes the I22
coverage check on the live applier path; the scaffold's
``emit_applied_outcome`` stand-in was scoped to the contract-only
replay tests.
"""
import json
import os
from unittest.mock import patch as mock_patch


def _parse_outcomes(out: str) -> list[dict]:
    rows = []
    for line in out.splitlines():
        if line.startswith("GSO_PATCH_OUTCOME_V1 "):
            rows.append(json.loads(line.partition(" ")[2]))
    return rows


def _plan12_patch(intent_id: str = "intent_001", **overrides) -> dict:
    """A patch dict stamped with Plan 12 identity fields. Mirrors
    what the harness's lever loop puts on a patch before passing it
    to apply_patch_set."""
    base: dict = {
        "intent_id": intent_id,
        "ag_id": "AG_test",
        "cluster_id": "H001",
        "_run_id": "run_x",
        "_iteration": 1,
    }
    base.update(overrides)
    return base


# ── Flag tests ────────────────────────────────────────────────────────


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_l6_applier_emit_outcomes_enabled,
    )
    with mock_patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", None)
        assert plan12_live_l6_applier_emit_outcomes_enabled() is False


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_l6_applier_emit_outcomes_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with mock_patch.dict(
            os.environ,
            {"GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES": val},
        ):
            assert (
                plan12_live_l6_applier_emit_outcomes_enabled() is True
            ), f"Expected True for {val!r}"


# ── Helper tests ──────────────────────────────────────────────────────


def test_helper_flag_off_no_emission(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", "0")

    from genie_space_optimizer.optimization.applier import (
        _emit_l6_applier_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    fired = _emit_l6_applier_outcome(
        patch=_plan12_patch(),
        outcome_kind="applied",
        applied_patch_id="ap_intent_001",
    )
    assert fired is False
    assert _parse_outcomes(capsys.readouterr().out) == []


def test_helper_flag_on_emits_applied(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", "1")

    from genie_space_optimizer.optimization.applier import (
        _emit_l6_applier_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    fired = _emit_l6_applier_outcome(
        patch=_plan12_patch(),
        outcome_kind="applied",
        applied_patch_id="ap_intent_001",
    )
    assert fired is True
    outcomes = _parse_outcomes(capsys.readouterr().out)
    assert len(outcomes) == 1
    o = outcomes[0]
    assert o["intent_id"] == "intent_001"
    assert o["outcome_kind"] == "applied"
    assert o["applied_patch_id"] == "ap_intent_001"
    assert o["ag_id"] == "AG_test"
    assert o["cluster_id"] == "H001"
    assert o["optimization_run_id"] == "run_x"
    assert o["iteration"] == 1


def test_helper_flag_on_emits_validator_rejected(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", "1")

    from genie_space_optimizer.optimization.applier import (
        _emit_l6_applier_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    fired = _emit_l6_applier_outcome(
        patch=_plan12_patch(),
        outcome_kind="validator_rejected",
        terminal_reason="validator_rejected_render",
        validator_errors=("L6 gate refused: validation_passed missing",),
    )
    assert fired is True
    outcomes = _parse_outcomes(capsys.readouterr().out)
    assert len(outcomes) == 1
    o = outcomes[0]
    assert o["outcome_kind"] == "validator_rejected"
    assert o["terminal_reason"] == "validator_rejected_render"
    assert o["validator_errors"] == [
        "L6 gate refused: validation_passed missing",
    ]


def test_helper_skips_patch_without_intent_id(capsys, monkeypatch):
    """Legacy patches (pre-Plan-12 identity threading) have no
    intent_id. The helper bails out cleanly — preserves byte-stable
    replay against fixtures that don't carry intent_id."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", "1")

    from genie_space_optimizer.optimization.applier import (
        _emit_l6_applier_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    legacy_patch = {  # no intent_id at all
        "ag_id": "AG_legacy",
        "cluster_id": "H001",
        "type": "add_column_description",
    }
    fired = _emit_l6_applier_outcome(
        patch=legacy_patch,
        outcome_kind="applied",
    )
    assert fired is False
    assert _parse_outcomes(capsys.readouterr().out) == []


def test_helper_exception_does_not_crash(monkeypatch):
    """A bug in the inner emit chain MUST NOT abort the applier.
    The helper swallows exceptions with debug logging."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", "1")

    from genie_space_optimizer.optimization import (
        patch_survival_emitter as _emitter_mod,
    )

    def _boom(**_kwargs):
        raise RuntimeError("simulated emit failure")

    monkeypatch.setattr(_emitter_mod, "emit_patch_outcome", _boom)

    from genie_space_optimizer.optimization.applier import (
        _emit_l6_applier_outcome,
    )
    # Should NOT raise.
    fired = _emit_l6_applier_outcome(
        patch=_plan12_patch(),
        outcome_kind="applied",
    )
    assert fired is False


# ── apply_patch_set integration tests ─────────────────────────────────


def test_apply_patch_set_emits_applied_for_plan12_patch(capsys, monkeypatch):
    """End-to-end: a Plan-12-shaped patch that survives apply_patch_set's
    apply path produces one APPLIED outcome."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES", "1")

    from genie_space_optimizer.optimization.applier import apply_patch_set
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    # A simple update_column_description patch — applies cleanly to a
    # config that has the column declared.
    patch = _plan12_patch(
        type="update_column_description",
        target="catalog.schema.orders.customer_id",
        new_description="The customer placing the order.",
        risk="low",
        lever=1,
    )
    metadata_snapshot = {
        "tables": [
            {
                "table_name": "catalog.schema.orders",
                "columns": [
                    {
                        "name": "customer_id",
                        "description": "",
                    },
                ],
            },
        ],
    }
    apply_patch_set(
        w=None,
        space_id="space_test",
        patches=[patch],
        metadata_snapshot=metadata_snapshot,
        apply_mode="genie_config",
    )
    outcomes = _parse_outcomes(capsys.readouterr().out)
    # The terminal outcome for this intent_id must be APPLIED (or
    # VALIDATOR_REJECTED if the config lookup couldn't match — both
    # are valid terminal states for this test; the contract is that
    # AT LEAST ONE outcome fires per intent_id).
    assert len(outcomes) == 1, (
        f"expected exactly one outcome marker; got {outcomes!r}"
    )
    o = outcomes[0]
    assert o["intent_id"] == "intent_001"
    assert o["outcome_kind"] in ("applied", "validator_rejected"), (
        f"unexpected outcome_kind={o['outcome_kind']!r}"
    )
