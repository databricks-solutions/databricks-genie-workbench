"""Plan 12 PR 7 Task 7.4 deferred writer-path swap tests.

The legacy ``_iter_proposal_attempts`` counter at ``harness.py:19243``
is declared but never incremented anywhere in the harness, so the
candidate-ledger entry always wrote ``proposal_attempts=0``. The
swap replaces this with ``derive_proposal_attempts_from_patch_outcomes``
over the patch-outcome emitter's registry.

Flag OFF preserves byte-stable legacy behaviour. Flag ON activates
the deriver-based count.
"""
import os
from unittest.mock import patch


# ── Flag tests ────────────────────────────────────────────────────────


def test_flag_on_by_default():
    """Track A / A1 promoted this flag to default-ON: the candidate
    ledger must report proposal_attempts from the same source of truth
    (the patch-outcome emitter) as patches_applied, so the funnel head
    can no longer be a permanently-zero artifact."""
    from genie_space_optimizer.common.config import (
        plan12_live_proposal_attempts_derive_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE", None)
        assert plan12_live_proposal_attempts_derive_enabled() is True


def test_flag_opt_out_with_falsy_values():
    """Operators can still disable the deriver per-deploy."""
    from genie_space_optimizer.common.config import (
        plan12_live_proposal_attempts_derive_enabled,
    )
    for val in ("false", "False", "FALSE", "0", "no", "off"):
        with patch.dict(
            os.environ,
            {"GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE": val},
        ):
            assert (
                plan12_live_proposal_attempts_derive_enabled() is False
            ), f"Expected False for {val!r}"


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_proposal_attempts_derive_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(
            os.environ,
            {"GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE": val},
        ):
            assert (
                plan12_live_proposal_attempts_derive_enabled() is True
            ), f"Expected True for {val!r}"


# ── Helper tests ──────────────────────────────────────────────────────


def test_flag_off_returns_legacy_counter(monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE", "0")

    from genie_space_optimizer.optimization.harness import (
        _derive_iter_proposal_attempts,
    )
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    # Even with outcomes registered, flag OFF returns legacy.
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        intent_id="intent_001",
        outcome_kind=PatchOutcomeKind.APPLIED,
    )
    assert _derive_iter_proposal_attempts(
        optimization_run_id="run_x",
        iteration=1,
        legacy_counter=0,
    ) == 0


def test_flag_on_uses_deriver(monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE", "1")

    from genie_space_optimizer.optimization.harness import (
        _derive_iter_proposal_attempts,
    )
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    for iid in ("intent_001", "intent_002", "intent_003"):
        emit_patch_outcome(
            optimization_run_id="run_x",
            iteration=1,
            ag_id="AG1",
            cluster_id="H001",
            intent_id=iid,
            outcome_kind=PatchOutcomeKind.APPLIED,
        )
    # Different iter — must NOT contribute.
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="AG1",
        cluster_id="H001",
        intent_id="intent_other_iter",
        outcome_kind=PatchOutcomeKind.APPLIED,
    )
    assert _derive_iter_proposal_attempts(
        optimization_run_id="run_x",
        iteration=1,
        legacy_counter=0,
    ) == 3


def test_flag_on_with_no_outcomes_returns_zero(monkeypatch):
    """Flag ON, but no outcomes were emitted this iteration → 0
    (matches the legacy behavior coincidentally; the cohort that
    would have a non-zero legacy counter doesn't exist in production
    today)."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE", "1")
    from genie_space_optimizer.optimization.harness import (
        _derive_iter_proposal_attempts,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )
    reset_patch_outcome_emitter()
    assert _derive_iter_proposal_attempts(
        optimization_run_id="run_x",
        iteration=1,
        legacy_counter=0,
    ) == 0


def test_deriver_exception_falls_back_to_legacy(monkeypatch):
    """A bug in the emitter introspection MUST fall back to the
    legacy counter — never crash the iteration's terminal write."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE", "1")

    from genie_space_optimizer.optimization import (
        patch_survival_emitter as _emitter_mod,
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated emitter introspection bug")

    monkeypatch.setattr(
        _emitter_mod, "emitted_intent_ids_for_iteration", _boom,
    )

    from genie_space_optimizer.optimization.harness import (
        _derive_iter_proposal_attempts,
    )
    result = _derive_iter_proposal_attempts(
        optimization_run_id="run_x",
        iteration=1,
        legacy_counter=42,  # the legacy fallback value
    )
    assert result == 42  # exception path returned legacy
