"""Trial 13i — verify ``run_state_machine_iteration_and_persist`` wires
``_derive_schema_columns`` into the SM ``TransformerContext``.

Before Trial 13i the SM lane shipped ``"schema_columns": []`` to every
Stage 1 LLM call because nobody set the field on ``TransformerContext``.
The post-13h workbench replay surfaced ``insufficient_blame_set``
declines on capture-only QIDs that the LLM correctly produced given an
empty grounding universe. This test pins the wiring by intercepting the
``TransformerContext`` the SM run consumes and asserting the field is
populated.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@dataclass(frozen=True)
class _FakeEv:
    blame_set: tuple[str, ...]


def _stub_initial_states_with_qid(qid: str):
    """Return a non-empty initial-states list so the SM runner does NOT
    early-exit before reaching ``TransformerContext`` construction."""
    state = MagicMock()
    state.qid = qid
    state.deepest_stage_reached = MagicMock()
    return [state]


def test_run_sm_iteration_sets_schema_columns_from_typed_evidence(
    tmp_path: Path,
) -> None:
    """When ``metadata_snapshot["_rca_evidence_typed"]`` is populated the
    SM ctx receives a non-empty ``schema_columns`` tuple sourced from
    the typed evidence union."""
    from genie_space_optimizer.optimization import optimizer as _opt

    typed = {
        "gs_021": _FakeEv(
            blame_set=(
                "main.public.orders.revenue",
                "main.public.orders.customer_id",
            )
        ),
        "gs_024": _FakeEv(
            blame_set=(
                "main.public.payments.amount",
                "main.public.payments.currency_code",
            )
        ),
    }
    metadata_snapshot = {"_rca_evidence_typed": typed}

    captured: dict = {}

    def _capture_ctx_sm():
        sm = MagicMock()

        def _run_iteration(initial_states, ctx):
            captured["ctx"] = ctx
            return []

        sm.run_iteration = _run_iteration
        return sm

    with patch.object(
        _opt,
        "_build_state_machine_initial_states",
        return_value=_stub_initial_states_with_qid("gs_021"),
    ), patch(
        "genie_space_optimizer.optimization.state_machine.registry.build_production_state_machine",
        _capture_ctx_sm,
    ):
        _opt.run_state_machine_iteration_and_persist(
            run_root=str(tmp_path),
            eval_rows=[{"inputs/question_id": "gs_021"}],
            iteration=0,
            run_id="trial13i-test",
            workspace_client=None,
            space_id="space_x",
            metadata_snapshot=metadata_snapshot,
        )

    ctx = captured["ctx"]
    # Typed-evidence-union source -> 4 FQNs across the two QIDs.
    assert len(ctx.schema_columns) == 4
    assert "main.public.orders.revenue" in ctx.schema_columns
    assert "main.public.payments.currency_code" in ctx.schema_columns
    # Provenance label must record the source for the canary marker.
    assert ctx.schema_columns_source == "typed_evidence_union"


def test_run_sm_iteration_records_empty_source_when_no_signals(
    tmp_path: Path,
) -> None:
    """No typed evidence, no metadata_snapshot["schema_columns"], no
    identifier allowlist -> ``"empty"`` (deploy-block canary)."""
    from genie_space_optimizer.optimization import optimizer as _opt

    captured: dict = {}

    def _capture_ctx_sm():
        sm = MagicMock()

        def _run_iteration(initial_states, ctx):
            captured["ctx"] = ctx
            return []

        sm.run_iteration = _run_iteration
        return sm

    with patch.object(
        _opt,
        "_build_state_machine_initial_states",
        return_value=_stub_initial_states_with_qid("gs_x"),
    ), patch(
        "genie_space_optimizer.optimization.state_machine.registry.build_production_state_machine",
        _capture_ctx_sm,
    ):
        _opt.run_state_machine_iteration_and_persist(
            run_root=str(tmp_path),
            eval_rows=[{"inputs/question_id": "gs_x"}],
            iteration=0,
            run_id="trial13i-test",
            workspace_client=None,
            space_id="space_x",
            metadata_snapshot={},
        )

    ctx = captured["ctx"]
    assert ctx.schema_columns == ()
    assert ctx.schema_columns_source == "empty"
