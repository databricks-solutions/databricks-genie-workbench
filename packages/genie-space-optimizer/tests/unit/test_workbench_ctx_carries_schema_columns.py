"""Trial 13i — verify the workbench's ``run_workbench_iteration`` wires
``_derive_schema_columns`` into the SM ``TransformerContext``.

Mirror of ``test_state_machine_ctx_carries_schema_columns.py`` for the
``local_lever_workbench`` seam. The workbench feeds Trial 13 typed
evidence in via ``metadata_snapshot["_rca_evidence_typed"]`` already;
this test asserts that ``ctx.schema_columns`` is populated alongside.
"""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest


@dataclass(frozen=True)
class _FakeEv:
    blame_set: tuple[str, ...]


@pytest.fixture
def fake_bundle() -> MagicMock:
    """A minimal ``WorkbenchInputBundle`` shaped enough for the runner
    to construct a TransformerContext. We bypass the rest of the SM by
    mocking ``StateMachine``."""
    bundle = MagicMock()
    bundle.space_id = "space_x"
    bundle.metadata_snapshot = {
        "version": 1,
        "data_sources": {"tables": [], "metric_views": []},
        "instructions": {
            "example_question_sqls": [],
            "text_instructions": [],
        },
        "config": {"sample_questions": []},
    }
    bundle.eval_rows = (
        {
            "inputs/question_id": "gs_021",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
    )

    case = MagicMock()
    case.qid = "gs_021"
    case.typed_evidence = {
        "blame_set": [
            "main.public.orders.revenue",
            "main.public.orders.customer_id",
        ],
    }
    bundle.hard_cases = (case,)
    return bundle


@pytest.fixture
def fake_config() -> MagicMock:
    cfg = MagicMock()
    cfg.llm_mode = "sm-tape"
    cfg.apply_mode = "fake-record"
    cfg.iteration = 0
    cfg.tape_path = None
    cfg.llm_model = None
    cfg.profile = None
    return cfg


def test_workbench_iteration_sets_schema_columns_from_typed_evidence(
    fake_bundle: MagicMock, fake_config: MagicMock,
) -> None:
    """The workbench typed-evidence rebuild must result in a non-empty
    ``ctx.schema_columns`` whose source is ``"typed_evidence_union"``."""
    from local_lever_workbench import local_runner as _lr

    captured: dict = {}

    class _FakeStateMachine:
        def __init__(self, transformers):
            captured["transformers"] = transformers

        def run_iteration(self, initial_states, ctx):
            captured["ctx"] = ctx
            return []

    # Make sure typed-evidence rebuild returns a stable shape.
    def _stub_rebuild(typed_payload):
        return _FakeEv(blame_set=tuple(typed_payload.get("blame_set", ())))

    def _stub_build_initial_states(*, eval_rows, iteration):
        s = MagicMock()
        s.qid = "gs_021"
        return [s]

    with (
        patch(
            "genie_space_optimizer.optimization.state_machine.orchestrator.StateMachine",
            _FakeStateMachine,
        ),
        patch(
            "genie_space_optimizer.optimization.state_machine.transformers.dispatch_input.build_initial_states_from_eval_rows",
            _stub_build_initial_states,
        ),
        patch(
            "local_lever_workbench.stage1_probe._rebuild_typed_evidence",
            _stub_rebuild,
        ),
        patch.object(_lr, "_build_registry", return_value=()),
        patch.object(_lr, "_tape_patch_or_noop"),
    ):
        _lr.run_workbench_iteration(fake_bundle, fake_config)

    ctx = captured["ctx"]
    assert "main.public.orders.revenue" in ctx.schema_columns
    assert "main.public.orders.customer_id" in ctx.schema_columns
    assert ctx.schema_columns_source == "typed_evidence_union"


def test_workbench_iteration_records_empty_source_for_capture_bundle(
    fake_config: MagicMock,
) -> None:
    """Capture bundles (no typed evidence) currently surface the
    Trial 13i bottleneck cleanly — the ctx carries ``schema_columns_source
    == "empty"`` so the canary fires before Stage 1 is invoked."""
    from local_lever_workbench import local_runner as _lr

    bundle = MagicMock()
    bundle.space_id = "space_x"
    bundle.metadata_snapshot = {
        "version": 1,
        "data_sources": {"tables": [], "metric_views": []},
        "instructions": {
            "example_question_sqls": [],
            "text_instructions": [],
        },
        "config": {"sample_questions": []},
    }
    bundle.eval_rows = (
        {"inputs/question_id": "gs_009"},
    )
    case = MagicMock()
    case.qid = "gs_009"
    case.typed_evidence = None  # capture path has no typed evidence
    bundle.hard_cases = (case,)

    captured: dict = {}

    class _FakeStateMachine:
        def __init__(self, transformers):
            captured["transformers"] = transformers

        def run_iteration(self, initial_states, ctx):
            captured["ctx"] = ctx
            return []

    def _stub_build_initial_states(*, eval_rows, iteration):
        s = MagicMock()
        s.qid = "gs_009"
        return [s]

    with (
        patch(
            "genie_space_optimizer.optimization.state_machine.orchestrator.StateMachine",
            _FakeStateMachine,
        ),
        patch(
            "genie_space_optimizer.optimization.state_machine.transformers.dispatch_input.build_initial_states_from_eval_rows",
            _stub_build_initial_states,
        ),
        patch.object(_lr, "_build_registry", return_value=()),
        patch.object(_lr, "_tape_patch_or_noop"),
    ):
        _lr.run_workbench_iteration(bundle, fake_config)

    ctx = captured["ctx"]
    assert ctx.schema_columns == ()
    assert ctx.schema_columns_source == "empty"
