from typing import ClassVar

from genie_space_optimizer.optimization.stages import StageContext, StageHandler


def test_stages_package_exports_context_and_protocol() -> None:
    assert StageContext is not None
    assert StageHandler is not None


def test_stage_context_has_required_fields() -> None:
    ctx = StageContext(
        run_id="r1",
        iteration=2,
        space_id="s1",
        domain="airline",
        catalog="main",
        schema="gso",
        apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda record: None,
        mlflow_anchor_run_id=None,
        feature_flags={},
    )

    assert ctx.run_id == "r1"
    assert ctx.iteration == 2
    assert ctx.apply_mode == "real"
    assert ctx.feature_flags == {}


def test_stage_handler_protocol_accepts_conforming_module() -> None:
    class FakeInput:
        pass

    class FakeOutput:
        pass

    class FakeStage:
        stage_key: ClassVar[str] = "evaluation_state"
        decision_producer = None

        def execute(self, ctx, inp):
            return FakeOutput()

    handler: StageHandler = FakeStage()
    assert handler.stage_key == "evaluation_state"
    out = handler.execute(None, FakeInput())
    assert isinstance(out, FakeOutput)


def test_stage_handler_is_runtime_checkable() -> None:
    """G-lite Task 1: StageHandler must be @runtime_checkable so the
    conformance test can use isinstance() checks."""
    # Protocols decorated with @runtime_checkable have a private
    # ``_is_runtime_protocol`` attribute set to True. Other Protocols
    # have it set to False or missing.
    assert getattr(StageHandler, "_is_runtime_protocol", False) is True


def test_stage_handler_isinstance_accepts_conforming_object() -> None:
    """G-lite Task 1: a class with execute() satisfies StageHandler at runtime."""

    class Conforming:
        stage_key = "evaluation_state"
        decision_producer = None

        def execute(self, ctx, inp):
            return None

    assert isinstance(Conforming(), StageHandler)


def test_stage_handler_isinstance_rejects_nonconforming_object() -> None:
    """G-lite Task 1: a class without execute() does NOT satisfy."""

    class NonConforming:
        stage_key = "evaluation_state"
        # no execute() method

    assert not isinstance(NonConforming(), StageHandler)
