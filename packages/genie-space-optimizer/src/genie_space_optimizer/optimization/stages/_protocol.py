"""StageHandler Protocol: the contract every stage module conforms to."""

from __future__ import annotations

from typing import Any, Callable, ClassVar, Protocol, TypeVar, runtime_checkable


StageInputT = TypeVar("StageInputT")
StageOutputT = TypeVar("StageOutputT")


@runtime_checkable
class StageHandler(Protocol[StageInputT, StageOutputT]):
    """Every stage module's execute callable conforms to this Protocol.

    ``stage_key`` matches one of the 9 canonical stage keys defined in
    ``stages/_registry.py`` (and later in Phase H's
    ``run_output_contract.PROCESS_STAGE_ORDER``). Phase G-lite makes
    this Protocol runtime-checkable so a conformance test can use
    ``isinstance(module, StageHandler)`` directly.

    Note: Protocols only check method presence at runtime, not
    ClassVar values. The conformance test in
    ``tests/unit/test_stage_conformance.py`` checks ``STAGE_KEY`` /
    ``decision_producer`` via ``hasattr`` + value validation in
    addition to the ``isinstance`` check on ``execute()``.
    """

    stage_key: ClassVar[str]
    decision_producer: ClassVar[Callable[..., Any] | None]

    def execute(
        self, ctx: Any, inp: StageInputT,
    ) -> StageOutputT: ...
