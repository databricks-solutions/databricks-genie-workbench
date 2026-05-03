"""StageHandler Protocol: the contract every stage module conforms to."""

from __future__ import annotations

from typing import Any, Callable, ClassVar, Protocol, TypeVar


StageInputT = TypeVar("StageInputT")
StageOutputT = TypeVar("StageOutputT")


class StageHandler(Protocol[StageInputT, StageOutputT]):
    """Every stage module's execute callable conforms to this Protocol.

    ``stage_key`` matches one of PROCESS_STAGE_ORDER's keys (defined in
    Phase H's run_output_contract.py). Phase G ratchets this Protocol
    into a runtime conformance test that fails CI when a new stage
    module is added without registering its stage_key.
    """

    stage_key: ClassVar[str]
    decision_producer: ClassVar[Callable[..., Any] | None]

    def execute(
        self, ctx: Any, inp: StageInputT,
    ) -> StageOutputT: ...
