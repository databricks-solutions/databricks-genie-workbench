"""StageHandler Protocol: the contract every stage module conforms to."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable


StageInputT = TypeVar("StageInputT")
StageOutputT = TypeVar("StageOutputT")


@runtime_checkable
class StageHandler(Protocol[StageInputT, StageOutputT]):
    """Every stage module's execute callable conforms to this Protocol.

    The Protocol intentionally requires only the ``execute`` method so
    that ``isinstance(module, StageHandler)`` succeeds for stage
    modules (which expose ``execute`` at module scope as an alias for
    their named verb — added by Phase G-lite Task 2).

    The 9 canonical stage keys live in ``stages/_registry.py`` (and
    later in Phase H's ``run_output_contract.PROCESS_STAGE_ORDER``).
    Each stage module surfaces its key as the module-level constant
    ``STAGE_KEY`` (uppercase). The conformance test in
    ``tests/unit/test_stage_conformance.py`` validates ``STAGE_KEY``
    via ``hasattr`` + value comparison, separately from the
    Protocol's runtime ``isinstance`` check.
    """

    def execute(
        self, ctx: Any, inp: StageInputT,
    ) -> StageOutputT: ...
