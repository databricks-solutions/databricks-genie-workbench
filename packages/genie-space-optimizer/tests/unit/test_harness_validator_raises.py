"""Pin that raise_on_violation=True actually raises."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_validator_raises_when_toggle_is_true() -> None:
    from genie_space_optimizer.optimization.harness import (
        _validate_journeys_at_iteration_end,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyContractViolationError,
    )

    events = [QuestionJourneyEvent(question_id="gs_001", stage="evaluated")]
    with pytest.raises(JourneyContractViolationError):
        _validate_journeys_at_iteration_end(
            events=events,
            eval_qids=["gs_001"],
            iteration=1,
            raise_on_violation=True,
        )
