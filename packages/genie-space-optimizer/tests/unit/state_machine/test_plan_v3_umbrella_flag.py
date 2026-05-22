"""Plan v3 umbrella flag — gates the state-machine iteration callsite."""
import os
from unittest.mock import patch


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan_v3_state_machine_iteration_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN_V3_STATE_MACHINE_ITERATION", None)
        assert plan_v3_state_machine_iteration_enabled() is False


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan_v3_state_machine_iteration_enabled,
    )
    for val in ("true", "1", "yes", "on"):
        with patch.dict(os.environ, {"GSO_PLAN_V3_STATE_MACHINE_ITERATION": val}):
            assert plan_v3_state_machine_iteration_enabled() is True, (
                f"Expected True for {val!r}"
            )


def test_notebook_stamps_flag_via_setdefault():
    """The lever-loop notebook must stamp GSO_PLAN_V3_STATE_MACHINE_ITERATION=true
    via setdefault so deployed runs activate the state machine while
    local unit tests stay byte-stable."""
    from pathlib import Path
    text = (
        Path(__file__).parent.parent.parent.parent
        / "src" / "genie_space_optimizer" / "jobs"
        / "run_lever_loop.py"
    ).read_text()
    assert "GSO_PLAN_V3_STATE_MACHINE_ITERATION" in text
    assert 'setdefault("GSO_PLAN_V3_STATE_MACHINE_ITERATION"' in text
