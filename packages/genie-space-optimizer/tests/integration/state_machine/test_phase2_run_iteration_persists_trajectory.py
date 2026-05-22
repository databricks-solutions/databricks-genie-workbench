"""After run_iteration, qstate_<qid>.json and trajectory_<qid>.json exist on disk."""
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from genie_space_optimizer.optimization.optimizer import (
    run_state_machine_iteration_and_persist,
)
from genie_space_optimizer.optimization.state_machine.persistence import (
    qstate_path,
    trajectory_path,
)


@dataclass
class _AbstainResponse:
    succeeded: bool = False
    parsed_output: object = None
    declined: object = "stub"


def test_iteration_creates_qstate_and_trajectory_files(tmp_path: Path):
    eval_rows = [{
        "question_id": "gs_dummy",
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "wrong",
        "sql": "SELECT 1",
        "expected_shape": "ROW_NUMBER",
    }]

    # Stage 1 abstain terminates immediately; the point is persistence contract.
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm._invoke_stage1_llm",
        return_value=_AbstainResponse(),
    ):
        run_state_machine_iteration_and_persist(
            eval_rows=eval_rows,
            iteration=1,
            run_id="run_xyz",
            run_root=tmp_path,
        )

    assert qstate_path(run_root=tmp_path, iteration=1, qid="gs_dummy").exists()
    assert trajectory_path(run_root=tmp_path, qid="gs_dummy").exists()
