"""Persistence: write/read a QuestionStateInIteration to/from JSON on disk."""
import json
from pathlib import Path

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.persistence import (
    qstate_path,
    read_qstate,
    write_qstate,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)


def test_qstate_path_format(tmp_path: Path):
    p = qstate_path(run_root=tmp_path, iteration=2, qid="gs_009")
    assert p == tmp_path / "iteration_2" / "qstate_gs_009.json"


def test_write_then_read_roundtrip(tmp_path: Path):
    s = build_initial_state(
        qid="gs_009",
        iteration=1,
        seen=HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    )
    p = write_qstate(run_root=tmp_path, state=s)
    assert p.exists()
    payload = json.loads(p.read_text())
    assert payload["qid"] == "gs_009"
    s2 = read_qstate(p)
    assert s2 == s


def test_write_creates_iteration_dir(tmp_path: Path):
    s = build_initial_state(
        qid="gs_009",
        iteration=5,
        seen=HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 5),
    )
    write_qstate(run_root=tmp_path, state=s)
    assert (tmp_path / "iteration_5").is_dir()
