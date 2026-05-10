"""C15 Phase 1 Task 1.8 — Chunk D stage boundary-fixture replay tests.

Covers four stages × two anchors = 8 parametrized replay cases:

  Stage                  | Anchors
  -----------------------|------------------------------
  acceptance_decision    | airline_iter01, 7now_iter01
  learning_next_action   | airline_iter01, 7now_iter01
  bundle_assembly        | airline_iter01, 7now_iter01
  run_manifest           | airline_iter01, 7now_iter01

Each test:
  1. Loads ``input.json`` → ``INPUT_CLASS.from_json()``.
  2. Calls ``execute(ctx=None/FakeCtx, inp)`` (the typed execute).
  3. Compares ``out.to_json()`` to ``expected_output.json`` key-by-key.

Acceptance criteria (plan §Task 1.8):
  * All 8 cases green.
  * Zero REDACTED text leaking into imported classes.
  * Tests are skipped (not failed) if a fixture directory is absent.

Anchors:
  * airline_1105451933925748_iter01 — D-6 anchor: missing_pre_rows
    rollback (rolled_back outcome, reason_code=missing_pre_rows).
  * 7now_960148942255012_iter01    — D-5/D-7 anchor: accepted_with_
    attribution_drift keep-the-win path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


# ── fixture root ──────────────────────────────────────────────────────────────

_FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "integration"
    / "fixtures"
)

_ANCHOR_AIRLINE = "airline_1105451933925748_iter01"
_ANCHOR_7NOW = "7now_960148942255012_iter01"

_STAGES = [
    "acceptance_decision",
    "learning_next_action",
    "bundle_assembly",
    "run_manifest",
]


def _fixture_path(anchor: str, stage: str, filename: str) -> Path:
    return _FIXTURE_ROOT / anchor / stage / filename


def _load_fixture(anchor: str, stage: str, filename: str) -> dict[str, Any] | None:
    p = _fixture_path(anchor, stage, filename)
    if not p.exists():
        return None
    return json.loads(p.read_text())


# ── minimal fake context (stages that call ctx.decision_emit / ctx.journey_emit) ─

class _FakeCtx:
    run_id: str = "test_replay_run_001"
    iteration: int = 1

    def decision_emit(self, record: Any) -> None:  # noqa: ANN401
        pass

    def journey_emit(self, **kwargs: Any) -> None:  # noqa: ANN401
        pass


# ── parametrize: (anchor, stage) pairs ───────────────────────────────────────

_PARAM_IDS = [
    (anchor, stage)
    for anchor in (_ANCHOR_AIRLINE, _ANCHOR_7NOW)
    for stage in _STAGES
]


def _param_id(anchor: str, stage: str) -> str:
    return f"{anchor}/{stage}"


# ── acceptance_decision replay ────────────────────────────────────────────────

@pytest.mark.parametrize("anchor", [_ANCHOR_AIRLINE, _ANCHOR_7NOW], ids=lambda a: a)
def test_acceptance_decision_replay(anchor: str) -> None:
    """Replay acceptance_decision: from_json → decide → to_json matches expected."""
    inp_data = _load_fixture(anchor, "acceptance_decision", "input.json")
    exp_data = _load_fixture(anchor, "acceptance_decision", "expected_output.json")
    if inp_data is None or exp_data is None:
        pytest.skip(f"acceptance_decision fixtures not found for anchor {anchor}")

    from genie_space_optimizer.optimization.stages.acceptance import (
        AcceptanceInput,
        decide,
    )

    inp = AcceptanceInput.from_json(inp_data)
    out = decide(_FakeCtx(), inp)
    got = out.to_json()

    # outcomes_by_ag: compare key-by-key for deterministic assertion messages
    assert set(got["outcomes_by_ag"].keys()) == set(exp_data["outcomes_by_ag"].keys()), (
        f"outcomes_by_ag keys mismatch: got={sorted(got['outcomes_by_ag'])} "
        f"exp={sorted(exp_data['outcomes_by_ag'])}"
    )
    for ag_id in exp_data["outcomes_by_ag"]:
        exp_ag = exp_data["outcomes_by_ag"][ag_id]
        got_ag = got["outcomes_by_ag"][ag_id]
        assert got_ag["outcome"] == exp_ag["outcome"], (
            f"[{anchor}/{ag_id}] outcome: got {got_ag['outcome']!r} "
            f"exp {exp_ag['outcome']!r}"
        )
        assert got_ag["reason_code"] == exp_ag["reason_code"], (
            f"[{anchor}/{ag_id}] reason_code: got {got_ag['reason_code']!r} "
            f"exp {exp_ag['reason_code']!r}"
        )
        assert sorted(got_ag.get("target_qids") or []) == sorted(
            exp_ag.get("target_qids") or []
        ), f"[{anchor}/{ag_id}] target_qids mismatch"

    # qid_resolutions
    assert got["qid_resolutions"] == exp_data["qid_resolutions"], (
        f"[{anchor}] qid_resolutions mismatch:\n"
        f"  got: {got['qid_resolutions']}\n"
        f"  exp: {exp_data['qid_resolutions']}"
    )

    # rolled_back_content_fingerprints (order-insensitive)
    assert sorted(got.get("rolled_back_content_fingerprints") or []) == sorted(
        exp_data.get("rolled_back_content_fingerprints") or []
    ), f"[{anchor}] rolled_back_content_fingerprints mismatch"


# ── learning_next_action replay ───────────────────────────────────────────────

@pytest.mark.parametrize("anchor", [_ANCHOR_AIRLINE, _ANCHOR_7NOW], ids=lambda a: a)
def test_learning_next_action_replay(anchor: str) -> None:
    """Replay learning_next_action: from_json → execute → to_json matches expected."""
    inp_data = _load_fixture(anchor, "learning_next_action", "input.json")
    exp_data = _load_fixture(anchor, "learning_next_action", "expected_output.json")
    if inp_data is None or exp_data is None:
        pytest.skip(f"learning_next_action fixtures not found for anchor {anchor}")

    from genie_space_optimizer.optimization.stages.learning import (
        LearningInput,
        execute,
    )

    inp = LearningInput.from_json(inp_data)
    out = execute(None, inp)
    got = out.to_json()

    # iteration_summaries: exactly one per fixture
    assert len(got["iteration_summaries"]) == len(exp_data["iteration_summaries"]), (
        f"[{anchor}] iteration_summaries count: "
        f"got {len(got['iteration_summaries'])} exp {len(exp_data['iteration_summaries'])}"
    )
    for i, (got_s, exp_s) in enumerate(
        zip(got["iteration_summaries"], exp_data["iteration_summaries"])
    ):
        assert got_s["iteration"] == exp_s["iteration"], (
            f"[{anchor}][summary {i}] iteration mismatch"
        )
        assert got_s["verdict"] == exp_s["verdict"], (
            f"[{anchor}][summary {i}] verdict: got {got_s['verdict']!r} "
            f"exp {exp_s['verdict']!r}"
        )
        assert got_s["attempted"] == exp_s["attempted"], (
            f"[{anchor}][summary {i}] attempted mismatch"
        )
        assert got_s["candidate_accuracy"] == pytest.approx(exp_s["candidate_accuracy"], abs=0.01), (
            f"[{anchor}][summary {i}] candidate_accuracy mismatch"
        )
        assert got_s["baseline_accuracy"] == pytest.approx(exp_s["baseline_accuracy"], abs=0.01), (
            f"[{anchor}][summary {i}] baseline_accuracy mismatch"
        )

    # terminate flag
    assert got["terminate"] == exp_data["terminate"], (
        f"[{anchor}] terminate flag mismatch"
    )


# ── bundle_assembly replay ────────────────────────────────────────────────────

@pytest.mark.parametrize("anchor", [_ANCHOR_AIRLINE, _ANCHOR_7NOW], ids=lambda a: a)
def test_bundle_assembly_replay(anchor: str) -> None:
    """Replay bundle_assembly: from_json → assemble → to_json matches expected.

    Binary criterion D-4: exception must be None (no AttributeError).
    list_normalizations must match expected.
    normalized_captures must round-trip through the stage with correct shapes.
    """
    inp_data = _load_fixture(anchor, "bundle_assembly", "input.json")
    exp_data = _load_fixture(anchor, "bundle_assembly", "expected_output.json")
    if inp_data is None or exp_data is None:
        pytest.skip(f"bundle_assembly fixtures not found for anchor {anchor}")

    from genie_space_optimizer.optimization.stages.bundle_assembly import (
        BundleAssemblyInput,
        execute,
    )

    inp = BundleAssemblyInput.from_json(inp_data)
    out = execute(None, inp)
    got = out.to_json()

    # D-4 criterion: no exception
    assert got["exception"] is None, (
        f"[{anchor}] bundle_assembly raised an exception: {got['exception']}"
    )

    # list_normalizations: order-insensitive
    assert sorted(got.get("list_normalizations") or []) == sorted(
        exp_data.get("list_normalizations") or []
    ), f"[{anchor}] list_normalizations mismatch"

    # normalized_captures: stage keys present
    exp_keys = set((exp_data.get("normalized_captures") or {}).keys())
    got_keys = set((got.get("normalized_captures") or {}).keys())
    assert got_keys == exp_keys, (
        f"[{anchor}] normalized_captures keys: got={sorted(got_keys)} "
        f"exp={sorted(exp_keys)}"
    )

    # Each normalized capture must be a dict (not a list) — D-4 safety check
    for stage_key, norm_capture in (got.get("normalized_captures") or {}).items():
        assert isinstance(norm_capture, dict), (
            f"[{anchor}] normalized_captures[{stage_key!r}] is not a dict: "
            f"{type(norm_capture).__name__}"
        )


# ── run_manifest replay ───────────────────────────────────────────────────────

@pytest.mark.parametrize("anchor", [_ANCHOR_AIRLINE, _ANCHOR_7NOW], ids=lambda a: a)
def test_run_manifest_replay(anchor: str) -> None:
    """Replay run_manifest: from_json → resolve_run_manifest → to_json matches expected.

    Binary criterion D-5: resolution_path must not be 'sentinel' when env keys
    are provided; fields_resolved must equal expected.
    """
    inp_data = _load_fixture(anchor, "run_manifest", "input.json")
    exp_data = _load_fixture(anchor, "run_manifest", "expected_output.json")
    if inp_data is None or exp_data is None:
        pytest.skip(f"run_manifest fixtures not found for anchor {anchor}")

    from genie_space_optimizer.optimization.stages.run_manifest import (
        RunManifestInput,
        execute,
    )

    inp = RunManifestInput.from_json(inp_data)
    out = execute(None, inp)
    got = out.to_json()

    # resolution_path
    assert got["resolution_path"] == exp_data["resolution_path"], (
        f"[{anchor}] resolution_path: got {got['resolution_path']!r} "
        f"exp {exp_data['resolution_path']!r}"
    )

    # fields_resolved (D-5 criterion: non-zero when env vars present)
    assert got["fields_resolved"] == exp_data["fields_resolved"], (
        f"[{anchor}] fields_resolved: got {got['fields_resolved']} "
        f"exp {exp_data['fields_resolved']}"
    )
    assert got["fields_total"] == exp_data["fields_total"], (
        f"[{anchor}] fields_total mismatch"
    )

    # dbutils flags
    assert got["dbutils_attempted"] == exp_data["dbutils_attempted"], (
        f"[{anchor}] dbutils_attempted mismatch"
    )
    assert got["dbutils_succeeded"] == exp_data["dbutils_succeeded"], (
        f"[{anchor}] dbutils_succeeded mismatch"
    )

    # D-5 criterion: when resolution_path != 'sentinel', IDs must not be 'unknown'
    if got["resolution_path"] != "sentinel":
        for field_name in (
            "databricks_job_id",
            "databricks_parent_run_id",
            "lever_loop_task_run_id",
        ):
            assert got[field_name] != "unknown", (
                f"[{anchor}] {field_name} is sentinel 'unknown' but "
                f"resolution_path={got['resolution_path']!r}"
            )
