"""C15 Phase 2 Task 2.6 — Chunk A stage boundary-fixture replay tests.

Covers four stages × two anchors = up to 8 parametrized replay cases:

  Stage                  | Anchors
  -----------------------|------------------------------
  evaluation_state       | airline_iter01, 7now_iter01
  rca_evidence           | airline_iter01, 7now_iter01
  cluster_formation      | airline_iter01, 7now_iter01
  strategist_context     | (no production fixture — new stage)

Fixture availability:
  evaluation_state, rca_evidence, cluster_formation — production archives
  exist (docs/runid_analysis/1099b152.../stages/01_evaluation_state/ etc.)
  but the PII audit in capture_stage_fixture.py blocks capture because
  eval rows carry free-text judge rationale (arbiter/rationale,
  genie_equivalent_eval, etc.) that are not in REDACTION_FIELDS. These
  fields are structural LLM outputs, not customer PII per se, but the
  audit treats any string > 200 chars as suspect. Adding these fields to
  REDACTION_FIELDS would be a separate PR.

  strategist_context — new stage introduced in C15 Phase 2. No production
  run has yet executed this stage; no archive evidence exists. Contract
  correctness is covered by tests/unit/test_strategist_context_contract.py.

  When a stage fixture directory is absent, cases_for_chunk() returns no
  cases for that stage and the test is simply not parametrized (skipped).
  This is not a test failure — it's the expected state for Phase 2.
  Fixture capture is tracked as a follow-up work item.
"""

from __future__ import annotations

import pytest

from tests.integration._replay_helpers import assert_replay_matches, cases_for_chunk


_CHUNK_A_STAGES = (
    "evaluation_state",
    "cluster_formation",
    "rca_evidence",
    "strategist_context",
)

_CASES = cases_for_chunk(_CHUNK_A_STAGES)

# pytest requires at least one parametrize value to avoid a confusing
# NOTSET skip. Use a sentinel that immediately skips when no fixtures exist.
_PARAMETRIZE_CASES = _CASES or [pytest.param("__no_fixture__", "__no_fixture__",
                                              id="no-fixture")]


@pytest.mark.parametrize("anchor,stage_key", _PARAMETRIZE_CASES)
def test_chunk_a_replay(anchor: str, stage_key: str) -> None:
    """Replay: from_json → execute → to_json matches expected_output.json."""
    if anchor == "__no_fixture__":
        pytest.skip(
            "No Chunk A boundary fixtures available yet. "
            "Capture is blocked by PII audit on eval_rows free-text fields "
            "(arbiter/rationale etc.). "
            "strategist_context is a new stage with no production evidence. "
            "Coverage provided by unit contract tests."
        )
    assert_replay_matches(anchor, stage_key)


def test_chunk_a_replay_infrastructure_is_wired() -> None:
    """Smoke test: the replay helper can locate anchor directories and
    cases_for_chunk() returns a list (possibly empty if no fixtures yet)."""
    from tests.integration._replay_helpers import anchor_dirs, cases_for_chunk

    dirs = anchor_dirs()
    # anchor dirs may exist even if no Chunk A fixtures are present
    assert isinstance(dirs, list)
    cases = cases_for_chunk(_CHUNK_A_STAGES)
    assert isinstance(cases, list)
    # Each case (if any) must be a pytest.param with (anchor, stage_key)
    for case in cases:
        assert len(case.values) == 2
