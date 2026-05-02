"""Pin that the eval-result carrier refresh works on rolled-back iterations.

Regression test for the Phase A burn-down bug where every iteration of two
separate real-Genie runs (airline + 7now) produced empty `eval_rows` because
`_latest_eval_result = full_result or {}` only ran on the accept branch of
the AG decision, while every iteration in those runs hit the rollback branch.
The fix moved the carrier refresh to right after `_run_gate_checks` returns,
via the `_extract_eval_result_from_gate` helper tested here.

These tests are pure-Python: they exercise the helpers directly without
standing up Spark, MLflow, Genie, or a full `_run_lever_loop` scope.
"""

from __future__ import annotations


def test_extract_eval_result_returns_full_result_on_accept_path() -> None:
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "passed": True,
        "full_result": {
            "question_ids": ["q_001", "q_002"],
            "scores": {"q_001": "yes", "q_002": "no"},
            "failure_question_ids": ["q_002"],
        },
    }
    out = _extract_eval_result_from_gate(gate_result)
    assert out["question_ids"] == ["q_001", "q_002"]
    assert out["scores"]["q_001"] == "yes"


def test_extract_eval_result_returns_failed_eval_result_on_rollback_path() -> None:
    """The bug: rolled-back iterations were silently skipping the carrier refresh.

    The fix uses `failed_eval_result` (which `_run_gate_checks` populates on
    rollback) so the carrier still tracks the most recent measurement.
    """
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "passed": False,
        "rollback_reason": "regression",
        "failed_eval_result": {
            "question_ids": ["q_001", "q_002", "q_003"],
            "scores": {"q_001": "yes", "q_002": "no", "q_003": "no"},
            "failure_question_ids": ["q_002", "q_003"],
        },
    }
    out = _extract_eval_result_from_gate(gate_result)
    assert out["question_ids"] == ["q_001", "q_002", "q_003"]
    assert set(out["failure_question_ids"]) == {"q_002", "q_003"}


def test_extract_eval_result_prefers_full_result_when_both_present() -> None:
    """Defensive: if a gate has both keys, full_result wins (the canonical accept payload)."""
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "full_result": {"question_ids": ["from_full"]},
        "failed_eval_result": {"question_ids": ["from_failed"]},
    }
    out = _extract_eval_result_from_gate(gate_result)
    assert out["question_ids"] == ["from_full"]


def test_extract_eval_result_returns_empty_when_neither_key_present() -> None:
    """Sentinel for "do not overwrite the carrier" — caller checks `if _gate_eval:`."""
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    assert _extract_eval_result_from_gate({}) == {}
    assert _extract_eval_result_from_gate({"passed": True}) == {}
    assert _extract_eval_result_from_gate({"full_result": None}) == {}
    assert _extract_eval_result_from_gate({"full_result": {}}) == {}


def test_extract_eval_result_handles_non_dict_input() -> None:
    """Defensive against accidental None / list inputs (gate_result should always
    be a dict, but the carrier-refresh path runs in a wrap-everything-defensively
    region of `_run_lever_loop`)."""
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    assert _extract_eval_result_from_gate(None) == {}  # type: ignore[arg-type]
    assert _extract_eval_result_from_gate([]) == {}  # type: ignore[arg-type]
    assert _extract_eval_result_from_gate("not a dict") == {}  # type: ignore[arg-type]


def test_build_fixture_eval_rows_uses_scores_when_available() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
    )

    rows = _build_fixture_eval_rows({
        "question_ids": ["q_001", "q_002", "q_003"],
        "scores": {"q_001": "yes", "q_002": "no", "q_003": "pass"},
        "arbiter_verdicts": {"q_001": "both_correct", "q_002": "ground_truth_correct"},
    })
    by_qid = {r["question_id"]: r for r in rows}
    assert by_qid["q_001"] == {
        "question_id": "q_001",
        "result_correctness": "yes",
        "arbiter": "both_correct",
    }
    assert by_qid["q_002"]["result_correctness"] == "no"
    assert by_qid["q_002"]["arbiter"] == "ground_truth_correct"
    assert by_qid["q_003"]["result_correctness"] == "yes"
    assert "arbiter" not in by_qid["q_003"]


def test_build_fixture_eval_rows_falls_back_to_failure_set_when_scores_missing() -> None:
    """When `scores` is empty, derive correctness from `failure_question_ids`."""
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
    )

    rows = _build_fixture_eval_rows({
        "question_ids": ["q_001", "q_002"],
        "failure_question_ids": ["q_002"],
    })
    by_qid = {r["question_id"]: r["result_correctness"] for r in rows}
    assert by_qid == {"q_001": "yes", "q_002": "no"}


def test_build_fixture_eval_rows_returns_empty_when_no_qids() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
    )

    assert _build_fixture_eval_rows({}) == []
    assert _build_fixture_eval_rows({"question_ids": []}) == []
    assert _build_fixture_eval_rows(None) == []  # type: ignore[arg-type]


def test_carrier_helpers_compose_to_recover_rolled_back_iter_data() -> None:
    """End-to-end shape test: gate rolls back → helpers produce a fixture-shape eval_rows.

    Pins the behavior the Phase A burn-down needed: a rolled-back gate must
    still feed real eval data into the iteration snapshot.
    """
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "passed": False,
        "rollback_reason": "content_regression",
        "failed_eval_result": {
            "question_ids": ["airline_gs_001", "airline_gs_002"],
            "scores": {"airline_gs_001": "yes", "airline_gs_002": "no"},
            "failure_question_ids": ["airline_gs_002"],
            "arbiter_verdicts": {"airline_gs_002": "ground_truth_correct"},
        },
    }
    eval_payload = _extract_eval_result_from_gate(gate_result)
    rows = _build_fixture_eval_rows(eval_payload)
    assert len(rows) == 2
    assert {r["question_id"] for r in rows} == {"airline_gs_001", "airline_gs_002"}
    by_qid = {r["question_id"]: r for r in rows}
    assert by_qid["airline_gs_001"]["result_correctness"] == "yes"
    assert by_qid["airline_gs_002"]["result_correctness"] == "no"
    assert by_qid["airline_gs_002"]["arbiter"] == "ground_truth_correct"


# ---------------------------------------------------------------------------
# _seed_eval_result_from_baseline_iter — Phase A burn-down cycle 4 follow-up.
#
# Cycle 4 (run 4fc43ffe) revealed a second failure mode: every iteration's AG
# hit the applier blast-radius gate, which dropped 7/7 patches and forced a
# `continue` BEFORE `_run_gate_checks` was called. The cycle-3 carrier-refresh
# fix runs after the gate, so it never fires for these iterations. Solution:
# lazy-seed the carrier from `baseline_iter` at iteration start whenever the
# carrier is empty. These tests pin the helper that powers that fallback.
# ---------------------------------------------------------------------------


def test_seed_eval_result_handles_list_rows_json() -> None:
    """The realistic shape: load_latest_full_iteration may return rows_json
    already deserialised as a list of dicts."""
    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    baseline_iter = {
        "rows_json": [
            {"question_id": "q_001", "result_correctness": "yes",
             "arbiter": "both_correct"},
            {"question_id": "q_002", "result_correctness": "no",
             "arbiter": "ground_truth_correct"},
            {"question_id": "q_003", "result_correctness": "yes"},
        ],
    }
    out = _seed_eval_result_from_baseline_iter(baseline_iter)
    assert out["question_ids"] == ["q_001", "q_002", "q_003"]
    assert out["scores"] == {"q_001": "yes", "q_002": "no", "q_003": "yes"}
    assert out["arbiter_verdicts"] == {
        "q_001": "both_correct",
        "q_002": "ground_truth_correct",
    }
    assert out["failure_question_ids"] == ["q_002"]


def test_seed_eval_result_handles_string_rows_json() -> None:
    """The other realistic shape: rows_json is a JSON-encoded string column."""
    import json

    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    baseline_iter = {
        "rows_json": json.dumps([
            {"question_id": "qid_a", "result_correctness": "no"},
            {"id": "qid_b", "result_correctness": "yes"},
        ]),
    }
    out = _seed_eval_result_from_baseline_iter(baseline_iter)
    assert out["question_ids"] == ["qid_a", "qid_b"]
    assert out["scores"] == {"qid_a": "no", "qid_b": "yes"}
    assert out["failure_question_ids"] == ["qid_a"]


def test_seed_eval_result_returns_empty_for_unusable_inputs() -> None:
    """Empty-dict sentinel is the contract caller relies on to fall through."""
    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    assert _seed_eval_result_from_baseline_iter(None) == {}
    assert _seed_eval_result_from_baseline_iter({}) == {}
    assert _seed_eval_result_from_baseline_iter({"rows_json": None}) == {}
    assert _seed_eval_result_from_baseline_iter({"rows_json": "[]"}) == {}
    assert _seed_eval_result_from_baseline_iter({"rows_json": "not json"}) == {}
    assert _seed_eval_result_from_baseline_iter({"rows_json": []}) == {}


def test_seed_eval_result_returns_empty_when_rows_lack_question_ids() -> None:
    """Cycle 4 specific failure mode: rows present but no question_id/id key.

    The new module logs a warning when this happens; the helper returns {} so
    the caller (lazy seed at iteration start) treats it as "no usable
    baseline" rather than emitting a degenerate `_latest_eval_result` with an
    empty `question_ids` list.
    """
    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    baseline_iter = {
        "rows_json": [
            {"result_correctness": "yes", "some_other_field": "abc"},
            {"arbiter": "both_correct"},
        ],
    }
    assert _seed_eval_result_from_baseline_iter(baseline_iter) == {}


def test_seed_eval_result_handles_supported_correctness_synonyms() -> None:
    """`_rc_str` returns lowercased values; the seed treats yes/true/1/pass as PASS."""
    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    baseline_iter = {
        "rows_json": [
            {"question_id": "q1", "result_correctness": "yes"},
            {"question_id": "q2", "result_correctness": "true"},
            {"question_id": "q3", "result_correctness": "1"},
            {"question_id": "q4", "result_correctness": "pass"},
            {"question_id": "q5", "result_correctness": "no"},
            {"question_id": "q6", "result_correctness": "FAIL"},
            {"question_id": "q7", "result_correctness": "anything_else"},
        ],
    }
    out = _seed_eval_result_from_baseline_iter(baseline_iter)
    assert out["scores"] == {
        "q1": "yes", "q2": "yes", "q3": "yes", "q4": "yes",
        "q5": "no", "q6": "no", "q7": "no",
    }
    assert set(out["failure_question_ids"]) == {"q5", "q6", "q7"}


def test_seed_eval_result_skips_non_dict_entries() -> None:
    """Defensive: rows_json that contains stray strings/None entries should be
    silently skipped rather than crashing the seed."""
    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    baseline_iter = {
        "rows_json": [
            "not_a_dict",
            None,
            {"question_id": "q_only", "result_correctness": "yes"},
            42,
        ],
    }
    out = _seed_eval_result_from_baseline_iter(baseline_iter)
    assert out["question_ids"] == ["q_only"]
    assert out["scores"] == {"q_only": "yes"}


def test_seed_eval_result_powers_lazy_snapshot_fallback() -> None:
    """End-to-end shape test for the cycle 4 fix.

    Setup mirrors the real failure: the AG decision skips the gate (applier
    drops all patches), so `_extract_eval_result_from_gate` never fires.
    The lazy seed at iteration start must produce a populated payload that
    `_build_fixture_eval_rows` then formats for the replay fixture.
    """
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
        _seed_eval_result_from_baseline_iter,
    )

    baseline_iter = {
        "rows_json": [
            {"question_id": "7now_gs_001", "result_correctness": "no",
             "arbiter": "ground_truth_correct"},
            {"question_id": "7now_gs_002", "result_correctness": "yes"},
        ],
    }
    seeded = _seed_eval_result_from_baseline_iter(baseline_iter)
    rows = _build_fixture_eval_rows(seeded)
    assert {r["question_id"] for r in rows} == {"7now_gs_001", "7now_gs_002"}
    by_qid = {r["question_id"]: r for r in rows}
    assert by_qid["7now_gs_001"]["result_correctness"] == "no"
    assert by_qid["7now_gs_001"]["arbiter"] == "ground_truth_correct"
    assert by_qid["7now_gs_002"]["result_correctness"] == "yes"


# ---------------------------------------------------------------------------
# _baseline_row_qid + cycle 5 MLflow eval-table row shape.
#
# Cycle 5 fired the new "Phase A: baseline payload had N rows but 0 carried a
# question_id/id key" diagnostic and printed the ACTUAL row keys from a real
# airline run:
#   ['_asi_source', 'arbiter/metadata', 'arbiter/rationale', 'arbiter/value',
#    'asset_routing/metadata', 'asset_routing/rationale', 'asset_routing/value',
#    'client_request_id', 'completeness/metadata', 'completeness/rationale',
#    'completeness/value', 'execution_duration', 'expected_asset/metadata',
#    'expected_asset/value', 'expected_response/metadata',
#    'expected_response/value', 'logical_accuracy/metadata',
#    'logical_accuracy/rationale', 'logical_accuracy/value', 'request']
#
# These tests pin that the helper extracts qids from THIS exact key-set so
# cycle 6 cannot regress on the same root cause.
# ---------------------------------------------------------------------------


CYCLE_5_MLFLOW_ROW_KEYS = (
    "_asi_source",
    "arbiter/metadata",
    "arbiter/rationale",
    "arbiter/value",
    "asset_routing/metadata",
    "asset_routing/rationale",
    "asset_routing/value",
    "client_request_id",
    "completeness/metadata",
    "completeness/rationale",
    "completeness/value",
    "execution_duration",
    "expected_asset/metadata",
    "expected_asset/value",
    "expected_response/metadata",
    "expected_response/value",
    "logical_accuracy/metadata",
    "logical_accuracy/rationale",
    "logical_accuracy/value",
    "request",
)


def _make_cycle_5_baseline_row(qid: str, arbiter_value: str = "both_correct") -> dict:
    """Construct a baseline row whose key-set exactly matches the cycle 5
    diagnostic output. Used by the regression tests below to prove the
    helper handles the real MLflow eval-table row shape, not just the
    canonical ``question_id`` shape.
    """
    row = {k: "stub" for k in CYCLE_5_MLFLOW_ROW_KEYS}
    row["client_request_id"] = qid
    row["arbiter/value"] = arbiter_value
    return row


def test_baseline_row_qid_extracts_client_request_id_from_mlflow_shape() -> None:
    """Cycle 5 regression: the realistic MLflow eval-table row carries the
    qid in `client_request_id`, not `question_id`/`id`."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = _make_cycle_5_baseline_row("airline_ticketing_and_fare_analysis_gs_016")
    assert _baseline_row_qid(row) == "airline_ticketing_and_fare_analysis_gs_016"


def test_baseline_row_qid_prefers_question_id_over_aliases() -> None:
    """Defensive: if BOTH `question_id` and `client_request_id` are present
    (e.g., a future row shape that adds the canonical key), prefer the
    canonical name to keep semantics with the snapshot/journey emit code."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = {
        "question_id": "canonical_qid",
        "client_request_id": "request_uuid_12345",
    }
    assert _baseline_row_qid(row) == "canonical_qid"


def test_baseline_row_qid_alias_fallback_order_matches_helper_doc() -> None:
    """Pin the documented fallback order: question_id → id → client_request_id
    → request_id → inputs/question_id → empty."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    assert _baseline_row_qid({"id": "from_id"}) == "from_id"
    assert _baseline_row_qid({"client_request_id": "from_crid"}) == "from_crid"
    assert _baseline_row_qid({"request_id": "from_rid"}) == "from_rid"
    assert _baseline_row_qid({"inputs/question_id": "from_inputs"}) == "from_inputs"
    assert _baseline_row_qid({"unrelated": "value"}) == ""
    assert _baseline_row_qid({}) == ""


def test_seed_eval_result_handles_cycle_5_mlflow_baseline_payload() -> None:
    """End-to-end pin: feed the helper a 24-row baseline payload that
    matches cycle 5's actual key-set verbatim. The helper must extract all
    24 qids (cycle 5 extracted 0 — the bug this fix closes)."""
    from genie_space_optimizer.optimization.harness import (
        _seed_eval_result_from_baseline_iter,
    )

    qids = [
        f"airline_ticketing_and_fare_analysis_gs_{i:03d}" for i in range(1, 25)
    ]
    baseline_iter = {
        "rows_json": [
            _make_cycle_5_baseline_row(qid, arbiter_value="ground_truth_correct")
            for qid in qids
        ],
    }
    out = _seed_eval_result_from_baseline_iter(baseline_iter)
    assert out["question_ids"] == qids
    assert len(out["question_ids"]) == 24
    assert all(out["scores"][q] == "no" for q in qids), (
        "Without a `result_correctness` key in the row, the helper defaults "
        "to 'no' — the gate result will refresh this on first eval."
    )
    assert all(out["arbiter_verdicts"][q] == "ground_truth_correct" for q in qids)
    assert out["failure_question_ids"] == qids


def test_seed_eval_result_cycle_5_payload_powers_fixture_eval_rows() -> None:
    """End-to-end shape test threading cycle 5's row shape all the way to
    the replay fixture's `eval_rows` list — the actual artifact Task 13
    Step 7's sanity script asserts is non-empty."""
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
        _seed_eval_result_from_baseline_iter,
    )

    qids = [f"airline_gs_{i:03d}" for i in range(1, 11)]
    baseline_iter = {
        "rows_json": [_make_cycle_5_baseline_row(q) for q in qids],
    }
    seeded = _seed_eval_result_from_baseline_iter(baseline_iter)
    rows = _build_fixture_eval_rows(seeded)
    assert len(rows) == 10
    assert {r["question_id"] for r in rows} == set(qids)
    for r in rows:
        assert "result_correctness" in r
        assert r["arbiter"] == "both_correct"


# ---------------------------------------------------------------------------
# Track D regression — _baseline_row_qid must prefer canonical qid sources
# (request.kwargs.question_id, inputs.question_id) over trace-id aliases
# (client_request_id, request_id) when both are present. Cycle 7 found a
# row shape where client_request_id contained an MLflow trace ID; the old
# helper returned that trace ID and corrupted the fixture. See
# docs/2026-05-02-track-a-fixture-reconstruction-and-qid-extractor-fix-plan.md.
# ---------------------------------------------------------------------------


def test_baseline_row_qid_prefers_inputs_question_id_over_client_request_id() -> None:
    """The cycle 7 row shape: client_request_id is a trace ID, the canonical
    qid lives at inputs.question_id. Helper must return the canonical qid."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = {
        "client_request_id": "tr-f74a86401aa0b8e292f602e0069d867d",
        "inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_024"},
    }
    assert _baseline_row_qid(row) == "airline_ticketing_and_fare_analysis_gs_024"


def test_baseline_row_qid_prefers_request_kwargs_question_id_over_client_request_id() -> None:
    """Some MLflow eval-table shapes nest the canonical qid inside
    request.kwargs (the JSON-encoded predict_fn payload)."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = {
        "client_request_id": "tr-aaa",
        "request": {"kwargs": {"question_id": "airline_q_canonical"}},
    }
    assert _baseline_row_qid(row) == "airline_q_canonical"


def test_baseline_row_qid_handles_request_as_json_string() -> None:
    """request can also be persisted as a JSON-encoded string. The helper
    must parse it and find the canonical qid."""
    import json
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = {
        "client_request_id": "tr-bbb",
        "request": json.dumps({"kwargs": {"question_id": "airline_q_from_json"}}),
    }
    assert _baseline_row_qid(row) == "airline_q_from_json"


def test_baseline_row_qid_falls_back_to_client_request_id_when_no_canonical_present() -> None:
    """Forward-compat: if no canonical qid source exists, the helper must
    still return *something* (the trace ID) so the carrier doesn't go empty.
    The downstream warning ('baseline payload had N rows but 0 carried')
    will fire if every row falls into this case, giving operator visibility."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = {"client_request_id": "tr-only"}
    assert _baseline_row_qid(row) == "tr-only"


def test_baseline_row_qid_top_level_question_id_still_wins_over_inputs() -> None:
    """The existing top-level alias chain still takes precedence over inputs
    (preserves cycle 5/6 fix semantics)."""
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    row = {
        "question_id": "top_level_canonical",
        "client_request_id": "tr-aaa",
        "inputs": {"question_id": "inputs_canonical"},
    }
    assert _baseline_row_qid(row) == "top_level_canonical"
