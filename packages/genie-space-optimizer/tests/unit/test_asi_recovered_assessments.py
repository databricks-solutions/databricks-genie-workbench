"""Phase 2.2: assessments fetched from recovered traces flow into rows.

When ``mlflow.genai.evaluate`` loses trace context, the row's
``trace`` and ``assessments`` columns are empty but
``_recover_trace_map`` reattaches a trace_id by qid. This test
verifies that ``_fetch_assessments_for_recovered_qids`` followed by
``_merge_row_sources`` reaches a non-empty rationale/metadata even
without the row-level trace join.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import evaluation as evl


def _make_fake_trace(judge: str, rationale: str, metadata: dict | None) -> SimpleNamespace:
    assessment = SimpleNamespace(name=judge, rationale=rationale, metadata=metadata)
    data = SimpleNamespace(assessments=[assessment])
    info = SimpleNamespace(assessments=[])
    return SimpleNamespace(data=data, info=info)


def test_fetch_assessments_returns_qid_keyed_map() -> None:
    trace_map = {"q1": "tid-1", "q2": "tid-2"}
    fakes = {
        "tid-1": _make_fake_trace(
            "schema_accuracy", "judge said wrong table", {"failure_type": "wrong_table"},
        ),
        "tid-2": _make_fake_trace(
            "result_correctness", "judge said wrong filter", None,
        ),
    }

    with patch.object(evl.mlflow, "get_trace", side_effect=lambda tid: fakes.get(tid)):
        out = evl._fetch_assessments_for_recovered_qids(trace_map)

    assert "q1" in out and "q2" in out
    assert out["q1"]["schema_accuracy"]["rationale"] == "judge said wrong table"
    assert out["q1"]["schema_accuracy"]["metadata"] == {"failure_type": "wrong_table"}
    # Rationale-only assessments still produce metadata via the
    # fallback rationale parser if it can extract something.
    assert "result_correctness" in out["q2"]


def test_fetch_assessments_skips_missing_traces() -> None:
    with patch.object(evl.mlflow, "get_trace", return_value=None):
        out = evl._fetch_assessments_for_recovered_qids({"q1": "tid-1"})
    assert out == {}


def test_fetch_assessments_tolerates_rpc_errors() -> None:
    def boom(tid: str) -> SimpleNamespace:  # pragma: no cover - exception path
        raise RuntimeError("rpc failed")

    with patch.object(evl.mlflow, "get_trace", side_effect=boom):
        out = evl._fetch_assessments_for_recovered_qids({"q1": "tid-1"})
    assert out == {}


def test_merge_row_sources_uses_recovered_when_trace_silent() -> None:
    row: dict = {"existing": "data"}
    recovered = {
        "schema_accuracy": {
            "rationale": "Genie picked wrong table",
            "metadata": {"failure_type": "wrong_table"},
        },
    }
    out = evl._merge_row_sources(
        row,
        assessment_map_row=None,
        cached_feedback_qid=None,
        recovered_assessments_qid=recovered,
    )
    assert out["schema_accuracy/rationale"] == "Genie picked wrong table"
    assert out["schema_accuracy/metadata"] == {"failure_type": "wrong_table"}
    assert out.get("_asi_source") == "recovered_trace"


def test_merge_row_sources_trace_beats_recovered() -> None:
    """When both trace and recovered have data, trace wins (highest authority)."""
    row: dict = {}
    trace_data = {
        "schema_accuracy": {
            "rationale": "from trace",
            "metadata": {"failure_type": "wrong_measure"},
        },
    }
    recovered = {
        "schema_accuracy": {
            "rationale": "from recovered",
            "metadata": {"failure_type": "wrong_table"},
        },
    }
    out = evl._merge_row_sources(
        row,
        assessment_map_row=trace_data,
        cached_feedback_qid=None,
        recovered_assessments_qid=recovered,
    )
    assert out["schema_accuracy/rationale"] == "from trace"
    assert out.get("_asi_source") == "trace"


def test_merge_row_sources_no_data_does_not_set_source() -> None:
    row: dict = {"x": 1}
    out = evl._merge_row_sources(row, None, None, None)
    assert "_asi_source" not in out
