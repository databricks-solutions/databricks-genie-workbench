"""Workbench bundle-normalizer tests.

These tests do not call any Databricks API and do not run the state
machine. They only exercise the bundle loader surface so the workbench
fails fast on shape regressions in the production replay corpus.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from local_lever_workbench.input_bundle import (
    DEFAULT_FAKE_SPACE_ID,
    from_bundle_json,
    from_production_replay,
    from_run_analysis_dir,
    minimal_metadata_snapshot,
)
from local_lever_workbench.models import WorkbenchInputBundle


@pytest.mark.workbench
def test_from_production_replay_loads_committed_corpus() -> None:
    """The committed sanitized corpus must produce a usable bundle.

    Locks in: provenance is ``production_replay``, at least three hard
    cases are returned, every case carries a non-empty row and a QID
    in the expected ``gs_NNN`` sanitization format.
    """
    bundle = from_production_replay()
    assert bundle.provenance.source_kind == "production_replay"
    assert len(bundle.hard_cases) >= 3, (
        "Production replay corpus dropped below 3 cases — regression. "
        "The corpus is the workbench's only real-row signal in V1."
    )
    for case in bundle.hard_cases:
        assert case.qid, f"case {case!r} carries empty QID"
        assert case.qid.startswith("gs_") or "gs_" in case.qid, (
            f"case qid={case.qid!r} does not follow the sanitized "
            f"gs_NNN naming convention; sanitization regression."
        )
        assert case.row, f"case {case.qid!r} carries empty row"
    assert bundle.space_id == DEFAULT_FAKE_SPACE_ID
    assert bundle.metadata_snapshot == minimal_metadata_snapshot()


@pytest.mark.workbench
def test_from_production_replay_filters_by_run_tag() -> None:
    """``run_tags`` filter must drop non-matching cases."""
    bundle = from_production_replay(run_tags=["98ec"])
    assert bundle.hard_cases, "98ec run_tag should match committed cases"
    # The sanitized corpus is keyed by run_tag in the filename — no
    # case file should leak into a 98ec-only filter from another tag.
    for case in bundle.hard_cases:
        assert case.expected_card_violations == (), (
            f"production_replay corpus drift: case {case.qid} carries "
            f"unexpected expected_card_violations={case.expected_card_violations}; "
            f"the V1 workbench corpus must pin '[]' after Trial 13 Phase 2."
        )


@pytest.mark.workbench
def test_from_production_replay_raises_on_no_match() -> None:
    """Filters with no match should raise rather than return an empty bundle.

    Returning an empty bundle silently is the same anti-pattern the
    pre-Trial 12 ``_failing_qids=[]`` derivation produced. The workbench
    must fail loud on "no data" so the operator notices.
    """
    with pytest.raises(ValueError, match="No production replay cases matched"):
        from_production_replay(run_tags=["does_not_exist"])


@pytest.mark.workbench
def test_bundle_roundtrips_via_json(tmp_path: Path) -> None:
    """Serialised bundle JSON must reload identically.

    Lets operators capture a bundle on one machine and replay it on
    another without losing provenance.
    """
    original = from_production_replay(run_tags=["98ec"])
    out = tmp_path / "bundle.json"
    original.to_json(out)

    reloaded = from_bundle_json(out)
    assert isinstance(reloaded, WorkbenchInputBundle)
    assert reloaded.provenance.source_kind == original.provenance.source_kind
    assert reloaded.hard_qids == original.hard_qids
    assert reloaded.metadata_snapshot == original.metadata_snapshot
    assert reloaded.space_id == original.space_id


@pytest.mark.workbench
def test_from_run_analysis_dir_recovers_eval_rows(tmp_path: Path) -> None:
    """A synthetic run-analysis bundle directory should yield hard rows.

    Construct a minimal evidence layout that mimics what the postmortem
    bundle assembler produces, then assert the loader admits exactly
    the rows that pass ``row_is_hard_failure``.
    """
    bundle_dir = tmp_path / "synthetic_run"
    evidence = bundle_dir / "evidence"
    evidence.mkdir(parents=True)
    fixture = {
        "iterations": [
            {
                "eval_rows": [
                    # Hard row: result_correctness=no, arbiter override
                    # not in the correct set.
                    {
                        "request": {
                            "kwargs": {"question_id": "qid_hard_1"},
                            "question": "Hard question 1?",
                        },
                        "result_correctness/value": "no",
                        "arbiter/value": "ground_truth_correct",
                    },
                    # Non-failing row: result_correctness=yes.
                    {
                        "request": {
                            "kwargs": {"question_id": "qid_ok"},
                            "question": "Easy question?",
                        },
                        "result_correctness/value": "yes",
                        "arbiter/value": "both_correct",
                    },
                ]
            }
        ]
    }
    (evidence / "replay_fixture_from_latest_export_synthetic.json").write_text(
        json.dumps(fixture)
    )

    bundle = from_run_analysis_dir(bundle_dir)
    assert bundle.provenance.source_kind == "run_analysis"
    qids = bundle.hard_qids
    assert "qid_hard_1" in qids, (
        f"hard row was dropped — admit_eval_rows projection drifted. "
        f"got={qids}"
    )
    assert "qid_ok" not in qids, (
        f"non-failing row was admitted as hard — workbench would waste "
        f"LLM calls on already-correct QIDs. got={qids}"
    )


@pytest.mark.workbench
def test_from_run_analysis_dir_raises_when_no_eval_rows(tmp_path: Path) -> None:
    """An empty bundle directory must raise — no silent zero-row runs."""
    empty_dir = tmp_path / "empty_run"
    (empty_dir / "evidence").mkdir(parents=True)
    with pytest.raises(ValueError, match="No recoverable eval rows"):
        from_run_analysis_dir(empty_dir)


@pytest.mark.workbench
def test_from_run_analysis_dir_prefers_workbench_capture_over_legacy(
    tmp_path: Path,
) -> None:
    """A ``workbench_eval_capture_v1`` fixture in the same evidence dir
    as a legacy ``replay_fixture_from_latest_export_*.json`` must win.

    Regression: before the schema-aware sort, the loader picked the
    alphabetically-first fixture. Legacy fixtures committed alongside
    a fresh MLflow capture would silently shadow the rich rows and
    Stage 1 would see only the 3-key projection (``question_id``,
    ``arbiter``, ``result_correctness``). The workbench then probed
    green on stripped rows, defeating the point of capturing in the
    first place.
    """
    bundle_dir = tmp_path / "schema_priority_run"
    evidence = bundle_dir / "evidence"
    evidence.mkdir(parents=True)

    # Legacy projected fixture — alphabetically FIRST by file stem.
    legacy = {
        "iterations": [
            {
                "eval_rows": [
                    {
                        "question_id": "qid_legacy",
                        "result_correctness/value": "no",
                        "arbiter/value": "ground_truth_correct",
                    }
                ]
            }
        ]
    }
    (evidence / "replay_fixture_from_latest_export_111.json").write_text(
        json.dumps(legacy)
    )

    # Fresh workbench capture — alphabetically LATER by file stem
    # (999 > 111), but the schema marker should pull it ahead anyway.
    fresh = {
        "_schema_version": "workbench_eval_capture_v1",
        "_provenance": {"source": "mlflow_eval_capture"},
        "eval_rows": [
            {
                "request": {
                    "kwargs": {"question_id": "qid_fresh"},
                    "question": "Fresh hard question?",
                },
                "result_correctness/value": "no",
                "arbiter/value": "ground_truth_correct",
            }
        ],
    }
    (evidence / "replay_fixture_from_latest_export_999.json").write_text(
        json.dumps(fresh)
    )

    bundle = from_run_analysis_dir(bundle_dir)
    assert bundle.hard_qids == ("qid_fresh",), (
        f"schema-aware sort regressed — loader picked the legacy "
        f"projected fixture over the fresh workbench capture. "
        f"got={bundle.hard_qids}"
    )
    # Provenance must name the capture we actually loaded, so postmortems
    # can trace back to the right artefact.
    assert any(
        "replay_fixture_from_latest_export_999" in a
        for a in bundle.provenance.source_artifacts
    ), (
        f"source_artifacts did not record the fresh capture; "
        f"provenance audit trail is wrong. "
        f"got={bundle.provenance.source_artifacts}"
    )
