"""Trial 29 W29.1 — typed inert-patch diagnostic record + postmortem persistence."""
from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.inert_patch_diagnostic import (
    Trial29InertPatchDiagnostic,
    load_inert_patch_diagnostics,
    persist_inert_patch_diagnostic,
)


def test_diagnostic_round_trip():
    d = Trial29InertPatchDiagnostic(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={
            "mechanism": "add_sql_snippet_filter",
            "filter_expr": "x IS NOT NULL",
        },
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature=(
            "add_sql_snippet_filter:filter:insufficient:"
            "rca=wrong_aggregation:behavior=unchanged"
        ),
        iteration=2,
        trial="trial29",
    )
    blob = d.model_dump()
    rebuilt = Trial29InertPatchDiagnostic.model_validate(blob)
    assert rebuilt == d


def test_persist_and_load_jsonl(tmp_path: Path):
    bundle_dir = tmp_path / "postmortem_bundle"
    bundle_dir.mkdir()
    d1 = Trial29InertPatchDiagnostic(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={"a": 1},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature="sig1",
        iteration=2,
        trial="trial29",
    )
    d2 = Trial29InertPatchDiagnostic(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        rejected_mechanism="add_example_sql",
        patch_json={"b": 2},
        pre_arbiter_score=0.5,
        post_arbiter_score=0.5,
        behavioral_diff="unchanged",
        signature="sig2",
        iteration=3,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(d1, bundle_dir=bundle_dir)
    persist_inert_patch_diagnostic(d2, bundle_dir=bundle_dir)

    out_file = bundle_dir / "trial29_inert_patch_diagnostics.jsonl"
    assert out_file.exists()
    raw_lines = out_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(raw_lines) == 2
    for line in raw_lines:
        assert json.loads(line)  # valid JSON per line

    loaded = load_inert_patch_diagnostics(bundle_dir)
    assert len(loaded) == 2
    assert loaded[0] == d1
    assert loaded[1] == d2


def test_persist_creates_bundle_dir_if_missing(tmp_path: Path):
    missing_dir = tmp_path / "does_not_exist_yet"
    d = Trial29InertPatchDiagnostic(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature="sig",
        iteration=1,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(d, bundle_dir=missing_dir)
    assert (missing_dir / "trial29_inert_patch_diagnostics.jsonl").exists()


def test_load_returns_empty_when_no_bundle(tmp_path: Path):
    """Byte-stable for green replays: empty bundle dir -> empty tuple."""
    assert load_inert_patch_diagnostics(tmp_path / "fresh") == ()
