"""Phase 3.6 Task 5 — CLI: capture_tape_from_mlflow.py."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "capture_tape_from_mlflow.py"


def test_script_exists_and_help_runs():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "experiment-id" in result.stdout
    assert "run-id" in result.stdout
    assert "export-json" in result.stdout
    assert "out" in result.stdout


def test_multi_run_filter_string_resolves_via_search_runs(
    tmp_path: Path, monkeypatch,
):
    """Phase 3.6 (2026-05-18) — ``--filter-string`` resolves all
    matching MLflow runs and walks ``search_traces`` once per
    resolved run id. Entries from every run land in the output tape.
    """
    export_path = tmp_path / "export.json"
    export_path.write_text(json.dumps({
        "fixture_id": "f1",
        "iterations": [{"iteration": 0, "eval_rows": [], "clusters": []}],
    }))

    fake_calls_by_run: dict[str, list[dict]] = {
        "RUN_A": [{
            "span_name": "stage_1_discovery",
            "iteration": -1, "ag_id": "", "cluster_id": "",
            "prompt_sha256": "a" * 64,
            "system_msg": "", "prompt": "pA", "response_text": "rA",
            "response_metadata": {},
        }],
        "RUN_B": [{
            "span_name": "lever_4_join_discovery",
            "iteration": -1, "ag_id": "AG_X", "cluster_id": "H1",
            "prompt_sha256": "b" * 64,
            "system_msg": "", "prompt": "pB", "response_text": "rB",
            "response_metadata": {},
        }],
    }

    def _fake_run(rid):
        info = MagicMock()
        info.run_id = rid
        r = MagicMock()
        r.info = info
        return r

    def _fake_search_runs(**kwargs):
        # Two sibling runs matched by the filter.
        return [_fake_run("RUN_A"), _fake_run("RUN_B")]

    seen_run_ids: list[str] = []

    def _fake_search_traces(*, experiment_ids, run_id, max_results):
        seen_run_ids.append(run_id)
        return [MagicMock(name=f"Trace-for-{run_id}", _rid=run_id)]

    fake_client = MagicMock(name="FakeMlflowClient")
    fake_client.search_runs.side_effect = _fake_search_runs
    fake_client.search_traces.side_effect = _fake_search_traces

    # Extractor returns the per-run fake call dicts based on the trace's
    # ``_rid`` attribute. The capture script now invokes the per-trace
    # form (extract_llm_calls_from_trace) so it can thread per-run
    # iteration_override tags; mock that one.
    def _fake_extract_one(
        trace, *, export_payload=None, iteration_override=None,
    ):
        rid = getattr(trace, "_rid", None)
        return iter(fake_calls_by_run.get(rid, []))

    from scripts import capture_tape_from_mlflow as _cli
    monkeypatch.setattr(
        _cli, "extract_llm_calls_from_trace", _fake_extract_one,
        raising=False,
    )
    from genie_space_optimizer.optimization import mlflow_trace_extractor
    monkeypatch.setattr(
        mlflow_trace_extractor,
        "extract_llm_calls_from_trace",
        _fake_extract_one,
    )

    from scripts import capture_tape_from_mlflow as cli
    monkeypatch.setattr(cli, "_build_mlflow_client", lambda: fake_client)

    out_path = tmp_path / "tape.json"
    rc = cli.main(argv=[
        "--experiment-id", "EXP_X",
        "--filter-string", 'tags."genie.optimization_run_id" = "X"',
        "--export-json",   str(export_path),
        "--out",           str(out_path),
        "--miss-policy",   "prompt_sha_only",
    ])
    assert rc == 0

    # Both runs were walked, in resolution order.
    assert seen_run_ids == ["RUN_A", "RUN_B"]

    from genie_space_optimizer.optimization.tape import LeverLoopTape
    tape = LeverLoopTape.from_json_file(out_path)
    assert len(tape.entries) == 2
    stages = sorted(e.key.stage for e in tape.entries)
    assert stages == ["lever_4_join_discovery", "stage_1_discovery"]


def test_end_to_end_assembly(tmp_path: Path, monkeypatch):
    """In-process invocation: stub MlflowClient + extractor, assert tape JSON shape."""
    export_path = tmp_path / "export.json"
    export_path.write_text(json.dumps({
        "fixture_id": "f1",
        "iterations": [
            {
                "iteration": 0,
                "eval_rows": [
                    {"qid": "gs_009", "passed": False, "got_sql": "..."},
                ],
                "clusters": [
                    {"cluster_id": "H001", "qids": ["gs_009"], "root_cause": "x"},
                ],
            },
        ],
    }))

    fake_calls = [{
        "span_name": "stage_1_discovery",
        "iteration": -1,
        "ag_id": "",
        "cluster_id": "",
        "prompt_sha256": "a" * 64,
        "system_msg": "sys",
        "prompt": "p",
        "response_text": "r",
        "response_metadata": {"model": "stub"},
    }]

    from genie_space_optimizer.optimization import mlflow_trace_extractor
    monkeypatch.setattr(
        mlflow_trace_extractor,
        "extract_llm_calls_from_trace",
        lambda trace, *, export_payload=None, iteration_override=None: iter(fake_calls),
    )

    fake_trace = MagicMock(name="FakeTrace")
    fake_client = MagicMock(name="FakeMlflowClient")
    fake_client.search_traces.return_value = [fake_trace]

    from scripts import capture_tape_from_mlflow as cli
    monkeypatch.setattr(cli, "_build_mlflow_client", lambda: fake_client)

    out_path = tmp_path / "tape.json"
    rc = cli.main(argv=[
        "--experiment-id", "EXP_X",
        "--run-id",        "RUN_X",
        "--export-json",   str(export_path),
        "--out",           str(out_path),
        "--miss-policy",   "prompt_sha_only",
    ])
    assert rc == 0

    from genie_space_optimizer.optimization.tape import LeverLoopTape
    tape = LeverLoopTape.from_json_file(out_path)
    assert tape.miss_policy == "prompt_sha_only"
    assert len(tape.entries) == 1
    assert tape.entries[0].key.stage == "stage_1_discovery"
    assert tape.entries[0].prompt == "p"
    # Side-tables from the export carry through.
    assert tape.evals_by_iteration.get(0)
    assert tape.evals_by_iteration[0][0]["qid"] == "gs_009"
    assert tape.clusters_by_iteration.get(0)


def test_capture_normalizes_one_indexed_export_iterations_to_zero(
    tmp_path: Path, monkeypatch,
):
    """Phase 3.6.1 (2026-05-18) — production exports use 1-indexed
    iteration counters; the capture script must shift them to
    0-indexed so the on-disk tape matches the replay harness's
    in-memory query semantics."""
    export_path = tmp_path / "export.json"
    export_path.write_text(json.dumps({
        "fixture_id": "f1",
        "iterations": [
            {
                "iteration": 1,                       # 1-indexed
                "eval_rows": [{"qid": "row-iter-1"}],
                "clusters": [{"cluster_id": "C1"}],
            },
            {
                "iteration": 2,
                "eval_rows": [{"qid": "row-iter-2"}],
                "clusters": [{"cluster_id": "C2"}],
            },
        ],
    }))

    from genie_space_optimizer.optimization import mlflow_trace_extractor
    monkeypatch.setattr(
        mlflow_trace_extractor,
        "extract_llm_calls_from_trace",
        lambda trace, *, export_payload=None, iteration_override=None: iter([]),
    )

    fake_client = MagicMock(name="FakeMlflowClient")
    fake_client.search_traces.return_value = []

    from scripts import capture_tape_from_mlflow as cli
    monkeypatch.setattr(cli, "_build_mlflow_client", lambda: fake_client)

    out_path = tmp_path / "tape.json"
    rc = cli.main(argv=[
        "--experiment-id", "EXP_X",
        "--run-id",        "RUN_X",
        "--export-json",   str(export_path),
        "--out",           str(out_path),
        "--miss-policy",   "prompt_sha_only",
    ])
    assert rc == 0

    from genie_space_optimizer.optimization.tape import LeverLoopTape
    tape = LeverLoopTape.from_json_file(out_path)
    # The export's iter=1 → tape key 0 ; iter=2 → tape key 1.
    assert sorted(tape.evals_by_iteration.keys()) == [0, 1]
    assert tape.evals_by_iteration[0][0]["qid"] == "row-iter-1"
    assert tape.evals_by_iteration[1][0]["qid"] == "row-iter-2"
    assert sorted(tape.clusters_by_iteration.keys()) == [0, 1]
    assert tape.clusters_by_iteration[0][0]["cluster_id"] == "C1"
    assert tape.clusters_by_iteration[1][0]["cluster_id"] == "C2"
    # Phase 3.7 (2026-05-18) — capture writes the current
    # TAPE_FORMAT_VERSION (5 as of historic_inject_cluster_only).
    assert tape.format_version == 5
    assert sorted(tape.iteration_payloads.keys()) == [0, 1]
    payload0 = tape.iteration_payloads[0]
    assert payload0["iteration"] == 1
    assert payload0["eval_scope"] == "full"
    assert payload0["rolled_back"] is False
    assert payload0["rows_json"][0]["qid"] == "row-iter-1"
    assert payload0["clusters"][0]["cluster_id"] == "C1"


def test_capture_backfills_source_cluster_ids_on_ags():
    """1B — when AG.source_cluster_ids is null/empty, the capture
    helper backfills it from patches[*].cluster_id (priority 2) or the
    iter pool (priority 3). The capture writes this into the tape's
    iteration_payloads so the replay harness reads AGs with populated
    source_cluster_ids and lever6's eligible_clusters path doesn't
    synthesize {cluster_id: ag_id}."""
    from scripts.capture_tape_from_mlflow import (
        _backfill_source_cluster_ids_in_place,
    )

    # Iter 1: empty patches → falls back to iter pool (clusters[]).
    # Iter 2: non-empty patches → uses patches' cluster_ids.
    payload = {
        "iterations": [
            {
                "iteration": 1,
                "iter_source_clusters_by_id": {},
                "clusters": [{"cluster_id": "H001"}, {"cluster_id": "H002"}],
                "strategist_response": {"action_groups": [
                    {"id": "AG1", "source_cluster_ids": None, "patches": []},
                ]},
            },
            {
                "iteration": 2,
                "iter_source_clusters_by_id": {},
                "clusters": [{"cluster_id": "X"}],
                "strategist_response": {"action_groups": [
                    {"id": "AG2", "source_cluster_ids": None,
                     "patches": [{"cluster_id": "X"}]},
                ]},
            },
        ],
    }
    _backfill_source_cluster_ids_in_place(payload)
    ag1 = payload["iterations"][0]["strategist_response"]["action_groups"][0]
    ag2 = payload["iterations"][1]["strategist_response"]["action_groups"][0]
    assert sorted(ag1["source_cluster_ids"]) == ["H001", "H002"]
    assert ag2["source_cluster_ids"] == ["X"]


def test_capture_reads_iteration_tag_from_run_metadata():
    """1A — _read_iteration_tag converts ``genie.iteration: "03"`` to
    0-indexed int 2; missing/non-numeric tags return None."""
    from scripts.capture_tape_from_mlflow import _read_iteration_tag

    def _mock_run(tags):
        return MagicMock(data=MagicMock(tags=tags))

    fake_client = MagicMock()
    fake_client.get_run.side_effect = lambda rid: {
        "R1": _mock_run({"genie.iteration": "03"}),
        "R2": _mock_run({"genie.iteration": "01", "genie.stage": "strategy"}),
        "R3": _mock_run({"genie.stage": "enrichment_snapshot"}),  # no iter tag
        "R4": _mock_run({"genie.iteration": "weird-value"}),  # bad
    }[rid]
    assert _read_iteration_tag(fake_client, "R1") == 2
    assert _read_iteration_tag(fake_client, "R2") == 0
    assert _read_iteration_tag(fake_client, "R3") is None
    assert _read_iteration_tag(fake_client, "R4") is None


def test_capture_writes_v4_tape_with_replay_mode_by_stage(
    tmp_path: Path, monkeypatch,
):
    """Phase 3.7 Task 6 — --replay-mode flag lands in tape JSON and
    survives load via ``LeverLoopTape.from_json_file``."""
    export_path = tmp_path / "export.json"
    export_path.write_text(json.dumps({
        "fixture_id": "f1",
        "iterations": [{"iteration": 1, "eval_rows": [{"qid": "g"}],
                        "clusters": [{"cluster_id": "C"}]}],
    }))

    from genie_space_optimizer.optimization import mlflow_trace_extractor
    monkeypatch.setattr(
        mlflow_trace_extractor,
        "extract_llm_calls_from_trace",
        lambda trace, *, export_payload=None, iteration_override=None: iter([]),
    )

    fake_client = MagicMock(name="FakeMlflowClient")
    fake_client.search_traces.return_value = []

    from scripts import capture_tape_from_mlflow as cli
    monkeypatch.setattr(cli, "_build_mlflow_client", lambda: fake_client)

    out_path = tmp_path / "tape.json"
    rc = cli.main(argv=[
        "--experiment-id", "EXP_X",
        "--run-id",        "RUN_X",
        "--export-json",   str(export_path),
        "--out",           str(out_path),
        "--miss-policy",   "prompt_sha_only",
        "--replay-mode",   "lever6_llm=historic_inject",
    ])
    assert rc == 0

    from genie_space_optimizer.optimization.tape import LeverLoopTape
    tape = LeverLoopTape.from_json_file(out_path)
    assert tape.format_version == 5
    assert tape.replay_mode_by_stage == {"lever6_llm": "historic_inject"}

    # Verify CLI rejects unknown modes.
    rc_bad = subprocess.run(
        [sys.executable, str(SCRIPT),
         "--experiment-id", "EXP_X",
         "--run-id", "RUN_X",
         "--export-json", str(export_path),
         "--out", str(out_path),
         "--replay-mode", "lever6_llm=magical_unicorn"],
        capture_output=True, text=True, check=False,
    )
    assert rc_bad.returncode != 0
    assert "magical_unicorn" in (rc_bad.stderr + rc_bad.stdout)
