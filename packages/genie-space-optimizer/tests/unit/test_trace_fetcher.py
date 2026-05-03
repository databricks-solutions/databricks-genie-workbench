import json
from pathlib import Path
from unittest.mock import MagicMock, patch


def _write_seed_manifest(tmp_path: Path) -> Path:
    from genie_space_optimizer.tools.evidence_layout import (
        Manifest,
        TraceFetchReason,
        TraceFetchRecommendation,
        bundle_paths_for,
        manifest_to_dict,
    )

    paths = bundle_paths_for(root=tmp_path, optimization_run_id="opt-abc")
    paths.evidence_dir.mkdir(parents=True)
    manifest = Manifest(
        schema_version=1,
        bundle_version=1,
        captured_at_utc="2026-05-04T12:00:00Z",
        inputs={"job_id": "j", "run_id": "r", "profile": "p"},
        resolved={"optimization_run_id": "opt-abc", "mlflow_experiment_id": "exp-1"},
        artifacts_pulled={"traces": ()},
        trace_fetch_recommendations=(
            TraceFetchRecommendation(
                reason=TraceFetchReason.INCOMPLETE_DECISION_TRACE,
                iteration=1,
                trace_ids=("tr-abc", "tr-def"),
                detail="reason_code=UNKNOWN on 2 abandoned proposals",
            ),
        ),
        exit_status="complete_with_gaps",
    )
    paths.manifest.write_text(json.dumps(manifest_to_dict(manifest)))
    return paths.root


def test_fetch_traces_writes_json_per_trace(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.trace_fetcher import fetch_traces

    bundle_root = _write_seed_manifest(tmp_path)

    fake_client = MagicMock()
    fake_client.get_trace.side_effect = lambda trace_id: MagicMock(
        to_dict=lambda: {"trace_id": trace_id, "events": [{"name": "demo"}]}
    )

    with patch(
        "genie_space_optimizer.tools.trace_fetcher.MlflowClient",
        return_value=fake_client,
    ):
        result = fetch_traces(
            bundle_root=bundle_root,
            trace_ids=["tr-abc", "tr-def"],
        )

    assert result["fetched"] == ["tr-abc", "tr-def"]
    assert result["failed"] == []
    assert (bundle_root / "evidence" / "traces" / "tr-abc.json").exists()
    payload = json.loads((bundle_root / "evidence" / "traces" / "tr-abc.json").read_text())
    assert payload["trace_id"] == "tr-abc"

    manifest = json.loads((bundle_root / "evidence" / "manifest.json").read_text())
    assert sorted(manifest["artifacts_pulled"]["traces"]) == sorted(
        [
            "evidence/traces/tr-abc.json",
            "evidence/traces/tr-def.json",
        ]
    )


def test_fetch_traces_from_recommendations_uses_manifest(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.trace_fetcher import fetch_traces

    bundle_root = _write_seed_manifest(tmp_path)

    fake_client = MagicMock()
    fake_client.get_trace.return_value = MagicMock(to_dict=lambda: {"events": []})

    with patch(
        "genie_space_optimizer.tools.trace_fetcher.MlflowClient",
        return_value=fake_client,
    ):
        result = fetch_traces(bundle_root=bundle_root, from_recommendations=True)

    assert sorted(result["fetched"]) == ["tr-abc", "tr-def"]


def test_fetch_traces_records_failures(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.trace_fetcher import fetch_traces

    bundle_root = _write_seed_manifest(tmp_path)

    fake_client = MagicMock()
    fake_client.get_trace.side_effect = RuntimeError("HTTP 429")

    with patch(
        "genie_space_optimizer.tools.trace_fetcher.MlflowClient",
        return_value=fake_client,
    ):
        result = fetch_traces(bundle_root=bundle_root, trace_ids=["tr-x"])

    assert result["fetched"] == []
    assert result["failed"] == [{"trace_id": "tr-x", "error": "RuntimeError: HTTP 429"}]


def test_main_returns_nonzero_on_missing_bundle(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.trace_fetcher import main as trace_fetcher_main

    rc = trace_fetcher_main(
        ["--bundle-dir", str(tmp_path / "missing"), "--trace-id", "tr-x"]
    )
    assert rc != 0
