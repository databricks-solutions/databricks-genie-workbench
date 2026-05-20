"""Plan 10 Phase C — fixture extractor.

Reads the two postmortem evidence bundles checked into
``packages/genie-space-optimizer/docs/runid_analysis/`` and emits
four anchor-replay fixtures the deploy-gate replay test consumes.

Idempotent: re-running on the same evidence produces byte-stable
JSON output. Run from repo root:

    python packages/genie-space-optimizer/tests/replay/fixtures/plan10/_extract.py
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[6]
FIXTURES_DIR = Path(__file__).parent

AIRLINE_BUNDLE = (
    REPO_ROOT
    / "packages/genie-space-optimizer/docs/runid_analysis"
    / "59a173d3-f71f-4901-90ad-e10f1084cd7f/evidence/gso_postmortem_bundle"
)
SEVENNOW_BUNDLE = (
    REPO_ROOT
    / "packages/genie-space-optimizer/docs/runid_analysis"
    / "ab65fefe-9bb5-411c-9818-f62633ec9cfd/evidence"
    / "gso_postmortem_bundle_latest_680468481811523/gso_postmortem_bundle"
)


@dataclass(frozen=True)
class _FixtureSpec:
    out_name: str
    bundle: Path
    source_run_id: str
    iteration: int
    cluster_id: str
    failure_shape: str  # Plan-10-named semantic shape
    suggested_repair_shape: str  # closed RepairShape enum value


_SPECS: tuple[_FixtureSpec, ...] = (
    _FixtureSpec(
        out_name="airline_gs_009_plural_top_n_collapse.json",
        bundle=AIRLINE_BUNDLE,
        source_run_id="59a173d3-f71f-4901-90ad-e10f1084cd7f",
        iteration=1,
        cluster_id="H001",
        failure_shape="plural_top_n_collapse",
        suggested_repair_shape="top_n_by_metric",
    ),
    _FixtureSpec(
        out_name="airline_gs_024_missing_filter.json",
        bundle=AIRLINE_BUNDLE,
        source_run_id="59a173d3-f71f-4901-90ad-e10f1084cd7f",
        iteration=1,
        cluster_id="H002",
        failure_shape="unrequested_filter",
        suggested_repair_shape="filter_compose",
    ),
    _FixtureSpec(
        out_name="7now_gs_013_wrong_filter_condition.json",
        bundle=SEVENNOW_BUNDLE,
        source_run_id="ab65fefe-9bb5-411c-9818-f62633ec9cfd",
        iteration=1,
        cluster_id="H001",
        failure_shape="day_vs_mtd_grain",
        suggested_repair_shape="period_over_period",
    ),
    _FixtureSpec(
        out_name="7now_gs_026_plural_top_n_collapse.json",
        bundle=SEVENNOW_BUNDLE,
        source_run_id="ab65fefe-9bb5-411c-9818-f62633ec9cfd",
        iteration=1,
        cluster_id="H002",
        failure_shape="zone_vp_collapse",
        suggested_repair_shape="top_n_by_metric",
    ),
)


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _find_cluster(bundle: Path, iteration: int, cluster_id: str) -> dict:
    cf_out = _load_json(
        bundle / f"iterations/iter_{iteration:02d}/stages"
        / "03_cluster_formation/output.json"
    )
    for c in cf_out.get("clusters", []) or []:
        if c.get("cluster_id") == cluster_id:
            return c
    raise LookupError(
        f"cluster_id={cluster_id!r} not found in "
        f"{bundle.name}/iter_{iteration:02d} cluster_formation output"
    )


def _rca_evidence_was_empty(bundle: Path, iteration: int) -> dict[str, bool]:
    """Verify the production claim: typed RCA evidence was empty.

    Both bundles either omit the rca_evidence stage entirely (airline)
    or show empty per_qid_evidence_typed (7now). Either is a confirmed
    leak-1 trigger.
    """
    rca_path = (
        bundle / f"iterations/iter_{iteration:02d}/stages"
        / "02_rca_evidence/output.json"
    )
    if not rca_path.exists():
        return {
            "rca_evidence_typed_per_qid_was_empty": True,
            "rca_kinds_by_qid_was_empty": True,
            "stage_was_skipped_entirely": True,
        }
    rca = _load_json(rca_path)
    return {
        "rca_evidence_typed_per_qid_was_empty": not (
            rca.get("per_qid_evidence_typed") or {}
        ),
        "rca_kinds_by_qid_was_empty": not (rca.get("rca_kinds_by_qid") or {}),
        "stage_was_skipped_entirely": False,
    }


def _build_fixture(spec: _FixtureSpec) -> dict:
    cluster = _find_cluster(spec.bundle, spec.iteration, spec.cluster_id)
    rca_state = _rca_evidence_was_empty(spec.bundle, spec.iteration)

    sql_ctx = (cluster.get("sql_contexts") or [{}])[0]
    failing_qid = (cluster.get("question_ids") or ["unknown"])[0]

    return {
        "fixture_id": spec.out_name.replace(".json", ""),
        "source_run_id": spec.source_run_id,
        "source_iteration": spec.iteration,
        "source_cluster_id": spec.cluster_id,
        "source_ag_id_synthetic": f"AG_REPLAY_PLAN10_{spec.cluster_id}",
        "failing_qid": failing_qid,
        "failure_shape": spec.failure_shape,
        "observed_root_cause": cluster.get("root_cause", ""),
        "production_evidence": {
            **rca_state,
            "comment": (
                "The Plan 5 LLM-direct dispatch gate "
                "(optimizer.py:_dispatch_lever_5b_for_cluster) closed silently "
                "because the deterministic RCA classifier returned no typed "
                "evidence for this QID. This left rca_evidence_typed empty for "
                "the cluster, falling back to the legacy path with zero "
                "structural candidates."
            ),
        },
        "cluster": cluster,
        "llm_cluster_seed": {
            "cluster_id": spec.cluster_id,
            "semantic_theme": cluster.get("root_cause", spec.failure_shape),
            "member_qids": list(cluster.get("question_ids", [])),
            "unifying_evidence": (
                (cluster.get("asi_counterfactual_fixes") or [""])[0]
            ),
            "suggested_repair_shape": spec.suggested_repair_shape,
            "primary_blame_set": list(cluster.get("asi_blame_set", [])),
            "confidence": "medium",
        },
        "minimal_metadata_snapshot": {
            "schema_columns": [],
            "instructions": {"example_question_sqls": []},
            "data_sources": {},
        },
        "failure_summary": {
            "question": sql_ctx.get("question", ""),
            "generated_sql": sql_ctx.get("generated_sql", ""),
            "expected_sql": sql_ctx.get("expected_sql", ""),
        },
        "expected_dispatch_outcome": {
            "current_code_returns": "empty_list",
            "current_code_failure_mode": (
                "leak1_silent_gate_closure_via_empty_rca_evidence_typed"
            ),
            "secondary_failure_mode_when_leak1_patched": (
                "leak2_synthesizer_strips_target_objects"
            ),
            "tertiary_failure_mode_when_leaks12_patched": (
                "leak3_production_call_site_never_threads_repair_proposal"
            ),
            "post_plan10_expected": (
                "non_empty_list_with_target_objects_populated"
            ),
        },
    }


def _write_fixture(spec: _FixtureSpec) -> Path:
    payload = _build_fixture(spec)
    out_path = FIXTURES_DIR / spec.out_name
    out_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return out_path


def main() -> None:
    for spec in _SPECS:
        path = _write_fixture(spec)
        print(f"  wrote {path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
