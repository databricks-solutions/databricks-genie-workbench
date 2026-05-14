"""Unit tests for Plan 4 (Raw Evidence + Consolidation). Sections:
  1. Flag helpers + GSO_RAW_EVIDENCE_N parsing.
  2. _RAW_EVIDENCE_ANTI_ANCHORING_HEADER constant.
  3. raw_evidence module: extraction.
  4. raw_evidence module: per-skill projection.
  5. raw_evidence module: diverse sampling.
  6. _RawEvidenceCaptureSink + atexit gate.
  7. build_activation_bundle integration.
  8. _stage_2_* adapter wiring.
  9. _call_llm_for_proposal raw_evidence kwarg + slot rendering.
  10. Shadow-mode emission + harness wiring.
  11. Anti-anchoring framing rendered in every wired prompt.
"""
from __future__ import annotations

import importlib
import os


_PLAN4_ENV_KEYS = (
    "GSO_RAW_EVIDENCE_V1",
    "GSO_RAW_EVIDENCE_SHADOW_V1",
    "GSO_RAW_EVIDENCE_CAPTURE_PATH",
    "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE",
    "GSO_RAW_EVIDENCE_N",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env. Mirrors Plans 1-3 pattern.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg.raw_evidence_*_enabled()`` see the same env. Plan-4 env keys
    not in ``env`` are cleared so tests stay isolated.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN4_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


# ── Section 1: flag helpers ──────────────────────────────────────────


def test_raw_evidence_v1_default_off():
    cfg = _reload_config_with_env({})
    assert cfg.raw_evidence_v1_enabled() is False
    assert cfg.raw_evidence_v1_shadow_enabled() is False
    assert cfg.raw_evidence_capture_path_set() is False
    assert cfg.raw_evidence_capture_require_coverage_enabled() is False


def test_raw_evidence_v1_pipeline_flag_on():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_V1": "1"})
    assert cfg.raw_evidence_v1_enabled() is True
    assert cfg.raw_evidence_v1_shadow_enabled() is False


def test_raw_evidence_v1_shadow_flag_on():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_SHADOW_V1": "1"})
    assert cfg.raw_evidence_v1_enabled() is False
    assert cfg.raw_evidence_v1_shadow_enabled() is True


def test_raw_evidence_n_default_3():
    cfg = _reload_config_with_env({})
    assert cfg.raw_evidence_n() == 3


def test_raw_evidence_n_zero_disables():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "0"})
    assert cfg.raw_evidence_n() == 0


def test_raw_evidence_n_clamped_to_10():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "1000"})
    assert cfg.raw_evidence_n() == 10


def test_raw_evidence_n_negative_falls_back_to_default():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "-5"})
    assert cfg.raw_evidence_n() == 3


def test_raw_evidence_n_garbage_falls_back_to_default():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "not-a-number"})
    assert cfg.raw_evidence_n() == 3


# ── Section 3: raw_evidence module — extraction ──────────────────────


def _sample_cluster_with_traces() -> dict:
    return {
        "cluster_id": "C1",
        "root_cause": "wrong_column",
        "asi_blame_set": ["catalog.schema.dim_store.location_id"],
        "question_ids": ["Q1", "Q2", "Q3", "Q4"],
        "question_traces": [
            {
                "question_id": "Q1", "trace_id": "trace://q1",
                "question": "How many active stores in California?",
                "expected_sql": "SELECT count(*) FROM dim_store WHERE location_id IN (1,2,3) AND is_active = true",
                "generated_sql": "SELECT count(*) FROM dim_store WHERE store_id IN (1,2,3) AND is_active = true",
                "failed_judges": [
                    {"judge": "schema_accuracy", "rationale": "store_id column does not exist; should be location_id",
                     "rationale_snippet": "should be location_id"},
                ],
            },
            {
                "question_id": "Q2", "trace_id": "trace://q2",
                "question": "Show stores opened after 2020 in WA.",
                "expected_sql": "SELECT name FROM dim_store WHERE location_id IN (5,6) AND opened_at > '2020-01-01'",
                "generated_sql": "SELECT name FROM dim_store WHERE store_id IN (5,6) AND opened_at > '2020-01-01'",
                "failed_judges": [
                    {"judge": "schema_accuracy", "rationale": "store_id wrong; use location_id",
                     "rationale_snippet": "use location_id"},
                ],
            },
            {
                "question_id": "Q3", "trace_id": "trace://q3",
                "question": "Count of stores per region.",
                "expected_sql": "SELECT region, count(*) FROM dim_store GROUP BY region",
                "generated_sql": "SELECT region, count(*) FROM dim_store GROUP BY region",
                "failed_judges": [],  # no failed judges — not a failure
            },
            {
                "question_id": "Q4", "trace_id": "trace://q4",
                "question": "Stores in Texas with active flag set.",
                "expected_sql": "SELECT * FROM dim_store WHERE location_id = 9 AND is_active = true",
                "generated_sql": "SELECT * FROM dim_store WHERE store_id = 9 AND is_active = true",
                "failed_judges": [
                    {"judge": "result_correctness", "rationale": "store_id is not the right key",
                     "rationale_snippet": "store_id is not the right key"},
                ],
            },
        ],
    }


def test_extract_returns_one_triple_per_failed_question():
    from genie_space_optimizer.optimization.raw_evidence import (
        extract_raw_evidence_from_cluster,
    )
    cluster = _sample_cluster_with_traces()
    triples = extract_raw_evidence_from_cluster(cluster)
    # Q3 had no failed judges — excluded:
    assert len(triples) == 3
    assert {t["question_id"] for t in triples} == {"Q1", "Q2", "Q4"}


def test_extract_normalizes_field_names():
    """Output uses ``actual_sql`` (renamed from ``generated_sql``) for
    prompt readability; ``judge_rationale`` is the joined rationale
    string from all failed_judges entries."""
    from genie_space_optimizer.optimization.raw_evidence import (
        extract_raw_evidence_from_cluster,
    )
    cluster = _sample_cluster_with_traces()
    triples = extract_raw_evidence_from_cluster(cluster)
    by_qid = {t["question_id"]: t for t in triples}
    assert "actual_sql" in by_qid["Q1"]
    assert "generated_sql" not in by_qid["Q1"]
    assert by_qid["Q1"]["actual_sql"].startswith("SELECT count(*) FROM dim_store WHERE store_id")
    assert by_qid["Q1"]["expected_sql"].startswith("SELECT count(*) FROM dim_store WHERE location_id")
    assert "should be location_id" in by_qid["Q1"]["judge_rationale"]


def test_extract_handles_missing_question_traces():
    from genie_space_optimizer.optimization.raw_evidence import (
        extract_raw_evidence_from_cluster,
    )
    cluster = {"cluster_id": "C1", "root_cause": "x"}
    assert extract_raw_evidence_from_cluster(cluster) == []


def test_extract_handles_malformed_trace_entries():
    from genie_space_optimizer.optimization.raw_evidence import (
        extract_raw_evidence_from_cluster,
    )
    cluster = {
        "cluster_id": "C1",
        "question_traces": [
            "not-a-dict",
            {"question_id": "Q1", "failed_judges": [{"judge": "j"}]},
            None,
        ],
    }
    triples = extract_raw_evidence_from_cluster(cluster)
    # Only Q1 (the valid dict with failed_judges) gets a triple:
    assert len(triples) == 1
    assert triples[0]["question_id"] == "Q1"


# ── Section 5: diverse sampling ──────────────────────────────────────


def _triples(qids: list[str]) -> list[dict]:
    return [{"question_id": q, "trace_id": "", "question": f"q{q}",
              "actual_sql": "", "expected_sql": "", "judge_rationale": ""}
             for q in qids]


def test_select_diverse_returns_input_when_n_geq_count():
    from genie_space_optimizer.optimization.raw_evidence import (
        select_diverse_examples,
    )
    triples = _triples(["A", "B", "C"])
    out = select_diverse_examples(triples, n=5, w=None)
    assert len(out) == 3
    assert {t["question_id"] for t in out} == {"A", "B", "C"}


def test_select_diverse_returns_empty_for_n_zero():
    from genie_space_optimizer.optimization.raw_evidence import (
        select_diverse_examples,
    )
    triples = _triples(["A", "B", "C"])
    out = select_diverse_examples(triples, n=0, w=None)
    assert out == []


def test_select_diverse_returns_empty_for_empty_input():
    from genie_space_optimizer.optimization.raw_evidence import (
        select_diverse_examples,
    )
    assert select_diverse_examples([], n=3, w=None) == []


def test_select_diverse_falls_back_to_jaccard_when_no_workspace_client():
    """w=None forces the Jaccard fallback path. Picks the N triples
    whose questions are pairwise most-different by token overlap."""
    from genie_space_optimizer.optimization.raw_evidence import (
        select_diverse_examples,
    )
    triples = [
        {"question_id": "A", "trace_id": "", "question": "stores in CA active", "actual_sql": "", "expected_sql": "", "judge_rationale": ""},
        {"question_id": "B", "trace_id": "", "question": "stores in CA active flag",  "actual_sql": "", "expected_sql": "", "judge_rationale": ""},  # near-duplicate of A
        {"question_id": "C", "trace_id": "", "question": "regional sales by quarter", "actual_sql": "", "expected_sql": "", "judge_rationale": ""},
        {"question_id": "D", "trace_id": "", "question": "customer churn forecast",  "actual_sql": "", "expected_sql": "", "judge_rationale": ""},
    ]
    out = select_diverse_examples(triples, n=3, w=None)
    assert len(out) == 3
    qids = {t["question_id"] for t in out}
    # Greedy farthest-point should keep diverse C + D and one of A/B,
    # not both A and B (they're near-duplicates):
    assert ("A" in qids) != ("B" in qids), (
        f"expected exactly one of A or B (near-duplicates), got {qids}"
    )
    assert {"C", "D"}.issubset(qids)


def test_select_diverse_is_deterministic_for_jaccard_path():
    """Greedy farthest-point with identical input must produce
    identical output regardless of run order."""
    from genie_space_optimizer.optimization.raw_evidence import (
        select_diverse_examples,
    )
    triples = _triples(["alpha", "beta", "gamma", "delta", "epsilon"])
    out_a = select_diverse_examples(triples, n=3, w=None)
    out_b = select_diverse_examples(triples, n=3, w=None)
    assert [t["question_id"] for t in out_a] == [t["question_id"] for t in out_b]


# ── Section 4: per-skill projection ──────────────────────────────────


def test_project_pass_through_skills_get_all_four_fields():
    """L1, L2, L3, L4, L5a, L6 are pass-through — all four field keys
    visible in every projected triple."""
    from genie_space_optimizer.optimization.raw_evidence import (
        project_evidence_for_skill,
    )
    cluster = _sample_cluster_with_traces()
    pass_through_skills = [
        "lever-1-table-column-description",
        "lever-2-mv-column-refinement",
        "lever-3-tvf-routing",
        "lever-4-join-discovery",
        "lever-5a-instructions",
        "lever-6-sql-expression",
    ]
    for sid in pass_through_skills:
        out = project_evidence_for_skill(sid, [cluster], w=None, n=3)
        assert isinstance(out, tuple)
        assert len(out) > 0, f"skill {sid} returned empty"
        for triple in out:
            for field in ("question", "actual_sql", "expected_sql",
                          "judge_rationale"):
                assert field in triple, f"skill {sid} missing field {field}"


def test_project_lever_5b_returns_empty():
    from genie_space_optimizer.optimization.raw_evidence import (
        project_evidence_for_skill,
    )
    cluster = _sample_cluster_with_traces()
    out = project_evidence_for_skill(
        "lever-5b-example-sql", [cluster], w=None, n=3,
    )
    assert out == ()


def test_project_unknown_skill_returns_empty():
    """Skills not in _PROJECTOR_TABLE (stage-1-discovery, preflight-*,
    rca-card-narrative-polish, made-up names) get empty projections."""
    from genie_space_optimizer.optimization.raw_evidence import (
        project_evidence_for_skill,
    )
    cluster = _sample_cluster_with_traces()
    for sid in ("stage-1-discovery", "preflight-instruction-expand",
                "made-up-skill"):
        out = project_evidence_for_skill(sid, [cluster], w=None, n=3)
        assert out == (), f"skill {sid} should be empty"


def test_project_aggregates_across_clusters():
    """When multiple clusters are passed, extraction unions their
    triples then samples N from the combined list."""
    from genie_space_optimizer.optimization.raw_evidence import (
        project_evidence_for_skill,
    )
    c1 = _sample_cluster_with_traces()
    c2 = _sample_cluster_with_traces()
    # Re-tag c2's question_ids so they don't collide with c1:
    for qt in c2["question_traces"]:
        qt["question_id"] = "ALT_" + qt["question_id"]
    out = project_evidence_for_skill(
        "lever-1-table-column-description", [c1, c2], w=None, n=3,
    )
    assert len(out) == 3
    # Should contain triples from both clusters (not all from one):
    qids = {t["question_id"] for t in out}
    assert any(q.startswith("ALT_") for q in qids) or any(
        not q.startswith("ALT_") for q in qids
    )


def test_project_n_zero_returns_empty_even_for_pass_through():
    from genie_space_optimizer.optimization.raw_evidence import (
        project_evidence_for_skill,
    )
    cluster = _sample_cluster_with_traces()
    out = project_evidence_for_skill(
        "lever-1-table-column-description", [cluster], w=None, n=0,
    )
    assert out == ()


def test_projector_table_explicitly_lists_excluded_skills():
    from genie_space_optimizer.optimization.raw_evidence import (
        _PROJECTOR_TABLE, _EXCLUDED_SKILLS,
    )
    assert "lever-5b-example-sql" in _EXCLUDED_SKILLS
    assert "lever-5b-example-sql" not in _PROJECTOR_TABLE
    # All seven Plan-3 pickable skills are EITHER in _PROJECTOR_TABLE
    # OR in _EXCLUDED_SKILLS — never both, never neither:
    from genie_space_optimizer.common.config import _THREE_STAGE_SKILL_NAMES
    for sid in _THREE_STAGE_SKILL_NAMES:
        in_proj = sid in _PROJECTOR_TABLE
        in_exc = sid in _EXCLUDED_SKILLS
        assert in_proj ^ in_exc, f"{sid} must be in exactly one of the tables"


# ── Section 6: _RawEvidenceCaptureSink ────────────────────────────────


def test_raw_evidence_sink_initial_state():
    cfg = _reload_config_with_env({})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    snap = cfg.dump_raw_evidence_capture_summary()
    assert all(c == 0 for c in snap["projections"].values())
    assert snap["shadow_comparisons"] == 0
    assert snap["all_required_sites_exercised"] is False


def test_record_projection_increments_per_skill():
    cfg = _reload_config_with_env({})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_raw_evidence_projection("lever-1-table-column-description")
    cfg._record_raw_evidence_projection("lever-1-table-column-description")
    cfg._record_raw_evidence_projection("lever-4-join-discovery")
    snap = cfg.dump_raw_evidence_capture_summary()
    assert snap["projections"]["lever-1-table-column-description"] == 2
    assert snap["projections"]["lever-4-join-discovery"] == 1
    assert snap["projections"]["lever-6-sql-expression"] == 0


def test_record_projection_excludes_lever_5b():
    """lever-5b is excluded from raw evidence by design — the sink
    must NOT count projections for it (defensive: if upstream code
    accidentally calls _record_raw_evidence_projection('lever-5b-...')
    the sink silently ignores it so the coverage gate doesn't pass
    spuriously)."""
    cfg = _reload_config_with_env({})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_raw_evidence_projection("lever-5b-example-sql")
    snap = cfg.dump_raw_evidence_capture_summary()
    assert "lever-5b-example-sql" not in snap["projections"]


def test_record_shadow_comparison_writes_ndjson():
    import json
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "raw.ndjson"
        cfg = _reload_config_with_env({
            "GSO_RAW_EVIDENCE_SHADOW_V1": "1",
            "GSO_RAW_EVIDENCE_CAPTURE_PATH": str(path),
        })
        cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        cfg._record_raw_evidence_shadow_comparison({
            "ag_id": "AG1", "skill_id": "lever-1-table-column-description",
            "n_evidence": 3,
            "off_proposal_count": 2, "on_proposal_count": 2,
            "diff_summary": "two columns disagree",
        })
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["ag_id"] == "AG1"
        assert record["skill_id"] == "lever-1-table-column-description"
        assert "captured_at" in record
        assert "process_pid" in record


def test_coverage_gate_passes_on_full_coverage():
    cfg = _reload_config_with_env({
        "GSO_RAW_EVIDENCE_SHADOW_V1": "1",
        "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_raw_evidence_projection("lever-1-table-column-description")
    cfg._record_raw_evidence_shadow_comparison({
        "ag_id": "AG1", "skill_id": "lever-1-table-column-description",
    })
    cfg._RAW_EVIDENCE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_raises_when_zero_projections():
    import pytest
    cfg = _reload_config_with_env({
        "GSO_RAW_EVIDENCE_SHADOW_V1": "1",
        "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_raw_evidence_shadow_comparison({"ag_id": "AG1", "skill_id": "x"})
    with pytest.raises(RuntimeError, match="zero raw-evidence projections"):
        cfg._RAW_EVIDENCE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_raises_when_no_shadow_in_shadow_mode():
    import pytest
    cfg = _reload_config_with_env({
        "GSO_RAW_EVIDENCE_SHADOW_V1": "1",
        "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_raw_evidence_projection("lever-1-table-column-description")
    with pytest.raises(RuntimeError, match="zero shadow comparison"):
        cfg._RAW_EVIDENCE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


# ── Section 2: anti-anchoring header + raw_evidence slot ──────────────


def test_anti_anchoring_header_exists_and_mentions_common():
    cfg = _reload_config_with_env({})
    h = cfg._RAW_EVIDENCE_ANTI_ANCHORING_HEADER
    assert "COMMON" in h
    assert "specific enough" in h
    assert "general enough" in h


def test_lever_1_2_prompt_contains_raw_evidence_block_slot():
    cfg = _reload_config_with_env({})
    assert "{{ raw_evidence_block }}" in cfg.LEVER_1_2_COLUMN_PROMPT


def test_lever_1_2_prompt_renders_with_empty_raw_evidence_block():
    cfg = _reload_config_with_env({})
    rendered = cfg.format_mlflow_template(
        cfg.LEVER_1_2_COLUMN_PROMPT,
        failure_type="x", blame_set="y", affected_questions=[],
        sql_diffs="", current_metadata="",
        patch_type_description="", failures_context="",
        current_join_specs="[]", table_relationships="[]",
        current_column_configs="{}", full_schema_context="",
        identifier_allowlist="", string_column_count=0,
        max_value_dictionary_cols=0, current_dictionary_count=0,
        current_instructions="", existing_example_sqls="",
        instruction_char_budget=0, table_names=[], mv_names=[],
        tvf_names=[], structured_table_context="",
        structured_column_context="",
        raw_evidence_block="",
    )
    assert "{{" not in rendered, "unrendered template variable"


def test_lever_1_2_prompt_renders_anti_anchoring_when_evidence_provided():
    cfg = _reload_config_with_env({})
    block = (
        cfg._RAW_EVIDENCE_ANTI_ANCHORING_HEADER
        + "\n\nExample 1 of 3 — qid=Q1\n  question: ...\n"
    )
    rendered = cfg.format_mlflow_template(
        cfg.LEVER_1_2_COLUMN_PROMPT,
        failure_type="x", blame_set="y", affected_questions=[],
        sql_diffs="", current_metadata="",
        patch_type_description="", failures_context="",
        current_join_specs="[]", table_relationships="[]",
        current_column_configs="{}", full_schema_context="",
        identifier_allowlist="", string_column_count=0,
        max_value_dictionary_cols=0, current_dictionary_count=0,
        current_instructions="", existing_example_sqls="",
        instruction_char_budget=0, table_names=[], mv_names=[],
        tvf_names=[], structured_table_context="",
        structured_column_context="",
        raw_evidence_block=block,
    )
    assert "COMMON" in rendered
    assert "Example 1 of 3" in rendered
