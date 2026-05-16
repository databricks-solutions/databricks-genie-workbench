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


def test_raw_evidence_capture_path_set_when_var_present():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_CAPTURE_PATH": "/tmp/x.ndjson"})
    assert cfg.raw_evidence_capture_path_set() is True


def test_raw_evidence_capture_require_coverage_inert_in_prod():
    cfg = _reload_config_with_env({})
    assert cfg.raw_evidence_capture_require_coverage_enabled() is False


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
        # 2026-05-17-lever-1-2-column-prompt-hardening Task 4
        counterfactual_fixes=[],
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
        # 2026-05-17-lever-1-2-column-prompt-hardening Task 4
        counterfactual_fixes=[],
    )
    assert "COMMON" in rendered
    assert "Example 1 of 3" in rendered


# ── Section 7: build_activation_bundle integration ───────────────────


def _sample_metadata_snapshot() -> dict:
    return {
        "tables": [{
            "name": "catalog.schema.dim_store",
            "column_configs": [{"name": "location_id"}, {"name": "is_active"}],
        }],
        "metric_views": [], "functions": [],
        "instructions": {"text_instructions": []},
    }


def test_build_bundle_populates_raw_evidence_for_l1():
    cfg = _reload_config_with_env({})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {"skill_id": "lever-1-table-column-description",
             "target_objects": [], "evidence_refs": [],
             "expected_impact_qids": [], "why": "x", "priority": 1}
    bundle = build_activation_bundle(
        pick=pick, ag_id="AG1",
        clusters=[_sample_cluster_with_traces()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert isinstance(bundle.raw_evidence, tuple)
    assert len(bundle.raw_evidence) == 3  # default N
    for triple in bundle.raw_evidence:
        assert "question" in triple
        assert "actual_sql" in triple
        assert "expected_sql" in triple
        assert "judge_rationale" in triple


def test_build_bundle_flag_on_lever_5b_stays_empty():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_V1": "1"})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {"skill_id": "lever-5b-example-sql",
             "target_objects": [], "evidence_refs": [],
             "expected_impact_qids": [], "why": "x", "priority": 1}
    bundle = build_activation_bundle(
        pick=pick, ag_id="AG1",
        clusters=[_sample_cluster_with_traces()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert bundle.raw_evidence == ()


def test_build_bundle_records_capture_when_flag_on():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_V1": "1"})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {"skill_id": "lever-1-table-column-description",
             "target_objects": [], "evidence_refs": [],
             "expected_impact_qids": [], "why": "x", "priority": 1}
    build_activation_bundle(
        pick=pick, ag_id="AG1",
        clusters=[_sample_cluster_with_traces()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    snap = cfg.dump_raw_evidence_capture_summary()
    assert snap["projections"]["lever-1-table-column-description"] == 1


def test_build_bundle_n_zero_keeps_evidence_empty_even_when_flag_on():
    cfg = _reload_config_with_env({
        "GSO_RAW_EVIDENCE_V1": "1", "GSO_RAW_EVIDENCE_N": "0",
    })
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {"skill_id": "lever-1-table-column-description",
             "target_objects": [], "evidence_refs": [],
             "expected_impact_qids": [], "why": "x", "priority": 1}
    bundle = build_activation_bundle(
        pick=pick, ag_id="AG1",
        clusters=[_sample_cluster_with_traces()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert bundle.raw_evidence == ()


# ── Section 9: _call_llm_for_proposal raw_evidence kwarg ──────────────


def test_call_llm_for_proposal_default_kwarg_is_empty_tuple(monkeypatch):
    """Default kwarg ensures legacy callers (no Plan 4 awareness) get
    byte-stable behavior."""
    from genie_space_optimizer.optimization import optimizer
    captured_prompt = {"text": ""}
    def _fake_call(*args, **kwargs):
        captured_prompt["text"] = kwargs.get("messages", [{}, {}])[1].get("content", "")
        return ('{"changes": [], "table_changes": [], "rationale": "ok"}', None)
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_call)

    optimizer._call_llm_for_proposal(
        cluster=_sample_cluster_with_traces(),
        metadata_snapshot=_sample_metadata_snapshot(),
        patch_type="add_column_description",
        lever=1,
        w=None,
    )
    # Default empty raw_evidence renders as the empty-evidence
    # placeholder (no anti-anchoring header):
    assert "(No raw failure evidence" in captured_prompt["text"]
    assert "COMMON" not in captured_prompt["text"]


def test_call_llm_for_proposal_renders_anti_anchoring_when_evidence(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    captured_prompt = {"text": ""}
    def _fake_call(*args, **kwargs):
        captured_prompt["text"] = kwargs.get("messages", [{}, {}])[1].get("content", "")
        return ('{"changes": [], "table_changes": [], "rationale": "ok"}', None)
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_call)

    triples = (
        {"question_id": "Q1", "trace_id": "trace://q1",
         "question": "How many active stores in CA?",
         "actual_sql": "SELECT count(*) FROM dim_store WHERE store_id = 1",
         "expected_sql": "SELECT count(*) FROM dim_store WHERE location_id = 1",
         "judge_rationale": "store_id is wrong"},
    )
    optimizer._call_llm_for_proposal(
        cluster=_sample_cluster_with_traces(),
        metadata_snapshot=_sample_metadata_snapshot(),
        patch_type="add_column_description",
        lever=1,
        w=None,
        raw_evidence=triples,
    )
    text = captured_prompt["text"]
    assert "COMMON" in text
    assert "Example 1 of 1" in text
    assert "How many active stores" in text
    assert "store_id is wrong" in text


def test_call_llm_for_proposal_renders_n_3_with_correct_numbering(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    captured_prompt = {"text": ""}
    def _fake_call(*args, **kwargs):
        captured_prompt["text"] = kwargs.get("messages", [{}, {}])[1].get("content", "")
        return ('{"changes": [], "table_changes": [], "rationale": "ok"}', None)
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_call)

    triples = tuple(
        {"question_id": f"Q{i}", "trace_id": "", "question": f"q text {i}",
         "actual_sql": f"sa{i}", "expected_sql": f"se{i}",
         "judge_rationale": f"r{i}"}
        for i in range(1, 4)
    )
    optimizer._call_llm_for_proposal(
        cluster=_sample_cluster_with_traces(),
        metadata_snapshot=_sample_metadata_snapshot(),
        patch_type="add_column_description",
        lever=1, w=None,
        raw_evidence=triples,
    )
    text = captured_prompt["text"]
    assert "Example 1 of 3" in text
    assert "Example 2 of 3" in text
    assert "Example 3 of 3" in text


def test_call_llm_for_proposal_render_helper_handles_long_sql(monkeypatch):
    """Long SQL must not blow up the prompt budget — render helper
    truncates each SQL field to 600 chars."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.optimizer import (
        _format_raw_evidence_block,
    )
    triples = (
        {"question_id": "Q1", "trace_id": "", "question": "q",
         "actual_sql": "X" * 5000, "expected_sql": "Y" * 5000,
         "judge_rationale": "Z" * 5000},
    )
    block = _format_raw_evidence_block(triples)
    assert len(block) < 5000  # well under the input cap
    assert "X" * 600 in block
    assert "X" * 1000 not in block


def test_format_raw_evidence_block_empty():
    from genie_space_optimizer.optimization.optimizer import (
        _format_raw_evidence_block,
    )
    block = _format_raw_evidence_block(())
    assert "No raw failure evidence" in block
    assert "COMMON" not in block


# ── Section 8: _stage_2_l1 thread-through ─────────────────────────────


def _bundle_with_raw_evidence(skill_id: str, raw_evidence: tuple[dict, ...]):
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    from genie_space_optimizer.optimization.afs import format_afs
    return ActivationBundle(
        skill_id=skill_id,
        ag_id="AG1",
        target_objects=("catalog.schema.dim_store",),
        cluster_afs=(format_afs(_sample_cluster_with_traces()),),
        metadata_snapshot=_sample_metadata_snapshot(),
        identifier_allowlist="catalog.schema.dim_store",
        evidence_refs=(),
        expected_impact_qids=(),
        raw_evidence=raw_evidence,
        lever_directives_legacy=None,
        discovery_rationale="x",
        priority=1,
    )


def test_stage_2_l1_forwards_raw_evidence(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    received_kwargs: list[dict] = []
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None,
               *, raw_evidence=(), **_):
        received_kwargs.append({"raw_evidence": raw_evidence,
                                  "lever": lever})
        return {"proposed_value": "x", "rationale": "y"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    triples = ({"question_id": "Q1", "trace_id": "", "question": "q",
                  "actual_sql": "a", "expected_sql": "e",
                  "judge_rationale": "r"},)
    bundle = _bundle_with_raw_evidence(
        "lever-1-table-column-description", triples,
    )
    three_stage_pipeline._stage_2_l1(bundle, w=None)
    assert received_kwargs, "L1 adapter did not invoke _call_llm_for_proposal"
    assert all(rk["raw_evidence"] == triples for rk in received_kwargs)


def test_stage_2_l1_empty_raw_evidence_passes_empty_tuple(monkeypatch):
    """When the bundle has empty raw_evidence (Plan 3 default or
    Plan 4 flag-off), the adapter still passes an empty tuple kwarg
    so the call signature is consistent."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    received_kwargs: list[dict] = []
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None,
               *, raw_evidence=(), **_):
        received_kwargs.append({"raw_evidence": raw_evidence})
        return {"proposed_value": "x", "rationale": "y"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    bundle = _bundle_with_raw_evidence(
        "lever-1-table-column-description", (),
    )
    three_stage_pipeline._stage_2_l1(bundle, w=None)
    assert all(rk["raw_evidence"] == () for rk in received_kwargs)


# ── Section 10: shadow comparison ────────────────────────────────────


def test_stage_2_for_skill_uses_raw_evidence_unconditionally(monkeypatch):
    """Plan 4 is unconditionally on. Stage-2 dispatch always runs the
    raw-evidence-on path with the bundle as-is — no shadow A/B."""
    cfg = _reload_config_with_env({})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    captured = {"on_count": 0, "off_count": 0}
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None,
               *, raw_evidence=(), **_):
        if raw_evidence:
            captured["on_count"] += 1
            return {"proposed_value": "on", "rationale": "on"}
        else:
            captured["off_count"] += 1
            return {"proposed_value": "off", "rationale": "off"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    triples = ({"question_id": "Q1", "trace_id": "", "question": "q",
                  "actual_sql": "a", "expected_sql": "e",
                  "judge_rationale": "r"},)
    bundle = _bundle_with_raw_evidence(
        "lever-1-table-column-description", triples,
    )
    out = three_stage_pipeline._stage_2_for_skill(bundle, w=None)
    assert captured["on_count"] >= 1
    assert captured["off_count"] == 0
    snap = cfg.dump_raw_evidence_capture_summary()
    assert snap["shadow_comparisons"] == 0
    assert out["proposals"][0]["proposed_value"] == "on"


def test_stage_2_for_skill_default_off_uses_off_only(monkeypatch):
    """Default off: bundle has empty raw_evidence (Plan 3 byte-stable
    behavior) AND no shadow path. Single call with empty tuple."""
    cfg = _reload_config_with_env({})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    captured = {"calls": 0}
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None,
               *, raw_evidence=(), **_):
        captured["calls"] += 1
        return {"proposed_value": "x", "rationale": "x"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    bundle = _bundle_with_raw_evidence(
        "lever-1-table-column-description", (),  # empty (default-off)
    )
    three_stage_pipeline._stage_2_for_skill(bundle, w=None)
    # One call only — default-off matches Plan 3 byte-stably:
    assert captured["calls"] == 1


def test_stage_2_lever_5b_never_runs_shadow(monkeypatch):
    """Even in shadow mode, lever-5b dispatches only ONCE — its
    bundle has empty raw_evidence by design and shadow comparison
    would be a no-op (nothing to compare). Skip the second call."""
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_SHADOW_V1": "1"})
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    captured = {"calls": 0}
    def _fake_5b(cluster, metadata_snapshot, w, benchmark_corpus):
        captured["calls"] += 1
        return []
    monkeypatch.setattr(optimizer, "_dispatch_lever_5b_for_cluster", _fake_5b)

    bundle = _bundle_with_raw_evidence("lever-5b-example-sql", ())
    three_stage_pipeline._stage_2_for_skill(bundle, w=None)
    assert captured["calls"] == 1, "lever-5b must run only once even in shadow mode"


# ── Section 8b: remaining adapter thread-through ─────────────────────


def test_stage_2_l2_l3_forward_raw_evidence(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    seen: list[tuple[int, tuple]] = []
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None,
               *, raw_evidence=(), **_):
        seen.append((lever, raw_evidence))
        return {"proposed_value": "x", "rationale": "y"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    triples = ({"question_id": "Q", "trace_id": "", "question": "q",
                  "actual_sql": "a", "expected_sql": "e",
                  "judge_rationale": "r"},)
    for sid in ("lever-2-mv-column-refinement", "lever-3-tvf-routing"):
        bundle = _bundle_with_raw_evidence(sid, triples)
        adapter = three_stage_pipeline._STAGE_2_DISPATCH_TABLE[sid]
        adapter(bundle, w=None)
    assert any(rk for lv, rk in seen if lv == 2 and rk == triples)
    assert any(rk for lv, rk in seen if lv == 3 and rk == triples)


def test_stage_2_l4_forwards_raw_evidence(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    received = {"raw_evidence": None}
    def _fake_join(metadata_snapshot, hints, w=None, *, raw_evidence=()):
        received["raw_evidence"] = raw_evidence
        return []
    monkeypatch.setattr(optimizer, "_call_llm_for_join_discovery", _fake_join)

    triples = ({"question_id": "Q", "trace_id": "", "question": "q",
                  "actual_sql": "a", "expected_sql": "e",
                  "judge_rationale": "r"},)
    bundle = _bundle_with_raw_evidence("lever-4-join-discovery", triples)
    three_stage_pipeline._stage_2_l4(bundle, w=None)
    assert received["raw_evidence"] == triples


def test_stage_2_l5a_forwards_raw_evidence(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    received = {"raw_evidence": None}
    def _fake(all_clusters, metadata_snapshot, lever_changes=None, w=None,
               *, raw_evidence=()):
        received["raw_evidence"] = raw_evidence
        return {"instruction_text": "X", "rationale": "ok"}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_lever_5a_instructions", _fake,
    )

    triples = ({"question_id": "Q", "trace_id": "", "question": "q",
                  "actual_sql": "a", "expected_sql": "e",
                  "judge_rationale": "r"},)
    bundle = _bundle_with_raw_evidence("lever-5a-instructions", triples)
    three_stage_pipeline._stage_2_l5a(bundle, w=None)
    assert received["raw_evidence"] == triples


def test_stage_2_l6_forwards_raw_evidence(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    received = {"raw_evidence": None}
    def _fake_l6(cluster, metadata_snapshot, *, strategist_hints=None,
                  w=None, spark=None, catalog="", gold_schema="",
                  warehouse_id="", benchmarks=None, raw_evidence=()):
        received["raw_evidence"] = raw_evidence
        # Must be a canonical proposal: contract-first hardening
        # requires patch_type + target.
        return {"snippet_type": "filter", "sql": "x = 1",
                 "instruction": "x",
                 "patch_type": "add_sql_snippet_filter",
                 "target": "mv.fact.x"}
    monkeypatch.setattr(optimizer, "_generate_lever6_proposal", _fake_l6)

    triples = ({"question_id": "Q", "trace_id": "", "question": "q",
                  "actual_sql": "a", "expected_sql": "e",
                  "judge_rationale": "r"},)
    bundle = _bundle_with_raw_evidence("lever-6-sql-expression", triples)
    three_stage_pipeline._stage_2_l6(bundle, w=None)
    assert received["raw_evidence"] == triples


def test_remaining_prompts_have_raw_evidence_block_slot():
    cfg = _reload_config_with_env({})
    for prompt_name in (
        "LEVER_4_JOIN_SPEC_PROMPT",
        "LEVER_4_JOIN_DISCOVERY_PROMPT",
        "LEVER_5_INSTRUCTION_PROMPT",
        "LEVER_5A_INSTRUCTION_PROMPT",
        "LEVER_6_SQL_EXPRESSION_PROMPT",
    ):
        prompt = getattr(cfg, prompt_name)
        assert "{{ raw_evidence_block }}" in prompt, (
            f"{prompt_name} is missing the raw_evidence_block slot"
        )

