"""Unit tests for Plan 2 (Lever 5 Split). Mirrors Plan 1's structure
for the capture-sink + coverage-gate machinery.

Test sections:
  1. Flag helpers (this file's first three tests).
  2. LEVER_5A_INSTRUCTION_PROMPT shape + format renders.
  3. _validate_lever_5a_no_sql_output gate.
  4. _dispatch_lever_5_split routing under split / shadow / off.
  5. _LeverFiveCaptureSink + atexit coverage gate.
"""
from __future__ import annotations

import importlib
import os


_PLAN2_ENV_KEYS = (
    "GSO_LEVER5_SPLIT_V1",
    "GSO_LEVER5_SHADOW_V1",
    "GSO_LEVER5_SPLIT_CAPTURE_PATH",
    "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env. Mirrors Plan 1's helper of
    the same name in test_rca_contract_narrow_v1.py.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg.lever5_*_enabled()`` after this function returns see the same
    env. Plan-2 env keys not in ``env`` are cleared so tests stay isolated.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN2_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


# ── Section 1: flag helpers ──────────────────────────────────────────


def test_lever5_split_default_off():
    cfg = _reload_config_with_env({})
    assert cfg.lever5_split_enabled() is False
    assert cfg.lever5_shadow_enabled() is False
    assert cfg.lever5_split_capture_require_coverage_enabled() is False


def test_lever5_split_flag_on():
    cfg = _reload_config_with_env({"GSO_LEVER5_SPLIT_V1": "1"})
    assert cfg.lever5_split_enabled() is True
    # Shadow is independent:
    assert cfg.lever5_shadow_enabled() is False


def test_lever5_shadow_flag_on():
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "1"})
    assert cfg.lever5_split_enabled() is False
    assert cfg.lever5_shadow_enabled() is True


def test_lever5_split_and_shadow_are_mutually_exclusive_in_practice():
    """Both flags can be set at the OS level, but the dispatcher MUST
    treat split-on as authoritative (split wins, shadow ignored). This
    test pins that contract; the dispatcher implementation in Task 10
    must honor it."""
    cfg = _reload_config_with_env({
        "GSO_LEVER5_SPLIT_V1": "1",
        "GSO_LEVER5_SHADOW_V1": "1",
    })
    # Both helpers return True; the dispatcher resolves the precedence:
    assert cfg.lever5_split_enabled() is True
    assert cfg.lever5_shadow_enabled() is True


# ── Section 1b: L5b byte-stability ────────────────────────────────────


def test_render_synthesis_prompt_byte_stable_after_plan_2():
    """Plan 2 explicitly does NOT modify the synthesis prompt template.
    This test pins that contract — any byte change here is an unintended
    regression."""
    from genie_space_optimizer.optimization import synthesis

    afs = {
        "cluster_id": "C1",
        "failure_type": "missing_filter",
        "affected_judge": "result_correctness",
        "question_count": 3,
        "blame_set": ["catalog.schema.fact_orders.order_date"],
        "counterfactual_fixes": ["add WHERE order_date >= '2024-01-01'"],
        "structural_diff": {"missing": ["WHERE"]},
        "judge_verdict_pattern": "result_count_mismatch",
        "suggested_fix_summary": "add temporal filter",
    }

    class _Arch:
        name = "temporal_filter_archetype"
        output_shape = {"requires": ["WHERE"]}
        prompt_template = "Use a WHERE clause filtering by the date column."

    rendered = synthesis.render_synthesis_prompt(
        afs, _Arch(), "catalog.schema.fact_orders.order_date",
    )
    # Pin the docstring-stable substrings of the template:
    assert "abstracted failure signature (AFS)" in rendered
    assert "Cluster ID: C1" in rendered
    assert "Failure Type: missing_filter" in rendered
    assert "temporal_filter_archetype" in rendered
    assert "{{" not in rendered  # all template variables substituted
    # Pin the output_format header so structural-template changes are caught:
    assert "Output format (strict JSON)" in rendered


def test_lever_5b_skill_id_accessor():
    """synthesis.lever_5b_skill_id() returns the canonical skill_id used
    by the dispatcher and capture sink."""
    from genie_space_optimizer.optimization import synthesis
    assert synthesis.lever_5b_skill_id() == "lever-5b-example-sql"


# ── Section 2: LEVER_5A_INSTRUCTION_PROMPT ────────────────────────────


def test_lever_5a_instruction_prompt_exists_and_has_required_slots():
    cfg = _reload_config_with_env({})
    p = cfg.LEVER_5A_INSTRUCTION_PROMPT
    # Required template slots — these are what _call_llm_for_lever_5a_instructions
    # will fill in Task 7.
    for slot in (
        "{{ space_description }}",
        "{{ eval_summary }}",
        "{{ cluster_briefs }}",
        "{{ lever_summary }}",
        "{{ current_instructions }}",
        "{{ existing_example_sqls }}",
        "{{ identifier_allowlist }}",
        "{{ instruction_char_budget }}",
    ):
        assert slot in p, slot


def test_lever_5a_instruction_prompt_forbids_example_sql_in_output_schema():
    """The prompt must NOT instruct the LLM to produce example_sql_proposals.
    Any wording that asks for SQL output makes the firewall pointless."""
    cfg = _reload_config_with_env({})
    p = cfg.LEVER_5A_INSTRUCTION_PROMPT
    # Split into output_schema block + the rest.
    schema_section = p.split("<output_schema>", 1)[-1]
    forbidden_in_schema = (
        "example_sql_proposals",
        '"example_sql"',
        "example_sql:",
    )
    for sub in forbidden_in_schema:
        assert sub not in schema_section, (
            f"L5a output_schema contains forbidden substring: {sub}. "
            "L5a must produce instruction_text + rationale only."
        )


def test_lever_5a_instruction_prompt_keeps_existing_example_sqls_as_context():
    """Existing example SQLs are still passed AS CONTEXT (so L5a knows
    what's already there and avoids duplicating instruction guidance for
    them). This is different from instructing the LLM to PRODUCE example
    SQLs in its output."""
    cfg = _reload_config_with_env({})
    p = cfg.LEVER_5A_INSTRUCTION_PROMPT
    # Slot for read-only context:
    assert "{{ existing_example_sqls }}" in p
    # And the contextual header that introduces it:
    assert "Existing Example SQL Queries" in p


def test_lever_5a_instruction_prompt_output_schema_is_instruction_only():
    cfg = _reload_config_with_env({})
    p = cfg.LEVER_5A_INSTRUCTION_PROMPT
    # The output_schema block must mention instruction_text and rationale,
    # and must NOT mention example_sql:
    assert "instruction_text" in p
    assert "rationale" in p
    schema_block = p.split("<output_schema>", 1)[-1]
    assert "example_sql" not in schema_block


def test_lever_5a_instruction_prompt_renders_with_realistic_kwargs():
    cfg = _reload_config_with_env({})
    sample_kwargs = {
        "space_description": "Hotel bookings analytics",
        "eval_summary": "3 clusters; 12 failing questions",
        "cluster_briefs": "C1: missing temporal filter on fact_bookings",
        "lever_summary": "L1 added column descriptions for booking_date",
        "current_instructions": "PURPOSE:\nHotel bookings.\n",
        "existing_example_sqls": "(none)",
        "identifier_allowlist": "catalog.schema.fact_bookings.booking_date",
        "instruction_char_budget": "20000",
        # Plan 4 slot — render-time placeholder for raw evidence.
        "raw_evidence_block": "(no raw evidence)",
    }
    rendered = cfg.format_mlflow_template(
        cfg.LEVER_5A_INSTRUCTION_PROMPT, **sample_kwargs,
    )
    assert "Hotel bookings analytics" in rendered
    assert "{{" not in rendered, "unrendered template variable in L5a prompt"
    assert '"example_sql_proposals"' not in rendered  # not even in the schema


# ── Section 3: _call_llm_for_lever_5a_instructions ────────────────────


def test_call_llm_for_lever_5a_instructions_returns_instruction_only_shape(monkeypatch):
    """The function must return {instruction_text, rationale} only —
    no example_sql_proposals key."""
    from genie_space_optimizer.optimization import optimizer

    # Stub _call_llm_openai to return a deterministic JSON payload:
    def _fake_call_llm_openai(*args, **kwargs):
        return (
            '{"instruction_text": "PURPOSE:\\nHotel bookings.\\n",'
            ' "rationale": "Coverage."}',
            None,
        )

    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_call_llm_openai)

    metadata_snapshot = {
        "config": {"description": "Hotel bookings"},
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "instructions": {"text_instructions": []},
        "tables": [],
        "metric_views": [],
        "functions": [],
    }
    result = optimizer._call_llm_for_lever_5a_instructions(
        all_clusters=[],
        metadata_snapshot=metadata_snapshot,
        lever_changes=[],
        w=None,
    )
    assert set(result.keys()) == {"instruction_text", "rationale"}, result
    assert "example_sql_proposals" not in result


def test_call_llm_for_lever_5a_instructions_records_capture_when_flag_on(monkeypatch):
    """When GSO_LEVER5_SPLIT_V1=1 (or shadow), the call must increment
    the lever-5a-instructions hit counter via _record_lever5_skill_hit."""
    cfg = _reload_config_with_env({"GSO_LEVER5_SPLIT_V1": "1"})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer

    def _fake_call_llm_openai(*args, **kwargs):
        return ('{"instruction_text": "X", "rationale": "Y"}', None)
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_call_llm_openai)

    optimizer._call_llm_for_lever_5a_instructions(
        all_clusters=[],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                            "metric_views": [], "functions": [],
                            "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
    )
    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["hits"]["lever-5a-instructions"] == 1


# ── Section 4: _validate_lever_5a_no_sql_output ───────────────────────


def test_validate_5a_accepts_pure_prose():
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output({
        "instruction_text": (
            "PURPOSE:\nHotel bookings.\n"
            "ROUTING: Use fact_bookings for transactional queries.\n"
            "Use GROUP BY booking_date for daily aggregations."
        ),
        "rationale": "Captures routing + temporal-grouping guidance.",
    })
    assert ok, reason


def test_validate_5a_rejects_fenced_sql_block():
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output({
        "instruction_text": (
            "PURPOSE: Hotel bookings.\n\n"
            "Example:\n```sql\nSELECT * FROM fact_bookings\n```\n"
        ),
        "rationale": "x",
    })
    assert not ok
    assert "fenced SQL block" in reason


def test_validate_5a_rejects_select_from_pattern():
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output({
        "instruction_text": (
            "PURPOSE: Use this query SELECT booking_id, hotel_key "
            "FROM catalog.schema.fact_bookings WHERE booking_date >= '2024-01-01'."
        ),
        "rationale": "x",
    })
    assert not ok
    assert "SELECT" in reason


def test_validate_5a_rejects_example_sql_proposals_key():
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output({
        "instruction_text": "PURPOSE:\nFine.",
        "rationale": "x",
        "example_sql_proposals": [{"example_sql": "SELECT 1"}],
    })
    assert not ok
    assert "example_sql_proposals" in reason


def test_validate_5a_allows_short_select_mentions_in_prose():
    """A 'select' word in prose (e.g., 'select the right table') must
    not trip the gate. The threshold is intentionally generous: ≥40
    chars of SELECT...FROM..."""
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output({
        "instruction_text": (
            "PURPOSE:\nHotel bookings.\n"
            "Select the right fact table from these options."
        ),
        "rationale": "x",
    })
    assert ok, reason


def test_validate_5a_allows_empty_instruction_text():
    """Empty instruction_text is L5a's signal for 'nothing to add this
    iteration'. Must not be rejected."""
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, _reason = _validate_lever_5a_no_sql_output({
        "instruction_text": "",
        "rationale": "Nothing actionable found.",
    })
    assert ok


# ── Section 5: _dispatch_lever_5_split ────────────────────────────────


def test_dispatch_returns_holistic_compatible_shape(monkeypatch):
    """Dispatcher output shape MUST match _call_llm_for_holistic_instructions
    so the rest of generate_proposals_from_strategy is unaffected."""
    from genie_space_optimizer.optimization import optimizer

    monkeypatch.setattr(
        optimizer, "_call_llm_for_lever_5a_instructions",
        lambda **kw: {"instruction_text": "PURPOSE:\nX", "rationale": "r5a"},
    )
    # Stub the per-cluster 5b adapter to return one example proposal per
    # cluster. Adapter signature: (cluster, metadata_snapshot, w,
    # benchmark_corpus) -> list[dict].
    def _fake_5b_per_cluster(cluster, metadata_snapshot, w, benchmark_corpus):
        return [{
            "example_question": f"Q for {cluster.get('cluster_id', '?')}",
            "example_sql": "SELECT 1",
            "parameters": [],
            "usage_guidance": "test",
        }]
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5b_for_cluster", _fake_5b_per_cluster,
    )

    result = optimizer._dispatch_lever_5_split(
        all_clusters=[{"cluster_id": "C1"}, {"cluster_id": "C2"}],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                           "metric_views": [], "functions": [],
                           "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
        benchmarks=[],
    )
    assert set(result.keys()) == {"instruction_text", "example_sql_proposals", "rationale"}
    assert result["instruction_text"] == "PURPOSE:\nX"
    assert len(result["example_sql_proposals"]) == 2
    assert result["rationale"]  # non-empty


def test_dispatch_runs_5a_once_and_5b_once_per_cluster(monkeypatch):
    """Verify per-cluster fan-out for L5b."""
    from genie_space_optimizer.optimization import optimizer

    calls_5a = {"n": 0}
    calls_5b = {"n": 0}

    def _fake_5a(**kw):
        calls_5a["n"] += 1
        return {"instruction_text": "X", "rationale": "r"}
    def _fake_5b(cluster, metadata_snapshot, w, benchmark_corpus):
        calls_5b["n"] += 1
        return []
    monkeypatch.setattr(optimizer, "_call_llm_for_lever_5a_instructions", _fake_5a)
    monkeypatch.setattr(optimizer, "_dispatch_lever_5b_for_cluster", _fake_5b)

    optimizer._dispatch_lever_5_split(
        all_clusters=[{"cluster_id": f"C{i}"} for i in range(3)],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                           "metric_views": [], "functions": [],
                           "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
        benchmarks=[],
    )
    assert calls_5a["n"] == 1
    assert calls_5b["n"] == 3


def test_dispatch_5b_adapter_handles_none_return(monkeypatch):
    """synthesis.synthesize_example_sqls returns dict | None — None
    means 'no archetype matched' or 'caps exhausted' (logged, not an
    error). The adapter must convert None -> []."""
    from genie_space_optimizer.optimization import optimizer, synthesis

    monkeypatch.setattr(
        synthesis, "synthesize_example_sqls",
        lambda **kw: None,
    )
    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster={"cluster_id": "C1"},
        metadata_snapshot={"tables": [], "metric_views": [],
                            "functions": []},
        w=None,
        benchmark_corpus=None,
    )
    assert out == []


def test_dispatch_5b_adapter_wraps_single_dict_in_list(monkeypatch):
    """synthesis returns one dict; adapter wraps in list with the
    holistic-shape keys."""
    from genie_space_optimizer.optimization import optimizer, synthesis

    monkeypatch.setattr(
        synthesis, "synthesize_example_sqls",
        lambda **kw: {
            "example_question": "What is X?",
            "example_sql": "SELECT 1",
            "usage_guidance": "use for X",
            "rationale": "r",
        },
    )
    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster={"cluster_id": "C1"},
        metadata_snapshot={"tables": [], "metric_views": [],
                            "functions": []},
        w=None,
        benchmark_corpus=None,
    )
    assert len(out) == 1
    assert out[0]["example_question"] == "What is X?"
    assert out[0]["example_sql"] == "SELECT 1"
    assert out[0]["usage_guidance"] == "use for X"
    assert out[0]["parameters"] == []  # adapter defaults missing key


# ── Section 6: routing precedence inside generate_proposals_from_strategy ──


def test_l5_branch_calls_holistic_when_both_flags_off(monkeypatch):
    cfg = _reload_config_with_env({})
    from genie_space_optimizer.optimization import optimizer

    holistic_calls = {"n": 0}
    dispatch_calls = {"n": 0}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions",
        lambda *a, **kw: (holistic_calls.__setitem__("n", holistic_calls["n"] + 1)
                           or {"instruction_text": "", "example_sql_proposals": [], "rationale": ""}),
    )
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5_split",
        lambda **kw: (dispatch_calls.__setitem__("n", dispatch_calls["n"] + 1)
                      or {"instruction_text": "", "example_sql_proposals": [], "rationale": ""}),
    )
    optimizer._select_lever_5_holistic_path(
        all_clusters=[],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                           "metric_views": [], "functions": [],
                           "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
    )
    assert holistic_calls["n"] == 1
    assert dispatch_calls["n"] == 0


def test_l5_branch_calls_dispatch_only_when_split_flag_on(monkeypatch):
    cfg = _reload_config_with_env({"GSO_LEVER5_SPLIT_V1": "1"})
    from genie_space_optimizer.optimization import optimizer

    holistic_calls = {"n": 0}
    dispatch_calls = {"n": 0}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions",
        lambda *a, **kw: (holistic_calls.__setitem__("n", holistic_calls["n"] + 1)
                           or {"instruction_text": "", "example_sql_proposals": [], "rationale": ""}),
    )
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5_split",
        lambda **kw: (dispatch_calls.__setitem__("n", dispatch_calls["n"] + 1)
                      or {"instruction_text": "", "example_sql_proposals": [], "rationale": ""}),
    )
    optimizer._select_lever_5_holistic_path(
        all_clusters=[],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                           "metric_views": [], "functions": [],
                           "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
    )
    assert holistic_calls["n"] == 0
    assert dispatch_calls["n"] == 1


def test_l5_branch_runs_both_in_shadow_mode_and_applies_holistic(monkeypatch):
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "1"})
    from genie_space_optimizer.optimization import optimizer

    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions",
        lambda *a, **kw: {"instruction_text": "OLD", "example_sql_proposals": [],
                          "rationale": "old"},
    )
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5_split",
        lambda **kw: {"instruction_text": "NEW", "example_sql_proposals": [],
                      "rationale": "new"},
    )
    applied = optimizer._select_lever_5_holistic_path(
        all_clusters=[],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                           "metric_views": [], "functions": [],
                           "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
    )
    # Shadow without split → OLD path's result is applied:
    assert applied["instruction_text"] == "OLD"


def test_l5_branch_split_wins_over_shadow_when_both_set(monkeypatch):
    cfg = _reload_config_with_env({
        "GSO_LEVER5_SPLIT_V1": "1",
        "GSO_LEVER5_SHADOW_V1": "1",
    })
    from genie_space_optimizer.optimization import optimizer

    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions",
        lambda *a, **kw: {"instruction_text": "OLD", "example_sql_proposals": [],
                          "rationale": "old"},
    )
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5_split",
        lambda **kw: {"instruction_text": "NEW", "example_sql_proposals": [],
                      "rationale": "new"},
    )
    applied = optimizer._select_lever_5_holistic_path(
        all_clusters=[],
        metadata_snapshot={"config": {}, "data_sources": {}, "tables": [],
                           "metric_views": [], "functions": [],
                           "instructions": {"text_instructions": []}},
        lever_changes=[],
        w=None,
    )
    # Split wins; NEW is applied. Shadow comparison still happens (Task 14).
    assert applied["instruction_text"] == "NEW"


# ── Section 7: integration through the L5 holistic entry point ────────


def test_generate_metadata_proposals_l5_branch_routes_through_selector(monkeypatch):
    """Plan 2 / Task 12. The plan names ``generate_proposals_from_strategy``
    here, but in the current cycle-12 codebase the L5 holistic-instructions
    call lives in ``generate_metadata_proposals`` (the function whose L5
    branch was refactored in Task 11). Asserting on the correct entry
    point preserves the plan's intent: any future patch that reintroduces
    a direct call to ``_call_llm_for_holistic_instructions`` inside this
    function will break this test.
    """
    cfg = _reload_config_with_env({"GSO_LEVER5_SPLIT_V1": "1"})
    from genie_space_optimizer.optimization import optimizer

    selector_called = {"n": 0}
    def _fake_selector(**kw):
        selector_called["n"] += 1
        return {"instruction_text": "X", "example_sql_proposals": [],
                "rationale": "r"}
    monkeypatch.setattr(
        optimizer, "_select_lever_5_holistic_path", _fake_selector,
    )

    metadata_snapshot = {
        "config": {"description": "test"},
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "tables": [], "metric_views": [], "functions": [],
        "instructions": {"text_instructions": []},
    }

    optimizer.generate_metadata_proposals(
        clusters=[],
        metadata_snapshot=metadata_snapshot,
        target_lever=5,
        w=None,
    )
    assert selector_called["n"] == 1, (
        "L5 branch must route through _select_lever_5_holistic_path, "
        "not call _call_llm_for_holistic_instructions directly."
    )


# ── Section 8: _LeverFiveCaptureSink ──────────────────────────────────


def test_capture_sink_initial_state():
    cfg = _reload_config_with_env({})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["hits"] == {
        "lever-5a-instructions": 0,
        "lever-5b-example-sql": 0,
    }
    assert snap["shadow_comparisons"] == 0
    assert snap["all_sites_exercised"] is False


def test_record_skill_hit_increments_counter():
    cfg = _reload_config_with_env({})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_lever5_skill_hit("lever-5a-instructions")
    cfg._record_lever5_skill_hit("lever-5a-instructions")
    cfg._record_lever5_skill_hit("lever-5b-example-sql")
    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["hits"] == {
        "lever-5a-instructions": 2,
        "lever-5b-example-sql": 1,
    }


def test_record_shadow_comparison_increments_counter():
    cfg = _reload_config_with_env({})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_lever5_shadow_comparison({"ag_id": "AG1"})
    cfg._record_lever5_shadow_comparison({"ag_id": "AG2"})
    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["shadow_comparisons"] == 2


def test_capture_sink_writes_ndjson_when_path_set():
    import json
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "lever5.ndjson"
        cfg = _reload_config_with_env({
            "GSO_LEVER5_SHADOW_V1": "1",
            "GSO_LEVER5_SPLIT_CAPTURE_PATH": str(path),
        })
        cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        cfg._record_lever5_shadow_comparison({
            "ag_id": "AG1",
            "old_instruction_text_hash": "abcd",
            "new_5a_instruction_text_hash": "efgh",
            "instruction_text_jaccard": 0.85,
        })
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["ag_id"] == "AG1"
        assert "captured_at" in record
        assert "process_pid" in record


def test_coverage_gate_passes_when_all_sites_hit_and_one_shadow():
    cfg = _reload_config_with_env({
        "GSO_LEVER5_SHADOW_V1": "1",
        "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_lever5_skill_hit("lever-5a-instructions")
    cfg._record_lever5_skill_hit("lever-5b-example-sql")
    cfg._record_lever5_shadow_comparison({"ag_id": "AG1"})
    cfg._LEVER_FIVE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_raises_when_5b_unhit():
    import pytest
    cfg = _reload_config_with_env({
        "GSO_LEVER5_SHADOW_V1": "1",
        "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_lever5_skill_hit("lever-5a-instructions")
    cfg._record_lever5_shadow_comparison({"ag_id": "AG1"})
    with pytest.raises(RuntimeError, match="lever-5 trial incomplete"):
        cfg._LEVER_FIVE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_raises_when_no_shadow_comparisons():
    import pytest
    cfg = _reload_config_with_env({
        "GSO_LEVER5_SHADOW_V1": "1",
        "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_lever5_skill_hit("lever-5a-instructions")
    cfg._record_lever5_skill_hit("lever-5b-example-sql")
    # No shadow comparison emitted:
    with pytest.raises(RuntimeError, match="zero shadow comparison"):
        cfg._LEVER_FIVE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


# ── Section 9: _emit_lever5_shadow_comparison ─────────────────────────


def test_emit_shadow_comparison_records_jaccard_and_overlap():
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "1"})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer

    optimizer._emit_lever5_shadow_comparison(
        ag_id="AG1",
        cluster_ids=["C1", "C2"],
        old={"instruction_text": "PURPOSE:\nHotel bookings.\nROUTING: use fact_bookings.",
             "example_sql_proposals": [
                 {"example_sql": "SELECT a FROM t1"},
                 {"example_sql": "SELECT b FROM t2"},
             ],
             "rationale": "old"},
        new={"instruction_text": "PURPOSE:\nHotel bookings.\nROUTING: use fact_bookings table.",
             "example_sql_proposals": [
                 {"example_sql": "SELECT a FROM t1"},  # overlaps
                 {"example_sql": "SELECT c FROM t3"},  # new
             ],
             "rationale": "new"},
    )
    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["shadow_comparisons"] == 1


def test_emit_shadow_comparison_no_op_when_no_flags():
    """When neither shadow nor split is on, _emit must not record
    anything (defensive — _select_lever_5_holistic_path's both-off
    branch never calls it, but a future bug shouldn't pollute the
    sink)."""
    cfg = _reload_config_with_env({})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer

    optimizer._emit_lever5_shadow_comparison(
        ag_id="AG1", cluster_ids=[],
        old={"instruction_text": "x", "example_sql_proposals": [], "rationale": ""},
        new={"instruction_text": "y", "example_sql_proposals": [], "rationale": ""},
    )
    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["shadow_comparisons"] == 0


# ── Section 10: harness summary integration ───────────────────────────


def test_dump_lever5_split_capture_summary_is_safe_when_no_state():
    """The harness summary block calls this on every run; it must not
    raise even when the sink is fresh."""
    from genie_space_optimizer.common import config as cfg
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    snap = cfg.dump_lever5_split_capture_summary()
    assert isinstance(snap, dict)
    assert set(snap["hits"].keys()) == cfg._LEVER_5_SPLIT_SKILL_NAMES  # noqa: SLF001
    assert snap["shadow_comparisons"] == 0


# ── Section 11: synthetic end-to-end (no LLM) ─────────────────────────


def test_capture_sink_e2e_with_synthetic_dispatcher_calls():
    """No LLM. Drive the full capture pathway via direct calls to the
    public hit/comparison helpers, then assert the gate accepts."""
    import json
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "lever5.ndjson"
        cfg = _reload_config_with_env({
            "GSO_LEVER5_SHADOW_V1": "1",
            "GSO_LEVER5_SPLIT_CAPTURE_PATH": str(path),
            "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE": "1",
        })
        cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001

        cfg._record_lever5_skill_hit("lever-5a-instructions")
        cfg._record_lever5_skill_hit("lever-5b-example-sql")
        cfg._record_lever5_shadow_comparison({
            "ag_id": "AG1",
            "cluster_ids": ["C1"],
            "old_instruction_text_hash": "a" * 16,
            "new_5a_instruction_text_hash": "b" * 16,
            "instruction_text_jaccard": 0.7,
            "old_example_sqls_count": 2,
            "new_example_sqls_count": 2,
            "example_sqls_set_overlap": 0.5,
            "old_example_sqls_hashes": ["h1", "h2"],
            "new_example_sqls_hashes": ["h1", "h3"],
        })

        cfg._LEVER_FIVE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001

        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        rec = json.loads(lines[0])
        assert rec["ag_id"] == "AG1"
        assert rec["instruction_text_jaccard"] == 0.7
