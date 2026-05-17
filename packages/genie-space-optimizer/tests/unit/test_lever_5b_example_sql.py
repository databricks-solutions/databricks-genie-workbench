"""Tests for the lever-5b example-SQL synthesis prompt scaffolding.

Plan reference: docs/prompt_improvements/2026-05-17-lever-5b-example-sql-hardening.md
Baseline:       docs/prompt_improvements/2026-05-17-lever-5b-example-sql-baseline.md
"""
from __future__ import annotations

import pytest


# ── Task 1: max_tokens constant ──


def test_lever_5b_example_sql_max_tokens_constant_exists_and_is_sized():
    """LEVER_5B_EXAMPLE_SQL_MAX_TOKENS must exist and be sized per baseline.

    Per baseline §4.2:
      Trial-5 observed: P95=267, max=267 output tokens (n=20).
      Conservative cap (50% headroom over P95): 400 tokens.
    Therefore the cap must be >= 300 and <= 600.
    """
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_5B_EXAMPLE_SQL_MAX_TOKENS"), (
        "LEVER_5B_EXAMPLE_SQL_MAX_TOKENS must be defined in "
        "genie_space_optimizer.common.config"
    )
    assert isinstance(config.LEVER_5B_EXAMPLE_SQL_MAX_TOKENS, int)
    assert 300 <= config.LEVER_5B_EXAMPLE_SQL_MAX_TOKENS <= 600, (
        f"LEVER_5B_EXAMPLE_SQL_MAX_TOKENS="
        f"{config.LEVER_5B_EXAMPLE_SQL_MAX_TOKENS} is outside the "
        f"evidence-based band [300, 600]"
    )


# ── Task 2: render_synthesis_prompt prepends _EXAMPLE_SYNTHESIS_CONTRACT_HEADER ──


def _mk_afs_and_archetype():
    """Minimal AFS + archetype pair used by render_synthesis_prompt tests."""
    from types import SimpleNamespace

    afs = {
        "cluster_id": "C1",
        "failure_type": "wrong_qualification",
        "blame_set": ["cat.sch.fact_sales"],
        "question_count": 2,
    }
    archetype = SimpleNamespace(
        name="top_n_by_metric",
        output_shape={"shape": "top_n"},
        prompt_template="Top-N by metric guidance.",
        patch_type="add_example_sql",
    )
    return afs, archetype


def test_render_synthesis_prompt_prepends_leak_safe_contract_header():
    """The L5b synthesis prompt MUST start with the leak-safe contract
    header (same wrapper the preflight path uses)."""
    from genie_space_optimizer.optimization.synthesis import (
        render_synthesis_prompt,
    )

    afs, archetype = _mk_afs_and_archetype()
    prompt = render_synthesis_prompt(afs, archetype, "cat.sch.fact_sales")

    assert (
        "leak_safe_example_synthesis_contract" in prompt
        or "Leak-safe example synthesis contract" in prompt
    ), (
        "render_synthesis_prompt MUST prepend _EXAMPLE_SYNTHESIS_CONTRACT_HEADER "
        "so the L5b path has parity with PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT."
    )


# ── Task 3: _render_lever_5b_afs_block ──


def test_render_lever_5b_afs_block_suppresses_empty_slots():
    """Suppresses AFS slots that are empty/unknown/zero/empty-dict."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_afs_block,
    )

    afs = {
        "cluster_id": "H002",
        "failure_type": "unknown",
        "affected_judge": "schema_accuracy",
        "question_count": 0,
        "blame_set": [],
        "counterfactual_fixes": ["Drop the WHERE filter"],
        "structural_diff": {},
        "judge_verdict_pattern": "",
        "suggested_fix_summary": "Root cause: unknown",
    }
    block = _render_lever_5b_afs_block(afs)

    assert "Cluster ID: H002" in block
    assert "Affected Judge: schema_accuracy" in block
    assert "Drop the WHERE filter" in block

    assert "Failure Type:" not in block
    assert "Affected Questions: 0" not in block
    assert "Blamed Objects:" not in block
    assert "Structural Diff Classification:" not in block
    assert "Judge Verdict Pattern:" not in block
    assert "Root cause: unknown" not in block


def test_render_lever_5b_afs_block_dedupes_and_caps_counterfactual_fixes():
    """Dedup + cap at 3 bullets, per-bullet 200 chars."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_afs_block,
    )

    long_text = ("X" * 250) + " end-marker"
    fixes = [
        "Remove the filter on PAYMENT_CURRENCY_CD = USD since the question says total payment amount in USD referring to the columns unit",
        "Remove the AND t.PAYMENT_CURRENCY_CD = USD filter since the user asked for total payment amount in USD",
        "Remove the filter on PAYMENT_CURRENCY_CD = USD since the column PAYMENT_AMT may already be in USD",
        "Remove the defensive filters on PAYMENT_CURRENCY_CD = USD, FORM_OF_PAYMENT_CD IS NOT NULL",
        long_text,
    ]
    afs = {"cluster_id": "C", "counterfactual_fixes": fixes}
    block = _render_lever_5b_afs_block(afs)

    cf_section = (
        block.split("Counterfactual Fixes")[1]
        if "Counterfactual Fixes" in block else ""
    )
    bullet_count = cf_section.count("\n  - ")
    assert 1 <= bullet_count <= 3, (
        f"counterfactual_fixes must be capped at <=3 bullets; got {bullet_count}"
    )
    for line in cf_section.split("\n"):
        if line.startswith("  - "):
            content = line[4:]
            assert len(content) <= 220, (
                f"per-fix length must be <= 220 chars; got {len(content)}"
            )


def test_render_lever_5b_afs_block_handles_all_empty_afs():
    """Minimal cluster_id line + no empty section headers."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_afs_block,
    )

    afs = {"cluster_id": "X"}
    block = _render_lever_5b_afs_block(afs)
    assert "Cluster ID: X" in block
    assert "Failure Type:" not in block
    assert "Counterfactual Fixes" not in block
    assert "Blamed Objects:" not in block


# ── Task 4: _build_lever_5b_scoped_allowlist ──


def test_build_lever_5b_scoped_allowlist_uses_blame_set():
    """When AFS has a blame_set, the scoped allowlist must NOT include
    unrelated tables."""
    from genie_space_optimizer.optimization.synthesis import (
        _build_lever_5b_scoped_allowlist,
    )

    metadata_snapshot = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.fact_sales",
                    "column_configs": [
                        {"column_name": "amount", "type_text": "DECIMAL"},
                    ],
                },
                {
                    "identifier": "cat.sch.unrelated_table",
                    "column_configs": [
                        {"column_name": "x", "type_text": "STRING"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "instructions": {"join_specs": []},
        "tables": [],
        "metric_views": [],
    }
    afs = {"blame_set": ["cat.sch.fact_sales"]}
    rendered = _build_lever_5b_scoped_allowlist(metadata_snapshot, afs)

    assert "fact_sales" in rendered
    assert "unrelated_table" not in rendered, (
        "scoped allowlist must NOT include tables unrelated to the "
        "AFS blame_set"
    )


def test_build_lever_5b_scoped_allowlist_falls_back_when_blame_empty():
    """No usable blame_set => full allowlist (safety fallback)."""
    from genie_space_optimizer.optimization.synthesis import (
        _build_lever_5b_scoped_allowlist,
    )

    metadata_snapshot = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.t1",
                    "column_configs": [
                        {"column_name": "a", "type_text": "STRING"},
                    ],
                },
                {
                    "identifier": "cat.sch.t2",
                    "column_configs": [
                        {"column_name": "b", "type_text": "STRING"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "instructions": {"join_specs": []},
        "tables": [],
        "metric_views": [],
    }
    afs = {"blame_set": []}
    rendered = _build_lever_5b_scoped_allowlist(metadata_snapshot, afs)
    # Both tables must appear under the fallback.
    assert "t1" in rendered and "t2" in rendered


def test_build_lever_5b_scoped_allowlist_promotes_column_to_parent_table():
    """Column-scoped blame entry (cat.sch.tbl.col) must include the
    parent table in relevant_objects."""
    from genie_space_optimizer.optimization.synthesis import (
        _build_lever_5b_scoped_allowlist,
    )

    metadata_snapshot = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.fact_sales",
                    "column_configs": [
                        {"column_name": "amount", "type_text": "DECIMAL"},
                        {"column_name": "region", "type_text": "STRING"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "instructions": {"join_specs": []},
        "tables": [],
        "metric_views": [],
    }
    # Column-scoped blame entry — parent table must be admitted.
    afs = {"blame_set": ["cat.sch.fact_sales.amount"]}
    rendered = _build_lever_5b_scoped_allowlist(metadata_snapshot, afs)
    assert "fact_sales" in rendered


# ── Task 5: render_synthesis_prompt consumes {{ afs_block }} ──


def test_render_synthesis_prompt_consumes_afs_block_slot():
    """render_synthesis_prompt MUST call _render_lever_5b_afs_block and
    pass the result as a single {{ afs_block }} slot."""
    from types import SimpleNamespace
    from genie_space_optimizer.optimization.synthesis import (
        render_synthesis_prompt,
    )

    afs = {
        "cluster_id": "C-X",
        "failure_type": "wrong_filter",
        "blame_set": ["catalog.schema.t.c1"],
        "counterfactual_fixes": ["Drop the WHERE filter"],
    }
    archetype = SimpleNamespace(
        name="simple_enumerate",
        output_shape={"requires_constructs": ["SELECT", "LIMIT"]},
        prompt_template="Simple SELECT LIMIT pattern.",
    )
    rendered = render_synthesis_prompt(
        afs, archetype, "VALID TABLES:\n- catalog.schema.t",
    )

    assert "Cluster ID: C-X" in rendered
    assert "Failure Type: wrong_filter" in rendered
    assert "Blamed Objects: catalog.schema.t.c1" in rendered
    assert "Drop the WHERE filter" in rendered

    # Legacy slot placeholders must not appear unrendered.
    assert "{{ cluster_id }}" not in rendered
    assert "{{ failure_type }}" not in rendered
    assert "{{ blame_set }}" not in rendered
    assert "{{ counterfactual_fixes }}" not in rendered


# ── Task 6: SKILL.md restructure (XML tags, examples, output spec sync) ──


def test_lever_5b_skill_md_has_xml_structure():
    """SKILL.md body wrapped in XML tags."""
    from genie_space_optimizer.common.config import LEVER_5B_EXAMPLE_SQL_PROMPT

    prompt = LEVER_5B_EXAMPLE_SQL_PROMPT

    required_tag_pairs = [
        ("<role>", "</role>"),
        ("<context>", "</context>"),
        ("<failure_signature>", "</failure_signature>"),
        ("<archetype>", "</archetype>"),
        ("<schema>", "</schema>"),
        ("<examples>", "</examples>"),
        ("<instructions>", "</instructions>"),
        ("<output_schema>", "</output_schema>"),
    ]
    for open_tag, close_tag in required_tag_pairs:
        assert open_tag in prompt, f"missing required tag {open_tag}"
        assert close_tag in prompt, f"missing required tag {close_tag}"
        assert prompt.index(open_tag) < prompt.index(close_tag), (
            f"{open_tag} must appear before {close_tag}"
        )


def test_lever_5b_skill_md_has_two_canonical_examples():
    """<examples> block contains 2 worked few-shot examples."""
    from genie_space_optimizer.common.config import LEVER_5B_EXAMPLE_SQL_PROMPT

    prompt = LEVER_5B_EXAMPLE_SQL_PROMPT
    start = prompt.index("<examples>")
    end = prompt.index("</examples>")
    examples_body = prompt[start:end]

    assert examples_body.count("<example>") == 2
    assert examples_body.count("</example>") == 2
    assert examples_body.count('"example_question":') == 2
    assert examples_body.count('"example_sql":') == 2
    assert examples_body.count('"usage_guidance":') == 2
    assert examples_body.count('"rationale":') == 2


def test_lever_5b_skill_md_output_schema_matches_pydantic_contract():
    """SKILL.md <output_schema> block must list the exact fields that
    Lever5bExampleSqlOutput declares."""
    from genie_space_optimizer.common.config import LEVER_5B_EXAMPLE_SQL_PROMPT
    from genie_space_optimizer.optimization.prompt_io import (
        Lever5bExampleSqlOutput,
    )

    prompt = LEVER_5B_EXAMPLE_SQL_PROMPT
    start = prompt.index("<output_schema>")
    end = prompt.index("</output_schema>")
    schema_body = prompt[start:end]

    expected_fields = set(Lever5bExampleSqlOutput.model_fields.keys())
    assert expected_fields == {
        "example_question", "example_sql", "usage_guidance", "rationale",
    }, (
        f"Lever5bExampleSqlOutput shape changed; update this test and "
        f"the SKILL.md <output_schema> block. fields={expected_fields}"
    )
    for field in expected_fields:
        assert f'"{field}"' in schema_body, (
            f"<output_schema> must reference Pydantic field {field!r}"
        )


def test_lever_5b_skill_md_uses_new_slot_names():
    """SKILL.md references the new {{ afs_block }} slot, not the 9 legacy slots."""
    from genie_space_optimizer.common.config import LEVER_5B_EXAMPLE_SQL_PROMPT

    prompt = LEVER_5B_EXAMPLE_SQL_PROMPT

    assert "{{ afs_block }}" in prompt
    for legacy in (
        "{{ cluster_id }}",
        "{{ failure_type }}",
        "{{ affected_judge }}",
        "{{ question_count }}",
        "{{ blame_set }}",
        "{{ counterfactual_fixes }}",
        "{{ structural_diff }}",
        "{{ judge_verdict_pattern }}",
        "{{ suggested_fix_summary }}",
    ):
        assert legacy not in prompt, (
            f"legacy slot {legacy} must be removed (replaced by afs_block)"
        )
    for surviving in (
        "{{ archetype_name }}",
        "{{ archetype_output_shape }}",
        "{{ archetype_prompt_template }}",
        "{{ identifier_allowlist }}",
    ):
        assert surviving in prompt, f"slot {surviving} must remain in template"


# ── Task 7: LEVER_5B_EXAMPLE_SQL_SYSTEM_MSG ──


def test_lever_5b_example_sql_system_msg_constant_exists():
    """Domain-framing system message constant."""
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_5B_EXAMPLE_SQL_SYSTEM_MSG")
    msg = config.LEVER_5B_EXAMPLE_SQL_SYSTEM_MSG
    assert isinstance(msg, str) and len(msg) > 80
    assert "Genie" in msg
    assert "example" in msg.lower()
    assert "JSON" in msg


def test_synthesize_example_sqls_uses_domain_system_message(monkeypatch):
    """synthesize_example_sqls passes LEVER_5B_EXAMPLE_SQL_SYSTEM_MSG."""
    from types import SimpleNamespace
    from genie_space_optimizer.common import config
    from genie_space_optimizer.optimization import synthesis

    captured: dict = {}

    def _spy_traced(*args, **kwargs):
        captured["system_msg"] = (
            args[1] if len(args) >= 2 else kwargs.get("system_msg")
        )
        captured["max_tokens"] = kwargs.get("max_tokens")
        return (
            '{"example_question": "q", "example_sql": "SELECT 1", '
            '"usage_guidance": "u", "rationale": "r"}',
            None,
        )

    from genie_space_optimizer.optimization import optimizer as _opt
    monkeypatch.setattr(_opt, "_traced_llm_call", _spy_traced)

    def _stub_validate(*args, **kwargs):
        from genie_space_optimizer.optimization.synthesis import GateResult
        return True, [GateResult(True, "stub")]
    monkeypatch.setattr(synthesis, "validate_synthesis_proposal", _stub_validate)

    from genie_space_optimizer.optimization import afs as _afs_mod
    monkeypatch.setattr(
        _afs_mod, "format_afs",
        lambda cluster: {
            "cluster_id": cluster.get("cluster_id", "?"),
            "blame_set": cluster.get("blame_set") or [],
            "counterfactual_fixes": cluster.get("counterfactual_fixes") or [],
        },
    )
    monkeypatch.setattr(_afs_mod, "validate_afs", lambda afs, corpus: None)

    archetype = SimpleNamespace(
        name="simple_enumerate",
        output_shape={"requires_constructs": ["SELECT"]},
        prompt_template=".",
        patch_type="add_example_sql",
    )

    synthesis.synthesize_example_sqls(
        cluster={"cluster_id": "C", "blame_set": [], "counterfactual_fixes": []},
        metadata_snapshot={
            "tables": [],
            "data_sources": {"tables": [], "metric_views": []},
        },
        benchmark_corpus=[],
        archetype=archetype,
        existing_example_sql_count=0,
    )

    assert captured.get("system_msg") == config.LEVER_5B_EXAMPLE_SQL_SYSTEM_MSG, (
        f"expected LEVER_5B_EXAMPLE_SQL_SYSTEM_MSG; "
        f"got {captured.get('system_msg')!r}"
    )
    # Task 8: max_tokens is wired in the same _traced_llm_call.
    assert captured.get("max_tokens") == config.LEVER_5B_EXAMPLE_SQL_MAX_TOKENS


# ── Task 9: structured retry-feedback helper ──


def test_render_lever_5b_retry_feedback_wraps_in_xml_tag():
    """Retry feedback wrapped in <retry_feedback> tags."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_retry_feedback,
    )
    feedback = _render_lever_5b_retry_feedback(
        gate="parse",
        reason="sqlglot parse failure: unexpected token at line 1",
        rejected_proposal={"example_sql": "SELEC bad"},
    )
    assert "<retry_feedback>" in feedback
    assert "</retry_feedback>" in feedback
    assert "parse" in feedback
    assert "sqlglot parse failure" in feedback
    assert "DIFFERENT" in feedback or "different" in feedback


def test_render_lever_5b_retry_feedback_omits_full_rejected_sql_when_long():
    """Long rejected example_sql truncated to ~300 chars + ellipsis."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_retry_feedback,
    )
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(200)) + " FROM t"
    feedback = _render_lever_5b_retry_feedback(
        gate="execute",
        reason="EMPTY_RESULT — 0 rows returned",
        rejected_proposal={"example_sql": long_sql},
    )
    assert "..." in feedback
    assert len(feedback) < 1500


# ── Task 10: gate_results recorded on rejection ──


def test_synthesize_example_sqls_records_gate_results_on_failure(monkeypatch):
    """When all gates fail, attach gate_failure attribute to active span."""
    from types import SimpleNamespace
    from genie_space_optimizer.optimization import synthesis

    span_attrs: dict = {}

    class _StubSpan:
        def set_attribute(self, k, v):
            span_attrs[k] = v

    # Patch mlflow.get_current_active_span before synthesis imports it.
    import mlflow
    monkeypatch.setattr(
        mlflow, "get_current_active_span", lambda: _StubSpan(), raising=False,
    )

    from genie_space_optimizer.optimization import optimizer as _opt
    monkeypatch.setattr(
        _opt, "_traced_llm_call",
        lambda *a, **kw: ("not json", None),
    )

    from genie_space_optimizer.optimization.synthesis import GateResult
    monkeypatch.setattr(
        synthesis, "validate_synthesis_proposal",
        lambda *a, **kw: (False, [GateResult(False, "parse", "stub fail")]),
    )

    from genie_space_optimizer.optimization import afs as _afs_mod
    monkeypatch.setattr(
        _afs_mod, "format_afs",
        lambda cluster: {
            "cluster_id": cluster.get("cluster_id", "?"),
            "blame_set": cluster.get("blame_set") or [],
            "counterfactual_fixes": cluster.get("counterfactual_fixes") or [],
        },
    )
    monkeypatch.setattr(_afs_mod, "validate_afs", lambda afs, corpus: None)

    archetype = SimpleNamespace(
        name="simple_enumerate",
        output_shape={"requires_constructs": ["SELECT"]},
        prompt_template=".",
        patch_type="add_example_sql",
    )

    synthesis.synthesize_example_sqls(
        cluster={"cluster_id": "C", "blame_set": [], "counterfactual_fixes": []},
        archetype=archetype,
        metadata_snapshot={
            "tables": [],
            "data_sources": {"tables": [], "metric_views": []},
        },
        benchmark_corpus=[],
        existing_example_sql_count=0,
    )

    assert "lever_5b.gate_failure" in span_attrs, (
        f"expected lever_5b.gate_failure attribute on span; "
        f"got attrs={list(span_attrs.keys())}"
    )
    failed = span_attrs["lever_5b.gate_failure"]
    assert "parse" in failed, (
        f"expected gate='parse' in attribute; got {failed!r}"
    )
