"""Tests for the lever-1 RCA-bridge SKILL.md template rendering and content.

Plan reference: docs/prompt_improvements/2026-05-17-lever-1-rca-bridge-hardening.md
"""
from __future__ import annotations

import pytest


def test_lever_1_rca_bridge_max_tokens_constant_exists_and_is_sized():
    """LEVER_1_RCA_BRIDGE_MAX_TOKENS must exist and be sized per baseline."""
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_1_RCA_BRIDGE_MAX_TOKENS"), (
        "LEVER_1_RCA_BRIDGE_MAX_TOKENS must be defined in "
        "genie_space_optimizer.common.config"
    )
    assert isinstance(config.LEVER_1_RCA_BRIDGE_MAX_TOKENS, int)
    assert 250 <= config.LEVER_1_RCA_BRIDGE_MAX_TOKENS <= 600, (
        f"LEVER_1_RCA_BRIDGE_MAX_TOKENS="
        f"{config.LEVER_1_RCA_BRIDGE_MAX_TOKENS} is outside the "
        f"evidence-based band [250, 600]"
    )


def test_lever_1_rca_bridge_prompt_has_xml_structure():
    """The RCA-bridge SKILL.md body must be wrapped in XML tags."""
    from genie_space_optimizer.common.config import LEVER_1_RCA_BRIDGE_PROMPT

    prompt = LEVER_1_RCA_BRIDGE_PROMPT

    required_tag_pairs = [
        ("<role>", "</role>"),
        ("<unified_rca_engine_contract>", "</unified_rca_engine_contract>"),
        ("<context>", "</context>"),
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


# ── Helper tests (Task 4) ─────────────────────────────────────────────


def test_render_rca_bridge_slots_produces_clean_strings():
    """_render_rca_bridge_slots must produce string-typed slot values
    that the SKILL.md template can render without Python-repr artifacts.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _render_rca_bridge_slots,
    )

    afs_projections = [
        {
            "failure_type": "wrong_column",
            "blame_set": ["catalog.schema.t.c1"],
            "structural_diff": {"wrong_columns": [{"actual": "c2", "expected": "c1"}]},
        },
        {
            "failure_type": "missing_definition",
            "blame_set": ["catalog.schema.t.c1"],
            "structural_diff": {},
        },
    ]

    slots = _render_rca_bridge_slots(
        is_table_level=False,
        table="catalog.schema.t",
        column="c1",
        afs_projections=afs_projections,
        expected_objects=["catalog.schema.t.c1"],
        actual_objects=["catalog.schema.t.c2", "catalog.schema.t.c3"],
        existing_synonyms=["c one", "first column"],
    )

    assert slots["target_label"] == "column catalog.schema.t.c1"
    assert slots["is_table_level"] is False
    assert slots["expected_objects_joined"] == "catalog.schema.t.c1"
    assert "[" not in slots["expected_objects_joined"]
    assert slots["actual_objects_joined"] == "catalog.schema.t.c2, catalog.schema.t.c3"
    assert "### Cluster 1" in slots["afs_projections_rendered"]
    assert "### Cluster 2" in slots["afs_projections_rendered"]
    assert "wrong_column" in slots["afs_projections_rendered"]
    assert "['" not in slots["afs_projections_rendered"]
    assert "- c one" in slots["existing_synonyms_rendered"]
    assert "- first column" in slots["existing_synonyms_rendered"]
    # Column-level: synonyms_instruction_rule non-empty, output_schema includes synonyms
    assert "ynonyms" in slots["synonyms_instruction_rule"]  # case-insensitive substring
    assert '"synonyms"' in slots["output_schema_block"]


def test_render_rca_bridge_slots_table_level():
    """Table-level case: target_label uses 'table ...' prefix; output_schema
    omits the synonyms key."""
    from genie_space_optimizer.optimization.optimizer import (
        _render_rca_bridge_slots,
    )

    slots = _render_rca_bridge_slots(
        is_table_level=True,
        table="catalog.schema.t",
        column="",
        afs_projections=[],
        expected_objects=[],
        actual_objects=[],
        existing_synonyms=[],
    )

    assert slots["target_label"] == "table catalog.schema.t"
    assert slots["is_table_level"] is True
    assert slots["expected_objects_joined"] == "(none)"
    assert slots["actual_objects_joined"] == "(none)"
    assert slots["existing_synonyms_rendered"] == "(none)"
    assert "(no failure clusters)" in slots["afs_projections_rendered"]
    # Table-level: synonyms_instruction_rule empty
    assert slots["synonyms_instruction_rule"] == ""
    # Table-level output_schema does NOT include synonyms as a JSON key
    assert '"synonyms"' not in slots["output_schema_block"]


def test_generate_lever1_rca_proposal_uses_rendered_slots(monkeypatch):
    """_generate_lever1_rca_proposal must pass the slot dict from
    _render_rca_bridge_slots to format_mlflow_template."""
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    real_format = optimizer.format_mlflow_template

    def _spy_format(template, **kwargs):
        captured["kwargs"] = kwargs
        return real_format(template, **kwargs)

    monkeypatch.setattr(optimizer, "format_mlflow_template", _spy_format)

    def _stub_llm(*args, **kwargs):
        captured["traced_kwargs"] = kwargs
        captured["traced_args"] = args
        return ('{"description": "ok", "synonyms": ["a b"]}', None)

    monkeypatch.setattr(optimizer, "_traced_llm_call", _stub_llm)

    class _StubTheme:
        rca_id = "rca-test"
        target_qids = ("q1",)

    patch_dict = {
        "type": "add_column_synonym",
        "table": "catalog.schema.t",
        "column": "c1",
        "intent": "Route 'shop' queries to c1, not c2.",
        "expected_objects": ["catalog.schema.t.c1"],
        "actual_objects": ["catalog.schema.t.c2"],
    }
    long_existing = "X" * 500
    metadata_snapshot = {
        "tables": [{
            "identifier": "catalog.schema.t",
            "columns": [
                {"name": "c1", "description": long_existing, "synonyms": ["c-one"]}
            ],
        }],
    }

    optimizer._generate_lever1_rca_proposal(
        _StubTheme(), patch_dict, metadata_snapshot,
    )

    kwargs = captured["kwargs"]
    expected_slot_names = {
        "target_label",
        "expected_objects_joined",
        "actual_objects_joined",
        "afs_projections_rendered",
        "existing_synonyms_rendered",
        "intent",
        "existing_description",
        "synonyms_instruction_rule",
        "output_schema_block",
    }
    assert expected_slot_names.issubset(set(kwargs.keys())), (
        f"missing slots; got {set(kwargs.keys())}; expected superset of {expected_slot_names}"
    )
    assert kwargs["target_label"] == "column catalog.schema.t.c1"
    assert kwargs["expected_objects_joined"] == "catalog.schema.t.c1"
    assert kwargs["actual_objects_joined"] == "catalog.schema.t.c2"
    assert "[" not in kwargs["expected_objects_joined"]
    # Task 3 — existing_description must be untruncated
    assert len(kwargs["existing_description"]) == 500


def test_generate_lever1_rca_proposal_passes_max_tokens(monkeypatch):
    """RCA-bridge call must pass LEVER_1_RCA_BRIDGE_MAX_TOKENS."""
    from genie_space_optimizer.common import config
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    def _spy_traced(*args, **kwargs):
        captured["kwargs"] = kwargs
        return ('{"description": "ok", "synonyms": ["a b"]}', None)

    monkeypatch.setattr(optimizer, "_traced_llm_call", _spy_traced)

    class _StubTheme:
        rca_id = "rca-test"
        target_qids = ("q1",)

    patch_dict = {
        "type": "add_column_synonym",
        "table": "catalog.schema.t",
        "column": "c1",
        "intent": "test",
        "expected_objects": [],
        "actual_objects": [],
    }
    optimizer._generate_lever1_rca_proposal(
        _StubTheme(), patch_dict, {"tables": []}
    )
    assert captured["kwargs"].get("max_tokens") == config.LEVER_1_RCA_BRIDGE_MAX_TOKENS


def test_generate_lever1_rca_proposal_passes_response_model(monkeypatch):
    """RCA-bridge call must pass response_model=Lever1RcaBridgeOutput."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.prompt_io import (
        Lever1RcaBridgeOutput,
    )

    captured: dict = {}

    def _spy_traced(*args, **kwargs):
        captured["kwargs"] = kwargs
        return ('{"description": "ok", "synonyms": ["a b"]}', None)

    monkeypatch.setattr(optimizer, "_traced_llm_call", _spy_traced)

    class _StubTheme:
        rca_id = "rca-test"
        target_qids = ("q1",)

    patch_dict = {
        "type": "add_column_synonym",
        "table": "catalog.schema.t",
        "column": "c1",
        "intent": "test",
        "expected_objects": [],
        "actual_objects": [],
    }
    optimizer._generate_lever1_rca_proposal(
        _StubTheme(), patch_dict, {"tables": []}
    )
    assert captured["kwargs"].get("response_model") is Lever1RcaBridgeOutput


def test_generate_lever1_rca_proposal_uses_domain_system_message(monkeypatch):
    """RCA-bridge call must use LEVER_1_2_SYSTEM_MSG."""
    from genie_space_optimizer.common import config
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    def _spy_traced(*args, **kwargs):
        captured["system_msg"] = (
            args[1] if len(args) >= 2 else kwargs.get("system_msg")
        )
        return ('{"description": "ok", "synonyms": ["a b"]}', None)

    monkeypatch.setattr(optimizer, "_traced_llm_call", _spy_traced)

    class _StubTheme:
        rca_id = "rca-test"
        target_qids = ("q1",)

    patch_dict = {
        "type": "add_column_synonym",
        "table": "catalog.schema.t",
        "column": "c1",
        "intent": "test",
        "expected_objects": [],
        "actual_objects": [],
    }
    optimizer._generate_lever1_rca_proposal(
        _StubTheme(), patch_dict, {"tables": []}
    )
    assert captured["system_msg"] == config.LEVER_1_2_SYSTEM_MSG
