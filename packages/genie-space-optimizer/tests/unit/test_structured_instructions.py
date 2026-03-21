"""Unit tests for structured instruction enforcement."""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.applier import (
    _apply_action_to_config,
    _get_general_instructions,
)
from genie_space_optimizer.optimization.optimizer import (
    generate_proposals_from_strategy,
    normalize_instructions,
)


class TestNormalizeInstructions:
    """Test the public normalize_instructions() wrapper."""

    def test_idempotency(self):
        text = (
            "PURPOSE:\n"
            "- This space serves the analytics team.\n\n"
            "ASSET ROUTING:\n"
            "- For revenue, use fact_sales.\n"
        )
        first = normalize_instructions(text)
        second = normalize_instructions(first)
        assert first == second

    def test_unstructured_to_structured(self):
        raw = (
            "Always use fact_sales for revenue questions.\n"
            "Default to last 12 months when no date range specified."
        )
        result = normalize_instructions(raw)
        assert "PURPOSE:" in result
        assert "fact_sales" in result
        assert "12 months" in result

    def test_preserves_existing_sections(self):
        text = (
            "ASSET ROUTING:\n"
            "- Use dim_product for product lookups.\n\n"
            "TEMPORAL FILTERS:\n"
            "- Default to current fiscal year.\n"
        )
        result = normalize_instructions(text)
        assert "ASSET ROUTING:" in result
        assert "TEMPORAL FILTERS:" in result
        assert "dim_product" in result
        assert "fiscal year" in result

    def test_empty_string(self):
        assert normalize_instructions("") == ""

    def test_strips_markdown(self):
        text = "## Purpose\n**Bold instructions** about `table_a`."
        result = normalize_instructions(text)
        assert "##" not in result
        assert "**" not in result


class TestApplierNormalization:
    """Test that _apply_action_to_config normalizes instruction output."""

    @staticmethod
    def _make_config(instructions_text: str = "") -> dict:
        if not instructions_text:
            return {"instructions": {"text_instructions": [{"content": []}]}}
        return {
            "instructions": {
                "text_instructions": [{"content": [instructions_text]}]
            }
        }

    def test_add_normalizes(self):
        config = self._make_config("Use fact_sales for revenue.")
        action = {
            "command": json.dumps({
                "op": "add",
                "section": "instructions",
                "new_text": "Default to last 12 months.",
            })
        }
        result = _apply_action_to_config(config, action)
        assert result is True
        output = _get_general_instructions(config)
        assert "PURPOSE:" in output or "CONSTRAINTS:" in output
        assert "fact_sales" in output
        assert "12 months" in output

    def test_rewrite_normalizes(self):
        config = self._make_config("old stuff")
        new_text = (
            "ASSET ROUTING:\n- Use dim_product for product info.\n\n"
            "TEMPORAL FILTERS:\n- Default to YTD.\n"
        )
        action = {
            "command": json.dumps({
                "op": "rewrite",
                "section": "instructions",
                "new_text": new_text,
            })
        }
        result = _apply_action_to_config(config, action)
        assert result is True
        output = _get_general_instructions(config)
        assert "ASSET ROUTING:" in output
        assert "TEMPORAL FILTERS:" in output

    def test_update_normalizes(self):
        config = self._make_config(
            "ASSET ROUTING:\n- Use fact_sales for revenue."
        )
        action = {
            "command": json.dumps({
                "op": "update",
                "section": "instructions",
                "old_text": "Use fact_sales for revenue.",
                "new_text": "Use fact_orders for order questions.",
            })
        }
        result = _apply_action_to_config(config, action)
        assert result is True
        output = _get_general_instructions(config)
        assert "fact_orders" in output

    def test_remove_normalizes(self):
        config = self._make_config(
            "ASSET ROUTING:\n- Use fact_sales.\n- Use dim_product.\n"
        )
        action = {
            "command": json.dumps({
                "op": "remove",
                "section": "instructions",
                "old_text": "- Use dim_product.",
            })
        }
        result = _apply_action_to_config(config, action)
        assert result is True
        output = _get_general_instructions(config)
        assert "dim_product" not in output
        assert "fact_sales" in output


class TestLever5RewriteProposal:
    """Verify generate_proposals_from_strategy emits rewrite_instruction for Lever 5."""

    _METADATA = {
        "tables": [{"name": "cat.sch.orders", "columns": []}],
        "functions": [],
        "metric_views": [],
        "config": {
            "instructions": {
                "text_instructions": [
                    {"content": ["PURPOSE:\n- Analytics space."]}
                ]
            }
        },
    }

    _STRATEGY_BASE = {
        "global_instruction_rewrite": "",
        "action_groups": [
            {"id": "AG1", "root_cause_summary": "test", "source_cluster_ids": ["C1"]}
        ],
    }

    def test_lever5_instruction_guidance_emits_rewrite(self):
        ag = {
            "id": "AG1",
            "root_cause_summary": "missing routing",
            "source_cluster_ids": ["C1"],
            "affected_questions": ["q1"],
            "lever_directives": {
                "5": {
                    "instruction_guidance": "ASSET ROUTING:\n- Use orders table for order questions.",
                    "example_sqls": [],
                },
            },
            "coordination_notes": "",
        }
        proposals = generate_proposals_from_strategy(
            strategy=self._STRATEGY_BASE,
            action_group=ag,
            metadata_snapshot=self._METADATA,
            target_lever=5,
        )
        instr_proposals = [
            p for p in proposals if p.get("patch_type") == "rewrite_instruction"
        ]
        assert len(instr_proposals) == 1
        p = instr_proposals[0]
        assert p["patch_type"] == "rewrite_instruction"
        assert "old_value" in p
        assert "ASSET ROUTING:" in p["proposed_value"]
