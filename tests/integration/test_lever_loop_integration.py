"""Integration test: lever loop — cluster → propose → apply → eval → accept/rollback."""

from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.applier import (
    _apply_action_to_config,
    apply_patch_set,
    classify_risk,
    proposals_to_patches,
    render_patch,
)
from genie_space_optimizer.optimization.optimizer import (
    _map_to_lever,
    cluster_failures,
    detect_conflicts_and_batch,
    detect_regressions,
)


class TestClusterToPropose:
    """Failure clustering → lever mapping → proposal structure."""

    def test_cluster_to_lever_mapping(self, sample_clusters):
        for cluster in sample_clusters:
            lever = _map_to_lever(
                cluster["root_cause"],
                asi_failure_type=cluster.get("asi_failure_type"),
            )
            assert 1 <= lever <= 5

    def test_proposals_to_patches_structure(self, sample_clusters):
        proposals = []
        for c in sample_clusters:
            proposals.append(
                {
                    "proposal_id": c["cluster_id"],
                    "lever": _map_to_lever(c["root_cause"]),
                    "proposed_value": "Fix description for orders",
                    "change_description": "Fix description",
                    "questions_fixed": len(c["question_ids"]),
                    "confidence": c["confidence"],
                    "asi": {
                        "failure_type": c.get("asi_failure_type", c["root_cause"]),
                        "blame_set": [c["asi_blame_set"]] if c["asi_blame_set"] else [],
                        "counterfactual_fixes": c.get("asi_counterfactual_fixes", []),
                    },
                }
            )
        patches = proposals_to_patches(proposals)
        assert len(patches) == len(proposals)
        for p in patches:
            assert "type" in p
            assert "lever" in p
            assert "risk_level" in p


class TestApplyPatchToConfig:
    """Render patch → apply to config → verify config mutated."""

    def test_single_lever_apply(self, sample_metadata):
        config = copy.deepcopy(sample_metadata)
        config["data_sources"] = {
            "tables": config.pop("tables"),
        }
        config.setdefault("instructions", {})
        config["instructions"]["text_instructions"] = [
            {"id": "instr_1", "content": ["Original instructions."]}
        ]

        patch = {
            "type": "add_instruction",
            "new_text": "Always filter by status = active.",
            "lever": 5,
        }
        rendered = render_patch(patch, "space-1", config)
        applied = _apply_action_to_config(config, rendered)
        assert applied is True

        instructions_text = config["instructions"]["text_instructions"][0]["content"]
        joined = "\n".join(instructions_text)
        assert "active" in joined


class TestApplyPatchSet:
    def test_apply_patch_set_returns_log(self, sample_metadata):
        patches = [
            {"type": "add_instruction", "new_text": "Use UTC.", "lever": 5},
        ]
        log = apply_patch_set(
            w=None,
            space_id="space-1",
            patches=patches,
            metadata_snapshot=sample_metadata,
        )
        assert "pre_snapshot" in log
        assert "post_snapshot" in log
        assert "applied" in log
        assert len(log["applied"]) == 1


class TestEvalAfterApply:
    """After applying patches, verify regression detection works."""

    def test_no_regression(self):
        prev = {"result_correctness": 80.0, "syntax_validity": 95.0}
        curr = {"result_correctness": 85.0, "syntax_validity": 96.0}
        regressions = detect_regressions(curr, prev)
        assert regressions == []

    def test_regression_triggers_rollback(self):
        prev = {"result_correctness": 80.0, "syntax_validity": 95.0}
        curr = {"result_correctness": 75.0, "syntax_validity": 96.0}
        regressions = detect_regressions(curr, prev, threshold=2.0)
        assert len(regressions) == 1
        assert regressions[0]["judge"] == "result_correctness"


class TestConflictBatching:
    def test_batching_preserves_all_proposals(self, sample_clusters):
        proposals = [
            {"patch_type": "add_description", "lever": 1},
            {"patch_type": "add_column_description", "lever": 1},
            {"patch_type": "add_instruction", "lever": 5},
        ]
        batches = detect_conflicts_and_batch(proposals)
        total = sum(len(b) for b in batches)
        assert total == 3
