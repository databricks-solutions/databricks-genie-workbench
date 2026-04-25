"""Phase 1.1: circular-reference safety in the model-version logger.

Covers two guarantees:

1. ``_project_space_config_for_artifact`` strips every ``_*`` key and
   keeps only the Genie-domain whitelist, so an optimizer-internal
   structure (``_failure_clusters``, ``_data_profile``, etc.) never
   reaches ``json.dump``.
2. ``_log_dict_artifact`` survives a deliberately-cyclic input by
   falling back to a de-cycled copy and logging it via the tempfile
   path when ``mlflow.log_dict`` raises
   ``ValueError: Circular reference detected``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization import models as models_mod


def test_projection_drops_internal_keys() -> None:
    parsed = {
        "data_sources": {"tables": [{"identifier": "cat.sch.t"}]},
        "instructions": {"text_instructions": []},
        "description": "Sales space",
        "title": "Sales",
        "_failure_clusters": [{"id": "H001"}],
        "_data_profile": {"cat.sch.t": {"row_count": 1000}},
        "_original_instruction_sections": {"PURPOSE": ["..."]},
        "_uc_columns": [{"col": "x"}],
        "_space_id": "abc",
        "_strategy": {"action_groups": []},
        "_anything_with_underscore_prefix": object(),
    }
    out = models_mod._project_space_config_for_artifact(parsed)
    assert "data_sources" in out
    assert "instructions" in out
    assert "description" in out
    assert all(not k.startswith("_") for k in out)
    assert "_failure_clusters" not in out
    assert "_data_profile" not in out
    assert "_strategy" not in out


def test_projection_handles_non_dict_input() -> None:
    assert models_mod._project_space_config_for_artifact(None) == {}
    assert models_mod._project_space_config_for_artifact([1, 2]) == {}
    assert models_mod._project_space_config_for_artifact("not a dict") == {}


def test_decycle_breaks_simple_self_loop() -> None:
    a: dict = {"x": 1}
    a["self"] = a  # direct self-cycle
    cleaned = models_mod._decycle(a)
    # The self-reference is replaced with a sentinel; ordinary keys remain.
    assert cleaned["x"] == 1
    assert cleaned["self"] == "<cycle>"


def test_decycle_breaks_indirect_loop() -> None:
    a: dict = {"name": "a"}
    b: dict = {"name": "b", "back": a}
    a["next"] = b
    cleaned = models_mod._decycle(a)
    # ``a`` -> ``next`` -> ``b`` -> ``back`` -> ``a`` becomes
    # ``a -> next -> b -> back -> "<cycle>"``.
    assert cleaned["name"] == "a"
    assert cleaned["next"]["name"] == "b"
    assert cleaned["next"]["back"] == "<cycle>"


def test_decycle_preserves_legitimate_shared_subtree() -> None:
    shared = {"k": "v"}
    parent = {"left": shared, "right": shared}  # not a cycle, just shared.
    cleaned = models_mod._decycle(parent)
    assert cleaned["left"] == {"k": "v"}
    assert cleaned["right"] == {"k": "v"}


def test_log_dict_artifact_falls_back_on_circular_reference() -> None:
    """First call raises Circular reference; fallback decycles and retries."""
    cyclic: dict = {"x": 1}
    cyclic["self"] = cyclic

    log_calls: list[dict] = []

    def fake_log_dict(payload: dict, artifact_file: str) -> None:
        log_calls.append({"payload": payload, "artifact_file": artifact_file})
        # First call: simulate the MLflow circular-reference exception.
        if len(log_calls) == 1:
            raise ValueError("Circular reference detected")
        # Subsequent call: success (with a de-cycled payload).

    with patch.object(models_mod.mlflow, "log_dict", fake_log_dict):
        models_mod._log_dict_artifact(cyclic, "ignored/path.json")

    assert len(log_calls) == 2, "expected one failed + one retry call"
    retried_payload = log_calls[1]["payload"]
    # The retry payload must NOT contain the original cycle.
    assert retried_payload["self"] == "<cycle>"


def test_log_dict_artifact_passes_through_non_circular_payload() -> None:
    payload = {"a": 1, "nested": {"b": 2}}
    captured: list[dict] = []

    def fake_log_dict(payload_arg: dict, artifact_file: str) -> None:
        captured.append({"payload": payload_arg, "file": artifact_file})

    with patch.object(models_mod.mlflow, "log_dict", fake_log_dict):
        models_mod._log_dict_artifact(payload, "p.json")

    assert len(captured) == 1
    assert captured[0]["payload"] == payload


def test_log_dict_artifact_propagates_unrelated_value_errors() -> None:
    """Only the circular-reference ValueError triggers fallback; others bubble."""

    def fake_log_dict(payload: dict, artifact_file: str) -> None:
        raise ValueError("schema validation failed")

    with patch.object(models_mod.mlflow, "log_dict", fake_log_dict):
        with pytest.raises(ValueError, match="schema validation failed"):
            models_mod._log_dict_artifact({"a": 1}, "p.json")
