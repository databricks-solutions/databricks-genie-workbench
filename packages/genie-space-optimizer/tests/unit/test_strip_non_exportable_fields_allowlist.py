"""Regression tests for :func:`strip_non_exportable_fields`.

Guards against the ``Invalid serialized_space: Cannot find field`` class
of Genie API rejection by ensuring the sanitizer keeps only the five
documented top-level ``serialized_space`` keys and drops everything else
(runtime annotations, legacy metadata, underscore-prefixed internal
state).
"""

from __future__ import annotations

import logging

from genie_space_optimizer.common.genie_client import (
    SERIALIZED_SPACE_TOP_LEVEL_KEYS,
    strip_non_exportable_fields,
)


def _minimal_serialized_space() -> dict:
    """A syntactically valid ``serialized_space`` dict with all allowed keys."""
    return {
        "version": 2,
        "config": {"sample_questions": []},
        "data_sources": {"tables": [], "metric_views": []},
        "instructions": {"text_instructions": []},
        "benchmarks": {"questions": []},
    }


def test_allowlist_keeps_only_documented_top_level_keys():
    cleaned = strip_non_exportable_fields(_minimal_serialized_space())
    assert set(cleaned.keys()) == SERIALIZED_SPACE_TOP_LEVEL_KEYS


def test_runtime_annotation_failure_clusters_is_stripped():
    config = _minimal_serialized_space()
    config["failure_clusters"] = [{"cluster_id": "C001"}]
    cleaned = strip_non_exportable_fields(config)
    assert "failure_clusters" not in cleaned
    # Real payload keys must still pass through untouched.
    assert cleaned["data_sources"] == {"tables": [], "metric_views": []}


def test_underscore_prefixed_runtime_keys_are_stripped():
    config = _minimal_serialized_space()
    config["_space_id"] = "abc123"
    config["_failure_clusters"] = [{"cluster_id": "C002"}]
    config["_cluster_synthesis_count"] = 7
    cleaned = strip_non_exportable_fields(config)
    for k in ("_space_id", "_failure_clusters", "_cluster_synthesis_count"):
        assert k not in cleaned


def test_legacy_non_exportable_metadata_is_stripped():
    config = _minimal_serialized_space()
    config["id"] = "space-xyz"
    config["title"] = "My space"
    config["description"] = "Human-facing description"
    config["creator"] = "alice@example.com"
    cleaned = strip_non_exportable_fields(config)
    for k in ("id", "title", "description", "creator"):
        assert k not in cleaned


def test_unknown_top_level_keys_emit_warning(caplog):
    config = _minimal_serialized_space()
    config["failure_clusters"] = []
    config["_space_id"] = "abc"
    config["mystery_field"] = {"unexpected": True}
    with caplog.at_level(logging.WARNING, logger="genie_space_optimizer.common.genie_client"):
        strip_non_exportable_fields(config)
    warning_text = "\n".join(r.getMessage() for r in caplog.records)
    # Unknown (non-metadata, non-underscore) keys must be called out so
    # operators notice accidental pollution.
    assert "failure_clusters" in warning_text
    assert "mystery_field" in warning_text
    # Underscore-prefixed keys are expected runtime state, not surprising,
    # and should not appear in the "unknown" bucket.
    assert "_space_id" not in warning_text.split("Known metadata dropped:")[0]


def test_nested_column_config_internal_keys_are_stripped():
    config = _minimal_serialized_space()
    config["data_sources"] = {
        "tables": [
            {
                "identifier": "cat.sch.tbl",
                "column_configs": [
                    {
                        "column_name": "c1",
                        "uc_comment": "should be stripped",
                        "data_type_source": "uc",
                        "description": ["keep me"],
                    }
                ],
            }
        ],
        "metric_views": [],
    }
    cleaned = strip_non_exportable_fields(config)
    cc = cleaned["data_sources"]["tables"][0]["column_configs"][0]
    assert "uc_comment" not in cc
    assert "data_type_source" not in cc
    assert cc.get("description") == ["keep me"]
