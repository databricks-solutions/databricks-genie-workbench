"""Unit tests for backend/services/config_fingerprint.py.

Pure-function coverage of the unwrap → canonicalize → hash contract that the
current-version endpoint relies on. No Databricks connectivity required.
"""

from __future__ import annotations

import json

from backend.services.config_fingerprint import (
    canonicalize,
    config_fingerprint,
    unwrap_serialized_space,
)


def _space(*, instruction: str = "Be helpful", with_version: bool = True) -> dict:
    space = {
        "data_sources": {"tables": [{"identifier": "cat.sch.t1"}]},
        "config": {"sample_questions": [{"id": "q1", "question": ["What is revenue?"]}]},
        "instructions": {"text_instructions": [{"id": "a1", "content": [instruction]}]},
    }
    if with_version:
        return {"version": 2, **space}
    return space


# ── unwrap_serialized_space ──────────────────────────────────────────────


def test_unwrap_bare_serialized_space() -> None:
    assert unwrap_serialized_space(_space()) == _space()


def test_unwrap_api_response_with_string_serialized_space() -> None:
    wrapper = {"serialized_space": json.dumps(_space()), "title": "My Agent"}
    assert unwrap_serialized_space(wrapper) == _space()


def test_unwrap_api_response_with_dict_serialized_space() -> None:
    wrapper = {"serialized_space": _space(), "title": "My Agent"}
    assert unwrap_serialized_space(wrapper) == _space()


def test_unwrap_parsed_space_wrapper() -> None:
    assert unwrap_serialized_space({"_parsed_space": _space()}) == _space()


def test_unwrap_prefers_pristine_serialized_space_over_parsed_copy() -> None:
    """When the wrapper has both, the original serialized_space wins — GSO's
    fetch stashes a parsed copy that preflight later mutates in place."""
    mutated = _space(instruction="enriched descriptions")
    mutated["_data_profile"] = {"cat.sch.t1": {"columns": []}}
    wrapper = {"serialized_space": json.dumps(_space()), "_parsed_space": mutated}
    assert unwrap_serialized_space(wrapper) == _space()


def test_unwrap_strips_internal_keys_from_parsed_space_fallback() -> None:
    """Snapshots that only retain the (mutated) _parsed_space copy still match:
    injected _-prefixed keys are stripped."""
    parsed = {**_space(), "_data_profile": {"cat.sch.t1": {"columns": []}}}
    assert unwrap_serialized_space({"_parsed_space": parsed}) == _space()


def test_unwrap_falls_back_to_parsed_space_on_corrupted_serialized_space() -> None:
    corrupted = {"serialized_space": "{not json", "_parsed_space": _space()}
    assert unwrap_serialized_space(corrupted) == _space()


def test_enriched_snapshot_fingerprint_matches_live() -> None:
    """P1a regression: a post-preflight config_snapshot (pristine payload +
    _parsed_space carrying _data_profile) must fingerprint identically to the
    live config of an unchanged agent."""
    snapshot = {
        "serialized_space": json.dumps(_space()),
        "_parsed_space": {**_space(), "_data_profile": {"cat.sch.t1": {"row_count": 42}}},
        "title": "My Agent",
    }
    live = {"serialized_space": json.dumps(_space()), "update_time": "2026-07-28T10:00:00Z"}
    assert config_fingerprint(snapshot) == config_fingerprint(live)
    assert config_fingerprint(snapshot) is not None


def test_unwrap_backfills_version_for_legacy_projected_rows() -> None:
    legacy = _space(with_version=False)
    assert unwrap_serialized_space(legacy) == _space()


def test_unwrap_rejects_garbage() -> None:
    assert unwrap_serialized_space(None) is None
    assert unwrap_serialized_space({}) is None
    assert unwrap_serialized_space({"title": "no config here"}) is None
    assert unwrap_serialized_space({"serialized_space": "{not json"}) is None
    assert unwrap_serialized_space({"serialized_space": ""}) is None


# ── canonicalize ─────────────────────────────────────────────────────────


def test_canonicalize_drops_top_level_benchmarks() -> None:
    with_benchmarks = {**_space(), "benchmarks": {"questions": [{"id": "b1"}]}}
    assert canonicalize(with_benchmarks) == canonicalize(_space())


def test_canonicalize_sorts_id_arrays() -> None:
    node = {
        "instructions": {
            "text_instructions": [
                {"id": "b2", "content": ["second"]},
                {"id": "a1", "content": ["first"]},
            ]
        }
    }
    result = canonicalize(node)
    ids = [item["id"] for item in result["instructions"]["text_instructions"]]
    assert ids == ["a1", "b2"]


def test_canonicalize_preserves_non_id_array_order() -> None:
    node = {"config": {"sample_questions": [{"question": ["b"]}, {"question": ["a"]}]}}
    result = canonicalize(node)
    questions = [item["question"] for item in result["config"]["sample_questions"]]
    assert questions == [["b"], ["a"]]


# ── config_fingerprint ───────────────────────────────────────────────────


def test_fingerprint_stable_across_shapes() -> None:
    """The same config must fingerprint identically in every stored shape."""
    bare = _space()
    wrapped_str = {"serialized_space": json.dumps(bare)}
    wrapped_dict = {"serialized_space": bare}
    parsed = {"_parsed_space": bare}
    fps = {
        config_fingerprint(bare),
        config_fingerprint(wrapped_str),
        config_fingerprint(wrapped_dict),
        config_fingerprint(parsed),
        config_fingerprint(json.dumps(wrapped_str)),  # raw Delta string
    }
    assert len(fps) == 1
    assert fps.pop() is not None


def test_fingerprint_ignores_key_order_and_benchmarks() -> None:
    a = {
        "version": 2,
        "instructions": {"text_instructions": [{"id": "a1", "content": ["x"]}]},
        "data_sources": {"tables": [{"identifier": "c.s.t"}]},
    }
    b = {
        "benchmarks": {"questions": [{"id": "b1", "question": ["q"]}]},
        "data_sources": {"tables": [{"identifier": "c.s.t"}]},
        "instructions": {"text_instructions": [{"id": "a1", "content": ["x"]}]},
        "version": 2,
    }
    assert config_fingerprint(a) == config_fingerprint(b)


def test_fingerprint_ignores_id_array_order() -> None:
    a = _space()
    b = _space()
    b["instructions"]["text_instructions"] = [
        {"id": "b2", "content": ["second"]},
        {"id": "a1", "content": ["first"]},
    ]
    a["instructions"]["text_instructions"] = list(reversed(b["instructions"]["text_instructions"]))
    assert config_fingerprint(a) == config_fingerprint(b)


def test_fingerprint_changes_on_meaningful_edit() -> None:
    assert config_fingerprint(_space(instruction="Be helpful")) != config_fingerprint(
        _space(instruction="Be terse")
    )


def test_fingerprint_legacy_row_matches_versioned_live() -> None:
    """A legacy champion row (version dropped) must match the live config it
    produced — the revert path backfills version=2 on PATCH, so the live GET
    comes back versioned."""
    assert config_fingerprint(_space(with_version=False)) == config_fingerprint(_space())


def test_fingerprint_returns_none_for_unusable_input() -> None:
    assert config_fingerprint(None) is None
    assert config_fingerprint("{not json") is None
    assert config_fingerprint({"unrelated": True}) is None
