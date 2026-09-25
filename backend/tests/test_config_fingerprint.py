"""Unit tests for backend/services/config_fingerprint.py.

Pure-function coverage of the unwrap → canonicalize → hash contract that the
current-version endpoint relies on. No Databricks connectivity required.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from backend.services.version_control.contracts import Comparison, canonical_json_hash, to_wire

from backend.services.config_fingerprint import (
    benchmark_fingerprint,
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


def test_existing_fingerprint_api_remains_compatible() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    assert callable(Canonicalizer)
    assert unwrap_serialized_space(_space()) == _space()
    assert canonicalize(_space()) == canonicalize(canonicalize(_space()))
    assert config_fingerprint(_space()) == (
        "cf766aabfb0dece9cb89c197a4ccf7e03e46cd0d503708fd05abfd5fee282eb5"
    )
    assert benchmark_fingerprint(_space()) == (
        "5c9253d29ca1fc9c4f7683bfe5dadd87e2dbce3e46af6ee80404bc7259b41e1e"
    )


def test_legacy_duplicate_ids_sort_while_vc_preserves_input_order() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    original = _space()
    entries = [
        {"id": "b", "content": ["First b"]},
        {"id": "a", "content": ["Only a"]},
        {"id": "b", "content": ["Second b"]},
    ]
    original["instructions"]["text_instructions"] = entries
    sorted_space = deepcopy(original)
    sorted_space["instructions"]["text_instructions"] = [
        entries[1], entries[0], entries[2]
    ]
    expected = [
        {"id": "a", "content": "Only a"},
        {"id": "b", "content": "First b"},
        {"id": "b", "content": "Second b"},
    ]
    assert canonicalize(original)["instructions"]["text_instructions"] == expected
    assert config_fingerprint(original) == config_fingerprint(sorted_space)
    adapter = Canonicalizer()
    snapshot = adapter.observe(original)
    assert to_wire(snapshot.canonical_state["config"])["instructions"][
        "text_instructions"
    ] == [expected[1], expected[0], expected[2]]
    assert snapshot.fingerprints.config != adapter.observe(sorted_space).fingerprints.config


def test_observe_preserves_exact_restorable_state_and_envelope() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    payload = _space(with_version=False)
    payload["_unknown_executable"] = {"secret": "retained", "steps": [2, 1]}
    envelope = {
        "serialized_space": json.dumps(payload, indent=2),
        "_parsed_space": _space(instruction="mutated preflight"),
        "description": "Exact 'description'\n",
        "title": "Not governed in v1",
        "space_id": "physical-id",
    }
    original = deepcopy(envelope)
    snapshot = Canonicalizer().observe(envelope)
    assert envelope == original
    assert to_wire(snapshot.response_envelope) == original
    assert to_wire(snapshot.serialized_space) == payload
    assert to_wire(snapshot.restorable_metadata) == {"description": envelope["description"]}
    assert snapshot.state_digest == snapshot.fingerprints.state_digest
    assert snapshot.response_envelope_digest == canonical_json_hash(
        "vc-envelope/1", {"envelope": original}
    )
    assert snapshot.raw_state_digest == canonical_json_hash(
        "vc-raw-state/1", {"serialized_space": payload, "metadata": {"description": envelope["description"]}}
    )
    envelope["_parsed_space"]["version"] = 999
    assert to_wire(snapshot.response_envelope) == original


def test_vc_config_fingerprint_ignores_all_internal_underscore_keys() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    clean = adapter.observe({"_parsed_space": _space()})
    polluted = adapter.observe(
        {"_parsed_space": {**_space(), "_preflight_notes": {"runs": 3}}}
    )
    assert polluted.fingerprints.config == clean.fingerprints.config
    assert polluted.fingerprints.metadata == clean.fingerprints.metadata
    assert polluted.fingerprints.benchmark == clean.fingerprints.benchmark
    # t2: the exact restorable state must retain internal bookkeeping keys.
    assert "_preflight_notes" in to_wire(polluted.serialized_space)


def _metric_view_fixture(name: str) -> tuple[dict, dict]:
    """Apply fixture source shapes to the same otherwise unchanged space."""
    path = Path(__file__).parent / "fixtures/vc_canonicalization" / f"{name}.json"
    fixture = json.loads(path.read_text())
    return {**_space(), **fixture["submitted"]}, {**_space(), **fixture["observed"]}


def test_metric_view_flattening_does_not_depend_on_mv_name_prefix() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    submitted, observed = _metric_view_fixture("metric_view_without_prefix")
    assert (
        adapter.observe(submitted).fingerprints.config
        == adapter.observe(observed).fingerprints.config
    )


def test_metric_view_flattening_honours_declared_table_type() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    submitted, observed = _metric_view_fixture("metric_view_declared_type")
    expected = adapter.observe(submitted)
    assert expected.fingerprints.config == adapter.observe(observed).fingerprints.config
    for marker in ("table_type", "type", "object_type"):
        marked = deepcopy(observed)
        entry = marked["data_sources"]["tables"][0]
        entry[marker] = entry.pop("table_type")
        assert adapter.observe(marked).fingerprints.config == expected.fingerprints.config
        # A duplicate identifier with different fields is still one source.
        entry["description"] = "Revenue"
        marked["data_sources"]["metric_views"] = submitted["data_sources"]["metric_views"]
        described = deepcopy(submitted)
        described["data_sources"]["metric_views"][0]["description"] = "Revenue"
        assert (
            adapter.observe(marked).fingerprints.config
            == adapter.observe(described).fingerprints.config
        )
        assert to_wire(adapter.observe(marked).serialized_space) == marked


def test_vc_data_sources_addressed_order_independent_with_unaddressed_entry() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    submitted = _space()
    submitted["data_sources"]["tables"] = [
        {"identifier": "cat.sch.a"},
        {"identifier": "cat.sch.b"},
        {"name": "no-identifier"},
    ]
    observed = deepcopy(submitted)
    tables = observed["data_sources"]["tables"]
    tables[0], tables[1] = tables[1], tables[0]
    assert (
        adapter.observe(submitted).fingerprints.config
        == adapter.observe(observed).fingerprints.config
    )


def test_absent_and_empty_metric_views_are_equal() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    empty, absent = _metric_view_fixture("metric_views_absent_empty")
    assert (
        adapter.observe(absent).fingerprints.config
        == adapter.observe(empty).fingerprints.config
    )
    for sources in ({}, {"tables": []}, {"metric_views": []}):
        snapshot = adapter.observe({**_space(), "data_sources": sources})
        assert to_wire(snapshot.canonical_state["config"]["data_sources"]) == {
            "sources": []
        }
    no_sources = adapter.observe({"instructions": {}})
    assert to_wire(no_sources.canonical_state["config"]["data_sources"]) == {
        "sources": []
    }


def test_metric_views_without_tables_key_matches_flattened_readback() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    submitted, observed = _metric_view_fixture("metric_views_without_tables")
    assert (
        adapter.observe(submitted).fingerprints.config
        == adapter.observe(observed).fingerprints.config
    )


def test_normalized_metric_views_quotes_fragments_and_wrappers_match() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    fixture = json.loads((Path(__file__).parent / "fixtures/vc_canonicalization/readback.json").read_text())
    adapter = Canonicalizer()
    submitted = fixture["submitted"]
    observed = fixture["observed"]
    legacy = {key: value for key, value in observed.items() if key != "version"}
    wrappers = [
        observed,
        {"serialized_space": json.dumps(observed), "_parsed_space": _space()},
        {"_parsed_space": {**legacy, "_data_profile": {"ignored": True}}},
    ]
    expected = adapter.observe(submitted)
    for wrapper in wrappers:
        actual = adapter.observe(wrapper)
        assert actual.fingerprints == expected.fingerprints
        assert actual.canonical_state == expected.canonical_state


# ── unwrap_serialized_space ──────────────────────────────────────────────


def test_unordered_ids_sort_but_meaningful_order_survives() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    original = _space()
    original["data_sources"]["tables"].append({"identifier": "cat.sch.t2"})
    original["instructions"]["text_instructions"].append({"id": "a2", "content": ["Second"]})
    original["instructions"]["sql_snippets"] = {
        "expressions": [{"id": "s1", "sql": ["SELECT ", "1"]}]
    }
    original["instructions"]["steps"] = [{"id": "step1"}, {"id": "step2"}]
    reordered = deepcopy(original)
    reordered["data_sources"]["tables"].reverse()
    reordered["instructions"]["text_instructions"].reverse()
    assert adapter.observe(original).fingerprints == adapter.observe(reordered).fingerprints
    for path in (
        ("instructions", "steps"),
        ("instructions", "sql_snippets", "expressions", 0, "sql"),
    ):
        changed = deepcopy(original)
        target = changed
        for key in path:
            target = target[key]
        target.reverse()
        assert adapter.observe(original).fingerprints.config != adapter.observe(changed).fingerprints.config
    changed = deepcopy(original)
    changed["instructions"]["text_instructions"][0]["content"] = ["helpful", "Be "]
    assert adapter.observe(original).fingerprints.config != adapter.observe(changed).fingerprints.config


def test_description_changes_only_metadata_fingerprint() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    envelope = {"serialized_space": _space(), "description": "Revenue", "title": "Title"}
    original = adapter.observe(envelope)
    changed = adapter.observe({**envelope, "description": "'Revenue'"})
    assert original.fingerprints.config == changed.fingerprints.config
    assert original.fingerprints.benchmark == changed.fingerprints.benchmark
    assert original.fingerprints.metadata != changed.fingerprints.metadata
    assert original.state_digest != changed.state_digest
    assert to_wire(changed.canonical_state["metadata"]) == {"description": "'Revenue'"}
    assert original.fingerprints == adapter.observe({**envelope, "title": "Other"}).fingerprints
    assert adapter.observe({"serialized_space": _space()}).fingerprints.metadata != (
        adapter.observe({"serialized_space": _space(), "description": ""}).fingerprints.metadata
    )


def test_benchmark_change_does_not_change_config_identity() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    space = {**_space(), "benchmarks": {"questions": [_benchmark_question("b1")]}}
    original = adapter.observe(space)
    changed_space = deepcopy(space)
    changed_space["benchmarks"]["questions"][0]["answer"][0]["content"] = ["SELECT 2"]
    changed = adapter.observe(changed_space)
    assert original.fingerprints.config == changed.fingerprints.config
    assert original.fingerprints.metadata == changed.fingerprints.metadata
    assert original.fingerprints.benchmark != changed.fingerprints.benchmark
    assert original.state_digest != changed.state_digest
    assert original.canonical_state["benchmark"] != changed.canonical_state["benchmark"]
    config_edit = {**space, "instructions": _space(instruction="Changed")["instructions"]}
    assert adapter.observe(config_edit).fingerprints.benchmark == original.fingerprints.benchmark
    empties = [_space(), {**_space(), "benchmarks": {}}, {**_space(), "benchmarks": {"questions": []}}]
    assert len({adapter.observe(empty).state_digest for empty in empties}) == 1
    quoted = deepcopy(space)
    quoted["benchmarks"]["questions"][0]["answer"][0]["content"] = ["SELECT '2'"]
    assert adapter.observe(quoted).fingerprints.benchmark != changed.fingerprints.benchmark


@pytest.mark.parametrize("envelope", [
    {"serialized_space": {}},
    {"serialized_space": "{not json", "_parsed_space": _space()},
    {"serialized_space": "null"},
    {"serialized_space": "[]"},
    {"serialized_space": "{}"},
    {"serialized_space": {"version": 2}},
    {"serialized_space": None, "_parsed_space": _space()},
    {"serialized_space": {"instructions": []}},
    {"serialized_space": {**_space(), "benchmarks": []}},
    {"serialized_space": {**_space(), "benchmarks": {"questions": "bad"}}},
    {"serialized_space": {**_space(), "benchmarks": {"questions": [None]}}},
    {"serialized_space": {**_space(), "data_sources": {"tables": "bad"}}},
    {"serialized_space": {**_space(), "version": True}},
    {"serialized_space": _space(), "description": []},
    {"description": "missing payload"},
    {},
    None,
])
def test_malformed_serialized_space_is_rejected_not_empty(envelope: dict) -> None:
    from backend.services.config_fingerprint import Canonicalizer

    with pytest.raises(ValueError, match="serialized_space|description|envelope"):
        Canonicalizer().observe(envelope)
    assert Canonicalizer().observe({"serialized_space": {"instructions": {}}})


def test_cross_canonicalizer_comparison_is_unknown_without_dual_compute() -> None:
    from backend.services.config_fingerprint import Canonicalizer

    adapter = Canonicalizer()
    original = adapter.observe({"serialized_space": _space(), "description": "Original"})
    changed = adapter.observe({"serialized_space": _space(), "description": "Changed"})
    historical_fingerprints = replace(original.fingerprints, canonicalizer_version="vc-c14n/99")
    historical = replace(original, fingerprints=historical_fingerprints, state_digest=historical_fingerprints.state_digest)
    assert adapter.compare(original, original) is Comparison.EQUAL
    assert adapter.compare(original, changed) is Comparison.DIFFERENT
    assert adapter.compare(original, historical) is Comparison.UNKNOWN
    assert adapter.compare(historical, historical) is Comparison.UNKNOWN
    with pytest.raises(ValueError, match="canonicalizer"):
        adapter.observe(_space(), version="vc-c14n/99")
    dual = Canonicalizer(dual_compute_version="vc-c14n/1")
    before = to_wire(historical)
    assert dual.compare(original, historical) is Comparison.EQUAL
    assert dual.compare(changed, historical) is Comparison.DIFFERENT
    assert to_wire(historical) == before
    missing_raw = replace(historical, response_envelope={})
    assert dual.compare(original, missing_raw) is Comparison.UNKNOWN
    with pytest.raises(ValueError, match="canonicalizer"):
        Canonicalizer(dual_compute_version="vc-c14n/99")


def test_canonicalizer_is_idempotent_and_hash_is_domain_separated() -> None:
    from backend.services.config_fingerprint import Canonicalizer
    from backend.services.version_control.contracts import Fingerprints, from_wire

    adapter = Canonicalizer()
    fixture_root = Path(__file__).parent / "fixtures"
    golden = json.loads((fixture_root / "vc_canonicalization/readback.json").read_text())
    for payload in (golden["submitted"], golden["observed"], _space()):
        original = adapter.observe({"serialized_space": payload, "description": "Résumé"})
        canonical = to_wire(original.canonical_state)
        repeated = adapter.observe({
            "serialized_space": {**canonical["config"], "benchmarks": canonical["benchmark"]},
            **canonical["metadata"],
        })
        assert original.canonical_state == repeated.canonical_state
        assert original.fingerprints == repeated.fingerprints
        assert adapter.semantic_diff(original, repeated) == []
        for component in ("config", "benchmark", "metadata"):
            assert getattr(original.fingerprints, component) == canonical_json_hash(
                f"vc-{component}/1", {"value": canonical[component]}
            )
    shared = json.loads((fixture_root / "vc_contracts/fingerprints.json").read_text())
    fingerprints = from_wire(Fingerprints, shared["examples"][0])
    assert fingerprints.state_digest == canonical_json_hash("vc-state/1", to_wire(fingerprints))
    assert canonical_json_hash("vc-state/1", {"config": "ab", "benchmark": "c"}) != (
        canonical_json_hash("vc-state/1", {"config": "a", "benchmark": "bc"})
    )
    assert canonical_json_hash("vc-config/1", {"value": {}}) != canonical_json_hash("vc-benchmark/1", {"value": {}})
    for before, after in ((False, 0), (1, 1.0), ({}, {"text_instructions": []})):
        left = adapter.observe({"instructions": {}, "future": before})
        right = adapter.observe({"instructions": {}, "future": after})
        assert left.state_digest != right.state_digest
        assert adapter.semantic_diff(left, right)
    absent = adapter.observe({"instructions": {}})
    empty = adapter.observe({"instructions": {"text_instructions": []}})
    assert absent.state_digest != empty.state_digest
    assert adapter.semantic_diff(absent, empty)


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


def test_canonicalize_joins_content_and_sql_fragments() -> None:
    submitted = {
        "instructions": {
            "text_instructions": [{"id": "a1", "content": ["PURPOSE:\n- Help"]}],
            "sql_snippets": {
                "filters": [{"id": "f1", "sql": ["region = ", "Enterprise"]}],
            },
        },
    }
    observed = {
        "instructions": {
            "text_instructions": [
                {"id": "a1", "content": ["PURPOSE:\n", "- Help"]},
            ],
            "sql_snippets": {
                "filters": [{"id": "f1", "sql": ["region = Enterprise"]}],
            },
        },
    }
    assert canonicalize(submitted) == canonicalize(observed)


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


def test_fingerprint_ignores_genie_fragment_and_boundary_quote_normalization() -> None:
    """Regression for live space 01f18ba0a8ce102ea2fbae1ffee6d600."""
    submitted = {
        "version": 2,
        "data_sources": {"tables": [{"identifier": "cat.sch.t1"}]},
        "instructions": {
            "text_instructions": [
                {
                    "id": "a1",
                    "content": [
                        "PURPOSE:\n- Highest churn risk means the segment with "
                        "the most churn events.\n",
                    ],
                }
            ],
            "sql_snippets": {
                "filters": [
                    {
                        "id": "f1",
                        "sql": ["cat.sch.t1.`Segment` = Enterprise"],
                    }
                ],
                "expressions": [
                    {
                        "id": "e1",
                        "sql": ["date_format(cat.sch.t1.`Month`, yyyy-MM)"],
                    }
                ],
            },
        },
    }
    live = {
        "version": 2,
        "data_sources": {"tables": [{"identifier": "cat.sch.t1"}]},
        "instructions": {
            "text_instructions": [
                {
                    "id": "a1",
                    "content": [
                        "PURPOSE:\n",
                        "- 'Highest churn risk' means the segment with the most "
                        "churn events.\n",
                    ],
                }
            ],
            "sql_snippets": {
                "filters": [
                    {
                        "id": "f1",
                        "sql": ["cat.sch.t1.`Segment` = 'Enterprise'"],
                    }
                ],
                "expressions": [
                    {
                        "id": "e1",
                        "sql": ["date_format(cat.sch.t1.`Month`, 'yyyy-MM')"],
                    }
                ],
            },
        },
    }
    assert config_fingerprint(submitted) == config_fingerprint(live)


def test_fingerprint_preserves_apostrophes_inside_words() -> None:
    assert config_fingerprint(_space(instruction="customer's revenue")) != (
        config_fingerprint(_space(instruction="customers revenue"))
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


# ── benchmark_fingerprint ────────────────────────────────────────────────


def _benchmark_question(
    question_id: str,
    *,
    question: str = "What is revenue?",
    sql: str = "SELECT SUM(revenue) FROM cat.sch.t1",
) -> dict:
    return {
        "id": question_id,
        "question": [question],
        "answer": [{"format": "SQL", "content": [sql]}],
    }


def test_benchmark_fingerprint_changes_when_questions_change() -> None:
    original = {
        **_space(),
        "benchmarks": {
            "questions": [
                _benchmark_question("b1"),
                _benchmark_question("b2", question="What is margin?"),
            ]
        },
    }
    removed = {
        **_space(),
        "benchmarks": {"questions": [_benchmark_question("b1")]},
    }
    added = {
        **original,
        "benchmarks": {
            "questions": [
                *original["benchmarks"]["questions"],
                _benchmark_question("b3", question="What is profit?"),
            ]
        },
    }
    updated = {
        **original,
        "benchmarks": {
            "questions": [
                _benchmark_question("b1", sql="SELECT SUM(net_revenue) FROM cat.sch.t1"),
                _benchmark_question("b2", question="What is margin?"),
            ]
        },
    }

    original_fp = benchmark_fingerprint(original)
    assert original_fp is not None
    assert benchmark_fingerprint(removed) != original_fp
    assert benchmark_fingerprint(added) != original_fp
    assert benchmark_fingerprint(updated) != original_fp


def test_benchmark_fingerprint_normalizes_order_and_fragments() -> None:
    submitted = {
        **_space(),
        "benchmarks": {
            "questions": [
                _benchmark_question("b2", question="What is margin?"),
                _benchmark_question("b1"),
            ]
        },
    }
    observed = {
        **_space(instruction="A config edit must not affect benchmark identity"),
        "benchmarks": {
            "questions": [
                {
                    "id": "b1",
                    "question": ["What is revenue?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": ["SELECT SUM(revenue) ", "FROM cat.sch.t1"],
                        }
                    ],
                },
                _benchmark_question("b2", question="What is margin?"),
            ]
        },
    }

    assert benchmark_fingerprint(submitted) == benchmark_fingerprint(observed)


def test_benchmark_fingerprint_preserves_sql_quote_semantics() -> None:
    quoted_literal = {
        **_space(),
        "benchmarks": {
            "questions": [
                _benchmark_question(
                    "b1",
                    sql="SELECT * FROM cat.sch.t1 WHERE status = 'active'",
                )
            ]
        },
    }
    unquoted_identifier = {
        **_space(),
        "benchmarks": {
            "questions": [
                _benchmark_question(
                    "b1",
                    sql="SELECT * FROM cat.sch.t1 WHERE status = active",
                )
            ]
        },
    }

    assert benchmark_fingerprint(quoted_literal) != benchmark_fingerprint(
        unquoted_identifier
    )


def test_benchmark_fingerprint_treats_empty_shapes_equivalently() -> None:
    absent = _space()
    empty_block = {**_space(), "benchmarks": {}}
    empty_questions = {**_space(), "benchmarks": {"questions": []}}

    fingerprints = {
        benchmark_fingerprint(absent),
        benchmark_fingerprint(empty_block),
        benchmark_fingerprint(empty_questions),
    }
    assert len(fingerprints) == 1
    assert fingerprints.pop() is not None


def test_benchmark_fingerprint_returns_none_for_malformed_benchmarks() -> None:
    assert benchmark_fingerprint(None) is None
    assert benchmark_fingerprint({"unrelated": True}) is None
    assert benchmark_fingerprint({**_space(), "benchmarks": []}) is None
    assert benchmark_fingerprint(
        {**_space(), "benchmarks": {"questions": "not-a-list"}}
    ) is None
