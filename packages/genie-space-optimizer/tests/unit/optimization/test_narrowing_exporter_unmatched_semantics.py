"""Track B (2026-05-16): partition unmatched narrowing records by
whether MLflow has any traces at all for that skill. Zero-trace
skills are informational (LLM was demonstrably not called this run);
some-trace skills with no time-match remain a failure."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"


def _load_exporter():
    spec = importlib.util.spec_from_file_location(
        "_export_narrowing_fixtures",
        _SCRIPTS_DIR / "export_narrowing_fixtures.py",
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_partition_unmatched_zero_traces_for_skill_is_informational():
    """Run B airline space scenario: capture record for
    ``lever-4-join-discovery`` exists (sink fired at prompt-assembly
    time), but the LLM call site was gated and produced ZERO MLflow
    traces with root span ``lever_4_join_discovery``. Exporter must
    classify this as informational, not failure."""
    mod = _load_exporter()
    unmatched = [{"skill_id": "lever-4-join-discovery", "rendered_at_ts": 1.0}]
    traces_by_root_name = {
        # The 2 other narrowing-side root names exist; lever-4 does not.
        "sql_expression_seeding_llm": [object()],
        "generate_proactive_instructions": [object()],
    }
    informational, failure = mod._partition_unmatched(
        unmatched=unmatched,
        traces_by_root_name=traces_by_root_name,
    )
    assert [r["skill_id"] for r in informational] == ["lever-4-join-discovery"]
    assert failure == []


def test_partition_unmatched_some_traces_for_skill_is_failure():
    """Counter-case: the skill DOES have traces in MLflow, but the
    exporter could not match any to this record (timing mismatch, or
    no Completions child span found, or empty prompt bytes). That is
    a real failure: a trace exists, our extractor can't read it."""
    mod = _load_exporter()
    unmatched = [{"skill_id": "lever-4-join-discovery", "rendered_at_ts": 1.0}]
    traces_by_root_name = {"lever_4_join_discovery": [object(), object()]}
    informational, failure = mod._partition_unmatched(
        unmatched=unmatched,
        traces_by_root_name=traces_by_root_name,
    )
    assert informational == []
    assert [r["skill_id"] for r in failure] == ["lever-4-join-discovery"]


def test_partition_unmatched_mixed_split_correctly():
    """One skill has zero traces, another has traces — they go to
    different partitions even when both unmatched records came in
    the same run."""
    mod = _load_exporter()
    unmatched = [
        {"skill_id": "lever-4-join-discovery", "rendered_at_ts": 1.0},
        {"skill_id": "preflight-sql-expression-seeding", "rendered_at_ts": 2.0},
    ]
    traces_by_root_name = {
        # lever-4 still has zero traces.
        "sql_expression_seeding_llm": [object()],
    }
    informational, failure = mod._partition_unmatched(
        unmatched=unmatched,
        traces_by_root_name=traces_by_root_name,
    )
    assert sorted(r["skill_id"] for r in informational) == ["lever-4-join-discovery"]
    assert sorted(r["skill_id"] for r in failure) == ["preflight-sql-expression-seeding"]


def test_partition_unmatched_unknown_skill_id_treated_as_failure():
    """Defensive: a skill_id that is not in SKILL_TO_ROOT_TRACE_NAMES
    has no candidate root names at all, which means zero traces will
    always be found for it. The right call is failure (the operator
    has a typo or the catalogue is out of date), not silent
    informational pass."""
    mod = _load_exporter()
    unmatched = [{"skill_id": "completely-bogus-skill", "rendered_at_ts": 1.0}]
    informational, failure = mod._partition_unmatched(
        unmatched=unmatched,
        traces_by_root_name={},
    )
    assert informational == []
    assert [r["skill_id"] for r in failure] == ["completely-bogus-skill"]
