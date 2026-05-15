"""Track A — guarantee the lever-4 join-discovery LLM call enters a
top-level MLflow span named ``lever_4_join_discovery``.

Without this, ``scripts/export_narrowing_fixtures.py`` cannot recover
the prompt/response bytes for this skill and the Plan-1 fixture set
stays at 2-of-3 (the strict ``test_fixtures_cover_all_three_skills_when_present``
gate then prevents committing a partial set)."""
from __future__ import annotations

from unittest.mock import MagicMock


def test_lever_4_join_discovery_opens_named_span(monkeypatch):
    from genie_space_optimizer.optimization import optimizer

    captured_span_names: list[str] = []

    class _FakeSpan:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def set_inputs(self, *_a, **_k): pass
        def set_outputs(self, *_a, **_k): pass
        def set_attributes(self, *_a, **_k): pass

    def _fake_start_span(*, name, span_type=None, **_):
        captured_span_names.append(name)
        return _FakeSpan()

    monkeypatch.setattr("mlflow.start_span", _fake_start_span)
    monkeypatch.setattr(
        optimizer, "_call_llm_openai",
        lambda *a, **k: ('{"join_specs": [], "rationale": ""}', MagicMock()),
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.evaluation._link_prompt_to_trace",
        lambda *_a, **_k: None,
    )

    optimizer._call_llm_for_join_discovery(
        metadata_snapshot={
            "data_sources": {}, "tables": [],
            "metric_views": [], "functions": [],
            "join_specs": [], "_join_overlaps": {},
        },
        hints=[{"left_table": "t", "right_table": "u"}],
        w=None,
        raw_evidence=(),
    )

    assert "lever_4_join_discovery" in captured_span_names, (
        f"lever-4 LLM call did not open the expected named span; "
        f"captured: {captured_span_names!r}"
    )
