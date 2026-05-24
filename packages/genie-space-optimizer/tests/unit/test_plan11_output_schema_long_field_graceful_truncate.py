"""Phase 6 (Trial 13) — Plan 11 output schema field caps relax + graceful
truncate validator.

The 98ec8950 shadow Plan 11 batch path emitted ``outcome="llm_error"
exception_class="ValueError"`` with ``string_too_long`` on
``result.diagnoses.1.generated_sql_issue`` (Pydantic ``max_length=300``)
and ``evidence_summary`` (``max_length=400``). The LLM returned
semantically correct diagnoses; local Pydantic validation discarded
them. Trial 13 Track 4 relaxes the caps 5× and replaces the
``string_too_long`` rejection with a graceful truncate validator —
oversize fields are clipped to ``cap`` chars with the trailing 3 chars
being ``"..."`` so postmortems can see the value was truncated.

Tests pin three contracts:

* New cap table (per the Trial 13 plan): ``rca_kind_label=200``,
  ``observed_failure=1000``, ``generated_sql_issue=1500``,
  ``expected_sql_shape=1500``, ``evidence_summary=2000``,
  ``unifying_evidence=2000``, ``repair_hypothesis=1500``,
  ``intent_name=200``.
* Graceful truncation: ``len(field) == cap`` with trailing ``"..."``.
* New ``_classify_llm_error`` taxonomy arm ``response_post_parse_field_length``
  for ``string_too_long`` errors raised post-parse (so future regressions
  surface a typed error_kind instead of falling through to ``unknown``).
* New marker ``GSO_PLAN11_POST_PARSE_FIELD_TRUNCATE_V1`` emitted when
  truncation actually happens.
"""
from __future__ import annotations

import json

import pytest


# ── Cap table ─────────────────────────────────────────────────────────


_DIAGNOSE_CAPS = {
    "rca_kind_label": 200,
    "observed_failure": 1000,
    "generated_sql_issue": 1500,
    "expected_sql_shape": 1500,
    "evidence_summary": 2000,
}

_CLUSTER_CAPS = {
    "unifying_evidence": 2000,
    "repair_hypothesis": 1500,
}

_SYNTHESIZE_CAPS = {
    "intent_name": 200,
}


# ── Helpers ──────────────────────────────────────────────────────────


def _build_diagnose_item(**overrides) -> dict:
    base = {
        "qid": "gs_009",
        "rca_kind_label": "missing_filter",
        "observed_failure": "wrong",
        "generated_sql_issue": "missing WHERE",
        "expected_sql_shape": "SELECT *",
        "blame_set": ["orders.date"],
        "evidence_summary": "lots of detail",
        "confidence": "high",
    }
    base.update(overrides)
    return base


def _build_cluster_item(**overrides) -> dict:
    base = {
        "semantic_theme": "missing_filter",
        "member_qids": ["gs_009"],
        "unifying_evidence": "shared evidence",
        "repair_hypothesis": "add WHERE clause",
        "primary_blame_set": ["orders.date"],
        "confidence": "high",
    }
    base.update(overrides)
    return base


def _build_synthesize_item(**overrides) -> dict:
    base = {
        "intent_name": "add_sql_snippet_filter",
        "intent_description": "Add a filter for date",
        "repair_hypothesis": "WHERE clause needed",
        "patch_type": "add_sql_snippet_filter",
        "rationale": "missing predicate",
        "confidence": "high",
        "patch_body": {"filter": "date >= 2024-01-01"},
        "blame_set": ["orders.date"],
        "target_qids": ["gs_009"],
    }
    base.update(overrides)
    return base


# ── Diagnose caps ─────────────────────────────────────────────────────


@pytest.mark.parametrize("field,cap", list(_DIAGNOSE_CAPS.items()))
def test_diagnose_field_cap_value(field: str, cap: int) -> None:
    """Per-field caps match the Trial 13 cap table."""
    from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
        DiagnosisItem,
    )

    json_schema = DiagnosisItem.model_json_schema()
    assert json_schema["properties"][field].get("maxLength") == cap, (
        f"DiagnosisItem.{field} cap drifted from Trial 13 table "
        f"({cap}); JSON schema reports "
        f"{json_schema['properties'][field].get('maxLength')!r}."
    )


@pytest.mark.parametrize("field,cap", list(_DIAGNOSE_CAPS.items()))
@pytest.mark.parametrize("multiplier", [1, 2, 5])
def test_diagnose_field_truncates_gracefully(
    field: str, cap: int, multiplier: int,
) -> None:
    """Oversize fields are truncated, NOT rejected."""
    from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
        DiagnosisItem,
    )

    overrun = "x" * (cap + 1) * multiplier
    item = DiagnosisItem(**_build_diagnose_item(**{field: overrun}))
    value = getattr(item, field)
    assert len(value) == cap, (
        f"DiagnosisItem.{field} should truncate to {cap} chars, got "
        f"len={len(value)}."
    )
    assert value.endswith("..."), (
        f"DiagnosisItem.{field} truncation should end with '...', "
        f"got {value[-10:]!r}."
    )


def test_diagnose_field_normal_length_passes_through() -> None:
    """Sanity: normal-length fields are unchanged."""
    from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
        DiagnosisItem,
    )

    item = DiagnosisItem(**_build_diagnose_item())
    assert item.rca_kind_label == "missing_filter"
    assert item.evidence_summary == "lots of detail"


# ── Cluster caps ──────────────────────────────────────────────────────


@pytest.mark.parametrize("field,cap", list(_CLUSTER_CAPS.items()))
def test_cluster_field_cap_value(field: str, cap: int) -> None:
    from genie_space_optimizer.skills.plan11_cluster.output_schema import (
        ClusterItem,
    )

    json_schema = ClusterItem.model_json_schema()
    assert json_schema["properties"][field].get("maxLength") == cap, (
        f"ClusterItem.{field} cap drifted from Trial 13 table ({cap})."
    )


@pytest.mark.parametrize("field,cap", list(_CLUSTER_CAPS.items()))
def test_cluster_field_truncates_gracefully(field: str, cap: int) -> None:
    from genie_space_optimizer.skills.plan11_cluster.output_schema import (
        ClusterItem,
    )

    overrun = "y" * (cap + 100)
    item = ClusterItem(**_build_cluster_item(**{field: overrun}))
    value = getattr(item, field)
    assert len(value) == cap and value.endswith("..."), (
        f"ClusterItem.{field} truncation drift: len={len(value)} "
        f"tail={value[-5:]!r}"
    )


# ── Synthesize caps ───────────────────────────────────────────────────


@pytest.mark.parametrize("field,cap", list(_SYNTHESIZE_CAPS.items()))
def test_synthesize_field_cap_value(field: str, cap: int) -> None:
    from genie_space_optimizer.skills.plan11_synthesize.output_schema import (
        ProposalItem,
    )

    json_schema = ProposalItem.model_json_schema()
    assert json_schema["properties"][field].get("maxLength") == cap, (
        f"ProposalItem.{field} cap drifted from Trial 13 table ({cap})."
    )


@pytest.mark.parametrize("field,cap", list(_SYNTHESIZE_CAPS.items()))
def test_synthesize_field_truncates_gracefully(
    field: str, cap: int,
) -> None:
    from genie_space_optimizer.skills.plan11_synthesize.output_schema import (
        ProposalItem,
    )

    overrun = "z" * (cap + 50)
    item = ProposalItem(**_build_synthesize_item(**{field: overrun}))
    value = getattr(item, field)
    assert len(value) == cap and value.endswith("..."), (
        f"ProposalItem.{field} truncation drift: len={len(value)} "
        f"tail={value[-5:]!r}"
    )


# ── _classify_llm_error taxonomy arm ──────────────────────────────────


def test_classify_llm_error_response_post_parse_field_length() -> None:
    """``string_too_long`` post-parse errors must classify to the
    typed ``response_post_parse_field_length`` arm rather than
    falling through to ``unknown`` or ``parse``."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        _classify_llm_error,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )

    request = LlmReasoningRequest(
        skill_id="plan11_diagnose",
        call_id="t13_field_length",
        result_cls=dict,
        system_msg="x",
        user_prompt="y",
        max_tokens=2048,
    )
    kind = _classify_llm_error(
        exception_class="ValueError",
        error_message=(
            "1 validation error for AbstainableEnvelope[Plan11DiagnoseOutput]\n"
            "result.diagnoses.1.evidence_summary\n"
            "  String should have at most 2000 characters [type=string_too_long]"
        ),
        tokens_input=4096,
        request=request,
    )
    assert kind == "response_post_parse_field_length", kind


# ── Truncate marker ───────────────────────────────────────────────────


def test_post_parse_field_truncate_marker_roundtrips() -> None:
    """Marker emits a stable line a postmortem can join on."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_post_parse_field_truncate_marker,
    )

    line = plan11_post_parse_field_truncate_marker(
        optimization_run_id="run_t13",
        iteration=1,
        skill_id="plan11_diagnose",
        field_path="result.diagnoses.1.evidence_summary",
        original_length=12000,
        truncated_length=2000,
    )
    assert line.startswith("GSO_PLAN11_POST_PARSE_FIELD_TRUNCATE_V1 "), line
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["optimization_run_id"] == "run_t13"
    assert payload["iteration"] == 1
    assert payload["skill_id"] == "plan11_diagnose"
    assert payload["field_path"] == "result.diagnoses.1.evidence_summary"
    assert payload["original_length"] == 12000
    assert payload["truncated_length"] == 2000
