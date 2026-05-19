"""Plan 3 Task 12 — RcaEvidenceBundle gains per_qid_evidence_typed sidecar."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceBundle,
)


def test_bundle_has_per_qid_evidence_typed_field() -> None:
    field_names = {f.name for f in dataclasses.fields(RcaEvidenceBundle)}
    assert "per_qid_evidence_typed" in field_names


def test_per_qid_evidence_typed_defaults_to_empty_dict() -> None:
    """Default must be a callable factory (empty dict), not the
    sentinel ``{}`` literal — mutable default would alias across
    instances."""
    out = RcaEvidenceBundle(
        per_qid_evidence={},
        rca_kinds_by_qid={},
        evidence_refs={},
        promoted_to_top_n_qids=(),
    )
    assert out.per_qid_evidence_typed == {}
    out2 = RcaEvidenceBundle(
        per_qid_evidence={},
        rca_kinds_by_qid={},
        evidence_refs={},
        promoted_to_top_n_qids=(),
    )
    out.per_qid_evidence_typed["a"] = "x"  # type: ignore[assignment]
    assert "a" not in out2.per_qid_evidence_typed


def test_bundle_round_trips_with_typed_sidecar_populated() -> None:
    evidence = PerQidRcaEvidence(
        qid="gs_001",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=("t.c",),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=(),
    )
    out = RcaEvidenceBundle(
        per_qid_evidence={
            "gs_001": {"rca_kind": "top_n_cardinality_collapse"},
        },
        rca_kinds_by_qid={"gs_001": "top_n_cardinality_collapse"},
        evidence_refs={"gs_001": ("trace://x",)},
        promoted_to_top_n_qids=(),
        per_qid_evidence_typed={"gs_001": evidence},
    )
    payload = out.to_json()
    assert "per_qid_evidence_typed" in payload
    assert payload["per_qid_evidence_typed"]["gs_001"]["qid"] == "gs_001"
    rebuilt = RcaEvidenceBundle.from_json(payload)
    assert rebuilt.per_qid_evidence_typed.keys() == {"gs_001"}
    # JsonRoundTrip rebuilds nested dataclasses only when the field
    # type is explicit; this carrier is ``dict[str, Any]`` so the
    # value re-emerges as a plain dict from JSON. The typed
    # round-trip is verified per-dataclass in T2.
    assert rebuilt.per_qid_evidence_typed["gs_001"]["qid"] == "gs_001"


def test_existing_contract_test_still_passes() -> None:
    """Sanity: instantiation without the new field still works
    (default factory)."""
    out = RcaEvidenceBundle(
        per_qid_evidence={
            "gs_001": {
                "rca_kind": "wrong_join_spec",
                "judge_verdict": "wrong_join_spec",
                "sql_diff": "SELECT *",
                "rca_id": "rca-001",
            }
        },
        rca_kinds_by_qid={"gs_001": "wrong_join_spec"},
        evidence_refs={"gs_001": ("trace://run1/iter/1/judge/gs_001",)},
        promoted_to_top_n_qids=(),
    )
    assert out.per_qid_evidence_typed == {}


def test_collect_populates_typed_sidecar_when_flag_enabled_and_llm_succeeds(
    monkeypatch,
) -> None:
    """Integration-lite: collect() dispatches per-qid through the LLM
    when the flag is ON and the LLM succeeds; both sidecar and legacy
    dict are populated (via PerQidRcaEvidence.to_legacy_dict)."""
    import json
    from unittest.mock import MagicMock, patch

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages.rca_evidence import (
        RcaEvidenceInput, collect,
    )

    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "true")

    envelope = json.dumps({
        "result": {
            "qid": "gs_001",
            "observed_failure": "returned 1 row instead of top 3",
            "generated_sql_issue": "missing LIMIT 3 and ORDER BY revenue DESC",
            "expected_sql_shape": "GROUP BY 1 ORDER BY 2 DESC LIMIT 3",
            "blame_set": ["sales.fact_sales.revenue"],
            "suggested_repair_family": "top_n_with_ordering",
            "repair_hint_patch_type": "add_example_sql",
            "confidence": "high",
            "quoted_evidence": ["judge: missing top-n"],
        },
        "declined": None,
    })

    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=100, completion_tokens=50, total_tokens=150,
    )
    client.chat.completions.create.return_value = completion

    inp = RcaEvidenceInput(
        eval_rows=(
            {
                "question_id": "gs_001",
                "genie_sql": "SELECT product, SUM(revenue) FROM sales.fact_sales GROUP BY 1",
            },
        ),
        hard_failure_qids=("gs_001",),
        soft_signal_qids=(),
        per_qid_judge={"gs_001": {"verdict": "missing_top_n"}},
        asi_metadata={"gs_001": {"failure_type": "missing_top_n"}},
    )

    class _Ctx:
        run_id = "test_run"
        iteration = 5

    with patch.object(optimizer, "_get_openai_client", return_value=client):
        bundle = collect(_Ctx(), inp)

    assert "gs_001" in bundle.per_qid_evidence_typed
    typed = bundle.per_qid_evidence_typed["gs_001"]
    assert typed.suggested_repair_family == "top_n_with_ordering"
    assert "gs_001" in bundle.per_qid_evidence
    legacy = bundle.per_qid_evidence["gs_001"]
    assert legacy["rca_kind"] == "top_n_cardinality_collapse"
    assert legacy["rca_id"].startswith("rca_llm_")


def test_collect_falls_back_to_deterministic_when_llm_declines(
    monkeypatch,
) -> None:
    """When the LLM declines, the typed sidecar stays empty but the
    legacy dict is still populated via _asi_finding_from_metadata."""
    import json
    from unittest.mock import MagicMock, patch

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages.rca_evidence import (
        RcaEvidenceInput, collect,
    )

    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "true")

    decline_envelope = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "two equally-plausible blame sets",
            "needed_evidence": ["disambiguating_judge_verdict"],
            "suggested_next_step": "skip",
        },
    })

    client = MagicMock()
    choice = MagicMock()
    choice.message.content = decline_envelope
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=80, completion_tokens=30, total_tokens=110,
    )
    client.chat.completions.create.return_value = completion

    inp = RcaEvidenceInput(
        eval_rows=(
            {"question_id": "gs_001", "genie_sql": "SELECT * FROM t"},
        ),
        hard_failure_qids=("gs_001",),
        soft_signal_qids=(),
        per_qid_judge={"gs_001": {"verdict": "wrong_join_spec"}},
        asi_metadata={"gs_001": {"failure_type": "wrong_join_spec"}},
    )

    class _Ctx:
        run_id = "test_run"
        iteration = 5

    with patch.object(optimizer, "_get_openai_client", return_value=client):
        bundle = collect(_Ctx(), inp)

    # Plan 8 Task 6 — deterministic fallback now populates the typed
    # sidecar from metadata (so Plan 4 LLM clustering / Plan 5 LLM intent
    # synthesis see fallback'd qids). The legacy dict is still populated
    # for byte-stable downstream consumers.
    assert "gs_001" in bundle.per_qid_evidence_typed
    assert "gs_001" in bundle.per_qid_evidence
    assert bundle.per_qid_evidence["gs_001"]["rca_kind"] == "join_spec_missing_or_wrong"


def test_collect_skips_llm_dispatch_when_flag_disabled(monkeypatch) -> None:
    """Flag-off: deterministic path only, NO OpenAI call."""
    from unittest.mock import MagicMock, patch

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages.rca_evidence import (
        RcaEvidenceInput, collect,
    )

    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "0")
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")

    inp = RcaEvidenceInput(
        eval_rows=(
            {"question_id": "gs_001", "genie_sql": "SELECT * FROM t"},
        ),
        hard_failure_qids=("gs_001",),
        soft_signal_qids=(),
        per_qid_judge={"gs_001": {"verdict": "wrong_join_spec"}},
        asi_metadata={"gs_001": {"failure_type": "wrong_join_spec"}},
    )

    class _Ctx:
        run_id = "test_run"
        iteration = 5

    with patch.object(optimizer, "_get_openai_client", return_value=client):
        bundle = collect(_Ctx(), inp)

    assert client.chat.completions.create.call_count == 0
    # Plan 8 Task 6 — deterministic fallback now populates the typed
    # sidecar even when the LLM path is flag-disabled.
    assert "gs_001" in bundle.per_qid_evidence_typed
    assert "gs_001" in bundle.per_qid_evidence
