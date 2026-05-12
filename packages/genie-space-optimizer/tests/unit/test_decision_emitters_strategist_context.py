"""Plan P-G — Stage 4 Strategist Context persistence unit tests.

Covers the two producer helpers in decision_emitters that emit
STRATEGIST_CONTEXT_ASSEMBLED (start of Stage 5, captures the typed
boundary) and STRATEGIST_CONTEXT_CONSUMED (LLM-call boundary, captures
what actually went into the prompt) with a hash on each so postmortem
can detect drift between Stage 4 assembly and Stage 5 consumption.

Evidence anchor:
runid_analysis/{ccf1d60d,31ecd96f}/evidence/gso_postmortem_bundle/operator_transcript.md
— Stage 4 is empty in every iter of both runs.
"""

from __future__ import annotations


def test_decision_type_has_strategist_context_assembled_and_consumed() -> None:
    """The two new DecisionType values exist and are JSON-stable strings."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    assert DecisionType.STRATEGIST_CONTEXT_ASSEMBLED.value == (
        "strategist_context_assembled"
    )
    assert DecisionType.STRATEGIST_CONTEXT_CONSUMED.value == (
        "strategist_context_consumed"
    )


def test_reason_code_has_context_drift_and_match_codes() -> None:
    """ReasonCode carries CONTEXT_ASSEMBLED / *_MATCHES_ASSEMBLED / *_DRIFTED."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    assert ReasonCode.CONTEXT_ASSEMBLED.value == "context_assembled"
    assert ReasonCode.CONTEXT_CONSUMED_MATCHES_ASSEMBLED.value == (
        "context_consumed_matches_assembled"
    )
    assert ReasonCode.CONTEXT_CONSUMED_DRIFTED.value == (
        "context_consumed_drifted"
    )


def test_type_to_section_includes_new_decision_types() -> None:
    """The new DecisionType values are mapped to a fixed transcript section
    so the section-coverage invariant (every DecisionType has a section)
    stays green."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        TYPE_TO_SECTION,
    )

    assert DecisionType.STRATEGIST_CONTEXT_ASSEMBLED in TYPE_TO_SECTION
    assert DecisionType.STRATEGIST_CONTEXT_CONSUMED in TYPE_TO_SECTION


def test_stage4_context_persistence_flag_default_off(
    monkeypatch,
) -> None:
    """Default-OFF preserves byte-stable replay."""
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    monkeypatch.delenv("GSO_STAGE4_CONTEXT_PERSISTENCE", raising=False)
    assert stage4_context_persistence_enabled() is False


def test_stage4_context_persistence_flag_truthy_values(
    monkeypatch,
) -> None:
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    for truthy in ("1", "true", "yes", "on", "TRUE"):
        monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", truthy)
        assert stage4_context_persistence_enabled() is True, truthy


def test_stage4_context_persistence_flag_falsy_values(
    monkeypatch,
) -> None:
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    for falsy in ("", "0", "false", "no", "off"):
        monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", falsy)
        assert stage4_context_persistence_enabled() is False, falsy


def test_strategist_context_assembled_record_emits_one_with_hash() -> None:
    """Producer emits exactly one ASSEMBLED record carrying the canonical
    SHA-256 hash of StrategistContextOutput.to_json()."""
    import hashlib
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_assembled_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )
    from genie_space_optimizer.optimization.stages.strategist_context import (
        StrategistContextOutput,
    )

    out = StrategistContextOutput(
        iteration=2,
        baseline_accuracy=0.5,
        hard_failure_qids=("gs_009", "gs_024"),
        clusters_by_qid={"gs_009": "H001", "gs_024": "H002"},
        rca_cards_grounded_only=(
            {"rca_id": "r1", "cluster_id": "H001", "grounding": "grounded"},
        ),
        rca_cards_ungrounded_count=1,
    )
    import json as _json
    expected_hash = "sha256:" + hashlib.sha256(
        _json.dumps(
            out.to_json(),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()

    record = strategist_context_assembled_record(
        run_id="run_abc", iteration=2, assembled_output=out,
    )

    assert record.decision_type == DecisionType.STRATEGIST_CONTEXT_ASSEMBLED
    assert record.outcome == DecisionOutcome.INFO
    assert record.reason_code == ReasonCode.CONTEXT_ASSEMBLED
    assert record.iteration == 2
    assert record.run_id == "run_abc"
    assert record.metrics.get("assembled_hash") == expected_hash
    assert record.metrics.get("rca_cards_grounded_only_count") == 1
    assert record.metrics.get("rca_cards_ungrounded_count") == 1
    assert record.metrics.get("hard_failure_qid_count") == 2
    assert record.affected_qids == ("gs_009", "gs_024")
    assert record.evidence_refs == ("stage:strategist_context",)
    # Plan P-G: expose top-level typed-boundary fields so the CONSUMED
    # producer can compute a structural diff without re-reading the JSON.
    top_fields = record.metrics.get("top_level_fields")
    assert isinstance(top_fields, tuple) and "rca_cards_grounded_only" in (
        top_fields
    )
    assert "baseline_accuracy" in top_fields
    assert "hard_failure_qids" in top_fields


def test_strategist_context_assembled_record_hash_is_deterministic() -> None:
    """Same StrategistContextOutput → same hash across calls."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_assembled_record,
    )
    from genie_space_optimizer.optimization.stages.strategist_context import (
        StrategistContextOutput,
    )

    out = StrategistContextOutput(iteration=1, baseline_accuracy=0.5)
    r1 = strategist_context_assembled_record(
        run_id="r", iteration=1, assembled_output=out,
    )
    r2 = strategist_context_assembled_record(
        run_id="r", iteration=1, assembled_output=out,
    )
    assert r1.metrics["assembled_hash"] == r2.metrics["assembled_hash"]


def test_strategist_context_consumed_record_matches_when_hashes_equal(
) -> None:
    """When assembled_hash == consumed_hash, the record reports MATCHES."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_consumed_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    record = strategist_context_consumed_record(
        run_id="r",
        iteration=3,
        consumed_payload={"a": 1, "b": [2, 3]},
        assembled_hash="sha256:" + "a" * 64,  # arbitrary fixed value
        assembled_top_level_fields=("a", "b"),
    )
    # Force a known-match by feeding the same canonical projection back.
    same_hash_record = strategist_context_consumed_record(
        run_id="r",
        iteration=3,
        consumed_payload={"a": 1, "b": [2, 3]},
        assembled_hash=record.metrics["consumed_hash"],
        assembled_top_level_fields=("a", "b"),
    )

    assert record.decision_type == DecisionType.STRATEGIST_CONTEXT_CONSUMED
    assert record.outcome == DecisionOutcome.INFO
    assert record.iteration == 3
    assert same_hash_record.reason_code == (
        ReasonCode.CONTEXT_CONSUMED_MATCHES_ASSEMBLED
    )
    assert same_hash_record.metrics["drift_detected"] is False
    # Identical key sets → diff buckets are empty / count of both = 2.
    assert same_hash_record.metrics["keys_only_in_consumed"] == ()
    assert same_hash_record.metrics["keys_only_in_assembled"] == ()
    assert same_hash_record.metrics["keys_in_both"] == 2


def test_strategist_context_consumed_record_emits_structural_diff(
) -> None:
    """The structural key-set diff is computable from typed assembled
    fields + consumed dict — even when the hashes alone don't tell us
    *what* differed."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_consumed_record,
    )

    record = strategist_context_consumed_record(
        run_id="r",
        iteration=2,
        consumed_payload={
            "rca_cards_grounded_only": [],  # in both
            "iq_scan_text": "blah",          # only in consumed
            "suggestions_text": "x",         # only in consumed
        },
        assembled_hash="sha256:" + "9" * 64,
        assembled_top_level_fields=(
            "iteration", "rca_cards_grounded_only", "baseline_accuracy",
        ),
    )

    keys_only_consumed = set(record.metrics["keys_only_in_consumed"])
    keys_only_assembled = set(record.metrics["keys_only_in_assembled"])
    assert keys_only_consumed == {"iq_scan_text", "suggestions_text"}
    assert keys_only_assembled == {"iteration", "baseline_accuracy"}
    assert record.metrics["keys_in_both"] == 1


def test_strategist_context_consumed_record_flags_drift_when_hashes_differ(
) -> None:
    """When assembled_hash != consumed_hash, the record reports DRIFTED."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_consumed_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    record = strategist_context_consumed_record(
        run_id="r",
        iteration=3,
        consumed_payload={"a": 1},
        assembled_hash="sha256:" + "b" * 64,
        assembled_top_level_fields=("a", "b"),
    )

    assert record.reason_code == ReasonCode.CONTEXT_CONSUMED_DRIFTED
    assert record.metrics["drift_detected"] is True
    assert record.metrics["assembled_hash"] == "sha256:" + "b" * 64
    # consumed_hash is the canonical hash of the payload, distinct from
    # the synthetic assembled_hash above.
    assert record.metrics["consumed_hash"] != record.metrics["assembled_hash"]
    # Structural drift: "b" is missing from the consumed dict.
    assert tuple(record.metrics["keys_only_in_assembled"]) == ("b",)
    assert tuple(record.metrics["keys_only_in_consumed"]) == ()


def test_strategist_context_consumed_record_handles_empty_assembled_hash(
) -> None:
    """Empty assembled_hash (Chunk A flag off) ⇒ MATCHES suppressed; the
    record still emits with drift_detected=False and a blank assembled
    hash, so the postmortem can see Stage 5 boundary even when Stage 4
    isn't computed."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_consumed_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    record = strategist_context_consumed_record(
        run_id="r",
        iteration=1,
        consumed_payload={"x": 0},
        assembled_hash="",
        assembled_top_level_fields=(),
    )

    assert record.reason_code == ReasonCode.CONTEXT_CONSUMED_MATCHES_ASSEMBLED
    assert record.metrics["drift_detected"] is False
    assert record.metrics["assembled_hash"] == ""
    assert record.metrics["consumed_hash"].startswith("sha256:")
    # No assembled fields supplied → diff is "unknown" not "all drifted":
    # both buckets are empty and keys_in_both=0 so the postmortem can
    # tell this case apart from a real structural drift.
    assert record.metrics["keys_only_in_assembled"] == ()
    assert record.metrics["keys_only_in_consumed"] == ()
    assert record.metrics["keys_in_both"] == 0


def test_assembled_and_consumed_records_pass_cross_checker() -> None:
    """Boundary records carry no per-qid rca_id / target_qids by design;
    the cross-checker MUST NOT flag them as wiring violations."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_assembled_record,
        strategist_context_consumed_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )
    from genie_space_optimizer.optimization.stages.strategist_context import (
        StrategistContextOutput,
    )

    out = StrategistContextOutput(iteration=1, baseline_accuracy=0.0)
    assembled = strategist_context_assembled_record(
        run_id="r", iteration=1, assembled_output=out,
    )
    consumed = strategist_context_consumed_record(
        run_id="r",
        iteration=1,
        consumed_payload={},
        assembled_hash=assembled.metrics["assembled_hash"],
    )

    violations = validate_decisions_against_journey(
        records=[assembled, consumed],
        events=[],  # boundary records have no required journey stage
    )

    assert violations == [], violations
