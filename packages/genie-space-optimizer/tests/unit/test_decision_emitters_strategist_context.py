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


def test_stage4_context_persistence_flag_default_on(
    monkeypatch,
) -> None:
    """2026-05-13 default-on flip: env-unset returns True. Rollback
    escape hatch is ``GSO_STAGE4_CONTEXT_PERSISTENCE=0``."""
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    monkeypatch.delenv("GSO_STAGE4_CONTEXT_PERSISTENCE", raising=False)
    assert stage4_context_persistence_enabled() is True


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

    # Empty string excluded — _flag_default_on treats it as unset
    # (defaults to True). Matches the canonical RCO-4b contract.
    for falsy in ("0", "false", "False", "no", "off"):
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


def test_optimizer_emits_assembled_record_when_flag_on(monkeypatch) -> None:
    """When GSO_STAGE_HANDLERS_CHUNK_A=1 AND GSO_STAGE4_CONTEXT_PERSISTENCE=1,
    calling the strategist's Stage-4 emit helper from optimizer.py routes
    one ASSEMBLED record through the supplied decision_emit callback."""
    from genie_space_optimizer.optimization.optimizer import (
        _emit_strategist_context_records_for_test_harness as _emit_helper,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    monkeypatch.setenv("GSO_STAGE_HANDLERS_CHUNK_A", "1")
    monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", "1")
    captured: list = []

    result = _emit_helper(
        clusters=[
            {
                "cluster_id": "H001",
                "question_ids": ["gs_009"],
                "rca_id": "r1",
                "grounding": "grounded",
            },
        ],
        reflection_buffer=[],
        metadata_snapshot={"_baseline_accuracy": 0.5},
        run_id="run_x",
        iteration=2,
        decision_emit=captured.append,
        mlflow_anchor_run_id=None,  # disables MLflow log; emit still fires
    )

    types = [r.decision_type for r in captured]
    assert DecisionType.STRATEGIST_CONTEXT_ASSEMBLED in types
    assembled = next(
        r for r in captured
        if r.decision_type == DecisionType.STRATEGIST_CONTEXT_ASSEMBLED
    )
    assert assembled.run_id == "run_x"
    assert assembled.iteration == 2
    assert assembled.metrics["assembled_hash"].startswith("sha256:")
    # The helper returns the (hash, fields) pair so the CONSUMED helper
    # (Task 8) can pass the structural-diff inputs into its producer.
    assert isinstance(result, dict)
    assert result["assembled_hash"].startswith("sha256:")
    assert "hard_failure_qids" in result["top_level_fields"]


def test_optimizer_skips_assembled_emit_when_flag_off(monkeypatch) -> None:
    """Flag-off (rollback escape) preserves replay byte stability — no emit."""
    from genie_space_optimizer.optimization.optimizer import (
        _emit_strategist_context_records_for_test_harness as _emit_helper,
    )

    monkeypatch.setenv("GSO_STAGE_HANDLERS_CHUNK_A", "1")
    # 2026-05-13: flag is default-on; assert the off-path by setting =0.
    monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", "0")
    captured: list = []

    result = _emit_helper(
        clusters=[],
        reflection_buffer=[],
        metadata_snapshot={},
        run_id="run_x",
        iteration=1,
        decision_emit=captured.append,
        mlflow_anchor_run_id=None,
    )

    assert captured == []
    # No-op result is shaped identically so the caller does not need to
    # branch on whether the flag was on.
    assert result == {"assembled_hash": "", "top_level_fields": ()}


def test_optimizer_persists_typed_output_to_mlflow_when_anchor_present(
    monkeypatch,
) -> None:
    """When mlflow_anchor_run_id is set, the typed boundary is log_text'd
    to gso_postmortem_bundle/iterations/iter_NN/stages/04_strategist_context/
    output.json. Tests monkeypatch the shim, not MlflowClient."""
    monkeypatch.setenv("GSO_STAGE_HANDLERS_CHUNK_A", "1")
    monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", "1")

    calls: list[dict] = []

    def _fake_log_text(*, run_id, text, artifact_file):
        calls.append({
            "run_id": run_id, "text": text, "artifact_file": artifact_file,
        })

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _fake_log_text,
    )

    from genie_space_optimizer.optimization.optimizer import (
        _emit_strategist_context_records_for_test_harness as _emit_helper,
    )

    _emit_helper(
        clusters=[
            {"cluster_id": "H001", "question_ids": ["q1"], "grounding": "grounded"},
        ],
        reflection_buffer=[],
        metadata_snapshot={},
        run_id="run_x",
        iteration=3,
        decision_emit=lambda r: None,
        mlflow_anchor_run_id="anchor_42",
    )

    assert len(calls) == 1
    assert calls[0]["run_id"] == "anchor_42"
    assert calls[0]["artifact_file"] == (
        "gso_postmortem_bundle/iterations/iter_03/stages/"
        "04_strategist_context/output.json"
    )
    # The text is the typed-output JSON, not the assembled hash.
    assert "rca_cards_grounded_only" in calls[0]["text"]


def test_emit_consumed_record_helper_uses_assembled_hash_from_local_state(
    monkeypatch,
) -> None:
    """The CONSUMED helper hashes the consumed payload and records drift
    against the assembled_hash returned by Task 7's helper."""
    from genie_space_optimizer.optimization.optimizer import (
        _emit_strategist_context_consumed_for_test_harness as _emit_consumed,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        ReasonCode,
    )

    monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", "1")
    captured: list = []

    _emit_consumed(
        consumed_payload={"a": 1, "b": [2, 3]},
        assembled_hash="sha256:" + "f" * 64,  # known-mismatch
        assembled_top_fields=("a", "b"),
        run_id="r",
        iteration=4,
        decision_emit=captured.append,
        mlflow_anchor_run_id=None,  # disables consumed.json persistence
    )

    assert len(captured) == 1
    rec = captured[0]
    assert rec.decision_type == DecisionType.STRATEGIST_CONTEXT_CONSUMED
    assert rec.reason_code == ReasonCode.CONTEXT_CONSUMED_DRIFTED
    assert rec.metrics["drift_detected"] is True


def test_emit_consumed_record_helper_is_noop_when_flag_off(
    monkeypatch,
) -> None:
    """Flag-off (rollback escape): no record emitted regardless of payload."""
    from genie_space_optimizer.optimization.optimizer import (
        _emit_strategist_context_consumed_for_test_harness as _emit_consumed,
    )

    # 2026-05-13: flag is default-on; assert the off-path by setting =0.
    monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", "0")
    captured: list = []

    _emit_consumed(
        consumed_payload={"a": 1},
        assembled_hash="",
        assembled_top_fields=(),
        run_id="r",
        iteration=1,
        decision_emit=captured.append,
        mlflow_anchor_run_id=None,
    )

    assert captured == []


def test_emit_consumed_record_helper_persists_consumed_json_when_anchor_present(
    monkeypatch,
) -> None:
    """When mlflow_anchor_run_id is set, the consumed payload lands at
    stages/04_strategist_context/consumed.json — co-located with
    output.json so postmortem can diff the two files directly."""
    monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", "1")

    calls: list[dict] = []

    def _fake_log_text(*, run_id, text, artifact_file):
        calls.append({
            "run_id": run_id, "text": text, "artifact_file": artifact_file,
        })

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _fake_log_text,
    )

    from genie_space_optimizer.optimization.optimizer import (
        _emit_strategist_context_consumed_for_test_harness as _emit_consumed,
    )

    _emit_consumed(
        consumed_payload={"hard_failure_qids": ["gs_009"], "iq_scan_text": "x"},
        assembled_hash="sha256:" + "0" * 64,
        assembled_top_fields=("hard_failure_qids", "iteration"),
        run_id="r",
        iteration=4,
        decision_emit=lambda r: None,
        mlflow_anchor_run_id="anchor_42",
    )

    assert len(calls) == 1
    assert calls[0]["run_id"] == "anchor_42"
    assert calls[0]["artifact_file"] == (
        "gso_postmortem_bundle/iterations/iter_04/stages/"
        "04_strategist_context/consumed.json"
    )
    # The persisted text is canonical JSON of the consumed dict.
    import json as _json
    parsed = _json.loads(calls[0]["text"])
    assert parsed["hard_failure_qids"] == ["gs_009"]
    assert parsed["iq_scan_text"] == "x"
