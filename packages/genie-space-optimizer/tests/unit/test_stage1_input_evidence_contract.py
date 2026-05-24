"""Phase 3 / Track 2 — :class:`Stage1InputEvidenceContract` is the
client-side pre-flight gate that prevents Stage 1 LLM calls when the
input evidence card is empty.

Symmetric to ``DatabricksEndpointRequestContract`` at the wire
boundary, this contract sits at the input boundary: if a payload
would arrive at the LLM with missing question text / SQL / blame
set / rca_evidence, we skip the call (no token spend), emit
``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1``, and short-circuit Stage 1
to a typed declined response. The Trial 11 root cause (every Stage 1
call burned tokens to return the same correct
``missing_schema_context`` decline) cannot recur once this gate is in
place.

Test coverage:

* ``validate(card) == []`` for every hydrated fixture row.
* One ``ConstraintViolation`` per missing field for empty inputs.
* ``Stage1InputCardEmptyError`` renders deterministically and carries
  the violations list.
* ``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1`` marker round-trips through
  JSON with stable field names.
* ``_invoke_stage1_llm`` returns ``_Stage1Response(succeeded=False,
  declined="evidence_card_empty:...")`` without invoking
  ``diagnose_failing_qids`` when the card is empty.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_stage1_input_card_empty_marker,
)
from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
    ConstraintViolation,
    Stage1InputCardEmptyError,
    Stage1InputEvidenceContract,
)
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    _Stage1Response,
    _invoke_stage1_llm,
    plan11_stage1_diagnosis,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)
EXPECTED_QIDS = ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"]


@pytest.fixture(scope="module")
def hydration_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["hydration_rows"]]


def _complete_card_for(qid: str) -> dict:
    return {
        "qid": qid,
        "question_text": "What is the total revenue?",
        "ground_truth_sql": "SELECT SUM(r) FROM t",
        "generated_sql": "SELECT SUM(r) FROM t WHERE 1=0",
        "judge_rationale": "filter excludes all rows",
        "blame_set_seed": ["t.r"],
        "rca_evidence": {
            "observed_failure": "all rows filtered",
            "generated_sql_issue": "wrong WHERE",
            "expected_sql_shape": "remove filter",
            "suggested_repair_family": "filter_remove",
            "confidence": "high",
        },
    }


# ── validate() ──────────────────────────────────────────────────────


def test_default_contract_passes_complete_card() -> None:
    assert DEFAULT_STAGE1_CONTRACT.validate(_complete_card_for("gs_009")) == []


@pytest.mark.parametrize("qid", EXPECTED_QIDS)
def test_default_contract_passes_every_hydrated_fixture_row(
    hydration_rows: list[dict], qid: str
) -> None:
    from genie_space_optimizer.optimization.eval_row_access import (
        build_stage1_evidence_card,
    )

    row = next(r for r in hydration_rows if r["_expected_qid"] == qid)
    card = build_stage1_evidence_card(qid, row)
    assert DEFAULT_STAGE1_CONTRACT.validate(card) == []


@pytest.mark.parametrize(
    "field, expected_tag",
    [
        ("question_text", "question_text_empty"),
        ("generated_sql", "generated_sql_empty"),
        ("blame_set_seed", "blame_set_empty"),
    ],
)
def test_contract_flags_individual_missing_fields(
    field: str, expected_tag: str
) -> None:
    card = _complete_card_for("gs_009")
    card[field] = "" if isinstance(card[field], str) else []
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert expected_tag in tags


def test_contract_flags_missing_expected_sql_when_judge_rationale_also_empty() -> None:
    card = _complete_card_for("gs_009")
    card["ground_truth_sql"] = ""
    card["judge_rationale"] = ""
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "expected_sql_or_judge_rationale_empty" in tags


def test_contract_passes_when_expected_sql_empty_but_judge_rationale_present() -> None:
    card = _complete_card_for("gs_009")
    card["ground_truth_sql"] = ""
    # judge_rationale still populated — typed-evidence path supplies it.
    assert DEFAULT_STAGE1_CONTRACT.validate(card) == []


def test_contract_flags_rca_evidence_when_all_subfields_empty() -> None:
    card = _complete_card_for("gs_009")
    card["rca_evidence"] = {
        "observed_failure": "",
        "generated_sql_issue": "",
        "expected_sql_shape": "",
        "suggested_repair_family": "",
        "confidence": "",
    }
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "rca_evidence_empty" in tags


def test_contract_passes_with_only_observed_failure_in_rca_evidence() -> None:
    card = _complete_card_for("gs_009")
    card["rca_evidence"] = {
        "observed_failure": "the judge rationale",
        "generated_sql_issue": "",
        "expected_sql_shape": "",
        "suggested_repair_family": "",
        "confidence": "",
    }
    assert DEFAULT_STAGE1_CONTRACT.validate(card) == []


def test_contract_aggregates_all_violations_on_empty_card() -> None:
    empty: dict = {
        "qid": "gs_009",
        "question_text": "",
        "ground_truth_sql": "",
        "generated_sql": "",
        "judge_rationale": "",
        "blame_set_seed": [],
        "rca_evidence": {
            "observed_failure": "",
            "generated_sql_issue": "",
            "expected_sql_shape": "",
            "suggested_repair_family": "",
            "confidence": "",
        },
    }
    tags = {v.field for v in DEFAULT_STAGE1_CONTRACT.validate(empty)}
    assert "question_text_empty" in tags
    assert "generated_sql_empty" in tags
    assert "blame_set_empty" in tags
    assert "rca_evidence_empty" in tags
    assert "expected_sql_or_judge_rationale_empty" in tags


# ── Stage1InputCardEmptyError ───────────────────────────────────────


def test_error_renders_deterministically() -> None:
    violations = [
        ConstraintViolation(
            field="question_text_empty",
            value="",
            constraint="must be non-empty",
        ),
        ConstraintViolation(
            field="blame_set_empty",
            value=[],
            constraint="must be non-empty",
        ),
    ]
    err = Stage1InputCardEmptyError(violations)
    assert err.violations == violations
    text = str(err)
    assert "question_text_empty" in text
    assert "blame_set_empty" in text


# ── marker emission ─────────────────────────────────────────────────


def test_plan11_stage1_input_card_empty_marker_round_trips() -> None:
    line = plan11_stage1_input_card_empty_marker(
        optimization_run_id="run_test",
        iteration=1,
        qid="gs_009",
        violations=["question_text_empty", "blame_set_empty"],
        field_sources={
            "question_text": "absent",
            "ground_truth_sql": "absent",
            "generated_sql": "absent",
            "judge_rationale": "absent",
            "blame_set_seed": "absent",
            "rca_evidence": "absent",
        },
    )
    assert line.startswith("GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1 ")
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["optimization_run_id"] == "run_test"
    assert payload["iteration"] == 1
    assert payload["qid"] == "gs_009"
    assert payload["violations"] == ["question_text_empty", "blame_set_empty"]
    assert payload["field_sources"]["question_text"] == "absent"


# ── _invoke_stage1_llm wiring ───────────────────────────────────────


def _fake_state(qid: str) -> QuestionStateInIteration:
    return QuestionStateInIteration(
        qid=qid,
        iteration=1,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=HardQidSeenRecord(
            eval_row_id=f"row_{qid}",
            predicate="row_is_hard_failure",
            score=0.0,
            baseline_sql="",
            expected_shape="",
            iteration_first_seen=1,
        ),
    )


def _ctx(rows: list[dict]) -> TransformerContext:
    vctx = ValidationContext(iteration=1, run_id="run_test", extras={})
    return TransformerContext(
        iteration=1,
        run_id="run_test",
        validation_context=vctx,
        baseline_eval_rows=tuple(dict(r) for r in rows),
        # Trial 13i — keep schema_columns broad and include the
        # fixture's ASI seed entries so the seed FQN normalizer (which
        # runs inside ``build_stage1_evidence_card`` when schema_columns
        # is non-empty) does not drop them as compound text. Mirrors
        # production where ``ctx.schema_columns`` is derived from
        # ``_rca_evidence_typed[*].blame_set`` and naturally contains
        # the same FQNs the row's ASI metadata references.
        schema_columns=("table.col_a", "table.col_b", "a", "b"),
        recent_diagnoses=(),
        extras={},
    )


def test_invoke_stage1_llm_skips_call_on_empty_card_and_returns_typed_decline() -> None:
    empty_row = {"question_id": "gs_empty"}
    ctx = _ctx([empty_row])
    state = _fake_state("gs_empty")

    with patch(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids"
    ) as diag:
        response = _invoke_stage1_llm(state, ctx)

    # No LLM call should have happened.
    diag.assert_not_called()
    assert isinstance(response, _Stage1Response)
    assert response.succeeded is False
    assert response.declined is not None
    assert response.declined.startswith("evidence_card_empty:")


def test_invoke_stage1_llm_proceeds_when_card_is_hydrated(
    hydration_rows: list[dict],
) -> None:
    ctx = _ctx(hydration_rows)
    state = _fake_state("gs_009")

    from genie_space_optimizer.optimization.stages.diagnose import (
        PerQidDiagnosis,
    )

    fake_diag = PerQidDiagnosis(
        qid="gs_009",
        rca_kind_label="top_n_missing",
        observed_failure="returns all rows",
        generated_sql_issue="missing LIMIT",
        expected_sql_shape="add LIMIT 10",
        blame_set=("orders.customer_id",),
        evidence_summary="LIMIT missing from ORDER BY",
        confidence="high",
    )
    with patch(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        return_value=[fake_diag],
    ) as diag:
        response = _invoke_stage1_llm(state, ctx)

    diag.assert_called_once()
    assert response.succeeded is True
    assert response.parsed_output is not None
    assert response.parsed_output.rca_kind_label == "top_n_missing"


def test_transformer_terminates_qid_on_empty_card(
    hydration_rows: list[dict],
) -> None:
    """End-to-end: ``_Plan11Stage1Transformer.transform`` must route
    ``evidence_card_empty:*`` through the abstain path to an
    ``OPTIMIZER_NO_CANDIDATES`` terminal record (no LLM tokens spent).
    """
    empty_row = {"question_id": "gs_empty_terminate"}
    ctx = _ctx([empty_row])
    state = _fake_state("gs_empty_terminate")

    out_state = plan11_stage1_diagnosis.transform(state, ctx)

    assert out_state.terminal is not None
    assert isinstance(out_state.terminal, TerminalRecord)
    assert out_state.terminal.kind == "OPTIMIZER_NO_CANDIDATES"
    assert "evidence_card_empty:" in out_state.terminal.reason


# ── contract is a frozen dataclass with stable defaults ─────────────


def test_contract_is_frozen_with_stable_required_fields() -> None:
    c = Stage1InputEvidenceContract()
    with pytest.raises((AttributeError, Exception)):
        c.min_blame_set_size = 99  # type: ignore[misc]
    # Defaults pinned.
    assert c.min_blame_set_size == 1
