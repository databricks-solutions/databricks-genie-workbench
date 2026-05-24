"""Phase 7 / Track 6 — CI golden: every Stage 1 input builder must
produce a payload that passes :class:`Stage1InputEvidenceContract` for
every documented production row shape.

Mirrors :mod:`tests.integration.test_databricks_request_contract_golden`
at the input boundary. If a future PR shrinks
:mod:`eval_row_access` coverage or registers a fourth Stage 1 input
builder without registering it here, this test fails at PR time
(before the change can reach a trial).

Three builders × five fixture shapes = 15 (builder, shape) pairs.
Each pair must:

* Hydrate a non-empty evidence card via the builder.
* Pass ``DEFAULT_STAGE1_CONTRACT.validate(card) == []``.
* Round-trip through ``Stage1InputEvidenceContract.field_sources``
  with every key resolving to ``"present"``.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.eval_row_access import (
    build_stage1_evidence_card,
)
from genie_space_optimizer.optimization.optimizer import (
    _build_plan11_failing_qids_from_raw,
    _build_plan11_failing_qids_from_typed_evidence,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PatchType,
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
)
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    _build_failing_qid_payload,
)

FIXTURE_PATH = (
    Path(__file__).parent.parent / "unit" / "fixtures" / "production_eval_rows.json"
)
EXPECTED_QIDS = ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"]


@pytest.fixture(scope="module")
def hydration_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["hydration_rows"]]


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


def _typed_evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid,
        observed_failure=f"observed failure for {qid}",
        generated_sql_issue=f"sql issue for {qid}",
        expected_sql_shape=f"expected shape for {qid}",
        blame_set=(f"catalog.schema.tbl_{qid}",),
        suggested_repair_family="add_sql_snippet_expression",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=(f"quote about {qid}",),
    )


# ──────────────────────────────────────────────────────────────────────
# Builder registry — the CI lock for "every Stage 1 input adapter".
#
# If a future PR adds a new Stage 1 input builder, it MUST register
# itself here and pass the contract for every fixture shape. The
# self-consistency check below counts entries.
# ──────────────────────────────────────────────────────────────────────


def _build_via_eval_row_access(qid: str, row: dict) -> dict:
    return build_stage1_evidence_card(qid, row)


def _build_via_diagnose_llm(qid: str, row: dict) -> dict:
    return _build_failing_qid_payload(_fake_state(qid), row)


def _build_via_optimizer_raw(qid: str, row: dict) -> dict:
    out = _build_plan11_failing_qids_from_raw(
        failing_qids=[qid], eval_rows=[row],
    )
    assert len(out) == 1, (
        f"_build_plan11_failing_qids_from_raw returned {len(out)} entries "
        f"for {qid}; expected 1"
    )
    return out[0]


def _build_via_optimizer_typed_evidence(qid: str, row: dict) -> dict:
    out = _build_plan11_failing_qids_from_typed_evidence(
        {qid: _typed_evidence(qid)}, eval_rows=[row],
    )
    assert len(out) == 1, (
        f"_build_plan11_failing_qids_from_typed_evidence returned {len(out)} "
        f"entries for {qid}; expected 1"
    )
    return out[0]


STAGE1_INPUT_BUILDERS: tuple[tuple[str, callable], ...] = (
    ("eval_row_access.build_stage1_evidence_card", _build_via_eval_row_access),
    (
        "diagnose_llm._build_failing_qid_payload",
        _build_via_diagnose_llm,
    ),
    (
        "optimizer._build_plan11_failing_qids_from_raw",
        _build_via_optimizer_raw,
    ),
    (
        "optimizer._build_plan11_failing_qids_from_typed_evidence",
        _build_via_optimizer_typed_evidence,
    ),
)


@pytest.mark.parametrize("builder_name,builder_fn", STAGE1_INPUT_BUILDERS)
@pytest.mark.parametrize("qid", EXPECTED_QIDS)
def test_every_stage1_input_builder_passes_contract_for_every_shape(
    hydration_rows: list[dict],
    builder_name: str,
    builder_fn,
    qid: str,
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == qid)
    card = builder_fn(qid, row)
    violations = DEFAULT_STAGE1_CONTRACT.validate(card)
    assert violations == [], (
        f"{builder_name} produced contract violations on shape "
        f"{row['_shape']}/{qid}: "
        + "; ".join(f"{v.field}={v.constraint}" for v in violations)
    )


@pytest.mark.parametrize("builder_name,builder_fn", STAGE1_INPUT_BUILDERS)
@pytest.mark.parametrize("qid", EXPECTED_QIDS)
def test_every_stage1_input_builder_reports_all_fields_present(
    hydration_rows: list[dict],
    builder_name: str,
    builder_fn,
    qid: str,
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == qid)
    card = builder_fn(qid, row)
    sources = DEFAULT_STAGE1_CONTRACT.field_sources(card)
    # Trial 13 (Phase 3): ``field_sources`` now reports the originating
    # row path string per field (e.g. ``"inputs/question"``) when the
    # builder recorded one, falling back to the literal ``"present"``
    # for unattributed cards. ``"absent"`` is the only sentinel that
    # means "no value"; everything else is a populated field.
    absent = [k for k, v in sources.items() if v == "absent"]
    assert absent == [], (
        f"{builder_name} produced absent fields {absent} for shape "
        f"{row['_shape']}/{qid}; card={card}"
    )


def test_stage1_input_builder_registry_is_at_least_four_entries() -> None:
    """Self-consistency check: shrinking the registry below four
    (one canonical adapter + three production builders) means the CI
    golden lost coverage. Forces future Stage 1 callers to register.
    """
    assert len(STAGE1_INPUT_BUILDERS) >= 4
