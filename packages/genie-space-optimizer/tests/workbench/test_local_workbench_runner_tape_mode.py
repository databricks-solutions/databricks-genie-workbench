"""End-to-end tape-mode workbench test.

Drives the local lever-loop workbench through the production state
machine using:

* the synthetic shape-ladder hydration rows under
  ``tests/unit/fixtures/production_eval_rows.json`` (so every Stage 1
  card passes by construction), and
* the reusable Stage 1/2/3 SM tape factories under
  ``tests/integration/sm_forward_tapes.py``.

Locks in:

  1. The workbench reaches at least ``APPLIED`` for the admitted hard
     QIDs without contacting any Databricks API.
  2. The recording applier captures one PATCH per applied proposal.
  3. The report renderer emits ``result.json`` and ``result.md`` with
     the expected funnel-stage summary.

This is the workbench's analogue to ``tests/integration/
test_sm_forward_pipeline_to_applied.py``; the workbench MUST track
that suite's apply boundary so a regression there does not silently
slip past the local rehearsal.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from local_lever_workbench.input_bundle import (
    DEFAULT_FAKE_SPACE_ID,
    minimal_metadata_snapshot,
)
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
    summarize_stage_progress,
)
from local_lever_workbench.models import (
    WorkbenchHardCase,
    WorkbenchInputBundle,
    WorkbenchProvenance,
    WorkbenchRunConfig,
)
from local_lever_workbench.report import build_run_result, write_report
from local_lever_workbench.stage1_probe import probe_bundle


def _stamp_hard_verdict(row: dict) -> dict:
    """Force ``row_is_hard_failure`` to admit the synthetic shape row.

    The shape-ladder fixtures in production_eval_rows.json do not
    always carry the verdict fields the production admission helper
    inspects, so stamp them here. Matches the helper used by
    ``test_sm_stage3_empty_synthesis_terminates.py``.

    Also overwrites ``arbiter/metadata.blame_set`` with bare-identifier
    column names that match the leaves of :func:`_synthetic_schema_columns`.
    The shape-ladder fixture ships compound text seeds like
    ``["table.col_a", "table.col_b"]`` which the Trial 13i FQN
    normalizer (rule 3) always drops, abstaining Stage 1 with
    ``seeds_unnormalizable``. Bare identifiers fall under rule 2 of
    :func:`schema_columns._normalize_seeds_to_fqn` — they leaf-match
    against the synthetic FQNs and resolve cleanly so the Stage 1 LLM
    sees a non-empty ``blame_set_seed``.
    """
    new = dict(row)
    new.setdefault("result_correctness/value", "no")
    new.setdefault("arbiter/value", "ground_truth_correct")
    arbiter_metadata = dict(new.get("arbiter/metadata") or {})
    arbiter_metadata["blame_set"] = ["col_a", "col_b"]
    new["arbiter/metadata"] = arbiter_metadata
    return new


def _synthetic_schema_columns() -> tuple[str, ...]:
    """Return synthetic 4-part FQNs to satisfy the Stage 1 pre-flight.

    Trial 13i added a ``schema_columns_min_size >= 1`` pre-flight check
    in :class:`Stage1InputEvidenceContract` that short-circuits to
    ``abstain: evidence_card_empty:missing_schema_columns`` when the
    run-level ``schema_columns`` channel is empty. In tape-mode the
    workspace_client is ``None`` so the production injector
    (``inject_schema_columns_into_metadata_snapshot``) returns
    ``source="api_error"`` and does not populate the channel. To exercise
    the full pipeline (HARD_QID_SEEN -> APPLIED) without contacting a
    Databricks API, the synthetic bundle must therefore pre-seed
    ``metadata_snapshot["schema_columns"]`` with at least one 4-part
    FQN. The values themselves are irrelevant to the tape-driven
    transformers — the contract checks only count and shape — but we
    use airline-themed identifiers to match the production_eval_rows
    fixture domain so postmortem markers read coherently.
    """
    return (
        "synth.workbench.tbl.col_a",
        "synth.workbench.tbl.col_b",
    )


def _load_hydration_bundle() -> WorkbenchInputBundle:
    """Build a workbench bundle from the synthetic shape-ladder rows."""
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "unit"
        / "fixtures"
        / "production_eval_rows.json"
    )
    payload = json.loads(fixture_path.read_text())
    rows = payload.get("hydration_rows") or []
    hard_rows: list[WorkbenchHardCase] = []
    for raw in rows:
        if not raw.get("_expected_hard"):
            continue
        qid = str(raw.get("_expected_qid") or "")
        if not qid:
            continue
        hard_rows.append(
            WorkbenchHardCase(
                qid=qid,
                row=_stamp_hard_verdict(dict(raw)),
                typed_evidence=None,
                expected_card_violations=(),
            )
        )
    assert hard_rows, "hydration corpus must declare at least one hard row"
    metadata_snapshot = dict(minimal_metadata_snapshot())
    # Pre-seed the run-level ``schema_columns`` channel so the Stage 1
    # contract's ``schema_columns_min_size >= 1`` pre-flight passes.
    # See ``_synthetic_schema_columns`` for the rationale.
    metadata_snapshot["schema_columns"] = list(_synthetic_schema_columns())
    return WorkbenchInputBundle(
        provenance=WorkbenchProvenance(source_kind="synthetic"),
        space_id=DEFAULT_FAKE_SPACE_ID,
        hard_cases=tuple(hard_rows),
        metadata_snapshot=metadata_snapshot,
    )


def _build_forward_tape(qids: tuple[str, ...]):
    """Reuse the integration tape factories so the workbench tracks them."""
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )

    tape = []
    # Stock multiple copies per stage per QID so applier failures /
    # narrow-replacement retries can cycle back without exhausting the
    # tape mid-iteration. Matches the production-shaped helper in
    # test_sm_forward_pipeline_to_applied.py.
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)
    return tape


@pytest.mark.workbench
@pytest.mark.integration
def test_workbench_tape_mode_reaches_applied_and_records_patch(
    tmp_path: Path,
) -> None:
    """Tape-mode workbench must reach APPLIED and record one PATCH/qid."""
    bundle = _load_hydration_bundle()
    qids = bundle.hard_qids

    tape = _build_forward_tape(qids)
    tape_path = tmp_path / "forward.jsonl"
    # The SM tape harness reads JSONL. Each line is a TapeEntry dict.
    tape_path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": e.kind,
                    "skill_id": e.skill_id,
                    "call_id": e.call_id,
                    "iteration": e.iteration,
                    "parsed_output": e.parsed_output,
                    "raw_text": e.raw_text,
                    "tokens_input": e.tokens_input,
                    "tokens_output": e.tokens_output,
                    "duration_ms": e.duration_ms,
                    "exception_class": e.exception_class,
                    "exception_message": e.exception_message,
                }
            )
            for e in tape
        )
    )

    output_dir = tmp_path / "workbench_run"
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "synthetic_bundle.json",
        output_dir=output_dir,
        llm_mode=LLM_MODE_TAPE,
        tape_path=tape_path,
        iteration=1,
    )

    stage1 = probe_bundle(bundle)
    assert stage1.all_pass, (
        f"hydration corpus must already pass Stage 1; got violations="
        f"{[(f.qid, list(f.violations)) for f in stage1.findings if f.violations]!r}"
    )

    artifacts = run_workbench_iteration(bundle, config)

    progress = summarize_stage_progress(artifacts)
    by_qid = {p.qid: p for p in progress}
    assert set(by_qid) == set(qids), (
        f"workbench admitted {sorted(by_qid)!r}; expected {sorted(qids)!r}"
    )
    applied_or_deeper = ("applied", "evaluated", "accepted")
    for qid in qids:
        assert by_qid[qid].deepest_stage in applied_or_deeper, (
            f"qid={qid} deepest_stage={by_qid[qid].deepest_stage!r}; "
            f"workbench did not reach APPLIED. terminal_reason="
            f"{by_qid[qid].terminal_reason!r}"
        )

    recorded = artifacts.recorder.as_tuple()
    assert recorded, (
        "recording applier captured no PATCHes despite reaching APPLIED. "
        "The applier-gate stub wiring drifted."
    )
    recorded_qids = {rp.qid for rp in recorded}
    assert recorded_qids == set(qids), (
        f"recorded PATCH QIDs {recorded_qids!r} != admitted QIDs "
        f"{set(qids)!r}; recording applier missed a QID."
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_workbench_writes_json_and_markdown_report(tmp_path: Path) -> None:
    """The report writer must produce ``result.json`` and ``result.md``."""
    bundle = _load_hydration_bundle()
    qids = bundle.hard_qids
    tape = _build_forward_tape(qids)
    tape_path = tmp_path / "forward.jsonl"
    tape_path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": e.kind,
                    "skill_id": e.skill_id,
                    "call_id": e.call_id,
                    "iteration": e.iteration,
                    "parsed_output": e.parsed_output,
                    "raw_text": e.raw_text,
                    "tokens_input": e.tokens_input,
                    "tokens_output": e.tokens_output,
                    "duration_ms": e.duration_ms,
                    "exception_class": e.exception_class,
                    "exception_message": e.exception_message,
                }
            )
            for e in tape
        )
    )

    output_dir = tmp_path / "workbench_run"
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "synthetic_bundle.json",
        output_dir=output_dir,
        llm_mode=LLM_MODE_TAPE,
        tape_path=tape_path,
        iteration=1,
    )
    stage1 = probe_bundle(bundle)
    artifacts = run_workbench_iteration(bundle, config)
    result = build_run_result(
        bundle=bundle, config=config, stage1=stage1, artifacts=artifacts,
    )
    json_path, md_path = write_report(result=result, output_dir=output_dir)

    assert json_path.exists() and md_path.exists()
    decoded = json.loads(json_path.read_text())
    assert decoded["deepest_stage_reached"] in (
        "applied",
        "evaluated",
        "accepted",
    )
    md_text = md_path.read_text()
    assert "deepest_stage_reached" in md_text
    assert "Funnel progress per QID" in md_text
