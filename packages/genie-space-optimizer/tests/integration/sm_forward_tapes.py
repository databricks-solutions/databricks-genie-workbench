"""Reusable tape factories for Stage 1, Stage 2, and Stage 3 replay.

Each factory returns a list of :class:`TapeEntry` objects suitable for
``TapeReplayHarness``. The full forward smoke test concatenates the
three stages so the SM can advance from HARD_QID_SEEN through
PROPOSED in one local run.

Negative tape factories at the bottom build deliberately broken
responses so the failure-mode tests can assert the SM reacts with the
right typed terminal — the same shapes production trials surfaced.
"""
from __future__ import annotations

from typing import Iterable

from tests.integration.sm_tape_replay import TapeEntry


# ── Stage 1: plan11_diagnose ──────────────────────────────────────────


def diagnose_response_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    rca_kind_label: str = "wrong_aggregation",
    blame_object: str = "main.public.orders.amount",
    confidence: str = "high",
) -> list[TapeEntry]:
    """Build one Stage 1 tape entry per QID.

    The production state machine drives the Stage 1 transformer once
    per QID (the orchestrator iterates ``run_until_settled`` per state
    and the Stage 1 transformer's ``transform`` invokes
    ``diagnose_failing_qids`` with the single-element payload for that
    QID). Replay therefore needs ``len(qids)`` entries on the
    ``plan11_diagnose`` skill, each returning the matching diagnosis.

    Defaults pick values that satisfy the
    ``plan11_stage1_diagnosis_marker`` ``diagnosis_actionable`` check
    (non-empty ``evidence_summary``, non-empty ``blame_set``, and
    ``rca_kind_label`` that is not the insufficient-evidence sentinel).

    ``blame_object`` is a fully qualified four-part identifier so the
    downstream Stage 3 ``_target_objects_from_blame_set`` adapter (which
    splits on ``.`` and requires 3 or 4 parts) yields a non-empty
    ``target_objects`` tuple. Without that the survival contract
    fails with ``missing_required_fields_target_objects``.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        diagnoses = [
            {
                "qid": qid,
                "rca_kind_label": rca_kind_label,
                "observed_failure": (
                    f"Generated SQL for {qid} did not match the expected shape."
                ),
                "generated_sql_issue": (
                    "Aggregation or filter clause was incorrect or missing."
                ),
                "expected_sql_shape": (
                    "Apply correct aggregation/filter to match ground-truth."
                ),
                "blame_set": [blame_object],
                "evidence_summary": (
                    "Judge rationale and ASI metadata both point to a single "
                    "structural fix on the blamed object."
                ),
                "confidence": confidence,
            }
        ]
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_diagnose",
                call_id=f"plan11_stage1_diagnose.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"diagnoses": diagnoses},
                raw_text="",
                tokens_input=1024,
                tokens_output=512,
                duration_ms=1500,
            )
        )
    return entries


# ── Stage 2: plan11_cluster ───────────────────────────────────────────


def cluster_response_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    semantic_theme: str = "wrong_aggregation_or_filter",
    repair_hypothesis: str = "Apply correct aggregation/filter clauses.",
    blame_object: str = "main.public.orders.amount",
    confidence: str = "high",
) -> list[TapeEntry]:
    """Build one Stage 2 tape entry per QID, each emitting a self-cluster.

    The Stage 2 ``transform_batch`` is wrapped by ``transform`` to
    operate on a 1-tuple — so the orchestrator drives one LLM call
    per QID. Each entry returns a cluster with that QID alone as
    member so the SM can advance ``DIAGNOSED → CLUSTERED`` for every
    input.

    Each cluster carries non-empty ``repair_hypothesis`` so the
    ``ClusterMembershipRecord.routing_evidence_kind`` validator
    accepts the projection.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        cluster = {
            "semantic_theme": semantic_theme,
            "member_qids": [qid],
            "unifying_evidence": (
                f"QID {qid} matches the {semantic_theme!r} pattern."
            ),
            "repair_hypothesis": repair_hypothesis,
            "primary_blame_set": [blame_object],
            "confidence": confidence,
        }
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_cluster",
                call_id=f"plan11_stage2_cluster.iter_{iteration}.hard.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"clusters": [cluster]},
                raw_text="",
                tokens_input=2048,
                tokens_output=512,
                duration_ms=1500,
            )
        )
    return entries


# ── Stage 3: plan11_synthesize ────────────────────────────────────────


def synthesize_response_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    patch_type: str = "add_example_sql",
    blame_object: str = "main.public.orders.amount",
    intent_name: str = "fix_aggregation_via_example",
    confidence: str = "high",
) -> list[TapeEntry]:
    """Build one Stage 3 tape entry per QID, each carrying a single proposal.

    Stage 3 is invoked once per state by the orchestrator. With the
    one-cluster-per-QID layout from :func:`cluster_response_tape`,
    Stage 3 runs once per QID and consumes one entry per call.

    ``add_example_sql`` is chosen as the default because:
      * It satisfies the ``Plan11SynthesizeOutput`` schema.
      * It passes the structural-repair gate
        (``example_sql`` is in :data:`_STRUCTURAL_PATCH_TYPE_TAGS` and
        :data:`PATCH_TYPE_SEMANTICS` classifies it as ``STRUCTURAL``).
      * It passes the typed
        ``RepairProposal.validate_survival_contract`` because
        ``target_objects`` (derived from a four-part
        ``catalog.schema.table.column`` ``blame_set``) is non-empty
        and ``target_qids`` is non-empty.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        proposal = {
            "intent_name": intent_name,
            "intent_description": (
                "Teach Genie the correct example SQL so the failing "
                "QID matches the expected aggregation shape."
            ),
            "repair_hypothesis": (
                "Provide a worked example illustrating the correct aggregation."
            ),
            "patch_type": patch_type,
            "rationale": (
                "Stage 1 diagnosis and Stage 2 clustering converged "
                "on a single repair shape; the worked example unblocks "
                f"QID {qid}."
            ),
            "confidence": confidence,
            "patch_body": {
                "example_question": "Compute the total amount per order.",
                "example_sql": (
                    "SELECT order_id, SUM(amount) AS total FROM orders "
                    "GROUP BY order_id;"
                ),
            },
            "blame_set": [blame_object],
            "target_qids": [qid],
        }
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_synthesize",
                call_id=f"plan11_stage3_synthesize.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"proposals": [proposal]},
                raw_text="",
                tokens_input=2048,
                tokens_output=1024,
                duration_ms=2500,
            )
        )
    return entries


# ── Forward pipeline composite ────────────────────────────────────────


def full_forward_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
) -> list[TapeEntry]:
    """Concatenate Stage 1, Stage 2, and Stage 3 tapes for the full forward run."""
    qids = list(qids)
    return [
        *diagnose_response_tape(qids, iteration=iteration),
        *cluster_response_tape(qids, iteration=iteration),
        *synthesize_response_tape(qids, iteration=iteration),
    ]


# ── Negative tapes ────────────────────────────────────────────────────


def diagnose_empty_response_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
) -> list[TapeEntry]:
    """Stage 1 returns the envelope shape with an empty ``diagnoses`` list per QID.

    Reproduces the ``contract_failure`` outcome path in
    ``diagnose_failing_qids`` — the LLM returned a structurally valid
    object but no per-QID diagnoses. Downstream the SM has nothing to
    advance with so every input QID terminates.
    """
    qids = list(qids)
    return [
        TapeEntry(
            kind="response",
            skill_id="plan11_diagnose",
            call_id=f"plan11_stage1_diagnose.iter_{iteration}.{qid}",
            iteration=iteration,
            qid=qid,
            parsed_output={"diagnoses": []},
            raw_text="",
            tokens_input=1024,
            tokens_output=64,
            duration_ms=900,
        )
        for qid in qids
    ]


def diagnose_non_actionable_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
) -> list[TapeEntry]:
    """Stage 1 returns one non-actionable diagnosis per QID.

    The Plan 11 ``diagnosis_actionable`` boolean flips false on these
    rows (zero ``blame_set`` and zero ``evidence_summary_chars``).
    The transformer still advances to DIAGNOSED — actionable=false
    is a quality signal, not a gate — but downstream stages may
    decline. The failure-regression test asserts the marker carries
    ``diagnosis_actionable=False`` so postmortems can attribute
    silent stalls.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        diagnoses = [
            {
                "qid": qid,
                "rca_kind_label": "insufficient evidence to determine root cause",
                "observed_failure": "",
                "generated_sql_issue": "",
                "expected_sql_shape": "",
                "blame_set": [],
                "evidence_summary": "",
                "confidence": "low",
            }
        ]
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_diagnose",
                call_id=f"plan11_stage1_diagnose.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"diagnoses": diagnoses},
                raw_text="",
                tokens_input=1024,
                tokens_output=200,
                duration_ms=900,
            )
        )
    return entries


def diagnose_non_actionable_zero_blame_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    rca_kind_label: str = "insufficient evidence to determine root cause",
    evidence_summary: str = "",
    observed_failure: str = "",
    generated_sql_issue: str = "",
    expected_sql_shape: str = "",
) -> list[TapeEntry]:
    """Stage 1 returns the **production-mirror** non-actionable shape per QID.

    Mirrors the Trial 12 (run ``dc89d1a9-...``) Stage 1 shadow batch
    distribution:
    ``plan11_stage1_diagnosis_actionable: {false: 21, true: 3}`` — i.e.
    the LLM kept declining with the insufficient-evidence sentinel,
    empty ``blame_set``, empty ``evidence_summary`` for the vast
    majority of hard QIDs, and the downstream stages silently advanced
    on the diagnosis anyway, producing ``empty_synthesis`` at Stage 3
    and zero applied patches.

    This factory is intentionally named differently from
    :func:`diagnose_non_actionable_tape` so the test corpus can pin two
    independent contracts:

    * The legacy ``diagnose_non_actionable_tape`` tests the existing
      Stage 1 marker behaviour (``diagnosis_actionable=False`` even on
      advance to DIAGNOSED).
    * This ``..._zero_blame_tape`` is consumed by the
      :mod:`test_sm_diagnosis_actionable_gate` contract test which
      asserts the **target** behaviour: ``diagnosis_actionable=False``
      AND ``blame_set_size=0`` must terminate the QID between
      DIAGNOSED and CLUSTERED with a typed
      ``diagnosis_not_actionable`` reason rather than silently
      advancing into Stage 2.

    Until the follow-up gate transformer lands, the SM advances and
    the contract test xfails — the failure pins the missing gate as
    the next implementation PR.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        diagnoses = [
            {
                "qid": qid,
                "rca_kind_label": rca_kind_label,
                "observed_failure": observed_failure,
                "generated_sql_issue": generated_sql_issue,
                "expected_sql_shape": expected_sql_shape,
                "blame_set": [],  # zero-blame is the load-bearing condition
                "evidence_summary": evidence_summary,
                "confidence": "low",
            }
        ]
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_diagnose",
                call_id=f"plan11_stage1_diagnose.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"diagnoses": diagnoses},
                raw_text="",
                tokens_input=1024,
                tokens_output=200,
                duration_ms=900,
            )
        )
    return entries


def cluster_drops_qids_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    surviving_qids: Iterable[str] = (),
) -> list[TapeEntry]:
    """Stage 2 returns *no* clusters — the realistic single-state drop path.

    One entry per input QID. ``cluster_plan11.cluster_diagnoses``
    treats a response with zero clusters as an LLM decline and
    surfaces ``cluster_returned_empty`` so the Stage 2 transformer
    terminates the input state with ``abstain: cluster_returned_empty``.

    Why not assert ``dropped_by_stage2_clustering``? In the production
    single-state batch (the state machine runs ``run_until_settled``
    per QID, so the batch is always one state wide),
    ``cluster_plan11.cluster_diagnoses`` validates LLM ``member_qids``
    against the input QID set and **replaces empty/all-filtered
    member lists with the full input set as a safety fallback**. That
    fallback always re-includes the single-state input, making the
    ``dropped_by_stage2_clustering`` transformer branch unreachable
    in this batching regime. The realistic Stage 2 drop is therefore
    an *empty cluster list*, which both reproduces a true Trial-11
    failure shape and keeps the test from depending on a code path
    only the batched orchestrator could exercise.

    ``surviving_qids`` is retained for forward compatibility but is
    intentionally ignored here — empty ``clusters`` is the only
    shape the single-state SM treats as a drop.
    """
    _ = surviving_qids  # documented contract; unused in single-state SM
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_cluster",
                call_id=(
                    f"plan11_stage2_cluster.iter_{iteration}.hard.{qid}"
                ),
                iteration=iteration,
                qid=qid,
                parsed_output={"clusters": []},
                raw_text="",
                tokens_input=1024,
                tokens_output=64,
                duration_ms=900,
            )
        )
    return entries


def synthesize_invalid_proposal_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
) -> list[TapeEntry]:
    """Stage 3 returns a proposal with an unknown patch_type per QID.

    ``synthesize.py`` skips items with unrecognised ``patch_type``;
    the resulting empty proposal list triggers
    ``ClusterSynthesisResult(skipped_reason="synth_none")``, which the
    Stage 3 transformer treats as ``stage3_returned_none`` and
    terminates each cluster member with ``OPTIMIZER_NO_CANDIDATES``.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        proposal = {
            "intent_name": "bogus_intent",
            "intent_description": "Not a real repair.",
            "repair_hypothesis": "Bogus hypothesis.",
            "patch_type": "this_patch_type_does_not_exist",
            "rationale": "Test: invalid patch_type to trigger contract failure.",
            "confidence": "low",
            "patch_body": {"bogus_field": True},
            "blame_set": [],
            "target_qids": [qid],
        }
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_synthesize",
                call_id=f"plan11_stage3_synthesize.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"proposals": [proposal]},
                raw_text="",
                tokens_input=1024,
                tokens_output=256,
                duration_ms=1500,
            )
        )
    return entries


def diagnose_request_envelope_invalid_tape(
    *,
    iteration: int = 1,
) -> list[TapeEntry]:
    """Stage 1 raises a BadRequestError mirroring the dc89d1a9 wire failure.

    Used by the failure-regression suite to prove the local pipeline
    surfaces ``request_envelope_invalid`` exactly as the production
    classifier would, so PR-1B/PR-1C regressions cause an offline test
    failure long before a deploy.
    """
    body = (
        "Error code: 400 - {'error_code': 'BAD_REQUEST', 'message': "
        "'tools.0.custom.name failed regex ^[a-zA-Z0-9_-]{1,128}$'}"
    )
    return [
        TapeEntry(
            kind="exception",
            skill_id="plan11_diagnose",
            call_id=f"plan11_stage1_diagnose.iter_{iteration}",
            iteration=iteration,
            exception_class="BadRequestError",
            exception_message=body,
            duration_ms=850,
        ),
    ]


def diagnose_overlong_response_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    rca_kind_label_chars: int = 200,
    observed_failure_chars: int = 4000,
    generated_sql_issue_chars: int = 4000,
    expected_sql_shape_chars: int = 4000,
    evidence_summary_chars: int = 4000,
    blame_object: str = "main.public.orders.amount",
) -> list[TapeEntry]:
    """Stage 1 returns diagnoses whose string fields exceed today's
    ``Plan11DiagnoseOutput`` Pydantic ``max_length`` caps.

    Production reality (run ``98ec8950-...`` postmortem):
    ``plan11_stage1_field_length_errors`` flagged at least one
    ``evidence_summary`` response longer than the 400-char cap that
    ``Plan11DiagnoseOutput`` declares today. Pydantic raised
    ``ValidationError`` and the legacy error classifier mistook the
    multiline pydantic message for a normal ``llm_error`` rather than
    a recoverable shape mismatch.

    This tape factory **does NOT, by itself**, exercise Pydantic
    validation — the :class:`TapeReplayHarness` injects ``parsed_output``
    directly into ``LlmReasoningResponse``, bypassing the schema
    validator. Validation is exercised by the companion unit test
    :mod:`tests.unit.test_plan11_diagnose_output_schema_caps` which
    calls ``Plan11DiagnoseOutput.model_validate(...)`` against the
    same payload shape.

    The factory exists so the follow-up "graceful truncation" PR has a
    canonical reproduction of the overlong shape to plug into its
    forward-pipeline assertion. That PR will route the tape through
    the truncation adapter and assert the SM advances rather than
    raising.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        diagnoses = [
            {
                "qid": qid,
                "rca_kind_label": "wrong_aggregation_" * (rca_kind_label_chars // 18 + 1),
                "observed_failure": "x" * observed_failure_chars,
                "generated_sql_issue": "x" * generated_sql_issue_chars,
                "expected_sql_shape": "x" * expected_sql_shape_chars,
                "blame_set": [blame_object],
                "evidence_summary": "x" * evidence_summary_chars,
                "confidence": "high",
            }
        ]
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_diagnose",
                call_id=f"plan11_stage1_diagnose.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"diagnoses": diagnoses},
                raw_text="",
                tokens_input=1024,
                tokens_output=4096,
                duration_ms=2200,
            )
        )
    return entries


def diagnose_long_abstain_tape(
    *,
    iteration: int = 1,
    explanation_chars: int = 4096,
) -> list[TapeEntry]:
    """Stage 1 declines with an unusually long abstain explanation.

    Used to confirm graceful handling of long abstains (PR-3 in the
    Stage 1 BadRequest plan). The tape branch uses ``parsed_output=None``
    to model a declined verdict; harness consumers convert this into
    ``succeeded=False, declined=None`` so the SM treats it as
    ``llm_error``. The complementary "declined" branch lives in the
    real ``LlmReasoningResponse``; the tape here is intentionally
    simpler so the regression test focuses on the recovery path.
    """
    return [
        TapeEntry(
            kind="response",
            skill_id="plan11_diagnose",
            call_id=f"plan11_stage1_diagnose.iter_{iteration}",
            iteration=iteration,
            parsed_output=None,
            raw_text="x" * explanation_chars,
            tokens_input=1024,
            tokens_output=128,
            duration_ms=950,
        ),
    ]


def synthesize_empty_body_proposal_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    patch_type: str = "add_example_sql",
    blame_object: str = "main.public.orders.amount",
) -> list[TapeEntry]:
    """Stage 3 returns a proposal whose ``patch_body`` is empty.

    The Stage 3 transformer constructs a ``RepairProposal`` whose
    ``original_patch_body == {}``; downstream
    :func:`validate_synthesis_output_for_state_machine` raises
    :class:`StageThreeContractError` because ``original_patch_body``
    is a required field and an empty dict is treated as missing.

    The transformer routes the error to
    :func:`_terminate_invariant` ⇒ the state terminates with
    ``kind="OPTIMIZER_INVARIANT_VIOLATION"`` and a typed
    ``outcome_reason`` quoting
    ``"Stage 3 RepairProposal missing required field(s):
    ['original_patch_body']"``.

    This is the most realistic *typed* failure available at the
    PROPOSED boundary in the post-Phase-3 wiring — the
    ``structural_repair_gate`` rejection branch
    (``intent==structural AND emitted!=STRUCTURAL``) is currently
    unreachable from valid :class:`PatchType` outputs because every
    structural-tagged enum value is also classified as
    ``PatchSemantic.STRUCTURAL``.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        proposal = {
            "intent_name": "empty_body_intent",
            "intent_description": (
                "Intentionally empty patch_body to trip the Stage 3 "
                "state-machine contract validator."
            ),
            "repair_hypothesis": "Empty body should fail contract.",
            "patch_type": patch_type,
            "rationale": "Test fixture: provoke StageThreeContractError.",
            "confidence": "low",
            "patch_body": {},
            "blame_set": [blame_object],
            "target_qids": [qid],
        }
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_synthesize",
                call_id=(
                    f"plan11_stage3_synthesize.iter_{iteration}.hard_001.{qid}"
                ),
                iteration=iteration,
                qid=qid,
                parsed_output={"proposals": [proposal]},
                raw_text="",
                tokens_input=2048,
                tokens_output=512,
                duration_ms=2000,
            )
        )
    return entries


def synthesize_empty_synthesis_for_actionable_cluster_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
) -> list[TapeEntry]:
    """Stage 3 returns the **production-mirror** empty-synthesis shape per QID.

    Mirrors the Trial 12 (run ``dc89d1a9-...``) shadow Stage 3
    distribution::

        plan11_stage3_outcomes: {empty_synthesis: 6}

    In production every actionable Stage 2 cluster reached Stage 3 and
    the LLM returned ``{"proposals": []}`` — a structurally valid
    envelope with **zero proposals**. The SM has nothing to apply, but
    today the failure mode is *silent*: the iteration completes without
    emitting a typed ``GSO_PATCH_OUTCOME_V1`` marker that postmortems
    can attribute back to "Stage 3 declined to synthesize" rather than
    a downstream applier failure or a missing structural patch.

    Use this factory together with an actionable Stage 1 + Stage 2 tape
    so the upstream stages succeed and Stage 3 is unambiguously the
    declining transformer.

    Companion contract:
    :mod:`test_sm_stage3_empty_synthesis_terminates` asserts the gate
    that does not yet exist — every empty-synthesis must terminate with
    ``reason="stage3_silent_decline"`` (or another typed Stage 3
    no-candidate reason) and emit a ``GSO_PATCH_OUTCOME_V1`` line so
    postmortems can rank it against other silent-decline modes.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_synthesize",
                call_id=f"plan11_stage3_synthesize.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"proposals": []},
                raw_text="",
                tokens_input=2048,
                tokens_output=64,
                duration_ms=1200,
            )
        )
    return entries


def synthesize_blast_radius_unsafe_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    patch_type: str = "add_example_sql",
    blame_object: str = "main.public.orders.amount",
    intent_name: str = "fix_aggregation_but_blast_radius_unsafe",
    collateral_qids: Iterable[str] = ("gs_outside_target_001", "gs_outside_target_002"),
    confidence: str = "high",
) -> list[TapeEntry]:
    """Stage 3 returns a contract-passing proposal that the blast-radius gate rejects.

    The proposal is identical in shape to :func:`synthesize_response_tape`
    (same ``patch_type``, blame set, target QIDs, ``example_question`` /
    ``example_sql`` body) so it passes Stage 3's typed contract and the
    structural-repair gate. The single addition is a non-empty
    ``passing_dependents`` field on ``patch_body`` that names QIDs
    *outside* ``target_qids``.

    :func:`proposal_grounding.patch_blast_radius_is_safe` reads
    ``patch.get("passing_dependents")`` and computes
    ``outside = [q for q in dependents if q not in target_set]``. When
    ``len(outside) > max_outside_target=0`` and ``high_collateral_risk``
    is not set, the helper returns
    ``{"safe": False, "reason": "blast_radius_exceeds_threshold",
        "passing_dependents_outside_target": outside}``.

    The ``blast_radius_batch`` transformer projects that into a
    typed :class:`ProposalAttempt` with
    ``outcome="blast_radius_rejected"`` and cycles the state back
    ``NORMALIZED → PROPOSED``. A ``GSO_GATE_REASONING_V1`` line
    surfaces ``gate=blast_radius_batch verdict=rejected
    reason=blast_radius_exceeds_threshold collateral_qids=[...]``.

    This is the canonical local reproduction of the Phase-3 reject
    path the production blast-radius scanner emits.
    """
    qids = list(qids)
    collateral = tuple(str(q) for q in collateral_qids if str(q))
    if not collateral:
        raise ValueError(
            "synthesize_blast_radius_unsafe_tape requires at least one "
            "collateral QID outside target_qids; otherwise the gate's "
            "safe-by-default fallback returns safe and the test is a "
            "false positive."
        )
    entries: list[TapeEntry] = []
    for qid in qids:
        proposal = {
            "intent_name": intent_name,
            "intent_description": (
                "Worked example that would fix the failing QID but "
                "whose blast radius now overlaps unrelated passing "
                "queries — the gate must reject."
            ),
            "repair_hypothesis": (
                "Provide a worked example illustrating the correct aggregation."
            ),
            "patch_type": patch_type,
            "rationale": (
                "Stage 1 + Stage 2 converged on the same fix shape, "
                "but the patch's passing_dependents include QIDs "
                "outside the target set."
            ),
            "confidence": confidence,
            "patch_body": {
                "example_question": "Compute the total amount per order.",
                "example_sql": (
                    "SELECT order_id, SUM(amount) AS total FROM orders "
                    "GROUP BY order_id;"
                ),
                "passing_dependents": list(collateral),
            },
            "blame_set": [blame_object],
            "target_qids": [qid],
        }
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_synthesize",
                call_id=f"plan11_stage3_synthesize.iter_{iteration}.{qid}",
                iteration=iteration,
                qid=qid,
                parsed_output={"proposals": [proposal]},
                raw_text="",
                tokens_input=2048,
                tokens_output=1024,
                duration_ms=2500,
            )
        )
    return entries


def synthesize_non_structural_proposal_tape(
    qids: Iterable[str],
    *,
    iteration: int = 1,
    patch_type: str = "add_metric_view_instruction",
    blame_object: str = "main.public.orders.amount",
    intent_name: str = "add_metric_view_instruction_hint",
    confidence: str = "low",
) -> list[TapeEntry]:
    """Stage 3 returns a typed, contract-passing proposal whose
    intended shape is *structural* but whose emitted semantic is not.

    ``structural_repair_gate`` rejects a proposal when
    ``intended_patch_shape == "structural"`` but the emitted patch
    semantic in :data:`PATCH_TYPE_SEMANTICS` is not
    ``PatchSemantic.STRUCTURAL``. ``add_metric_view_instruction``
    is the canonical example:

      * The substring tag ``metric_view`` matches
        ``_STRUCTURAL_PATCH_TYPE_TAGS`` ⇒ intended=structural.
      * :data:`PATCH_TYPE_SEMANTICS` classifies the type as
        ``PatchSemantic.INSTRUCTION`` ⇒ emitted=instruction.
      * ``enforce_structural_repair_shape`` therefore returns
        ``rejected`` with reason
        ``structural_gate_dropped_instruction_only``.

    Plain ``add_instruction`` does NOT reject — both its intended
    and emitted shape are non-structural, which the gate admits as
    a legacy fail-open case. Using ``add_metric_view_instruction``
    is the explicit Plan-9 mismatch shape the gate was wired to
    catch, and matches the failure shape Trial 12 saw when the LLM
    promised a structural repair and delivered an instruction.

    The survival contract still passes (``blame_set`` and
    ``target_qids`` are populated), so the proposal lives long
    enough for the structural gate to surface a *typed* rejection
    via ``ProposalAttempt.outcome="structural_repair_rejected"``.
    """
    qids = list(qids)
    entries: list[TapeEntry] = []
    for qid in qids:
        proposal = {
            "intent_name": intent_name,
            "intent_description": (
                "Add a Genie instruction reminding the optimizer of the "
                "correct aggregation, which is by itself non-structural."
            ),
            "repair_hypothesis": (
                "An instruction hint should be sufficient — it is not."
            ),
            "patch_type": patch_type,
            "rationale": (
                "Stage 1 + Stage 2 converged; instructions feel cheap "
                "but the structural gate rejects them."
            ),
            "confidence": confidence,
            "patch_body": {
                "instruction_text": (
                    "Always aggregate by order_id when computing totals."
                ),
            },
            "blame_set": [blame_object],
            "target_qids": [qid],
        }
        entries.append(
            TapeEntry(
                kind="response",
                skill_id="plan11_synthesize",
                call_id=(
                    f"plan11_stage3_synthesize.iter_{iteration}.hard_001.{qid}"
                ),
                iteration=iteration,
                qid=qid,
                parsed_output={"proposals": [proposal]},
                raw_text="",
                tokens_input=2048,
                tokens_output=1024,
                duration_ms=2500,
            )
        )
    return entries


__all__ = [
    "cluster_drops_qids_tape",
    "cluster_response_tape",
    "diagnose_empty_response_tape",
    "diagnose_long_abstain_tape",
    "diagnose_non_actionable_tape",
    "diagnose_non_actionable_zero_blame_tape",
    "diagnose_overlong_response_tape",
    "diagnose_request_envelope_invalid_tape",
    "diagnose_response_tape",
    "full_forward_tape",
    "synthesize_blast_radius_unsafe_tape",
    "synthesize_empty_body_proposal_tape",
    "synthesize_empty_synthesis_for_actionable_cluster_tape",
    "synthesize_invalid_proposal_tape",
    "synthesize_non_structural_proposal_tape",
    "synthesize_response_tape",
]
