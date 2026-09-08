"""The metric view attach + lift phase: a consented UC object, measured in isolation.

Runs inside the ``optimize`` task, after iteration-0's baseline eval and before the
first lever patch (MV-D16). It is a *phase* in the strict sense the rules use:
gated off by default, wrapped so no failure of its own can reach the task, and
communicating with the rest of the run only through Delta tables keyed by
``run_id``.

Why the position matters
------------------------
Iteration-0 must measure the space **without** the metric view. Attaching earlier
would make the baseline corpus post-attach SQL, and that corpus is what the
advisor phase fingerprints when proposing the *next* metric view — so an attached
view would end up biasing the case for its own successor. With the attach here,
baseline is pre-attach, the lift eval isolates what the attach alone did, and the
levers then tune on whatever foundation survived the lift verdict.

What this phase never does
--------------------------
It issues no UC DDL. Under MV-D1 the backend creates the metric view under OBO
before the job is submitted; this job runs as the service principal and only
*attaches* an object someone already consented to. On regression it detaches by
restoring the pre-attach config snapshot and **never drops the object** — dropping
is an explicit backend endpoint, not an automatic consequence of a measurement.

Why validation is a hard gate rather than a warning
---------------------------------------------------
The consent chain is worth what its weakest check is worth. Four things must hold
before a single identifier is attached: the consent row exists, its verdict is
``SUFFICIENT``, it was re-verified at trigger time, and every requested object was
recorded ``CREATED`` by the same identity that granted the consent. Any mismatch
skips the whole phase with a recorded reason — never a partial attach of the
identifiers that happened to check out, because a request carrying one bad
identifier has told us something about the request as a whole.

How a kept attach survives the rest of the run (MV-D18)
-------------------------------------------------------
Nothing downstream re-deploys a config: ``publish_and_audit`` promotes the
champion in Delta only. So a kept attach stays live through every loop outcome,
because each lever rollback restores that iteration's ``pre_snapshot``, which is
the post-attach config the loop was handed. What does *not* follow the live space
is iteration 0's recorded ``observed_config_json``, written before this phase ran
— and when no lever attempt is accepted, iteration 0 is the champion. Since a
champion revert resolves to that column, the phase re-points it at the
post-attach config on the kept path, and reconciliation at end of run demotes any
``ATTACHED`` row the final config does not actually reference.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    MV_ATTACH_PHASE_NAME,
    MV_PROVENANCE_USER_CREATED,
)

from .applier import apply_patch_set, rollback
from .eval_runner import FULL, EvalRunResult, LiftReport, lift_report
from .mv_state import (
    load_mv_candidates,
    load_mv_consent,
    load_mv_created_objects,
    update_mv_created_object_status,
)
from .state import update_iteration_observed_config, write_patch, write_stage

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ── Outcomes ─────────────────────────────────────────────────────────────

STATUS_COMPLETE = "COMPLETE"
STATUS_SKIPPED = "SKIPPED"
STATUS_FAILED = "FAILED"

SKIP_NOT_REQUESTED = "NOT_REQUESTED"
"""No ``mv_attach_views`` / ``mv_consent_id`` parameters. Zero cost when off."""

SKIP_NO_CONSENT_ROW = "NO_CONSENT_ROW"
"""``genie_opt_mv_consents`` has no row for the probe id the job was given."""

SKIP_CONSENT_NOT_SUFFICIENT = "CONSENT_NOT_SUFFICIENT"
"""The consent exists and its verdict is not ``SUFFICIENT``.

Recorded separately from a missing row: an INSUFFICIENT verdict means the probe
ran and answered no, which is a different operational story from a lost record.
"""

SKIP_CONSENT_NOT_REVERIFIED = "CONSENT_NOT_REVERIFIED"
"""``reverified_at_trigger`` is unset, so the entitlement was never re-checked
against the state of the world at trigger time. A stale SUFFICIENT is not a
SUFFICIENT."""

SKIP_NO_CREATED_OBJECT = "NO_CREATED_OBJECT"
"""A requested identifier has no ``CREATED`` row for this run. The job attaches
only what the trigger flow just created — never an object it merely found."""

SKIP_CREATOR_MISMATCH = "CREATOR_MISMATCH"
"""A created object's ``created_by`` is not the consent's ``granted_by``. Two
identities in one chain means the object was not created under the consent that
is being used to justify attaching it."""

SKIP_BASELINE_UNUSABLE = "BASELINE_UNUSABLE"
"""Iteration-0's eval failed or produced no rows, so there is nothing to measure
lift *against*. Attaching anyway would ship an unmeasured structural change."""

SKIP_NO_AFFECTED_QUESTIONS = "NO_AFFECTED_QUESTIONS"
"""No proposal recorded the benchmark questions this view was meant to help.

The lift eval scores the affected subset, so an empty subset is not a smaller
measurement — it is no measurement, and the attach does not proceed without one.
"""

SKIP_NO_EVAL_RUNNER = "NO_EVAL_RUNNER"
"""No eval seam was supplied. Same reasoning as an unusable baseline: the attach
is only permitted where its effect can be measured."""

SKIP_ATTACH_NOT_APPLIED = "ATTACH_NOT_APPLIED"
"""``apply_patch_set`` deployed nothing — e.g. every identifier was already on
``data_sources.metric_views``, or the config PATCH failed."""

SKIP_LIFT_EVAL_UNUSABLE = "LIFT_EVAL_UNUSABLE"
"""The lift eval did not reach a gradeable state. The attach is reverted rather
than left in place unmeasured, and the objects stay ``CREATED``."""

VERDICT_ATTACHED = "ATTACHED"
VERDICT_DETACHED = "DETACHED"

CONSENT_VERDICT_SUFFICIENT = "SUFFICIENT"
CREATED_STATUS = "CREATED"

MV_ATTACH_PATCH_TYPE = "mv_attach_data_source"

LIFT_EVAL_LABEL = "mv_lift"
"""``eval_scope`` label on the lift eval run, so the isolated measurement is
distinguishable from a lever attempt in ``genie_opt_iterations`` and in the
workspace's own eval-run list."""

_MV_LEVER = 2
"""Metric views are Lever 2. The attach is not an LLM lever (MV-D16) but it is
still Lever-2 work, and the patch row records the lever a reader would expect."""

_ATTACH_ITERATION = 0
"""``genie_opt_patches`` rows are keyed by (run_id, iteration, lever,
patch_index) and iteration 0 is the baseline, which applies no patches — so the
attach records against iteration 0 without colliding, and reads as what it is:
applied on top of the baseline, before attempt 1."""


@dataclass(frozen=True)
class AttachOutcome:
    """What the phase did, in a shape that survives into a stage row.

    ``config`` is the configuration the caller should carry forward: the
    post-attach config when the attach was kept, and the pre-attach config when it
    was skipped or reverted. It is deliberately excluded from ``detail()`` — a
    stage row is operator-facing and a full space config is not a status.
    """

    status: str
    skip_reason: str | None = None
    error: str | None = None
    verdict: str | None = None
    requested: tuple[str, ...] = ()
    attached: tuple[str, ...] = ()
    detached: tuple[str, ...] = ()
    suggestion_ids: tuple[str, ...] = ()
    attach_patch_id: str | None = None
    baseline_eval_run_id: str | None = None
    lift_eval_run_id: str | None = None
    affected_question_count: int = 0
    delta_affected: float | None = None
    delta_suite: float | None = None
    regressed_question_count: int = 0
    config: dict[str, Any] | None = field(default=None, repr=False)

    def detail(self) -> dict[str, Any]:
        """The ``genie_opt_stages.detail_json`` payload.

        Identifiers, ids and counts only — no question text and no SQL. A stage
        row is not a leakage exemption.
        """
        return {
            "phase": MV_ATTACH_PHASE_NAME,
            "status": self.status,
            "skip_reason": self.skip_reason,
            "error": self.error,
            "verdict": self.verdict,
            "requested": list(self.requested),
            "attached": list(self.attached),
            "detached": list(self.detached),
            "suggestion_ids": list(self.suggestion_ids),
            "attach_patch_id": self.attach_patch_id,
            "baseline_eval_run_id": self.baseline_eval_run_id,
            "lift_eval_run_id": self.lift_eval_run_id,
            "affected_question_count": self.affected_question_count,
            "delta_affected": self.delta_affected,
            "delta_suite": self.delta_suite,
            "regressed_question_count": self.regressed_question_count,
        }


# ── Helpers ──────────────────────────────────────────────────────────────


def parse_attach_views(raw: str | Sequence[str] | None) -> list[str]:
    """Parse the ``mv_attach_views`` job parameter into identifiers.

    Accepts the JSON list the parameter carries, a bare comma-separated string
    (what a human types into a widget by hand), or an already-parsed sequence.
    Anything unparseable yields an empty list, which the phase treats as "not
    requested" — a malformed parameter must not become a guessed attach.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            decoded = [part for part in text.split(",")]
        if isinstance(decoded, str):
            decoded = [decoded]
        if not isinstance(decoded, list):
            logger.warning("mv_attach: ignoring non-list mv_attach_views parameter")
            return []
        raw = decoded
    out: list[str] = []
    for item in raw:
        identifier = str(item).strip()
        if identifier and identifier not in out:
            out.append(identifier)
    return out


def _eval_result_from_output(eval_output: Mapping[str, Any]) -> EvalRunResult:
    """Rebuild the seam's own result type from the loop's eval-output dict.

    The loop stores ``build_eval_output_from_official``'s dict rather than the
    ``EvalRunResult`` it came from, and ``lift_report`` compares result objects.
    Every field below is read straight back out of that dict — this reconstructs
    the seam's input, it does not reshape the report.
    """
    rows = [dict(row) for row in (eval_output.get("rows") or []) if isinstance(row, Mapping)]
    num_questions = int(eval_output.get("total_questions") or len(rows))
    return EvalRunResult(
        eval_run_id=str(eval_output.get("eval_run_id") or ""),
        status=str(eval_output.get("eval_run_status") or ""),
        num_correct=int(eval_output.get("correct_count") or 0),
        num_done=int(eval_output.get("num_done") or num_questions),
        num_needs_review=int(eval_output.get("num_needs_review") or 0),
        num_questions=num_questions,
        rows=rows,
        wall_clock_seconds=float(eval_output.get("_eval_wall_clock_seconds") or 0.0),
    )


def _baseline_usable(eval_output: Mapping[str, Any]) -> bool:
    if eval_output.get("eval_run_failed"):
        return False
    if not str(eval_output.get("eval_run_id") or ""):
        return False
    return bool(eval_output.get("rows"))


def is_regression(report: LiftReport) -> bool:
    """Whether the lift verdict requires a detach.

    A negative delta on the affected questions is a regression by definition. A
    delta of exactly zero *with* regressed questions is also one: the view broke
    specific answers and paid for them with unrelated luck elsewhere, which is not
    a reason to keep a structural change. A positive delta stands even if one
    question moved the wrong way — that is the trade the measurement exists to
    quantify, and the report is persisted so a reviewer sees both halves.
    """
    if report.delta_affected < 0:
        return True
    return report.delta_affected == 0 and bool(report.regressed_question_ids)


def _affected_question_ids(
    spark: SparkSession,
    *,
    space_id: str,
    catalog: str,
    schema: str,
    suggestion_ids: Sequence[str],
) -> list[str]:
    """Benchmark question ids recorded on the proposals these objects came from.

    Candidates are space-scoped and outlive the run that proposed them (MV-D7),
    which is exactly why this reads by ``target_space_id``: under MV-D1 the
    proposal was written by an earlier run than the one attaching it.
    """
    wanted = {str(sid) for sid in suggestion_ids if str(sid)}
    if not wanted:
        return []
    try:
        candidates = load_mv_candidates(
            spark, catalog, schema, target_space_id=space_id,
        )
    except Exception:
        logger.warning("mv_attach: could not read candidates for %s", space_id, exc_info=True)
        return []

    out: list[str] = []
    for candidate in candidates:
        if str(candidate.get("suggestion_id") or "") not in wanted:
            continue
        evidence = candidate.get("evidence")
        if not isinstance(evidence, Mapping):
            continue
        for qid in evidence.get("benchmark_questions") or ():
            text = str(qid).strip()
            if text and text not in out:
                out.append(text)
    return out


def _attach_patches(identifiers: Sequence[str]) -> list[dict[str, Any]]:
    return [
        {
            "type": MV_ATTACH_PATCH_TYPE,
            "target": identifier,
            "new_text": identifier,
            "old_text": "",
            "lever": _MV_LEVER,
            "risk_level": "high",
            "asset": {"identifier": identifier},
            "proposal_id": f"mv-attach-{index + 1}",
            "patch_family": "mv_attach",
        }
        for index, identifier in enumerate(identifiers)
    ]


def _attach_patch_reference(run_id: str, patch_index: int) -> str:
    """A locatable reference to the ``genie_opt_patches`` row just written.

    ``genie_opt_patches`` has no surrogate key — its identity is
    (run_id, iteration, lever, patch_index) — so the created-object row records
    that tuple rather than inventing an id no query could resolve.
    """
    return f"{run_id}:{_ATTACH_ITERATION}:{_MV_LEVER}:{patch_index}"


# ── The phase ────────────────────────────────────────────────────────────


def run_mv_attach_phase(
    spark: SparkSession,
    *,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    attach_views: str | Sequence[str] | None,
    consent_probe_id: str,
    config: dict[str, Any],
    baseline_eval: Mapping[str, Any],
    w: Any = None,
    eval_runner: Any = None,
    apply_mode: str = "genie_config",
    benchmark_corpus: Any = None,
) -> AttachOutcome:
    """Attach consented metric views, measure the lift, detach on regression.

    **Never raises.** Total isolation is the contract: this is an addition to a
    task whose job is optimization, so any failure of its own must cost its own
    output and nothing else. The returned ``config`` is what the caller carries
    forward, so a failed phase leaves the loop running against the configuration
    it already had.
    """
    identifiers = parse_attach_views(attach_views)
    probe_id = str(consent_probe_id or "").strip()
    if not identifiers or not probe_id:
        return _record(
            spark,
            AttachOutcome(
                status=STATUS_SKIPPED,
                skip_reason=SKIP_NOT_REQUESTED,
                requested=tuple(identifiers),
                config=config,
            ),
            run_id=run_id,
            catalog=catalog,
            schema=schema,
        )

    try:
        outcome = _attach_and_measure(
            spark,
            run_id=run_id,
            space_id=space_id,
            catalog=catalog,
            schema=schema,
            identifiers=identifiers,
            probe_id=probe_id,
            config=config,
            baseline_eval=baseline_eval,
            w=w,
            eval_runner=eval_runner,
            apply_mode=apply_mode,
            benchmark_corpus=benchmark_corpus,
        )
    except Exception as exc:
        logger.warning(
            "mv_attach: phase failed; optimization is unaffected", exc_info=True,
        )
        outcome = AttachOutcome(
            status=STATUS_FAILED,
            error=f"{type(exc).__name__}: {exc}",
            requested=tuple(identifiers),
            config=config,
        )

    return _record(spark, outcome, run_id=run_id, catalog=catalog, schema=schema)


def _attach_and_measure(
    spark: SparkSession,
    *,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    identifiers: list[str],
    probe_id: str,
    config: dict[str, Any],
    baseline_eval: Mapping[str, Any],
    w: Any,
    eval_runner: Any,
    apply_mode: str,
    benchmark_corpus: Any,
) -> AttachOutcome:
    requested = tuple(identifiers)

    def _skip(reason: str, **extra: Any) -> AttachOutcome:
        return AttachOutcome(
            status=STATUS_SKIPPED,
            skip_reason=reason,
            requested=requested,
            config=config,
            **extra,
        )

    # ── Consent ──────────────────────────────────────────────────
    consent = load_mv_consent(spark, probe_id, catalog, schema)
    if not consent:
        return _skip(SKIP_NO_CONSENT_ROW)
    if str(consent.get("verdict") or "").upper() != CONSENT_VERDICT_SUFFICIENT:
        return _skip(SKIP_CONSENT_NOT_SUFFICIENT)
    if not consent.get("reverified_at_trigger"):
        return _skip(SKIP_CONSENT_NOT_REVERIFIED)
    granted_by = str(consent.get("granted_by") or "").strip().lower()
    if not granted_by:
        return _skip(SKIP_CREATOR_MISMATCH)

    # ── Created objects ──────────────────────────────────────────
    created_rows = load_mv_created_objects(
        spark, run_id, catalog, schema, status=CREATED_STATUS,
    )
    by_name = {
        str(row.get("full_name") or "").strip().lower(): row for row in created_rows
    }
    matched: list[dict[str, Any]] = []
    for identifier in identifiers:
        row = by_name.get(identifier.strip().lower())
        if row is None:
            logger.warning(
                "mv_attach: %s has no CREATED row for run %s — skipping the phase",
                identifier, run_id,
            )
            return _skip(SKIP_NO_CREATED_OBJECT)
        # MV-D24 narrow relaxation: a USER_CREATED row is a *verified*
        # bring-your-own registration — the backend asserted the object is a
        # metric view, recovered and validated its YAML, and recorded the
        # verifying user as created_by. That verification IS the consent
        # coverage this guard exists to require, so it does not need to match
        # the consent's granted_by. The guard still fires for OBO_CREATED rows
        # (and legacy NULL provenance), which is where a creator/consent
        # mismatch would signal an object the consent never authorized.
        provenance = str(row.get("provenance") or "").strip().upper()
        is_user_created = provenance == MV_PROVENANCE_USER_CREATED
        if (
            not is_user_created
            and str(row.get("created_by") or "").strip().lower() != granted_by
        ):
            logger.warning(
                "mv_attach: %s was created by a different identity than the "
                "consent's granted_by — skipping the phase", identifier,
            )
            return _skip(SKIP_CREATOR_MISMATCH)
        matched.append(row)

    suggestion_ids = tuple(str(row.get("suggestion_id") or "") for row in matched)

    # ── Measurability ────────────────────────────────────────────
    if not _baseline_usable(baseline_eval):
        return _skip(SKIP_BASELINE_UNUSABLE, suggestion_ids=suggestion_ids)
    if eval_runner is None:
        return _skip(SKIP_NO_EVAL_RUNNER, suggestion_ids=suggestion_ids)

    affected = _affected_question_ids(
        spark,
        space_id=space_id,
        catalog=catalog,
        schema=schema,
        suggestion_ids=suggestion_ids,
    )
    if not affected:
        return _skip(SKIP_NO_AFFECTED_QUESTIONS, suggestion_ids=suggestion_ids)

    baseline_run = _eval_result_from_output(baseline_eval)
    baseline_eval_run_id = baseline_run.eval_run_id

    # ── Attach ───────────────────────────────────────────────────
    # force_apply because the type is HIGH_RISK and would otherwise be queued
    # rather than applied. Forcing is correct here and only here: the consent row
    # validated above *is* the human approval that force_apply normally lacks.
    apply_log = apply_patch_set(
        w,
        space_id,
        _attach_patches(identifiers),
        config,
        apply_mode=apply_mode,
        force_apply=True,
        benchmark_corpus=benchmark_corpus,
    )
    applied = list(apply_log.get("applied") or [])
    if not apply_log.get("patch_deployed") or not applied:
        return _skip(
            SKIP_ATTACH_NOT_APPLIED,
            suggestion_ids=suggestion_ids,
            baseline_eval_run_id=baseline_eval_run_id,
            affected_question_count=len(affected),
        )

    attached_config = apply_log.get("post_snapshot") or config
    attached = tuple(
        str(entry.get("action", {}).get("target") or "") for entry in applied
    )
    patch_reference = _attach_patch_reference(run_id, 0)
    _write_attach_patch_rows(
        spark, applied, run_id=run_id, catalog=catalog, schema=schema,
    )
    for suggestion_id in suggestion_ids:
        _update_object(
            spark,
            run_id=run_id,
            suggestion_id=suggestion_id,
            catalog=catalog,
            schema=schema,
            status=CREATED_STATUS,
            attach_patch_id=patch_reference,
            baseline_eval_run_id=baseline_eval_run_id,
        )

    # ── Lift ─────────────────────────────────────────────────────
    lift_run = eval_runner.run_subset(space_id, affected, LIFT_EVAL_LABEL)
    if not lift_run.succeeded:
        return _detach(
            spark,
            apply_log=apply_log,
            w=w,
            space_id=space_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
            config=config,
            outcome=AttachOutcome(
                status=STATUS_SKIPPED,
                skip_reason=SKIP_LIFT_EVAL_UNUSABLE,
                requested=requested,
                suggestion_ids=suggestion_ids,
                attach_patch_id=patch_reference,
                baseline_eval_run_id=baseline_eval_run_id,
                lift_eval_run_id=lift_run.eval_run_id or None,
                affected_question_count=len(affected),
            ),
            suggestion_ids=suggestion_ids,
            lift_report_json=None,
            status=CREATED_STATUS,
        )

    report = lift_report(baseline_run, lift_run, affected)
    report_json = json.dumps(report.to_dict(), default=str)
    base = AttachOutcome(
        status=STATUS_COMPLETE,
        requested=requested,
        suggestion_ids=suggestion_ids,
        attach_patch_id=patch_reference,
        baseline_eval_run_id=baseline_eval_run_id,
        lift_eval_run_id=lift_run.eval_run_id,
        affected_question_count=len(affected),
        delta_affected=report.delta_affected,
        delta_suite=report.delta_suite,
        regressed_question_count=len(report.regressed_question_ids),
    )

    if is_regression(report):
        return _detach(
            spark,
            apply_log=apply_log,
            w=w,
            space_id=space_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
            config=config,
            outcome=base,
            suggestion_ids=suggestion_ids,
            lift_report_json=report_json,
            status=VERDICT_DETACHED,
        )

    for suggestion_id in suggestion_ids:
        _update_object(
            spark,
            run_id=run_id,
            suggestion_id=suggestion_id,
            catalog=catalog,
            schema=schema,
            status=VERDICT_ATTACHED,
            attach_patch_id=patch_reference,
            baseline_eval_run_id=baseline_eval_run_id,
            post_attach_eval_run_id=lift_run.eval_run_id,
            lift_report_json=report_json,
        )
    # MV-D18. Iteration 0's observed config was committed before this phase ran,
    # and it becomes the champion whenever no lever attempt is accepted — the case
    # where a champion revert would otherwise strip a view that just proved
    # itself. The submitted config_json stays pre-attach: the baseline score
    # really was measured without the view.
    update_iteration_observed_config(
        spark,
        run_id,
        _ATTACH_ITERATION,
        catalog=catalog,
        schema=schema,
        observed_config_snapshot=attached_config,
        eval_scope=FULL,
    )
    return replace(
        base,
        verdict=VERDICT_ATTACHED,
        attached=attached,
        config=attached_config,
    )


def _detach(
    spark: SparkSession,
    *,
    apply_log: dict[str, Any],
    w: Any,
    space_id: str,
    run_id: str,
    catalog: str,
    schema: str,
    config: dict[str, Any],
    outcome: AttachOutcome,
    suggestion_ids: Sequence[str],
    lift_report_json: str | None,
    status: str,
) -> AttachOutcome:
    """Restore the pre-attach snapshot and record the verdict.

    Detach is a whole-snapshot revert through ``applier.rollback`` — the same
    primitive the loop's own accept/reject gate uses. ``integration/revert.py`` is
    a backend surface whose active-run guard rejects mid-run by design (MV-D16).
    The UC object is never dropped here, whatever the verdict.
    """
    result = rollback(apply_log, w, space_id)
    if result.get("status") == "error":
        logger.warning(
            "mv_attach: detach failed for run %s — the metric view may still be "
            "attached: %s", run_id, result.get("errors"),
        )
    restored = result.get("restored_config") or apply_log.get("pre_snapshot") or config

    for suggestion_id in suggestion_ids:
        _update_object(
            spark,
            run_id=run_id,
            suggestion_id=suggestion_id,
            catalog=catalog,
            schema=schema,
            status=status,
            attach_patch_id=outcome.attach_patch_id,
            baseline_eval_run_id=outcome.baseline_eval_run_id,
            post_attach_eval_run_id=outcome.lift_eval_run_id,
            lift_report_json=lift_report_json,
        )

    return replace(
        outcome,
        verdict=VERDICT_DETACHED if status == VERDICT_DETACHED else None,
        attached=(),
        detached=tuple(outcome.requested),
        config=restored,
    )


def _write_attach_patch_rows(
    spark: SparkSession,
    applied: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    catalog: str,
    schema: str,
) -> None:
    """Persist the attach into the patch audit trail. Best-effort by design.

    A missing audit row must not cost an attach that already happened, so this
    logs and continues — the created-object row is the lifecycle record, and it is
    written on the path that matters.
    """
    for index, entry in enumerate(applied):
        patch = dict(entry.get("patch") or {})
        action = dict(entry.get("action") or {})
        try:
            write_patch(
                spark,
                run_id,
                _ATTACH_ITERATION,
                _MV_LEVER,
                index,
                {
                    "patch_type": MV_ATTACH_PATCH_TYPE,
                    "scope": "genie_config",
                    "risk_level": action.get("risk_level", "high"),
                    "target_object": action.get("target", patch.get("target", "")),
                    "patch": patch,
                    "command": action.get("command"),
                    "rollback": action.get("rollback_command"),
                    "proposal_id": patch.get("proposal_id", ""),
                    "applied_patch_type": MV_ATTACH_PATCH_TYPE,
                    "patch_family": "mv_attach",
                },
                catalog,
                schema,
            )
        except Exception:
            logger.warning(
                "mv_attach: could not write the patch audit row for %s",
                action.get("target", "?"), exc_info=True,
            )


def _update_object(
    spark: SparkSession,
    *,
    run_id: str,
    suggestion_id: str,
    catalog: str,
    schema: str,
    status: str,
    attach_patch_id: str | None = None,
    baseline_eval_run_id: str | None = None,
    post_attach_eval_run_id: str | None = None,
    lift_report_json: str | None = None,
) -> None:
    if not suggestion_id:
        return
    try:
        update_mv_created_object_status(
            spark,
            catalog=catalog,
            schema=schema,
            run_id=run_id,
            suggestion_id=suggestion_id,
            status=status,
            attach_patch_id=attach_patch_id,
            baseline_eval_run_id=baseline_eval_run_id,
            post_attach_eval_run_id=post_attach_eval_run_id,
            lift_report_json=lift_report_json,
        )
    except Exception:
        logger.warning(
            "mv_attach: could not record status %s for suggestion %s",
            status, suggestion_id, exc_info=True,
        )


# ── End-of-run reconciliation (MV-D18) ───────────────────────────────────


RECONCILE_PHASE_NAME = f"{MV_ATTACH_PHASE_NAME}_reconcile"

RECONCILE_DEMOTION_REASON = "NOT_IN_FINAL_CONFIG"


def attached_identifiers(config: Mapping[str, Any] | None) -> set[str]:
    """The identifiers on ``data_sources.metric_views``, lowercased.

    Reads the same shelf the applier writes (``applier._apply_action_to_config``'s
    ``metric_views`` branch), so "is it attached" is answered by the config itself
    rather than by a status column claiming to describe it.
    """
    if not isinstance(config, Mapping):
        return set()
    sources = config.get("data_sources")
    if not isinstance(sources, Mapping):
        return set()
    out: set[str] = set()
    for entry in sources.get("metric_views") or ():
        if isinstance(entry, Mapping):
            identifier = str(entry.get("identifier") or "").strip().lower()
            if identifier:
                out.add(identifier)
    return out


def reconcile_attached_objects(
    spark: SparkSession,
    *,
    run_id: str,
    catalog: str,
    schema: str,
    config: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Make ``genie_opt_mv_created_objects.status`` true of the final config.

    An ``ATTACHED`` row pointing at a space that no longer references the view is
    worse than no row at all: Prompt 9's re-run flow and Prompt 13's UI both read
    this column and would offer to keep, or claim credit for, an attachment that
    is not there. So the run does not end on an unverified status — every
    ``ATTACHED`` row is checked against the configuration the run actually left
    behind, and one that fails is demoted to ``DETACHED``.

    Demote rather than delete, and never touch the UC object: the object was
    created under a consent and is still there. ``DETACHED`` is the true statement
    about it. Verified rows are left completely alone so this is idempotent across
    the loop's several exits.

    **Demote-only, and never promote** (MV-D18). The one status this writes is
    ``DETACHED``, and only ``ATTACHED`` rows are considered — a ``CREATED`` or
    ``DETACHED`` row is skipped even when its identifier IS on the final config.
    That asymmetry is the point: the presence of an identifier proves only that
    something put it there, not that it came through the consent gate, so
    promoting on that evidence would let an attach that bypassed MV-D1 acquire a
    legitimate-looking ``ATTACHED`` status. ``ATTACHED`` is written in exactly one
    place — the attach phase, after it has checked the consent row and the created
    object — and reconciliation is a truth check on that claim, not a second way
    to make it. The status filter is applied twice, on the read and again per row,
    so the property survives an edit to either.

    Never raises. Returns the counts for the caller's diagnostic.
    """
    result: dict[str, Any] = {"checked": 0, "verified": 0, "demoted": 0, "identifiers": []}
    try:
        rows = load_mv_created_objects(
            spark, run_id, catalog, schema, status=VERDICT_ATTACHED,
        )
    except Exception:
        logger.warning(
            "mv_attach: could not read created objects to reconcile run %s",
            run_id, exc_info=True,
        )
        return result

    live = attached_identifiers(config)
    demoted: list[str] = []
    for row in rows:
        # Second half of the demote-only property. The read above already filters
        # to ATTACHED; re-checking here means a future edit that widens or drops
        # that filter cannot turn this into a promotion path.
        if str(row.get("status") or "").strip().upper() != VERDICT_ATTACHED:
            continue
        result["checked"] += 1
        full_name = str(row.get("full_name") or "").strip()
        if full_name.lower() in live:
            result["verified"] += 1
            continue
        demoted.append(full_name)
        logger.warning(
            "mv_attach: %s is recorded ATTACHED but is not on the final config for "
            "run %s — demoting to DETACHED", full_name or "?", run_id,
        )
        _update_object(
            spark,
            run_id=run_id,
            suggestion_id=str(row.get("suggestion_id") or ""),
            catalog=catalog,
            schema=schema,
            status=VERDICT_DETACHED,
        )
    result["demoted"] = len(demoted)
    result["identifiers"] = demoted

    if result["checked"]:
        try:
            write_stage(
                spark,
                run_id,
                RECONCILE_PHASE_NAME.upper(),
                STATUS_COMPLETE,
                task_key="optimize",
                catalog=catalog,
                schema=schema,
                detail={
                    "phase": RECONCILE_PHASE_NAME,
                    "checked": result["checked"],
                    "verified": result["verified"],
                    "demoted": result["demoted"],
                    "demoted_identifiers": demoted,
                    "reason": RECONCILE_DEMOTION_REASON if demoted else None,
                },
            )
        except Exception:
            logger.warning(
                "mv_attach: could not write the reconciliation stage row", exc_info=True,
            )
    return result


def _record(
    spark: SparkSession,
    outcome: AttachOutcome,
    *,
    run_id: str,
    catalog: str,
    schema: str,
) -> AttachOutcome:
    try:
        write_stage(
            spark,
            run_id,
            MV_ATTACH_PHASE_NAME.upper(),
            outcome.status,
            task_key="optimize",
            catalog=catalog,
            schema=schema,
            detail=outcome.detail(),
            error_message=outcome.error,
        )
    except Exception:
        logger.warning("mv_attach: could not write the phase stage row", exc_info=True)
    return outcome
