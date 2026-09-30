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

The eval shape (MV-D114 d3-d5)
------------------------------
The lift is one full-suite eval of the attached space, supplied by the caller as
``post_attach_eval``. The verdict is scored on the affected subset only, and a
regression counts only when it lands inside that subset. A subset with no
question graded on both sides is no measurement, so the attach is reverted. A
kept attach hands that full eval back, since it is the measurement of the space
the levers then tune.

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

import copy
import json
import logging
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    MV_ATTACH_PHASE_NAME,
    MV_PROVENANCE_USER_CREATED,
)

from .applier import apply_patch_set, rollback
from .champion import BaselineReset
from .eval_runner import FULL, EvalRunResult, LiftReport, lift_report
from .mv_advisor import _generated_sql_of
from .mv_fingerprint import extract_measures
from .mv_state import (
    load_mv_candidates,
    load_mv_consent,
    load_mv_created_objects,
    mv_candidate_fingerprint,
    update_mv_created_object_status,
)
from .state import (
    load_stages,
    update_iteration_observed_config,
    write_patch,
    write_stage,
)

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
"""No live benchmark question is recorded on the proposal or uses one of its
measures (MV-D114 d1/d2).

The lift verdict scores the affected subset, so an empty subset is not a smaller
measurement — it is no measurement, and the attach does not proceed without one.
"""

SKIP_NO_EVAL_RUNNER = "NO_EVAL_RUNNER"
"""No post-attach eval was supplied. Same reasoning as an unusable baseline: the
attach is only permitted where its effect can be measured."""

SKIP_ATTACH_NOT_APPLIED = "ATTACH_NOT_APPLIED"
"""``apply_patch_set`` deployed nothing — e.g. every identifier was already on
``data_sources.metric_views``, or the config PATCH failed."""

SKIP_LIFT_EVAL_UNUSABLE = "LIFT_EVAL_UNUSABLE"
"""The lift eval did not reach a gradeable state. The attach is reverted rather
than left in place unmeasured, and the objects stay ``CREATED``."""

SKIP_LIFT_NOT_GRADED = "LIFT_NOT_GRADED"
"""The post-attach eval was usable, but no affected question was graded in both
runs — each one was needs-review on a side or absent from one (MV-D114 d5).

A delta over zero graded questions is 0.0 by construction, not a measured wash,
so the attach is reverted like an unusable eval and the objects stay ``CREATED``.
"""

VERDICT_ATTACHED = "ATTACHED"
VERDICT_DETACHED = "DETACHED"

ROLLBACK_REVERTED = "reverted"
ROLLBACK_FAILED = "failed"

_REVERT_ATTEMPTS = 2
"""A revert is retried once. A raised exception counts as an attempt, so a
flaky PATCH gets a second chance without an unbounded loop inside the task."""

CONSENT_VERDICT_SUFFICIENT = "SUFFICIENT"
CREATED_STATUS = "CREATED"

MV_ATTACH_PATCH_TYPE = "mv_attach_data_source"

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

    ``post_attach_eval`` is the full-suite eval output of a kept attach, handed
    back so the caller can carry it as the new baseline; it is ``None`` on every
    other path. It is excluded from ``detail()`` and from ``repr`` for the same
    reason as ``config`` and one more: it carries per-question rows, SQL
    included, and neither a stage row nor a log line may.
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
    graded_affected_count: int = 0
    post_attach_accuracy: float | None = None
    rollback_status: str | None = None
    rollback_error: str | None = None
    post_attach_eval: dict[str, Any] | None = field(default=None, repr=False)

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
            "graded_affected_count": self.graded_affected_count,
            "post_attach_accuracy": self.post_attach_accuracy,
            "rollback_status": self.rollback_status,
            "rollback_error": self.rollback_error,
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


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _subset_regressions(report: LiftReport) -> list[str]:
    """The regressed questions that are inside the affected subset, in report order."""
    subset = set(report.question_subset)
    return [qid for qid in report.regressed_question_ids if qid in subset]


def is_regression(report: LiftReport) -> bool:
    """Whether the lift verdict requires a detach.

    A negative delta on the affected questions is a regression by definition. A
    delta of exactly zero *with* regressed questions is also one: the view broke
    specific answers and paid for them with unrelated luck elsewhere, which is not
    a reason to keep a structural change. A positive delta stands even if one
    question moved the wrong way — that is the trade the measurement exists to
    quantify, and the report is persisted so a reviewer sees both halves.

    MV-D114 d4: only a regression inside the affected subset counts. The post
    run is the full suite, so its regressed list also carries questions the view
    does not touch, and those are eval noise rather than evidence against the
    view. When the post run covers only the subset, this is the same rule as
    before.
    """
    if report.delta_affected < 0:
        return True
    return report.delta_affected == 0 and bool(_subset_regressions(report))


def _live_question_ids(baseline_eval: Mapping[str, Any]) -> list[str]:
    """MV-D114 d1: the question ids iteration-0's own eval graded, in its order."""
    out: list[str] = []
    for row in baseline_eval.get("rows") or ():
        if isinstance(row, Mapping):
            qid = str(row.get("question_id") or "").strip()
            if qid and qid not in out:
                out.append(qid)
    return out


def _expected_sql_of(row: Mapping[str, Any]) -> str:
    for key in ("expected_sql", "inputs/expected_response"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _measure_matched_ids(
    rows: Sequence[Mapping[str, Any]], *, space_id: str, fingerprints: set[str],
) -> set[str]:
    """Baseline questions whose generated or expected SQL uses a member measure.

    The fingerprint is the advisor's own (mv_advisor.py:1503-1505), so "uses this
    measure" means exactly what it meant when the view was proposed. Expected SQL
    is read here to choose ids and nothing else — no SQL leaves this function.
    """
    out: set[str] = set()
    if not fingerprints:
        return out
    for row in rows:
        qid = str(row.get("question_id") or "").strip()
        if not qid:
            continue
        for sql in (_generated_sql_of(row), _expected_sql_of(row)):
            if not sql:
                continue
            try:
                measures = extract_measures(sql)
            except Exception:  # noqa: BLE001, S112 - unlogged: the error may quote the SQL
                continue
            if any(
                m.canonical_expr
                and mv_candidate_fingerprint(space_id, m.canonical_expr, m.source_tables)
                in fingerprints
                for m in measures
            ):
                out.add(qid)
                break
    return out


def _affected_question_ids(
    spark: SparkSession,
    *,
    space_id: str,
    catalog: str,
    schema: str,
    suggestion_ids: Sequence[str],
    baseline_eval: Mapping[str, Any],
) -> list[str]:
    """The live benchmark questions the proposals these objects came from affect.

    Candidates are space-scoped and outlive the run that proposed them (MV-D7),
    which is exactly why this reads by ``target_space_id``: under MV-D1 the
    proposal was written by an earlier run than the one attaching it.

    MV-D114 d1: the subset is drawn only from the ids iteration-0's eval actually
    graded. A recorded id that is not live — a curated-provenance handle such as
    ``trusted_asset:…`` or ``sql_snippet:…``, or a question retired since the
    proposing run — cannot be measured, so it is dropped rather than scored.

    MV-D114 d2: a view is measured on every member it bundles, not the anchor
    alone — the bundle's member-union ids and each member's own ids count — and
    on any live question whose baseline generated or expected SQL uses one of
    its measures. The measure match is what gives a curated (IQ-scan) candidate,
    which records no benchmark question at all, a subset to be measured on.
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

    recorded: set[str] = set()
    fingerprints: set[str] = set()
    for candidate in candidates:
        if str(candidate.get("suggestion_id") or "") not in wanted:
            continue
        fp = str(candidate.get("dedup_fingerprint") or "").strip()
        if fp:
            fingerprints.add(fp)
        evidence = candidate.get("evidence")
        if not isinstance(evidence, Mapping):
            continue
        for key in ("benchmark_question_ids", "benchmark_questions"):
            recorded.update(str(q).strip() for q in evidence.get(key) or () if str(q).strip())
        for member in evidence.get("measures") or ():
            if isinstance(member, Mapping):
                member_fp = str(member.get("dedup_fingerprint") or "").strip()
                if member_fp:
                    fingerprints.add(member_fp)
                recorded.update(
                    str(q).strip() for q in member.get("benchmark_question_ids") or () if str(q).strip()
                )
    rows = [r for r in baseline_eval.get("rows") or () if isinstance(r, Mapping)]
    matched = _measure_matched_ids(rows, space_id=space_id, fingerprints=fingerprints)
    return [qid for qid in _live_question_ids(baseline_eval) if qid in recorded or qid in matched]


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
    post_attach_eval: Callable[[], Mapping[str, Any]] | None = None,
    apply_mode: str = "genie_config",
    benchmark_corpus: Any = None,
) -> AttachOutcome:
    """Attach consented metric views, measure the lift, detach on regression.

    ``post_attach_eval`` runs one full-suite eval of the space as it stands and
    returns the loop's eval-output dict (the shape ``baseline_eval`` has). It is
    called once, after the attach is applied (MV-D114 d3).

    **Never raises an Exception.** Total isolation is the contract: this is an
    addition to a task whose job is optimization, so any failure of its own must
    cost its own output and nothing else. The returned ``config`` is what the
    caller carries forward, so a failed phase leaves the loop running against the
    configuration it already had. A BaseException (interrupt, cancellation) after
    the attach deployed is not swallowed: the attach is reverted and its objects
    written back to ``CREATED`` first, then it propagates.
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
            post_attach_eval=post_attach_eval,
            apply_mode=apply_mode,
            benchmark_corpus=benchmark_corpus,
        )
    # Pre-deploy failures only: _attach_and_measure reverts every post-deploy one.
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
    post_attach_eval: Callable[[], Mapping[str, Any]] | None,
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
    if post_attach_eval is None:
        return _skip(SKIP_NO_EVAL_RUNNER, suggestion_ids=suggestion_ids)

    affected = _affected_question_ids(
        spark,
        space_id=space_id,
        catalog=catalog,
        schema=schema,
        suggestion_ids=suggestion_ids,
        baseline_eval=baseline_eval,
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

    # MV-D114 d6: from here the view is live on the space, so every exit either
    # settles through the lift verdict or reverts.
    patch_reference = _attach_patch_reference(run_id, 0)
    attached_config = apply_log.get("post_snapshot") or config
    settled = False
    try:
        outcome = _measure_attached(
            spark,
            run_id=run_id,
            space_id=space_id,
            catalog=catalog,
            schema=schema,
            config=config,
            w=w,
            post_attach_eval=post_attach_eval,
            requested=requested,
            suggestion_ids=suggestion_ids,
            affected=affected,
            baseline_run=baseline_run,
            apply_log=apply_log,
            applied=applied,
            attached_config=attached_config,
            patch_reference=patch_reference,
        )
        settled = True
        return outcome
    except Exception as exc:
        logger.warning(
            "mv_attach: failed after the attach deployed for run %s; reverting",
            run_id, exc_info=True,
        )
        outcome = _detach(
            spark,
            apply_log=apply_log,
            w=w,
            space_id=space_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
            config=config,
            outcome=AttachOutcome(
                status=STATUS_FAILED,
                error=f"{type(exc).__name__}: {exc}",
                requested=requested,
                suggestion_ids=suggestion_ids,
                attach_patch_id=patch_reference,
                baseline_eval_run_id=baseline_eval_run_id,
                affected_question_count=len(affected),
            ),
            suggestion_ids=suggestion_ids,
            lift_report_json=None,
            status=CREATED_STATUS,
        )
        settled = True
        return outcome
    finally:
        if not settled:
            # BaseException (interrupt, cancellation): revert best-effort, then let it
            # propagate. The stage row is not written — the task is going down.
            _revert(apply_log, w, space_id)
            # The kept path may already have written ATTACHED, and end-of-run
            # reconciliation never runs in a task going down. Reverted or not,
            # the row goes back to CREATED: not attached, may still be live.
            try:
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
            except Exception:  # noqa: BLE001 - best-effort while the task goes down
                logger.warning(
                    "mv_attach: could not demote created objects to CREATED for run %s",
                    run_id,
                )


def _measure_attached(
    spark: SparkSession,
    *,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    config: dict[str, Any],
    w: Any,
    post_attach_eval: Callable[[], Mapping[str, Any]],
    requested: tuple[str, ...],
    suggestion_ids: tuple[str, ...],
    affected: Sequence[str],
    baseline_run: EvalRunResult,
    apply_log: dict[str, Any],
    applied: Sequence[Mapping[str, Any]],
    attached_config: dict[str, Any],
    patch_reference: str,
) -> AttachOutcome:
    """Record the attach, measure its lift, and keep or detach it.

    Runs only once the attach has deployed, and only under
    ``_attach_and_measure``'s guard: anything this raises is reverted there.
    """
    baseline_eval_run_id = baseline_run.eval_run_id
    attached = tuple(
        str(entry.get("action", {}).get("target") or "") for entry in applied
    )
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
    # MV-D114 d3: one full-suite eval of the attached space. The verdict below
    # reads the affected subset out of it (d4).
    post_output = dict(post_attach_eval())
    if not _baseline_usable(post_output):
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
                lift_eval_run_id=str(post_output.get("eval_run_id") or "") or None,
                affected_question_count=len(affected),
            ),
            suggestion_ids=suggestion_ids,
            lift_report_json=None,
            status=CREATED_STATUS,
        )

    lift_run = _eval_result_from_output(post_output)
    report = lift_report(baseline_run, lift_run, affected)
    report_json = json.dumps(report.to_dict(), default=str)

    # MV-D114 d5: zero graded affected questions is no measurement at all.
    if report.graded_affected_count == 0:
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
                skip_reason=SKIP_LIFT_NOT_GRADED,
                requested=requested,
                suggestion_ids=suggestion_ids,
                attach_patch_id=patch_reference,
                baseline_eval_run_id=baseline_eval_run_id,
                lift_eval_run_id=lift_run.eval_run_id or None,
                affected_question_count=len(affected),
                graded_affected_count=0,
            ),
            suggestion_ids=suggestion_ids,
            lift_report_json=report_json,
            status=CREATED_STATUS,
        )

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
        regressed_question_count=len(_subset_regressions(report)),
        graded_affected_count=report.graded_affected_count,
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
        post_attach_eval=post_output,
        post_attach_accuracy=_as_float(post_output.get("overall_accuracy")),
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

    A revert that fails every attempt is reported, not papered over (MV-D114
    d6). The outcome is ``FAILED`` with ``rollback_status`` ``failed``, and
    nothing is claimed detached. The object row is written ``CREATED`` whatever
    status was asked for: under MV-D18 ``CREATED`` means "not attached by a kept
    lift, may still be live", which is the true statement when the PATCH did not
    land, whereas ``DETACHED`` would assert a removal that never happened. The
    lift's verdict is kept, since the measurement stands even though acting on
    it failed. The pre-attach config is still what the caller carries forward:
    the view was rejected or never measured, so the loop must not tune on top of
    it, and any later deploy of the carried config does not re-send it.
    """
    reverted, errors = _revert(apply_log, w, space_id)
    verdict = VERDICT_DETACHED if status == VERDICT_DETACHED else outcome.verdict
    pre_attach = copy.deepcopy(apply_log.get("pre_snapshot") or config)

    if not reverted:
        logger.error(
            "mv_attach: revert failed after %d attempts for run %s on space %s; "
            "the metric view(s) %s may still be attached",
            _REVERT_ATTEMPTS, run_id, space_id, ", ".join(outcome.requested),
        )
        for suggestion_id in suggestion_ids:
            _update_object(
                spark,
                run_id=run_id,
                suggestion_id=suggestion_id,
                catalog=catalog,
                schema=schema,
                status=CREATED_STATUS,
                attach_patch_id=outcome.attach_patch_id,
                baseline_eval_run_id=outcome.baseline_eval_run_id,
                post_attach_eval_run_id=outcome.lift_eval_run_id,
                lift_report_json=lift_report_json,
            )
        return replace(
            outcome,
            status=STATUS_FAILED,
            error=_rollback_error_text(outcome.error, errors),
            verdict=verdict,
            rollback_status=ROLLBACK_FAILED,
            rollback_error="; ".join(errors),
            attached=(),
            detached=(),
            config=pre_attach,
        )

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
        rollback_status=ROLLBACK_REVERTED,
        attached=(),
        detached=tuple(outcome.requested),
        config=pre_attach,
    )


def _revert(apply_log: dict[str, Any], w: Any, space_id: str) -> tuple[bool, list[str]]:
    """Restore the pre-attach snapshot, up to ``_REVERT_ATTEMPTS`` times.

    Returns ``(True, [])`` on the first attempt that does not report an error,
    else ``(False, errors)``: the distinct error messages across every attempt.
    ``applier.rollback`` reports fixed messages or API exception text, never the
    config it tried to write.
    """
    errors: list[str] = []
    for _attempt in range(_REVERT_ATTEMPTS):
        try:
            result = rollback(apply_log, w, space_id)
        except Exception as exc:  # noqa: BLE001 - a raised revert is a failed attempt
            messages = [f"{type(exc).__name__}: {exc}"]
        else:
            if result.get("status") != "error":
                return True, []
            messages = [str(e) for e in result.get("errors") or ()] or ["rollback reported an error"]
        for message in messages:
            if message not in errors:
                errors.append(message)
    return False, errors


def _rollback_error_text(original: str | None, errors: Sequence[str]) -> str:
    text = f"ROLLBACK_FAILED: {'; '.join(errors)}"
    return f"{text} (after {original})" if original else text


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
    """The identifiers on ``data_sources.metric_views`` and ``data_sources.tables``, lowercased.

    The applier writes ``metric_views`` (``applier._apply_action_to_config``), but
    Genie moves a metric view to ``tables`` on write and exports it there, so a
    config read back from the space carries it under ``tables``. Both shelves count,
    as in the backend's attach-at-approval (``backend/services/mv_create.py``). Callers
    match only views they created, and a Unity Catalog name is one object, so a
    ``tables`` match is that view. "Is it attached" is answered by the config itself
    rather than by a status column claiming to describe it.
    """
    if not isinstance(config, Mapping):
        return set()
    sources = config.get("data_sources")
    if not isinstance(sources, Mapping):
        return set()
    out: set[str] = set()
    for key in ("metric_views", "tables"):
        for entry in sources.get(key) or ():
            if isinstance(entry, Mapping):
                identifier = str(entry.get("identifier") or "").strip().lower()
                if identifier:
                    out.add(identifier)
    return out


def kept_attach_baseline_reset(
    spark: SparkSession, run_id: str, catalog: str, schema: str,
) -> BaselineReset | None:
    """The re-baseline a kept attach gave the loop, or ``None``.

    Read from the run's latest ``MV_ATTACH`` stage row, and only when its verdict
    is ``ATTACHED`` (MV-D114 d7). Publish scores iteration 0 at its post-attach
    accuracy, because the stored iteration-0 row keeps its pre-attach score
    (MV-D18). The reset names the baseline eval it was measured against, so a
    restarted task's own iteration-0 row, whose phase never ran or skipped, keeps
    its score. Never raises.
    """
    try:
        stages = load_stages(spark, run_id, catalog, schema)
        records = stages.to_dict("records") if hasattr(stages, "to_dict") else []
    except Exception as exc:  # noqa: BLE001 - publish falls back to the stored rows
        logger.warning(
            "mv_attach: could not read the attach stage for run %s (%s)",
            run_id, type(exc).__name__,
        )
        return None
    if not isinstance(records, list):
        return None
    stage_name = MV_ATTACH_PHASE_NAME.upper()
    details: list[Mapping[str, Any]] = []
    for row in records:
        if not isinstance(row, Mapping) or str(row.get("stage") or "").upper() != stage_name:
            continue
        detail = row.get("detail_json")
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except (TypeError, ValueError):
                continue
        if isinstance(detail, Mapping):
            details.append(detail)
    if not details:
        return None
    latest = details[-1]
    if str(latest.get("verdict") or "").upper() != VERDICT_ATTACHED:
        return None
    accuracy = _as_float(latest.get("post_attach_accuracy"))
    if accuracy is None:
        return None
    return BaselineReset(str(latest.get("baseline_eval_run_id") or ""), accuracy)


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


UNMEASURED_PHASE_NAME = f"{MV_ATTACH_PHASE_NAME}_unmeasured"

UNMEASURED_REASON = "CREATED_BUT_LIVE"


def report_unmeasured_attachments(
    spark: SparkSession,
    *,
    run_id: str,
    catalog: str,
    schema: str,
    config: Mapping[str, Any] | None,
    live_config: Callable[[], Mapping[str, Any] | None] | None = None,
) -> list[str]:
    """Name every view this run attached, never kept, and still leaves live.

    A revert that fails twice (MV-D114 d6) records the object ``CREATED`` with
    its ``attach_patch_id``, while the space may still carry the view. That row
    is not a kept lift, so it is not ``ATTACHED``, and ``reconcile_attached_objects``
    never looks at it. If the space still carries the identifier, the run ends
    with an unmeasured view in the space, and this is where that is said.

    The check is against the live space config, read through ``live_config``.
    The in-memory ``config`` cannot answer it: after a failed revert the phase
    hands the loop the pre-attach config, so memory never carries the view the
    space may still hold. ``live_config`` is called only when a patched
    ``CREATED`` row exists, and when it is absent, raises or returns nothing,
    the report falls back to ``config``.

    **Report only.** It never promotes the row (MV-D18 is demote-only:
    ``ATTACHED`` is written by the attach phase alone) and never drops the UC
    object (``DETACH_ONLY_NEVER_DROP``). It writes no status. It uses its own
    stage name so ``reconcile_attached_objects``'s return shape and stage row
    stay as they are. A ``CREATED`` row without an ``attach_patch_id`` was never
    attached by this run, so a matching identifier is not this run's to flag.

    Never raises. Returns the full names it reported.
    """
    try:
        rows = load_mv_created_objects(
            spark, run_id, catalog, schema, status=CREATED_STATUS,
        )
    except Exception:
        logger.warning(
            "mv_attach: could not read created objects to report unmeasured "
            "attachments for run %s", run_id, exc_info=True,
        )
        return []

    patched = [
        row for row in rows
        if str(row.get("status") or "").strip().upper() == CREATED_STATUS
        and str(row.get("attach_patch_id") or "").strip()
    ]
    if not patched:
        return []

    observed: Mapping[str, Any] | None = None
    if live_config is not None:
        try:
            observed = live_config()
        except Exception as exc:  # noqa: BLE001 - the fallback is the in-memory config
            logger.warning(
                "mv_attach: could not read the live space config for run %s (%s); "
                "reporting against the in-memory config",
                run_id, type(exc).__name__,
            )
    on_config = attached_identifiers(
        observed if isinstance(observed, Mapping) else config,
    )
    live: list[str] = []
    for row in patched:
        full_name = str(row.get("full_name") or "").strip()
        if full_name and full_name.lower() in on_config:
            live.append(full_name)

    if not live:
        return live

    logger.warning(
        "mv_attach: %s recorded CREATED after an unreverted attach and still on the "
        "space config for run %s — the view may be live without a measured lift",
        ", ".join(live), run_id,
    )
    try:
        write_stage(
            spark,
            run_id,
            UNMEASURED_PHASE_NAME.upper(),
            STATUS_COMPLETE,
            task_key="optimize",
            catalog=catalog,
            schema=schema,
            detail={
                "phase": UNMEASURED_PHASE_NAME,
                "identifiers": live,
                "reason": UNMEASURED_REASON,
            },
        )
    except Exception:
        logger.warning(
            "mv_attach: could not write the unmeasured-attachment stage row",
            exc_info=True,
        )
    return live


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
