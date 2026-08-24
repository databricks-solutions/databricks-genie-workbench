"""Delta persistence for the metric view advisor (MV-D7).

Three stateful stores, all registered in ``ddl._ALL_DDL`` and created by
``state.ensure_optimization_tables``:

- ``genie_opt_mv_candidates`` — advisor proposals, keyed on
  ``(target_space_id, dedup_fingerprint)``. A candidate outlives the run that
  proposed it, because ``create_and_attach`` acts on proposals a user approved
  from an earlier run (MV-D1's two-run consent model).
- ``genie_opt_mv_consents`` — entitlement probes and the consent decisions taken
  against them, keyed on ``probe_id``. Written before any run exists.
- ``genie_opt_mv_created_objects`` — Unity Catalog metric views the backend
  created under OBO, keyed on ``(run_id, suggestion_id)``, with a status that
  mutates over the object's life.

They are stateful entities rather than stage-handoff blobs, which is why they
are tables and not ``genie_opt_artifacts`` rows. The rendered DDL *text* for a
candidate does live in ``genie_opt_artifacts``, with ``content_hash`` set to the
candidate's ``dedup_fingerprint`` so the two stores cross-reference.

Every writer upserts through :func:`~genie_space_optimizer.common.delta_helpers.merge_row`,
so a retried phase or a re-proposing run never duplicates a row. Accessors take
``spark``, ``catalog`` and ``schema`` explicitly and expose the POV Part 4 field
names (``score_components``, ``evidence``, ``provenance``, ``alternatives``,
``conflicts``, ``probe_results``); the ``*_json`` suffix is a storage detail.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    TABLE_MV_CANDIDATES,
    TABLE_MV_CONSENTS,
    TABLE_MV_CREATED_OBJECTS,
)
from genie_space_optimizer.common.delta_helpers import (
    _fqn,
    merge_row,
    read_table,
    run_query,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ── Vocabularies ─────────────────────────────────────────────────────────

MV_CANDIDATE_TYPES: tuple[str, ...] = (
    "NEW_METRIC_VIEW",
    "REPLACE_RAW_TABLE",
    "ADD_MEASURE",
    "CONFLICT",
)
"""POV Part 4 proposal ``type``. ``CONFLICT`` is emitted instead of a
suggestion when a fingerprint collides with an instruction or trusted asset
that defines the same concept differently — it is never auto-resolved."""

MV_CANDIDATE_DECISIONS: tuple[str, ...] = ("approved", "rejected")
"""Terminal human decisions. An undecided candidate carries ``decision`` NULL."""

MV_CONSENT_VERDICTS: tuple[str, ...] = ("SUFFICIENT", "INSUFFICIENT", "UNKNOWN")

MV_CREATED_OBJECT_STATUSES: tuple[str, ...] = (
    "CREATED",
    "ATTACHED",
    "DETACHED",
    "DROPPED",
)

MV_ON_REGRESSION_ACTIONS: tuple[str, ...] = (
    "DETACH_ONLY_NEVER_DROP",
    "SANDBOX_AUTO_DROP",
)
"""``SANDBOX_AUTO_DROP`` is legitimate only for a scratch schema that exists
solely for the run. Outside sandbox mode the UC object is never auto-dropped."""

_CANDIDATE_JSON_COLUMNS = frozenset({
    "score_components_json",
    "evidence_json",
    "provenance_json",
    "alternatives_json",
    "conflicts_json",
})

_CONSENT_JSON_COLUMNS = frozenset({"probe_results_json"})

_CREATED_OBJECT_JSON_COLUMNS = frozenset({"lift_report_json"})
"""Byte-preserving transport, not a decode list: readers get ``lift_report_json``
as the verbatim ``LiftReport.to_dict()`` string this module was handed."""


# ── Idempotency key ──────────────────────────────────────────────────────


def mv_candidate_fingerprint(
    space_id: str,
    canonical_measure_expr: str,
    source_set: Iterable[str],
) -> str:
    """Return the MV-D7 dedup key for a candidate.

    ``sha256(space_id | canonical_measure_expr | sorted_source_set)`` per POV
    §7.9. Sorting the source set makes the key insensitive to the order tables
    were discovered in, so the same measure over the same joins upserts onto one
    row no matter how the corpus scan walked it.

    ``canonical_measure_expr`` is taken as already canonical — normalizing the
    expression is the corpus scanner's job, and doing it twice in two places is
    how two components end up disagreeing about what the key is.
    """
    if not space_id:
        raise ValueError("space_id is required to fingerprint a candidate")
    if not canonical_measure_expr:
        raise ValueError("canonical_measure_expr is required to fingerprint a candidate")

    sources = sorted({str(s) for s in source_set if str(s)})
    material = "|".join([space_id, canonical_measure_expr, ",".join(sources)])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _opt_json(value: Any) -> str | None:
    """Serialize a JSON payload column, or ``None`` when there is nothing to store."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except (TypeError, ValueError):
        logger.warning("Dropping non-JSON-serializable metric view payload", exc_info=True)
        return None


def _parse_json_columns(row: dict[str, Any], columns: Iterable[str]) -> dict[str, Any]:
    """Decode ``*_json`` storage columns back to the POV Part 4 field names."""
    out = dict(row)
    for column in columns:
        raw = out.pop(column, None)
        field = column[: -len("_json")]
        if isinstance(raw, str) and raw.strip():
            try:
                out[field] = json.loads(raw)
            except (TypeError, ValueError):
                logger.warning("Invalid JSON in %s; surfacing raw text", column)
                out[field] = raw
        else:
            out[field] = raw
    return out


# ── Candidates ───────────────────────────────────────────────────────────


def upsert_mv_candidate(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    target_space_id: str,
    suggestion_id: str,
    dedup_fingerprint: str,
    candidate_type: str,
    confidence_score: float | None = None,
    tier: str | None = None,
    proposed_object: str | None = None,
    score_components: dict | None = None,
    evidence: dict | None = None,
    provenance: dict | None = None,
    alternatives: list | None = None,
    conflicts: list | None = None,
    requested_mode: str | None = None,
    effective_mode: str | None = None,
) -> str:
    """Upsert one advisor proposal; return its ``dedup_fingerprint``.

    Keyed on ``(target_space_id, dedup_fingerprint)``, so a later run that
    re-derives the same measure refreshes the existing candidate instead of
    duplicating it. Human decision columns are deliberately not written here —
    a re-proposing run must not resurrect a candidate the user rejected. Use
    :func:`record_mv_candidate_decision` for those.
    """
    if candidate_type not in MV_CANDIDATE_TYPES:
        raise ValueError(
            f"candidate_type must be one of {MV_CANDIDATE_TYPES}, got {candidate_type!r}"
        )
    if not dedup_fingerprint:
        raise ValueError("dedup_fingerprint is required")
    if not target_space_id:
        raise ValueError("target_space_id is required")

    now = _now()
    value_cols: dict[str, Any] = {
        "suggestion_id": suggestion_id,
        "run_id": run_id,
        "candidate_type": candidate_type,
        "confidence_score": float(confidence_score) if confidence_score is not None else None,
        "tier": tier,
        "proposed_object": proposed_object,
        "score_components_json": _opt_json(score_components),
        "evidence_json": _opt_json(evidence),
        "provenance_json": _opt_json(provenance),
        "alternatives_json": _opt_json(alternatives),
        "conflicts_json": _opt_json(conflicts),
        "requested_mode": requested_mode,
        "effective_mode": effective_mode,
        "updated_at": now,
    }

    merge_row(
        spark,
        catalog,
        schema,
        TABLE_MV_CANDIDATES,
        {"target_space_id": target_space_id, "dedup_fingerprint": dedup_fingerprint},
        value_cols,
        insert_only_cols={"created_at": now, "approved_for_rerun": False},
        base64_string_columns=set(_CANDIDATE_JSON_COLUMNS),
    )
    logger.info(
        "Upserted metric view candidate %s (%s, tier=%s) for space %s run %s",
        suggestion_id, candidate_type, tier, target_space_id, run_id,
    )
    return dedup_fingerprint


def record_mv_candidate_decision(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    target_space_id: str,
    dedup_fingerprint: str,
    decision: str,
    decided_by: str,
    suppressed_until: str | None = None,
    approved_for_rerun: bool | None = None,
) -> None:
    """Record a human approve/reject against one candidate.

    ``approved_for_rerun`` defaults to ``True`` for an approval and ``False``
    for a rejection — MV-D1 gates ``create_and_attach`` on that flag, so it
    tracks the decision rather than being set independently. A rejection should
    also carry ``suppressed_until`` so the advisor stops re-surfacing the
    fingerprint for its decay window.
    """
    if decision not in MV_CANDIDATE_DECISIONS:
        raise ValueError(
            f"decision must be one of {MV_CANDIDATE_DECISIONS}, got {decision!r}"
        )

    if approved_for_rerun is None:
        approved_for_rerun = decision == "approved"

    now = _now()
    merge_row(
        spark,
        catalog,
        schema,
        TABLE_MV_CANDIDATES,
        {"target_space_id": target_space_id, "dedup_fingerprint": dedup_fingerprint},
        {
            "decision": decision,
            "decided_by": decided_by,
            "decided_at": now,
            "suppressed_until": suppressed_until,
            "approved_for_rerun": bool(approved_for_rerun),
            "updated_at": now,
        },
    )
    logger.info(
        "Candidate %s for space %s %s by %s (approved_for_rerun=%s)",
        dedup_fingerprint, target_space_id, decision, decided_by, approved_for_rerun,
    )


def load_mv_candidates(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    target_space_id: str | None = None,
    run_id: str | None = None,
    approved_for_rerun: bool | None = None,
) -> list[dict[str, Any]]:
    """Read candidates, newest first, with JSON columns decoded.

    At least one of ``target_space_id`` or ``run_id`` must be given so a caller
    cannot accidentally scan every space. Returns ``[]`` when the table is
    absent or nothing matches.
    """
    if not target_space_id and not run_id:
        raise ValueError("load_mv_candidates requires target_space_id or run_id")

    where: list[str] = []
    if target_space_id:
        where.append(f"target_space_id = '{target_space_id}'")
    if run_id:
        where.append(f"run_id = '{run_id}'")
    if approved_for_rerun is not None:
        where.append(f"approved_for_rerun = {str(bool(approved_for_rerun)).lower()}")

    fqn = _fqn(catalog, schema, TABLE_MV_CANDIDATES)
    query = (
        f"SELECT * FROM {fqn} WHERE {' AND '.join(where)} "
        "ORDER BY confidence_score DESC NULLS LAST, created_at DESC"
    )
    try:
        df = run_query(spark, query)
    except Exception:
        logger.debug("load_mv_candidates: no rows for %s", where, exc_info=True)
        return []
    if df.empty:
        return []
    return [
        _parse_json_columns(row, _CANDIDATE_JSON_COLUMNS)
        for row in df.to_dict(orient="records")
    ]


# ── Consents ─────────────────────────────────────────────────────────────


def upsert_mv_consent(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    probe_id: str,
    granted_by: str,
    target_catalog: str,
    target_schema: str,
    verdict: str,
    run_id: str | None = None,
    materialize_consented: bool = False,
    probe_results: dict | None = None,
    granted_at: str | None = None,
    downgrade_reason: str | None = None,
) -> str:
    """Upsert one consent record; return its ``probe_id``.

    ``run_id`` is NULL until the consent is carried into a run at trigger time.
    ``reverified_at_trigger`` is never set here — it is stamped by
    :func:`mark_mv_consent_reverified` immediately before an OBO write, so a
    stale authorization cannot be mistaken for a fresh one.
    """
    if verdict not in MV_CONSENT_VERDICTS:
        raise ValueError(
            f"verdict must be one of {MV_CONSENT_VERDICTS}, got {verdict!r}"
        )
    if not probe_id:
        raise ValueError("probe_id is required")

    now = _now()
    value_cols: dict[str, Any] = {
        "run_id": run_id,
        "granted_by": granted_by,
        "granted_at": granted_at or now,
        "target_catalog": target_catalog,
        "target_schema": target_schema,
        "materialize_consented": bool(materialize_consented),
        "probe_results_json": _opt_json(probe_results),
        "verdict": verdict,
        "downgrade_reason": downgrade_reason,
        "updated_at": now,
    }

    merge_row(
        spark,
        catalog,
        schema,
        TABLE_MV_CONSENTS,
        {"probe_id": probe_id},
        value_cols,
        base64_string_columns=set(_CONSENT_JSON_COLUMNS),
    )
    logger.info(
        "Upserted metric view consent %s for %s on %s.%s (verdict=%s, materialize=%s)",
        probe_id, granted_by, target_catalog, target_schema,
        verdict, materialize_consented,
    )
    return probe_id


def mark_mv_consent_reverified(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    probe_id: str,
    run_id: str | None = None,
    verdict: str | None = None,
    downgrade_reason: str | None = None,
) -> None:
    """Stamp ``reverified_at_trigger`` after re-running the entitlement probe.

    Called immediately before the backend's OBO write. Pass ``verdict`` when
    re-verification changed it and ``downgrade_reason`` when that downgraded the
    run — this path records a downgrade, never an upgrade.
    """
    now = _now()
    updates: dict[str, Any] = {"reverified_at_trigger": now, "updated_at": now}
    if run_id is not None:
        updates["run_id"] = run_id
    if verdict is not None:
        if verdict not in MV_CONSENT_VERDICTS:
            raise ValueError(
                f"verdict must be one of {MV_CONSENT_VERDICTS}, got {verdict!r}"
            )
        updates["verdict"] = verdict
    if downgrade_reason is not None:
        updates["downgrade_reason"] = downgrade_reason

    merge_row(
        spark,
        catalog,
        schema,
        TABLE_MV_CONSENTS,
        {"probe_id": probe_id},
        updates,
    )
    logger.info("Re-verified metric view consent %s at %s", probe_id, now)


def load_mv_consent(
    spark: SparkSession,
    probe_id: str,
    catalog: str,
    schema: str,
) -> dict[str, Any] | None:
    """Return one consent row with ``probe_results`` decoded, or ``None``."""
    try:
        df = read_table(
            spark, catalog, schema, TABLE_MV_CONSENTS, filters={"probe_id": probe_id},
        )
    except Exception:
        logger.debug("load_mv_consent: could not read %s", probe_id, exc_info=True)
        return None
    if df.empty:
        return None
    return _parse_json_columns(df.iloc[0].to_dict(), _CONSENT_JSON_COLUMNS)


# ── Created objects ──────────────────────────────────────────────────────


def upsert_mv_created_object(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    suggestion_id: str,
    full_name: str,
    created_by: str,
    status: str = "CREATED",
    attach_patch_id: str | None = None,
    baseline_eval_run_id: str | None = None,
    post_attach_eval_run_id: str | None = None,
    on_regression_action: str = "DETACH_ONLY_NEVER_DROP",
    provenance: str | None = None,
) -> str:
    """Upsert the record of a metric view created under OBO for ``run_id``.

    Keyed on ``(run_id, suggestion_id)``. ``created_by`` is the consenting user,
    never the service principal — the job has no OBO token and never issues
    metric view DDL. ``provenance`` (MV-D24) is ``OBO_CREATED`` by default; the
    bring-your-own registration path writes ``USER_CREATED``.
    """
    from genie_space_optimizer.common.config import MV_PROVENANCE_OBO_CREATED

    if status not in MV_CREATED_OBJECT_STATUSES:
        raise ValueError(
            f"status must be one of {MV_CREATED_OBJECT_STATUSES}, got {status!r}"
        )
    if on_regression_action not in MV_ON_REGRESSION_ACTIONS:
        raise ValueError(
            f"on_regression_action must be one of {MV_ON_REGRESSION_ACTIONS}, "
            f"got {on_regression_action!r}"
        )
    if not full_name:
        raise ValueError("full_name is required")

    now = _now()
    merge_row(
        spark,
        catalog,
        schema,
        TABLE_MV_CREATED_OBJECTS,
        {"run_id": run_id, "suggestion_id": suggestion_id},
        {
            "full_name": full_name,
            "created_by": created_by,
            "status": status,
            "attach_patch_id": attach_patch_id,
            "baseline_eval_run_id": baseline_eval_run_id,
            "post_attach_eval_run_id": post_attach_eval_run_id,
            "on_regression_action": on_regression_action,
            "provenance": provenance or MV_PROVENANCE_OBO_CREATED,
            "updated_at": now,
        },
        insert_only_cols={"created_at": now},
    )
    logger.info(
        "Upserted created metric view %s (status=%s) for run %s suggestion %s",
        full_name, status, run_id, suggestion_id,
    )
    return full_name


def update_mv_created_object_status(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    suggestion_id: str,
    status: str,
    attach_patch_id: str | None = None,
    baseline_eval_run_id: str | None = None,
    post_attach_eval_run_id: str | None = None,
    lift_report_json: str | None = None,
) -> None:
    """Advance a created object's lifecycle status, optionally recording eval ids.

    The two eval-run ids are what make the isolated metric-view lift auditable:
    ``baseline_eval_run_id`` is measured with the view created but not attached,
    ``post_attach_eval_run_id`` immediately after the attach patch and before any
    lever fires. ``lift_report_json`` is ``LiftReport.to_dict()`` serialized
    verbatim — the shape is frozen, so callers must not reshape it. Only
    non-``None`` arguments are written.
    """
    if status not in MV_CREATED_OBJECT_STATUSES:
        raise ValueError(
            f"status must be one of {MV_CREATED_OBJECT_STATUSES}, got {status!r}"
        )

    now = _now()
    updates: dict[str, Any] = {"status": status, "updated_at": now}
    if attach_patch_id is not None:
        updates["attach_patch_id"] = attach_patch_id
    if baseline_eval_run_id is not None:
        updates["baseline_eval_run_id"] = baseline_eval_run_id
    if post_attach_eval_run_id is not None:
        updates["post_attach_eval_run_id"] = post_attach_eval_run_id
    if lift_report_json is not None:
        updates["lift_report_json"] = lift_report_json

    merge_row(
        spark,
        catalog,
        schema,
        TABLE_MV_CREATED_OBJECTS,
        {"run_id": run_id, "suggestion_id": suggestion_id},
        updates,
        base64_string_columns=set(_CREATED_OBJECT_JSON_COLUMNS),
    )
    logger.info(
        "Metric view for run %s suggestion %s is now %s", run_id, suggestion_id, status,
    )


def load_mv_created_objects(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Read the created-object rows for ``run_id``, newest first."""
    where = [f"run_id = '{run_id}'"]
    if status is not None:
        if status not in MV_CREATED_OBJECT_STATUSES:
            raise ValueError(
                f"status must be one of {MV_CREATED_OBJECT_STATUSES}, got {status!r}"
            )
        where.append(f"status = '{status}'")

    fqn = _fqn(catalog, schema, TABLE_MV_CREATED_OBJECTS)
    try:
        df = run_query(
            spark,
            f"SELECT * FROM {fqn} WHERE {' AND '.join(where)} ORDER BY created_at DESC",
        )
    except Exception:
        logger.debug(
            "load_mv_created_objects: no rows for run %s", run_id, exc_info=True,
        )
        return []
    if df.empty:
        return []
    return df.to_dict(orient="records")


def load_mv_created_object_by_name(
    spark: SparkSession,
    full_name: str,
    catalog: str,
    schema: str,
) -> dict[str, Any] | None:
    """Return the newest created-object row for a UC name, or ``None``.

    The explicit-drop endpoint needs this: it is handed an object name and must
    confirm the row exists and reached ``DETACHED`` before it will drop anything.
    """
    fqn = _fqn(catalog, schema, TABLE_MV_CREATED_OBJECTS)
    try:
        df = run_query(
            spark,
            f"SELECT * FROM {fqn} WHERE full_name = '{full_name}' "
            "ORDER BY updated_at DESC",
        )
    except Exception:
        logger.debug(
            "load_mv_created_object_by_name: could not read %s", full_name, exc_info=True,
        )
        return None
    if df.empty:
        return None
    return df.iloc[0].to_dict()


__all__ = [
    "MV_CANDIDATE_DECISIONS",
    "MV_CANDIDATE_TYPES",
    "MV_CONSENT_VERDICTS",
    "MV_CREATED_OBJECT_STATUSES",
    "MV_ON_REGRESSION_ACTIONS",
    "load_mv_candidates",
    "load_mv_consent",
    "load_mv_created_object_by_name",
    "load_mv_created_objects",
    "mark_mv_consent_reverified",
    "mv_candidate_fingerprint",
    "record_mv_candidate_decision",
    "update_mv_created_object_status",
    "upsert_mv_candidate",
    "upsert_mv_consent",
    "upsert_mv_created_object",
]
