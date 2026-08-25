"""Metric view create-and-attach under OBO (Prompt 9, MV-D1/D20/D22).

The GSO job runs as the service principal and never issues metric view DDL — a
create is a user's write and must execute under the user's identity. This module
is the backend seam the engine's :func:`trigger_optimization` calls through its
``mv_attach_hook``: after the run row exists and before the job is submitted, it

  1. re-verifies the recorded consent against a fresh OBO probe (downgrade-only,
     MV-D1) — any mismatch abandons the whole create and the run proceeds as
     ``suggest_only``;
  2. for each approved candidate, recovers the immutable rendered ``yaml_text``
     from the ``mv_candidate_ddl`` artifact (MV-D22 — it does NOT regenerate),
     re-wraps it for the *consented* target via :func:`mv_yaml.create_ddl`
     (necessary because the render-time ``proposed_object`` is derived from the
     source-table location, before consent exists and possibly differing from
     it), re-validates under the fresh probe, and **hard-aborts that suggestion**
     if revalidation demands a rung below the one the YAML was rendered for;
  3. creates the view under OBO, confirms it, and records the created-object
     ledger row (SP write into GSO storage, keyed on ``(run_id, suggestion_id)``).

A per-suggestion failure drops that suggestion; the run still proceeds. If every
approved suggestion drops, the handoff downgrades the run to ``suggest_only``.

The revalidation abort makes create-time safety independent of MV-D13 continuing
to hold: even though the persisted YAML is rendered at the warehouse's
conservative floor today (so a stricter fresh probe cannot fire the guard), the
guard is wired so a future capability change cannot silently create the wrong
artifact.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import uuid
from dataclasses import dataclass, field

from backend.models import MvConsentVerification, MvCreatedObject
from backend.services.auth import (
    get_service_principal_client,
    require_obo_workspace_client,
)
from backend.services import mv_entitlement
from genie_space_optimizer.common.config import (
    MV_PROVENANCE_OBO_CREATED,
    MV_PROVENANCE_USER_CREATED,
)

logger = logging.getLogger(__name__)


@dataclass
class MvAttachHandoff:
    """What the engine reads back from the create hook (duck-typed there).

    ``attach_views`` are the fully-qualified identifiers the job attaches;
    ``consent_id`` is the ``probe_id`` carried as ``mv_consent_id`` (MV-D16);
    ``action_mode`` is the effective mode after any downgrade.
    """

    attach_views: list[str] = field(default_factory=list)
    consent_id: str = ""
    action_mode: str = "suggest_only"
    downgrade_reason: str | None = None
    created: list[MvCreatedObject] = field(default_factory=list)


def _gso_storage() -> tuple[str, str, str]:
    """Return ``(catalog, schema, warehouse_id)`` for GSO state, or empties."""
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    warehouse_id = os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get(
        "SQL_WAREHOUSE_ID", ""
    )
    return catalog, schema, warehouse_id


def _source_tables_from_consent(consent: dict) -> list[str]:
    """Recover the SELECT securables the consent was granted against.

    The fresh probe must check the same source tables the consent covered, or a
    SELECT revoked since consent would not surface and :func:`mv_entitlement.verify`
    would wave a now-unauthorized write through. They round-trip in the stored
    ``probe_results`` as the SELECT privilege rows.
    """
    results = consent.get("probe_results")
    tables: list[str] = []
    if isinstance(results, dict):
        for row in results.get("privileges") or []:
            if isinstance(row, dict) and row.get("privilege") == "SELECT":
                securable = row.get("securable")
                if securable:
                    tables.append(str(securable))
    return list(dict.fromkeys(tables))


def _rung_below(downgrade_to: str | None, stored_strategy: str | None) -> bool:
    """True when revalidation demands a rung below where the YAML was rendered.

    :func:`mv_yaml.validate` only ever returns ``downgrade_to = subquery_source``
    (nested joins requested but the runtime does not report the floor as granted).
    So a downgrade that differs from the stored strategy means the persisted YAML
    claims a rung the fresh probe will not grant — the MV-D22 abort condition.
    Under MV-D13 this cannot fire for warehouse-rendered YAML; the guard exists
    so that stops being a silent dependency.
    """
    return bool(downgrade_to) and downgrade_to != (stored_strategy or "")


def _load_ddl_artifact(
    sp_ws, warehouse_id: str, *, catalog: str, schema: str, fingerprint: str
) -> dict | None:
    """Read the ``mv_candidate_ddl`` artifact for one candidate by content hash.

    ``content_hash`` is the dedup fingerprint (MV-D7), so this joins the artifact
    to the candidate without depending on which run rendered it.
    """
    from genie_space_optimizer.backend.utils import safe_json_parse
    from genie_space_optimizer.common.warehouse import sql_warehouse_query

    escaped = fingerprint.replace("'", "''")
    try:
        df = sql_warehouse_query(
            sp_ws,
            warehouse_id,
            f"SELECT artifact_json FROM {catalog}.{schema}.genie_opt_artifacts "
            f"WHERE artifact_kind = 'mv_candidate_ddl' AND content_hash = '{escaped}' "
            "ORDER BY created_at DESC LIMIT 1",
        )
    except Exception:
        logger.warning(
            "Could not read mv_candidate_ddl artifact for %s", fingerprint, exc_info=True
        )
        return None
    if getattr(df, "empty", True):
        return None
    payload = safe_json_parse(df.iloc[0].to_dict().get("artifact_json"))
    return payload if isinstance(payload, dict) else None


def _object_exists(obo_ws, warehouse_id: str, full_name: str) -> bool:
    from genie_space_optimizer.common.warehouse import sql_warehouse_query

    try:
        df = sql_warehouse_query(obo_ws, warehouse_id, f"DESCRIBE TABLE {full_name}")
        return not getattr(df, "empty", True)
    except Exception:
        return False


def _confirm_metric_view(obo_ws, warehouse_id: str, full_name: str) -> bool:
    """Confirm the created object is a metric view and is queryable.

    ``DESCRIBE EXTENDED`` reports the object type; an empty probe select confirms
    the semantic layer resolves without pulling data.
    """
    from genie_space_optimizer.common.warehouse import sql_warehouse_query

    try:
        df = sql_warehouse_query(
            obo_ws, warehouse_id, f"DESCRIBE EXTENDED {full_name}"
        )
    except Exception:
        logger.warning("DESCRIBE EXTENDED failed for %s", full_name, exc_info=True)
        return False
    text = " ".join(str(v) for v in df.to_numpy().ravel()) if not getattr(df, "empty", True) else ""
    if "METRIC_VIEW" not in text.upper():
        logger.warning("Created object %s is not reported as a metric view", full_name)
        return False
    try:
        sql_warehouse_query(obo_ws, warehouse_id, f"SELECT 1 FROM {full_name} LIMIT 0")
    except Exception:
        logger.warning("Created metric view %s is not queryable", full_name, exc_info=True)
        return False
    return True


def _consented_full_name(consent: dict, proposed_object: str) -> str:
    """Re-target the render-time name to the consented catalog/schema (MV-D22)."""
    base = (proposed_object or "").split(".")[-1]
    return f"{consent['target_catalog']}.{consent['target_schema']}.{base}"


def verify_consent(
    *, probe_id: str, space_id: str, catalog: str, schema: str, warehouse_id: str
) -> tuple[MvConsentVerification | None, dict | None]:
    """Re-verify a recorded consent against a fresh OBO probe (MV-D1).

    Returns ``(verification, consent_row)``. A missing consent row yields
    ``(None, None)`` — the caller downgrades. ``verify`` itself downgrades on any
    identity/target/compute/verdict mismatch; it never upgrades a stored verdict.
    """
    from genie_space_optimizer.common.warehouse import wh_load_mv_consent

    sp_ws = get_service_principal_client()
    consent = wh_load_mv_consent(sp_ws, warehouse_id, probe_id, catalog, schema)
    if not consent:
        return None, None

    fresh = mv_entitlement.probe(
        catalog=consent["target_catalog"],
        schema=consent["target_schema"],
        space_id=space_id,
        source_tables=_source_tables_from_consent(consent),
        warehouse_id=warehouse_id,
    )
    return mv_entitlement.verify(consent, fresh), consent


def create_and_attach_for_run(
    run_id: str,
    *,
    space_id: str,
    probe_id: str,
    approved_suggestion_ids: list[str] | None = None,
    materialize: bool = False,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> MvAttachHandoff:
    """Create the approved metric views under OBO and return the attach handoff.

    Called by ``trigger_optimization``'s ``mv_attach_hook`` with the run's
    ``run_id``. Never raises for a create problem — a per-suggestion failure
    drops that suggestion and the run proceeds; a whole-run problem (no consent,
    downgraded re-verification, nothing approved) returns a ``suggest_only``
    handoff.
    """
    from genie_space_optimizer.common.warehouse import (
        wh_load_mv_candidates,
        wh_upsert_mv_created_object,
    )
    from genie_space_optimizer.optimization.mv_yaml import create_ddl, validate

    if materialize:
        # Materialization is a separate consent (MV-D7) and a separate DDL path;
        # create-and-attach installs a (non-materialized) metric view only.
        logger.info(
            "mv_materialize requested for run %s but not applied: create-and-attach "
            "installs a non-materialized metric view", run_id,
        )

    verification, consent = verify_consent(
        probe_id=probe_id, space_id=space_id,
        catalog=catalog, schema=schema, warehouse_id=warehouse_id,
    )
    if consent is None or verification is None:
        # No consent row exists, so there is nothing to stamp — the run is
        # suggest_only by absence, and /mv-created has no consent to read.
        return MvAttachHandoff(
            action_mode="suggest_only",
            downgrade_reason="no consent record was found for this probe",
        )

    def _stamp_consent(
        *, verdict: str | None = None, downgrade_reason: str | None = None
    ) -> None:
        """Close the consent→run loop on the row the probe already wrote.

        The Spark twin ``mark_mv_consent_reverified`` had no warehouse peer and
        no caller, so the backend trigger flow left ``run_id`` /
        ``downgrade_reason`` NULL on every consent — and ``/mv-created`` (which
        reads the consent by run) surfaced ``downgrade_reason`` as ``None`` even
        when a run auto-downgraded (Tier-2 Scenario B). Stamping here, as the SP
        that owns the table, records which run the consent bound to and why it
        downgraded, on both the downgrade and success paths. Best-effort: a
        stamp failure must not abort a create that already succeeded.
        """
        from genie_space_optimizer.common.warehouse import (
            wh_mark_mv_consent_reverified,
        )

        try:
            wh_mark_mv_consent_reverified(
                get_service_principal_client(), warehouse_id,
                catalog=catalog, schema=schema, probe_id=probe_id,
                run_id=run_id, verdict=verdict, downgrade_reason=downgrade_reason,
            )
        except Exception:
            logger.warning(
                "Could not stamp consent %s for run %s", probe_id, run_id,
                exc_info=True,
            )

    if verification.effective_mode != "create_and_attach":
        _stamp_consent(
            verdict=verification.verdict,
            downgrade_reason=verification.downgrade_reason,
        )
        return MvAttachHandoff(
            action_mode="suggest_only",
            consent_id=probe_id,
            downgrade_reason=verification.downgrade_reason,
        )

    fresh_probe = verification.fresh_probe
    sp_ws = get_service_principal_client()
    obo_ws = require_obo_workspace_client()

    candidates = wh_load_mv_candidates(
        sp_ws, warehouse_id, catalog, schema,
        target_space_id=space_id, approved_for_rerun=True,
    )
    approved = set(approved_suggestion_ids or [])
    if approved:
        candidates = [c for c in candidates if c.get("suggestion_id") in approved]

    attach_views: list[str] = []
    created: list[MvCreatedObject] = []

    for candidate in candidates:
        suggestion_id = str(candidate.get("suggestion_id") or "")
        fingerprint = str(candidate.get("dedup_fingerprint") or "")
        if not suggestion_id or not fingerprint:
            continue
        try:
            # MV-D22 replay body. Two sources, one shape: an in-job run writes a
            # run-partitioned ``mv_candidate_ddl`` artifact, so that is tried
            # first; a standalone advice run (MV-D23) has no run-keyed artifact
            # and carries the rendered body on the candidate row itself
            # (``yaml_text`` + ``evidence.join_strategy``). The artifact is the
            # authority when present — it pins ``join_strategy`` beside the body
            # — and the candidate row is the fallback, never a second render.
            artifact = _load_ddl_artifact(
                sp_ws, warehouse_id, catalog=catalog, schema=schema,
                fingerprint=fingerprint,
            )
            if artifact and artifact.get("yaml_text"):
                yaml_text = str(artifact["yaml_text"])
                stored_strategy = artifact.get("join_strategy")
                proposed_object = str(artifact.get("proposed_object") or "")
            elif candidate.get("yaml_text"):
                yaml_text = str(candidate["yaml_text"])
                evidence = candidate.get("evidence") or {}
                stored_strategy = evidence.get("join_strategy") if isinstance(evidence, dict) else None
                proposed_object = str(candidate.get("proposed_object") or "")
            else:
                logger.warning(
                    "No rendered yaml_text for suggestion %s; skipping", suggestion_id
                )
                continue

            full_name = _consented_full_name(consent, proposed_object)

            # MV-D22 replay-with-revalidation. NOT_COMPARED (no oracle at trigger
            # time) is a clean firewall, not a failure — the body is immutable and
            # was echo-checked at render, so ``report.ok`` governs, not echo_check.
            report = validate(yaml_text, capabilities=fresh_probe.capabilities)
            if not report.ok:
                logger.warning(
                    "Revalidation of suggestion %s failed (%s); dropping",
                    suggestion_id, "; ".join(report.errors) or "no detail",
                )
                continue
            if _rung_below(report.downgrade_to, stored_strategy):
                logger.warning(
                    "Revalidation of suggestion %s demands rung %s below stored %s; "
                    "aborting create (MV-D22)",
                    suggestion_id, report.downgrade_to, stored_strategy,
                )
                continue

            if _object_exists(obo_ws, warehouse_id, full_name):
                logger.warning(
                    "%s already exists; refusing to clobber it for suggestion %s",
                    full_name, suggestion_id,
                )
                continue

            from genie_space_optimizer.common.warehouse import sql_warehouse_execute

            sql_warehouse_execute(
                obo_ws, warehouse_id, create_ddl(full_name, yaml_text)
            )
            if not _confirm_metric_view(obo_ws, warehouse_id, full_name):
                # The create statement ran but the object is not a usable metric
                # view; drop the half-made object so nothing is left behind.
                try:
                    sql_warehouse_execute(
                        obo_ws, warehouse_id, f"DROP VIEW IF EXISTS {full_name}"
                    )
                except Exception:
                    logger.warning("Could not clean up %s after a failed create", full_name)
                continue

            wh_upsert_mv_created_object(
                sp_ws, warehouse_id,
                catalog=catalog, schema=schema,
                run_id=run_id, suggestion_id=suggestion_id,
                full_name=full_name, created_by=fresh_probe.checked_as,
                status="CREATED",
                provenance=MV_PROVENANCE_OBO_CREATED,
            )
            attach_views.append(full_name)
            created.append(MvCreatedObject(
                run_id=run_id, suggestion_id=suggestion_id, full_name=full_name,
                created_by=fresh_probe.checked_as, status="CREATED",
                provenance=MV_PROVENANCE_OBO_CREATED,
                on_regression_action="DETACH_ONLY_NEVER_DROP",
            ))
            logger.info("Created metric view %s for run %s", full_name, run_id)
        except Exception:
            logger.warning(
                "Create failed for suggestion %s; dropping it from the run",
                suggestion_id, exc_info=True,
            )
            continue

    if not attach_views:
        # Consent survived re-verification but nothing built (revalidation drops,
        # collisions). The verdict stays SUFFICIENT — this is a create-time
        # outcome, not a consent downgrade — but the run and its reason are still
        # stamped so /mv-created can explain the empty result.
        downgrade_reason = "no metric view could be created for the approved candidates"
        _stamp_consent(verdict=verification.verdict, downgrade_reason=downgrade_reason)
        return MvAttachHandoff(
            action_mode="suggest_only",
            consent_id=probe_id,
            downgrade_reason=downgrade_reason,
        )
    _stamp_consent(verdict=verification.verdict)
    return MvAttachHandoff(
        attach_views=attach_views,
        consent_id=probe_id,
        action_mode="create_and_attach",
        created=created,
    )


# ── Bring-your-own registration (MV-D24) ───────────────────────────────────

_UC_IDENT_PART = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass
class MvRegisterResult:
    """Outcome of a bring-your-own registration (MV-D24).

    ``registered`` is the verdict; on refusal ``reason`` carries the specific
    gate that failed (not a metric view, not visible, validation) so the user
    can act on it. On success ``run_id`` is the sentinel advice run that hosts
    the ``USER_CREATED`` ledger row and ``suggestion_id`` is the row's key.
    ``warnings`` are advisory lints (e.g. a non-generated version string) that
    did not block registration.
    """

    registered: bool
    full_name: str
    provenance: str = MV_PROVENANCE_USER_CREATED
    run_id: str | None = None
    suggestion_id: str | None = None
    reason: str | None = None
    warnings: list[str] = field(default_factory=list)


def _valid_uc_identifier(full_name: str) -> bool:
    """Three plain UC parts (letters, digits, underscore). Refuses anything else.

    The identifier is user input that is interpolated into ``DESCRIBE`` and, for
    other rows, ``DROP VIEW`` — so it is constrained to unquoted identifier
    characters and refused otherwise rather than trusted or best-effort escaped.
    """
    parts = [p.strip().strip("`") for p in (full_name or "").split(".")]
    return len(parts) == 3 and all(bool(_UC_IDENT_PART.match(p)) for p in parts)


def _obo_identity(obo_ws) -> str:
    try:
        return str(obo_ws.current_user.me().user_name or "").strip()
    except Exception:
        return ""


def _recover_registered_metric_view(
    obo_ws, warehouse_id: str, full_name: str
) -> tuple[bool, str | None, str | None]:
    """Verify under OBO that ``full_name`` is a metric view; recover its YAML.

    Returns ``(ok, yaml_text, reason)``. Read under the caller's OBO client so
    the caller's own visibility governs — an object they cannot see is refused,
    not resolved through the SP. ``DESCRIBE TABLE EXTENDED ... AS JSON`` carries
    both the ``type`` assertion and the ``view_text`` YAML body.
    """
    from genie_space_optimizer.backend.utils import safe_json_parse
    from genie_space_optimizer.common.warehouse import sql_warehouse_query

    fq = ".".join(f"`{p.strip().strip('`')}`" for p in full_name.split("."))
    try:
        df = sql_warehouse_query(
            obo_ws, warehouse_id, f"DESCRIBE TABLE EXTENDED {fq} AS JSON"
        )
    except Exception:
        logger.warning("register: DESCRIBE failed for %s", full_name, exc_info=True)
        return (
            False, None,
            f"{full_name} could not be described — it may not exist, or you may "
            "not have access to it",
        )
    if getattr(df, "empty", True):
        return False, None, f"{full_name} returned no metadata"

    envelope = None
    for cell in df.to_numpy().ravel():
        parsed = safe_json_parse(cell)
        if isinstance(parsed, dict):
            envelope = parsed
            break
    if not isinstance(envelope, dict):
        return (
            False, None,
            f"{full_name}: could not parse the DESCRIBE ... AS JSON envelope",
        )

    type_str = str(envelope.get("type") or "").strip().upper()
    if type_str != "METRIC_VIEW":
        return (
            False, None,
            f"{full_name} is not a metric view (type={type_str or 'unknown'}); "
            "only a metric view can be registered",
        )

    view_text = (
        envelope.get("view_text")
        or envelope.get("View Text")
        or envelope.get("view_definition")
        or ""
    )
    if not isinstance(view_text, str) or not view_text.strip():
        return (
            False, None,
            f"{full_name}: its definition (view_text) is not visible to you — "
            "you may not be its owner, so it cannot be verified",
        )
    return True, view_text, None


def _claim_matches_view(
    sp_ws, warehouse_id: str, *, catalog: str, schema: str,
    space_id: str, suggestion_id: str, yaml_text: str,
) -> tuple[bool, str | None]:
    """Check a claim that this view implements a specific proposal (MV-D24).

    The claim is *checked, not trusted*: each of the view's measures is
    fingerprinted through the same extractor + ``mv_candidate_fingerprint`` the
    corpus scan uses, and the claimed candidate's ``dedup_fingerprint`` must be
    among them. A mismatch refuses the claim (the user can register without it).
    """
    import yaml as _yaml

    from genie_space_optimizer.common.warehouse import wh_load_mv_candidates
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures
    from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

    candidates = wh_load_mv_candidates(
        sp_ws, warehouse_id, catalog, schema, target_space_id=space_id
    )
    claimed = next(
        (c for c in candidates if str(c.get("suggestion_id") or "") == suggestion_id),
        None,
    )
    if claimed is None:
        return False, f"no proposal {suggestion_id} exists for this space to claim"
    target_fp = str(claimed.get("dedup_fingerprint") or "")
    if not target_fp:
        return False, f"proposal {suggestion_id} has no fingerprint to compare against"

    try:
        definition = _yaml.safe_load(yaml_text) or {}
    except Exception:
        definition = {}
    source = str((definition.get("source") or "")).strip()
    fingerprints: set[str] = set()
    for measure in definition.get("measures") or []:
        if not isinstance(measure, dict):
            continue
        expr = str(measure.get("expr") or "").strip()
        if not expr or not source:
            continue
        try:
            refs = extract_measures(f"SELECT {expr} AS m FROM {source}")
        except Exception:
            continue
        for ref in refs:
            sources = ref.source_tables or (source,)
            fingerprints.add(
                mv_candidate_fingerprint(space_id, ref.canonical_expr, sources)
            )

    if target_fp in fingerprints:
        return True, None
    return (
        False,
        f"{definition.get('source') or 'the view'} does not appear to implement "
        f"proposal {suggestion_id} (measure fingerprint mismatch); register "
        "without claiming a proposal if it is a different view",
    )


def register_user_created_view(
    *,
    space_id: str,
    full_name: str,
    claimed_suggestion_id: str | None = None,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> MvRegisterResult:
    """Register a user-created metric view so the app can attach it (MV-D24).

    The copied-DDL path's return trip: the user created the view themselves, in
    their own SQL editor, under their own identity; this verifies it under OBO
    and records a ``USER_CREATED`` ledger row so the normal attach-and-lift path
    can run on the next run.

    Verification, not trust (invariant 2): an identifier that is not a metric
    view, not visible to the caller, or whose YAML fails the safety lint is
    refused with the reason and **nothing is written**. Sequencing (item 3): the
    ledger row is the *last* fallible step — every verification gate passes
    first, so a row that fails to write surfaces as "registration failed, retry"
    rather than leaving a verified-but-unrecorded view the attach phase skips.
    """
    from genie_space_optimizer.common.warehouse import (
        wh_create_advice_run,
        wh_ensure_optimization_tables,
        wh_upsert_mv_created_object,
    )
    from genie_space_optimizer.optimization.mv_yaml import validate_registered

    full_name = (full_name or "").strip()
    if not _valid_uc_identifier(full_name):
        return MvRegisterResult(
            registered=False, full_name=full_name,
            reason="identifier must be a three-part catalog.schema.name using "
            "letters, digits, and underscores",
        )

    obo_ws = require_obo_workspace_client()
    sp_ws = get_service_principal_client()

    ok, yaml_text, reason = _recover_registered_metric_view(
        obo_ws, warehouse_id, full_name
    )
    if not ok or not yaml_text:
        return MvRegisterResult(registered=False, full_name=full_name, reason=reason)

    report = validate_registered(yaml_text)
    if not report.ok:
        return MvRegisterResult(
            registered=False, full_name=full_name,
            reason="the metric view failed validation: "
            + ("; ".join(report.errors) or "no detail"),
        )

    if claimed_suggestion_id:
        matched, why = _claim_matches_view(
            sp_ws, warehouse_id, catalog=catalog, schema=schema,
            space_id=space_id, suggestion_id=claimed_suggestion_id,
            yaml_text=yaml_text,
        )
        if not matched:
            return MvRegisterResult(
                registered=False, full_name=full_name, reason=why,
            )
        suggestion_id = claimed_suggestion_id
    else:
        # A stable synthetic id keyed on the object, so re-registering the same
        # view upserts one row rather than accreting duplicates. The ``user_``
        # prefix (underscore, not a colon) keeps it inside the suggestion_id
        # charset the lifecycle routes validate.
        suggestion_id = "user_" + hashlib.sha256(
            full_name.lower().encode("utf-8")
        ).hexdigest()[:32]

    created_by = _obo_identity(obo_ws)
    if not created_by:
        return MvRegisterResult(
            registered=False, full_name=full_name,
            reason="could not resolve the registering user's identity",
        )

    # All gates passed. The ledger row is the LAST fallible step (item 3): the
    # table bootstrap and the sentinel advice run precede it, and the row write
    # itself is final — if it raises, the caller reports failure and the view is
    # simply unregistered (the attach phase skips an unrecorded identifier),
    # never verified-but-half-recorded (invariant 2).
    wh_ensure_optimization_tables(sp_ws, warehouse_id, catalog, schema)
    run_id = str(uuid.uuid4())
    wh_create_advice_run(
        sp_ws, warehouse_id,
        run_id=run_id, space_id=space_id, domain="",
        catalog=catalog, schema=schema,
        triggered_by=created_by, llm_model="",
    )
    wh_upsert_mv_created_object(
        sp_ws, warehouse_id,
        catalog=catalog, schema=schema,
        run_id=run_id, suggestion_id=suggestion_id,
        full_name=full_name, created_by=created_by,
        status="CREATED", provenance=MV_PROVENANCE_USER_CREATED,
    )
    logger.info(
        "Registered USER_CREATED metric view %s for space %s (run %s)",
        full_name, space_id, run_id,
    )
    return MvRegisterResult(
        registered=True, full_name=full_name,
        run_id=run_id, suggestion_id=suggestion_id,
        warnings=list(report.warnings),
    )


__all__ = [
    "MvAttachHandoff",
    "MvRegisterResult",
    "create_and_attach_for_run",
    "register_user_created_view",
    "verify_consent",
]
