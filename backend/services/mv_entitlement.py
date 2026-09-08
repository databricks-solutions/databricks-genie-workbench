"""Metric view entitlement probe and consent service (POV §7.3.1, MV-D8).

The metric view advisor may create a real Unity Catalog object. The GSO job runs
as the app service principal, so it is structurally the wrong identity to ask
"may this be created?" — a probe of the SP would authorize a write the person who
asked for it is not entitled to make. Everything here therefore runs under the
signed-in user's OBO client (:func:`require_obo_workspace_client`) and never
falls back to the SP. ``GET /api/auto-optimize/permissions/{space_id}`` probes the
SP and is deliberately not reused.

Two questions get answered, and they fail in opposite directions:

- **Privileges** — ownership first, then ``grants.get_effective``. Ownership of
  the target schema/catalog (read from object metadata the OBO user can always
  see) authorizes create-in-schema directly, so an owner is never blocked by a
  grant read they cannot perform. Otherwise ``get_effective`` resolves group
  inheritance server-side. A read that FAILS (commonly the OBO token is not
  scoped for the UC-grants API) is reported UNKNOWN, not DENIED — a false
  "insufficient, ask an admin to GRANT" on a user who already has access is the
  worse error. A genuine missing privilege is actionable, so each denial
  contributes a GRANT statement to ``remediation_sql``. We never attempt a trial
  ``CREATE``: a probe that writes is not a probe.
- **Capabilities** — the runtime floors from MV-D8. These are Databricks Runtime
  versions, and a SQL warehouse reports only a DBSQL version, so they are often
  genuinely undecidable. Undecidable is reported as ``UNKNOWN`` rather than
  guessed, and the two kinds of ``UNKNOWN`` resolve differently: an optional
  capability withholds a YAML feature (``mv_yaml`` steps down the join ladder),
  while ``mv_create_edit`` never blocks authorization — blocking it would deny
  every warehouse-only user for a floor no GRANT can satisfy.

The consent record is the durable half. It stores the probe verdict so the
trigger flow (Prompt 9) can re-run the probe and compare: :func:`verify` returns
``suggest_only`` on any mismatch, so a privilege revoked between configuration
and trigger downgrades the run instead of failing the write. It downgrades only —
a consent can never be upgraded by a later probe.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from databricks.sdk.service.catalog import SecurableType

from backend.models import (
    MvCapabilityRow,
    MvCheckStatus,
    MvConsentVerification,
    MvPrivilegeRow,
    MvProbeResult,
)
from backend.services.auth import get_service_principal_client, require_obo_workspace_client
from genie_space_optimizer.common.config import (
    MV_CAPABILITY_CREATE_EDIT,
    MV_CAPABILITY_FLOORS,
    MV_OPTIONAL_CAPABILITIES,
)

logger = logging.getLogger(__name__)

# ``ALL PRIVILEGES`` satisfies any specific privilege we ask about.
_ALL_PRIVILEGES = "ALL_PRIVILEGES"

# UC rejects anything else, and these names are interpolated into the
# remediation GRANT text we hand back to the user.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")

_VERSION_RE = re.compile(r"^\s*(\d+)\.(\d+)")


class MvProbeError(Exception):
    """Raised when the probe cannot run at all (bad identifier, no OBO client)."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ident(name: str) -> str:
    """Backtick-quote an identifier for the remediation SQL text."""
    return f"`{name.replace('`', '``')}`"


def _require_identifier(value: str, label: str) -> str:
    value = (value or "").strip()
    if not _IDENTIFIER_RE.match(value):
        raise MvProbeError(f"{label} is not a valid Unity Catalog identifier: {value!r}")
    return value


def _split_table(fqn: str) -> tuple[str, str, str]:
    parts = [p.strip().strip("`") for p in (fqn or "").split(".")]
    if len(parts) != 3:
        raise MvProbeError(f"source table must be a three-part name, got {fqn!r}")
    return (
        _require_identifier(parts[0], "source table catalog"),
        _require_identifier(parts[1], "source table schema"),
        _require_identifier(parts[2], "source table name"),
    )


# ── Privilege reads ──────────────────────────────────────────────────────


def _effective_privileges(
    ws: Any, securable_type: str, full_name: str, principal: str,
) -> set[str]:
    """Return the user's effective privilege names on one securable.

    ``get_effective`` already expands group membership and parent-object
    inheritance, so a privilege inherited from a catalog-level grant or held via
    a group is present here — which is why this is the read used instead of the
    direct ``grants.get`` assignment list.
    """
    resp = ws.grants.get_effective(
        securable_type=securable_type, full_name=full_name, principal=principal,
    )
    privileges: set[str] = set()
    for assignment in getattr(resp, "privilege_assignments", None) or []:
        assigned_to = (getattr(assignment, "principal", "") or "").lower()
        if assigned_to and assigned_to != principal.lower():
            continue
        for entry in getattr(assignment, "privileges", None) or []:
            privilege = getattr(entry, "privilege", None)
            name = getattr(privilege, "value", None) or privilege
            if name:
                privileges.add(str(name).upper().replace(" ", "_"))
    return privileges


def _privilege_row(
    ws: Any,
    *,
    privilege: str,
    securable_type: str,
    full_name: str,
    principal: str,
    cache: dict[tuple[str, str], set[str] | str],
    owned: bool = False,
) -> MvPrivilegeRow:
    """Evaluate one privilege check, reusing one read per securable.

    ``owned`` short-circuits to GRANTED: an owner of this securable (or of an
    ancestor) holds every privilege on it, and ownership is read from metadata the
    OBO user can always see — so this resolves the common case (a schema owner
    creating in their own schema) without depending on the UC-grants read the OBO
    token may not be scoped for.
    """
    label = f"{privilege.replace('_', ' ')} on {full_name}"
    if owned:
        return MvPrivilegeRow(
            label=label, privilege=privilege, securable=full_name,
            status="GRANTED", detail=f"{principal} owns {full_name} (or an ancestor)",
        )
    key = (securable_type, full_name)
    if key not in cache:
        try:
            cache[key] = _effective_privileges(ws, securable_type, full_name, principal)
        except Exception as exc:  # noqa: BLE001 - surfaced as a row detail
            cache[key] = _read_failure_detail(exc, securable_type, full_name)
    cached = cache[key]

    if isinstance(cached, str):
        # The read itself failed — most often because the OBO token is not scoped
        # for the UC-grants API (the documented OBO-scope gap), NOT because the
        # user lacks the privilege. Report UNKNOWN, never DENIED: a false
        # "insufficient — ask an admin to GRANT" on a user who already has access
        # (verified deployed-review case: a schema owner) is the worse error.
        # Ownership above resolves owners affirmatively; this leaves the rest
        # honestly undecided rather than wrongly blocked.
        return MvPrivilegeRow(
            label=label, privilege=privilege, securable=full_name,
            status="UNKNOWN", detail=cached,
        )

    granted = privilege in cached or _ALL_PRIVILEGES in cached
    return MvPrivilegeRow(
        label=label, privilege=privilege, securable=full_name,
        status="GRANTED" if granted else "DENIED",
        detail=None if granted else f"{principal} does not hold {privilege} on {full_name}",
    )


def _read_failure_detail(exc: BaseException, securable_type: str, full_name: str) -> str:
    message = str(exc) or exc.__class__.__name__
    if "does not exist" in message.lower() or "not_found" in message.lower():
        return f"{securable_type.lower()} {full_name} not found or not visible to you"
    logger.warning("Effective-privilege read failed for %s: %s", full_name, message)
    return f"could not read privileges on {full_name}: {message}"


def _securable_owner(
    ws: Any, securable_type: str, full_name: str, owner_cache: dict[str, str | None],
) -> str | None:
    """The owner of one securable, or None (best-effort, OBO-readable).

    Owner is metadata the user can read whenever they can see the object — unlike
    ``grants.get_effective``, whose UC-grants API the OBO token is often not scoped
    for. This is why ownership is consulted for authorization even though the
    effective-privilege read is preferred: a schema owner can create in their
    schema regardless of whether the OBO token can read the grant that says so.
    """
    if full_name in owner_cache:
        return owner_cache[full_name]
    owner: str | None = None
    try:
        if securable_type == SecurableType.CATALOG.value:
            owner = getattr(ws.catalogs.get(full_name), "owner", None)
        elif securable_type == SecurableType.SCHEMA.value:
            owner = getattr(ws.schemas.get(full_name), "owner", None)
        elif securable_type == SecurableType.TABLE.value:
            owner = getattr(ws.tables.get(full_name), "owner", None)
    except Exception:  # noqa: BLE001 - owner is one signal; absence just means "no ownership proof"
        logger.info("Could not read owner of %s %s", securable_type, full_name, exc_info=True)
        owner = None
    owner_cache[full_name] = owner
    return owner


def _principal_owns(owner: str | None, principal: str, user_groups: set[str]) -> bool:
    """Does ``principal`` (or one of their groups) own this securable?"""
    if not owner:
        return False
    normalized = owner.strip().lower()
    return normalized == principal.lower() or normalized in user_groups


# ── Runtime capability reads (MV-D8) ─────────────────────────────────────


def _parse_version(value: str | None) -> tuple[int, int] | None:
    match = _VERSION_RE.match(str(value or ""))
    return (int(match.group(1)), int(match.group(2))) if match else None


def _observed_runtime(ws: Any, warehouse_id: str) -> tuple[str, str | None]:
    """Return ``(runtime_kind, version)`` for the compute the probe can see.

    ``current_version()`` populates ``dbr_version`` only on a cluster; on a SQL
    warehouse just ``dbsql_version`` comes back, and there is no published
    DBSQL-to-DBR mapping to convert it with.
    """
    if not warehouse_id:
        return ("UNAVAILABLE", None)
    try:
        from genie_space_optimizer.common.warehouse import sql_warehouse_query

        df = sql_warehouse_query(
            ws,
            warehouse_id,
            "SELECT current_version().dbr_version AS dbr, "
            "current_version().dbsql_version AS dbsql",
        )
    except Exception as exc:  # noqa: BLE001 - degrades to UNKNOWN rows
        logger.warning("Could not read current_version(): %s", exc)
        return ("UNAVAILABLE", None)
    if getattr(df, "empty", True):
        return ("UNAVAILABLE", None)

    row = df.iloc[0].to_dict()
    dbr = row.get("dbr")
    if dbr and str(dbr).lower() not in ("none", "null"):
        return ("DBR", str(dbr))
    dbsql = row.get("dbsql")
    if dbsql and str(dbsql).lower() not in ("none", "null"):
        return ("DBSQL", str(dbsql))
    return ("UNAVAILABLE", None)


def _capability_rows(
    runtime_kind: str, version: str | None, warehouse_id: str | None = None,
) -> list[MvCapabilityRow]:
    """Build one row per MV-D8 floor, stamped with the compute it was read on.

    ``observed_warehouse_id`` makes the row's provenance explicit, because a
    capability is a property of the compute and not of the user: one observed on
    warehouse A says nothing about a create executed on warehouse B. Trigger-time
    re-verification compares it, so that swap downgrades instead of carrying a
    stale claim into the write.
    """
    observed = _parse_version(version) if runtime_kind == "DBR" else None
    rows: list[MvCapabilityRow] = []
    for capability, required, label in MV_CAPABILITY_FLOORS:
        optional = capability in MV_OPTIONAL_CAPABILITIES
        floor = _parse_version(required)
        if observed is None or floor is None:
            status: MvCheckStatus = "UNKNOWN"
            detail = (
                f"compute reports a {runtime_kind} version, and the floor is stated "
                f"as DBR {required} — no mapping exists, so this is undecided"
                if runtime_kind == "DBSQL"
                else f"no runtime version observed; DBR {required} could not be checked"
            )
        else:
            status = "GRANTED" if observed >= floor else "DENIED"
            detail = None if status == "GRANTED" else f"requires DBR {required}"
        rows.append(MvCapabilityRow(
            capability=capability, label=label, required_dbr=required,
            observed_version=version, runtime_kind=runtime_kind,  # type: ignore[arg-type]
            observed_warehouse_id=warehouse_id or None,
            status=status, optional=optional, detail=detail,
        ))
    return rows


def _observed_warehouse(capabilities: list[Any] | None) -> str | None:
    """Return the warehouse a probe's capability rows were observed against.

    Accepts model rows or the plain dicts a stored ``probe_results`` payload
    round-trips as, so :func:`verify` can compare a persisted probe with a fresh
    one without reconstructing models.
    """
    for row in capabilities or []:
        value = (
            row.get("observed_warehouse_id")
            if isinstance(row, dict)
            else getattr(row, "observed_warehouse_id", None)
        )
        if value:
            return str(value)
    return None


# ── Probe ────────────────────────────────────────────────────────────────


def probe(
    *,
    catalog: str,
    schema: str,
    space_id: str,
    source_tables: list[str] | None = None,
    warehouse_id: str | None = None,
) -> MvProbeResult:
    """Read the signed-in user's entitlement to create a metric view.

    Read-only by construction: effective-privilege reads, a Genie ACL read, and
    one ``SELECT current_version()``. No DDL, and no trial ``CREATE`` — a probe
    that writes is not a probe.
    """
    ws = require_obo_workspace_client()
    catalog = _require_identifier(catalog, "catalog")
    schema = _require_identifier(schema, "schema")
    tables = [".".join(_split_table(t)) for t in (source_tables or [])]

    try:
        me = ws.current_user.me()
        principal = (me.user_name or "").strip()
        user_groups = {g.display.lower() for g in (me.groups or []) if g.display}
    except Exception as exc:  # noqa: BLE001
        raise MvProbeError(f"could not resolve the signed-in user: {exc}") from exc
    if not principal:
        raise MvProbeError("could not resolve the signed-in user")

    schema_fqn = f"{catalog}.{schema}"
    cache: dict[tuple[str, str], set[str] | str] = {}

    # Ownership is authoritative and OBO-readable. An owner of the target schema
    # (or catalog) can create a metric view there, so the three-privilege triple
    # is a decomposition of "can this user create in catalog.schema" that
    # ownership answers YES to directly. Owning the schema also implies catalog
    # USE (you cannot own/operate a schema without it), which is why owning the
    # schema satisfies the USE_CATALOG check too.
    owner_cache: dict[str, str | None] = {}
    owns_catalog = _principal_owns(
        _securable_owner(ws, SecurableType.CATALOG.value, catalog, owner_cache),
        principal, user_groups,
    )
    owns_schema = owns_catalog or _principal_owns(
        _securable_owner(ws, SecurableType.SCHEMA.value, schema_fqn, owner_cache),
        principal, user_groups,
    )

    def _table_owned(table: str) -> bool:
        """SELECT is satisfied by owning the table or any ancestor of it."""
        if owns_catalog:
            return True
        parts = table.split(".")
        if len(parts) == 3:
            tbl_schema = f"{parts[0]}.{parts[1]}"
            if tbl_schema == schema_fqn and owns_schema:
                return True
            if _principal_owns(
                _securable_owner(ws, SecurableType.SCHEMA.value, tbl_schema, owner_cache),
                principal, user_groups,
            ):
                return True
        return _principal_owns(
            _securable_owner(ws, SecurableType.TABLE.value, table, owner_cache),
            principal, user_groups,
        )

    checks: list[tuple[str, str, str, bool]] = [
        ("USE_CATALOG", SecurableType.CATALOG.value, catalog, owns_catalog or owns_schema),
        ("USE_SCHEMA", SecurableType.SCHEMA.value, schema_fqn, owns_schema),
        ("CREATE_TABLE", SecurableType.SCHEMA.value, schema_fqn, owns_schema),
    ]
    checks += [
        ("SELECT", SecurableType.TABLE.value, table, _table_owned(table))
        for table in dict.fromkeys(tables)
    ]

    privileges = [
        _privilege_row(
            ws, privilege=privilege, securable_type=securable_type,
            full_name=full_name, principal=principal, cache=cache, owned=owned,
        )
        for privilege, securable_type, full_name, owned in checks
    ]

    space_row = _space_manage_row(ws, space_id, principal, user_groups)
    if space_row is not None:
        privileges.append(space_row)

    probed_warehouse = (
        warehouse_id if warehouse_id is not None else _default_warehouse_id()
    )
    runtime_kind, version = _observed_runtime(ws, probed_warehouse)
    capabilities = _capability_rows(runtime_kind, version, probed_warehouse)

    missing = [row.label for row in privileges if row.status != "GRANTED"]
    missing += [
        f"{row.label} (requires DBR {row.required_dbr})"
        for row in capabilities
        if row.status == "DENIED" and not row.optional
    ]

    verdict = _verdict(privileges, capabilities)
    results: dict[str, MvCheckStatus] = {row.label: row.status for row in privileges}
    results.update({row.capability: row.status for row in capabilities})

    return MvProbeResult(
        probe_id=uuid.uuid4().hex,
        checked_as=principal,
        target=schema_fqn,
        checked_at=_now_iso(),
        results=results,
        privileges=privileges,
        capabilities=capabilities,
        verdict=verdict,
        missing=missing,
        remediation_sql=_remediation_sql(privileges, principal, catalog, schema),
    )


def _default_warehouse_id() -> str:
    return os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", "")


def _space_manage_row(
    ws: Any, space_id: str, principal: str, user_groups: set[str],
) -> MvPrivilegeRow | None:
    """Check CAN_MANAGE on the Genie Agent whose config would be patched.

    CAN_EDIT is not enough: attaching the view rewrites
    ``data_sources.metric_views``, which is a manage-level change.
    """
    space_id = (space_id or "").strip()
    if not space_id:
        return None
    from genie_space_optimizer.common.genie_client import user_can_manage_space

    can_manage = user_can_manage_space(
        ws, space_id, user_email=principal, user_groups=user_groups,
        acl_client=get_service_principal_client(),
    )
    return MvPrivilegeRow(
        label=f"CAN MANAGE on Genie Agent {space_id}",
        privilege="CAN_MANAGE",
        securable=space_id,
        status="GRANTED" if can_manage else "DENIED",
        detail=None if can_manage else f"{principal} lacks CAN MANAGE on the Genie Agent",
    )


def _verdict(
    privileges: list[MvPrivilegeRow], capabilities: list[MvCapabilityRow],
) -> str:
    """SUFFICIENT only when every required check is affirmatively GRANTED.

    Capability rows are required only when they are decidably below the floor:
    ``mv_create_edit`` DENIED is a real block, while its ``UNKNOWN`` is left to
    the write itself, and the optional rows only shape the YAML.
    """
    statuses = [row.status for row in privileges]
    statuses += [
        row.status for row in capabilities
        if row.capability == MV_CAPABILITY_CREATE_EDIT and row.status == "DENIED"
    ]
    if any(status == "DENIED" for status in statuses):
        return "INSUFFICIENT"
    if any(status == "UNKNOWN" for status in statuses):
        return "UNKNOWN"
    return "SUFFICIENT"


def _remediation_sql(
    privileges: list[MvPrivilegeRow], principal: str, catalog: str, schema: str,
) -> str | None:
    """Render the GRANT statements that would close every privilege gap.

    Text only — the app never executes these. The grantor has to be someone who
    can grant, which is usually not the person reading the probe.
    """
    statements: list[str] = []
    for row in privileges:
        if row.status == "GRANTED" or row.privilege == "CAN_MANAGE":
            continue
        privilege = row.privilege.replace("_", " ")
        if row.securable == catalog:
            securable = f"CATALOG {_ident(catalog)}"
        elif row.securable == f"{catalog}.{schema}":
            securable = f"SCHEMA {_ident(catalog)}.{_ident(schema)}"
        else:
            parts = row.securable.split(".")
            securable = "TABLE " + ".".join(_ident(part) for part in parts)
        statements.append(f"GRANT {privilege} ON {securable} TO {_ident(principal)};")
    return "\n".join(statements) if statements else None


# ── Consent ──────────────────────────────────────────────────────────────


def record_consent(
    result: MvProbeResult,
    *,
    materialize_consented: bool = False,
    run_id: str | None = None,
) -> bool:
    """Persist the probe as a consent record; return whether the write landed.

    Written through the SQL-warehouse twin of ``mv_state.upsert_mv_consent``
    because the backend has no SparkSession. ``materialize_consented`` is a
    separate decision from create-and-attach and defaults to False, so a user
    who consents to a view has not thereby consented to materialize it.
    """
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "")
    warehouse_id = _default_warehouse_id()
    if not (catalog and schema and warehouse_id):
        logger.warning("Metric view consent not persisted: GSO storage is not configured")
        return False

    from genie_space_optimizer.common.warehouse import wh_upsert_mv_consent

    target_catalog, _, target_schema = result.target.partition(".")
    try:
        wh_upsert_mv_consent(
            get_service_principal_client(),
            warehouse_id,
            catalog=catalog,
            schema=schema,
            probe_id=result.probe_id,
            granted_by=result.checked_as,
            target_catalog=target_catalog,
            target_schema=target_schema,
            verdict=result.verdict,
            run_id=run_id,
            materialize_consented=materialize_consented,
            probe_results=result.model_dump(mode="json"),
            granted_at=result.checked_at,
        )
        return True
    except Exception as exc:  # noqa: BLE001 - a probe is still useful unpersisted
        logger.warning("Could not persist metric view consent: %s", exc, exc_info=True)
        return False


def verify(
    consent: dict[str, Any] | None, fresh_probe: MvProbeResult,
) -> MvConsentVerification:
    """Re-verify a recorded consent against a probe taken just now.

    Called by the trigger flow immediately before any OBO create. Downgrades
    only: ``create_and_attach`` survives exactly when the fresh probe is still
    SUFFICIENT for the same identity, the same target, and the same compute that
    was consented to. A stored verdict is never allowed to override a worse fresh
    one.

    A missing consent row is a downgrade, never an absence that waves the write
    through. Because :func:`record_consent` is best-effort, that row can be
    absent in production after a successful probe, so "no record" has to mean
    "not authorized" — the alternative would let a warehouse hiccup during the
    probe silently widen what the trigger is allowed to do.
    """
    if not consent:
        return MvConsentVerification(
            probe_id=fresh_probe.probe_id,
            effective_mode="suggest_only",
            verdict=fresh_probe.verdict,
            downgrade_reason="no consent record was found for this probe",
            fresh_probe=fresh_probe,
        )

    probe_id = str(consent.get("probe_id") or fresh_probe.probe_id)
    stored_verdict = str(consent.get("verdict") or "UNKNOWN").upper()
    stored_target = ".".join(
        part for part in (consent.get("target_catalog"), consent.get("target_schema")) if part
    )
    granted_by = (consent.get("granted_by") or "").strip().lower()
    stored_results = consent.get("probe_results")
    stored_warehouse = _observed_warehouse(
        (stored_results or {}).get("capabilities") if isinstance(stored_results, dict) else None
    )
    fresh_warehouse = _observed_warehouse(fresh_probe.capabilities)

    reason: str | None = None
    if stored_verdict != "SUFFICIENT":
        reason = f"consent was recorded with verdict {stored_verdict}"
    elif granted_by and granted_by != fresh_probe.checked_as.strip().lower():
        reason = (
            f"consent was granted by {consent.get('granted_by')} but the probe ran as "
            f"{fresh_probe.checked_as}"
        )
    elif stored_target and stored_target != fresh_probe.target:
        reason = (
            f"consent targets {stored_target} but the probe checked {fresh_probe.target}"
        )
    elif stored_warehouse and stored_warehouse != fresh_warehouse:
        # A capability is a property of the compute. Consenting on warehouse A
        # and writing on warehouse B carries a claim nothing observed.
        reason = (
            f"capabilities were observed on warehouse {stored_warehouse} but "
            f"re-verification ran on {fresh_warehouse or 'no warehouse'}"
        )
    elif fresh_probe.verdict != "SUFFICIENT":
        reason = (
            f"re-verification returned {fresh_probe.verdict}: "
            + ("; ".join(fresh_probe.missing) or "no detail reported")
        )

    return MvConsentVerification(
        probe_id=probe_id,
        effective_mode="suggest_only" if reason else "create_and_attach",
        verdict=fresh_probe.verdict,
        downgrade_reason=reason,
        fresh_probe=fresh_probe,
    )


__all__ = [
    "MvProbeError",
    "probe",
    "record_consent",
    "verify",
]
