"""Shared MLflow Prompt Registry probe.

Used by three layers of defense:
- Workbench ``/permissions/{space_id}`` — fast UI-time feedback (read probe).
- Workbench ``/trigger`` — server-side gate (read probe) before launching the job.
- GSO preflight (``jobs/run_preflight.py`` + ``optimization/preflight.py``) —
  write-path probe that exercises the exact ``mlflow.genai.register_prompt``
  call the baseline eval will later make, failing the job fast rather than
  half-way through baseline if the feature/permissions are missing.

Design invariants (Bug #1 follow-up — "probe–workload parity"):

1. **SDK-native probe.** The read probe calls ``mlflow.genai.search_prompts``
   — the same library ``register_prompt`` uses in the job. We do NOT encode
   a Databricks REST URL in this module. If Databricks moves the endpoint,
   MLflow ships the fix and our probe tracks it. Hand-rolled
   ``api_client.do("GET"/"POST", …)`` for feature-availability is forbidden.
2. **Fail-closed.** ``ProbeResult.available`` is True only when the positive
   code path completed without exception. Any non-OK exit, including
   unexpected exceptions, leaves ``available = False``.
3. **Closed-world classifier.** Every unrecognized vendor ``error_code`` is
   ``reason_code == "vendor_bug"`` with ``actionable_by = "platform"`` —
   never silently rendered as a customer-actionable message. A
   ``gso.prompt_registry.vendor_bug`` log line is emitted for alerting.
4. **Two-axis outcome.** ``actionable_by`` distinguishes customer-actionable
   failures (admin flips a toggle, grants a privilege) from platform-
   actionable failures (our bug or Databricks' bug). UI, HTTP status codes,
   and alert routing all branch off this axis.

This module lives in the GSO package because the preflight job runs with only
the GSO wheel installed; the Workbench backend also imports from the GSO
package (e.g. ``trigger_optimization``), so hosting the shared probe here is
the natural place that both can use.
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

ProbeMode = Literal["read", "write"]

# ── Stable reason codes ──────────────────────────────────────────────────
# These are the contract between the probe and its consumers (UI, alerting,
# tests). The UI switches its rendered copy off these strings; changing them
# is a breaking change. ``REASON_UNKNOWN`` is retained for back-compat with
# external consumers that keyed off it during the pre-Bug-#1-follow-up
# window, but no new code path emits it — unclassified errors are
# ``REASON_VENDOR_BUG``.
REASON_FEATURE_NOT_ENABLED = "feature_not_enabled"       # customer-actionable
REASON_MISSING_UC_PERMISSIONS = "missing_uc_permissions"  # customer-actionable
REASON_REGISTRY_PATH_NOT_FOUND = "registry_path_not_found"  # customer-actionable
REASON_MISSING_SP_SCOPE = "missing_sp_scope"             # customer-actionable (redeploy)
REASON_VENDOR_BUG = "vendor_bug"                         # platform-actionable
REASON_UNKNOWN = "unknown"                               # deprecated; do not emit
REASON_OK = "ok"
REASON_PROBE_ERROR = "probe_error"                       # our code bug (bad args)

ACTIONABLE_BY_CUSTOMER = "customer"
ACTIONABLE_BY_PLATFORM = "platform"

# Closed-world sets — touch these together with the matching UI branch.
_CUSTOMER_REASONS: frozenset[str] = frozenset({
    REASON_FEATURE_NOT_ENABLED,
    REASON_MISSING_UC_PERMISSIONS,
    REASON_REGISTRY_PATH_NOT_FOUND,
    REASON_MISSING_SP_SCOPE,
})

# Vendor error codes that indicate a platform-side mismatch: the URL we
# (or our SDK) asked for does not exist, has invalid params, or returned an
# internal error. None of these are customer-actionable.
_VENDOR_BUG_CODES: frozenset[str] = frozenset({
    "ENDPOINT_NOT_FOUND",
    "INVALID_PARAMETER_VALUE",
    "INVALID_STATE",
    "INTERNAL_ERROR",
    "IO_ERROR",
    "TEMPORARILY_UNAVAILABLE",
    "REQUEST_LIMIT_EXCEEDED",  # transient; customer can retry, we should alert
    "DEADLINE_EXCEEDED",
})

# Vendor codes that mean "the SP token is missing a scope or isn't
# authenticated" — customer-actionable because the workspace admin or the
# app deployer needs to redeploy / re-grant scope.
_SCOPE_CODES: frozenset[str] = frozenset({
    "UNAUTHENTICATED",
    "INSUFFICIENT_OAUTH_SCOPE",
})

# Kept in sync with ``evaluation.PROMPT_REGISTRY_REQUIRED_PRIVILEGES`` — we
# duplicate it here rather than importing so the probe stays independent of
# ``evaluation.py`` (which transitively imports heavy MLflow/pyspark deps
# that break in unit-test and lightweight-job contexts).
PROMPT_REGISTRY_REQUIRED_PRIVILEGES: tuple[str, ...] = (
    "CREATE FUNCTION", "EXECUTE", "MANAGE",
)


# ── ProbeResult ──────────────────────────────────────────────────────────


@dataclass
class ProbeResult:
    """Outcome of a prompt-registry probe.

    ``available`` is True only when the positive code path completed without
    exception. Any non-OK exit (including unexpected exceptions) MUST leave
    ``available = False`` — fail-closed is an invariant enforced by tests.

    Two-axis outcome (Bug #1 follow-up):
    - ``reason_code`` — fine-grained stable code, UI picks rendered copy.
    - ``actionable_by`` — ``"customer"`` or ``"platform"``. Drives:
      * UI chip color (blocker vs. platform-notice)
      * HTTP status on /trigger (412 vs. 503)
      * Alert routing (customer support vs. on-call)

    ``vendor_error_code`` is the raw ``DatabricksError.error_code`` string
    (or ``None``). Surfaced verbatim in the UI mono block so operators can
    grep logs without a round-trip.
    """

    available: bool = False
    reason_code: str = REASON_VENDOR_BUG
    # Default is platform-actionable: unknown failures are OUR problem, not
    # the customer's, until proven otherwise by an explicit classifier hit.
    actionable_by: str = ACTIONABLE_BY_PLATFORM
    user_message: str = ""
    raw_error: str | None = None
    vendor_error_code: str | None = None
    missing_privileges: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)


# ── Error classification ─────────────────────────────────────────────────


def _extract_error_code(exc: BaseException) -> str | None:
    """Return the structured Databricks error code if present.

    The Databricks SDK attaches ``error_code`` on ``DatabricksError`` subclasses.
    We prefer that over ``str(exc)`` to avoid brittle locale/wording matching.
    """
    code = getattr(exc, "error_code", None)
    if isinstance(code, str) and code:
        return code
    return None


def _actionable_for(reason: str) -> str:
    """Two-axis label derived from the stable reason code."""
    if reason in _CUSTOMER_REASONS:
        return ACTIONABLE_BY_CUSTOMER
    if reason == REASON_OK:
        return ACTIONABLE_BY_CUSTOMER  # trivially not our problem
    if reason == REASON_PROBE_ERROR:
        # probe_error means OUR code was called wrong (bad args, missing
        # env) — platform-actionable from the customer's perspective.
        return ACTIONABLE_BY_PLATFORM
    # vendor_bug, unknown (legacy), anything else → platform.
    return ACTIONABLE_BY_PLATFORM


def _classify_by_message(message: str, uc_schema: str | None) -> dict[str, Any]:
    """Text-based fallback for exceptions without a structured ``error_code``.

    Mirrors the buckets that ``evaluation._classify_prompt_registration_error``
    uses for the write path so the UI renders the same copy regardless of
    whether the read or write probe caught the error.
    """
    lowered = (message or "").lower()
    permission_markers = (
        "permission",
        "privilege",
        "not authorized",
        "forbidden",
        "insufficient",
        "access denied",
        "permission_denied",
    )
    schema_target = uc_schema or "<catalog>.<schema>"

    if any(marker in lowered for marker in permission_markers):
        missing = [
            p for p in PROMPT_REGISTRY_REQUIRED_PRIVILEGES if p.lower() in lowered
        ] or list(PROMPT_REGISTRY_REQUIRED_PRIVILEGES)
        return {
            "reason": REASON_MISSING_UC_PERMISSIONS,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": missing,
            "remediation": (
                f"Grant {', '.join(missing)} on schema {schema_target} to the "
                "Databricks App service principal used by job tasks."
            ),
        }

    if (
        "feature_disabled" in lowered
        or ("not enabled" in lowered and ("prompt" in lowered or "registry" in lowered))
        or ("preview" in lowered and ("prompt" in lowered or "genai" in lowered))
    ):
        return {
            "reason": REASON_FEATURE_NOT_ENABLED,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": [],
            "remediation": (
                "Enable MLflow Prompt Registry on the workspace. "
                "Contact your workspace admin or enable the GenAI preview in workspace settings."
            ),
        }

    if "does not exist" in lowered or "resource_does_not_exist" in lowered:
        return {
            "reason": REASON_REGISTRY_PATH_NOT_FOUND,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": [],
            "remediation": f"Verify catalog/schema exists and is accessible: {schema_target}.",
        }

    # Unrecognized message text → closed-world platform bucket.
    return {
        "reason": REASON_VENDOR_BUG,
        "actionable_by": ACTIONABLE_BY_PLATFORM,
        "missing_privileges": [],
        "remediation": (
            "Prompt Registry returned an unrecognized error; this is a "
            "platform issue. Retry; if it persists, contact FE support."
        ),
    }


def _classify_exception(exc: BaseException, uc_schema: str | None) -> dict[str, Any]:
    """Map an exception to a stable reason bucket.

    Closed-world by design: anything we don't explicitly recognize becomes
    ``REASON_VENDOR_BUG`` with ``actionable_by = "platform"``. That prevents
    a new Databricks error code from silently rendering as a customer-
    actionable message (the Bug #1 follow-up root cause).
    """
    error_code = _extract_error_code(exc)
    schema_target = uc_schema or "<catalog>.<schema>"

    # ── Customer-actionable buckets (known error codes) ────────────────
    if error_code == "FEATURE_DISABLED":
        return {
            "reason": REASON_FEATURE_NOT_ENABLED,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": [],
            "remediation": (
                "Enable MLflow Prompt Registry on the workspace. "
                "Contact your workspace admin or enable the GenAI preview in workspace settings."
            ),
            "error_code": error_code,
        }
    if error_code in ("PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS"):
        return {
            "reason": REASON_MISSING_UC_PERMISSIONS,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": list(PROMPT_REGISTRY_REQUIRED_PRIVILEGES),
            "remediation": (
                f"Grant {', '.join(PROMPT_REGISTRY_REQUIRED_PRIVILEGES)} on schema {schema_target} "
                "to the Databricks App service principal used by job tasks."
            ),
            "error_code": error_code,
        }
    if error_code in ("RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"):
        return {
            "reason": REASON_REGISTRY_PATH_NOT_FOUND,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": [],
            "remediation": f"Verify catalog/schema exists and is accessible: {schema_target}.",
            "error_code": error_code,
        }
    if error_code in _SCOPE_CODES:
        return {
            "reason": REASON_MISSING_SP_SCOPE,
            "actionable_by": ACTIONABLE_BY_CUSTOMER,
            "missing_privileges": [],
            "remediation": (
                "The app's service principal token is missing the MLflow Prompt Registry "
                "scope. Redeploy the Genie Workbench app so its OAuth token picks up the "
                "current workspace preview scopes, or have a UC admin re-grant the SP access."
            ),
            "error_code": error_code,
        }

    # ── Platform-actionable: known vendor-bug codes ────────────────────
    if error_code in _VENDOR_BUG_CODES:
        return {
            "reason": REASON_VENDOR_BUG,
            "actionable_by": ACTIONABLE_BY_PLATFORM,
            "missing_privileges": [],
            "remediation": (
                "The Prompt Registry call hit a platform-side error. This is not "
                "something a workspace admin can fix. Retry; if it persists, contact "
                "Databricks FE support with the error_code and run id."
            ),
            "error_code": error_code,
        }

    # ── Free-text classifier fallback ──────────────────────────────────
    # Some older/inconsistent responses don't carry a structured
    # ``error_code``; fall back to message matching. The fallback is also
    # closed-world: unmatched text → vendor_bug.
    classified = _classify_by_message(str(exc), uc_schema)
    classified["error_code"] = error_code
    classified.setdefault("missing_privileges", [])
    classified.setdefault("actionable_by", _actionable_for(classified["reason"]))
    return classified


def _build_user_message(reason: str, remediation: str, error_code: str | None = None) -> str:
    """Human-readable message. Paired with ``reason_code`` so the UI can swap it."""
    if reason == REASON_FEATURE_NOT_ENABLED:
        return (
            "MLflow Prompt Registry is not enabled on this workspace. "
            "Contact your workspace admin to enable it."
        )
    if reason == REASON_MISSING_UC_PERMISSIONS:
        return (
            "The service principal does not have permission to use MLflow Prompt Registry. "
            + remediation
        )
    if reason == REASON_REGISTRY_PATH_NOT_FOUND:
        return "MLflow Prompt Registry target schema was not found. " + remediation
    if reason == REASON_MISSING_SP_SCOPE:
        return (
            "The app's service principal is missing the Prompt Registry OAuth scope. "
            + remediation
        )
    if reason == REASON_VENDOR_BUG:
        code_hint = f" (error_code: {error_code})" if error_code else ""
        return (
            "MLflow Prompt Registry returned an unexpected platform error"
            f"{code_hint}. The error has been logged server-side. Click "
            "Re-check to retry; if the problem persists, contact Databricks "
            "FE support with the run id and the error_code below."
        )
    # Legacy REASON_UNKNOWN or anything else we forgot — never render as
    # "admin go enable the toggle".
    return (
        "MLflow Prompt Registry is unavailable. "
        "See logs for details or contact your workspace admin."
    )


def _probe_result_from_classification(
    classification: dict[str, Any],
    exc: BaseException,
    *,
    mode: ProbeMode,
    probe_name: str | None = None,
) -> ProbeResult:
    """Build a ProbeResult from a classifier output + the raw exception.

    Centralised so ``_probe_read`` and ``_probe_write`` share exactly the
    same shape — probe-workload parity also means probe-result parity.
    """
    reason = classification.get("reason", REASON_VENDOR_BUG)
    actionable_by = classification.get("actionable_by") or _actionable_for(reason)
    error_code = classification.get("error_code")

    # Platform-actionable failures are OUR bug (or Databricks'). Emit a
    # distinct ERROR-level structured log so alerting can fire on it
    # without grep-matching free-form warnings.
    if actionable_by == ACTIONABLE_BY_PLATFORM and reason != REASON_OK:
        try:
            import mlflow as _mlflow  # type: ignore
            mlflow_version = getattr(_mlflow, "__version__", "?")
        except Exception:  # noqa: BLE001
            mlflow_version = "?"
        logger.error(
            "gso.prompt_registry.vendor_bug code=%s reason=%s mode=%s "
            "mlflow=%s err=%s",
            error_code,
            reason,
            mode,
            mlflow_version,
            str(exc)[:300],
        )
    else:
        logger.warning(
            "Prompt Registry probe failed: code=%s reason=%s mode=%s err=%s",
            error_code,
            reason,
            mode,
            str(exc)[:300],
        )

    diagnostics: dict[str, Any] = {"mode": mode, "error_code": error_code}
    if probe_name:
        diagnostics["probe_name"] = probe_name

    return ProbeResult(
        available=False,
        reason_code=reason,
        actionable_by=actionable_by,
        user_message=_build_user_message(
            reason, classification.get("remediation", ""), error_code,
        ),
        raw_error=str(exc)[:1000],
        vendor_error_code=error_code,
        missing_privileges=classification.get("missing_privileges", []) or [],
        diagnostics=diagnostics,
    )


# ── Read probe (SDK-native) ──────────────────────────────────────────────


# Unity Catalog identifiers: letters, digits, underscore, hyphen. We reject
# anything else so single-quote interpolation into the filter_string is safe
# and we never send a malformed filter to the SDK.
_UC_IDENT_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_\-]*$")


def _build_uc_filter_string(uc_schema: str | None) -> str:
    """Build the filter_string for ``mlflow.genai.search_prompts``.

    Unity Catalog prompt registries require filters of exactly the shape::

        catalog = 'catalog_name' AND schema = 'schema_name'

    Any other shape (``name LIKE '...'``, a free-form clause, etc.) returns
    ``INVALID_PARAMETER_VALUE`` from the server. We split ``uc_schema`` on
    the first dot, validate both identifiers, and only emit a filter when
    both are safe — otherwise return ``""`` to probe without a filter
    (which still validates the feature gate; permission errors will be
    broader but the probe won't lie about ``INVALID_PARAMETER_VALUE``).
    """
    if not uc_schema or "." not in uc_schema:
        return ""
    catalog, _, schema = uc_schema.partition(".")
    if not _UC_IDENT_RE.match(catalog) or not _UC_IDENT_RE.match(schema):
        logger.warning(
            "Prompt Registry probe received uc_schema with unexpected chars; "
            "probing without filter. uc_schema=%r",
            uc_schema,
        )
        return ""
    return f"catalog = '{catalog}' AND schema = '{schema}'"


def _probe_read(uc_schema: str | None) -> ProbeResult:
    """Probe Prompt Registry via the SAME MLflow SDK the job uses.

    Calls ``mlflow.genai.search_prompts`` — the SDK owns the URL so the probe
    tracks upstream changes automatically. Scoped to ``uc_schema`` when
    available so permission errors classify accurately.

    Auth is picked up from the process environment (DATABRICKS_HOST +
    DATABRICKS_CLIENT_ID/SECRET on Databricks Apps), which matches what
    ``mlflow.genai.register_prompt`` will use in the job. That is the
    entire point: probe and workload share one auth + one URL surface.
    """
    try:
        import mlflow  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        # mlflow failed to import — this is our packaging/env problem,
        # not the customer's. Mark platform-actionable.
        logger.error(
            "gso.prompt_registry.vendor_bug code=MLFLOW_IMPORT_FAILED "
            "reason=%s mode=read err=%s",
            REASON_VENDOR_BUG,
            str(exc)[:300],
        )
        return ProbeResult(
            available=False,
            reason_code=REASON_VENDOR_BUG,
            actionable_by=ACTIONABLE_BY_PLATFORM,
            user_message=_build_user_message(REASON_VENDOR_BUG, "", "MLFLOW_IMPORT_FAILED"),
            raw_error=str(exc)[:1000],
            vendor_error_code="MLFLOW_IMPORT_FAILED",
            diagnostics={"mode": "read", "probe": "mlflow.genai.search_prompts"},
        )

    search_fn = getattr(getattr(mlflow, "genai", None), "search_prompts", None)
    if not callable(search_fn):
        # Pinned mlflow predates search_prompts. Our own packaging issue.
        logger.error(
            "gso.prompt_registry.vendor_bug code=SDK_SYMBOL_MISSING "
            "reason=%s mode=read mlflow=%s",
            REASON_VENDOR_BUG,
            getattr(mlflow, "__version__", "?"),
        )
        return ProbeResult(
            available=False,
            reason_code=REASON_VENDOR_BUG,
            actionable_by=ACTIONABLE_BY_PLATFORM,
            user_message=_build_user_message(REASON_VENDOR_BUG, "", "SDK_SYMBOL_MISSING"),
            raw_error="mlflow.genai.search_prompts not available in this mlflow version",
            vendor_error_code="SDK_SYMBOL_MISSING",
            diagnostics={
                "mode": "read",
                "probe": "mlflow.genai.search_prompts",
                "mlflow_version": getattr(mlflow, "__version__", "?"),
            },
        )

    # Scope the search to our target catalog.schema. Unity Catalog prompt
    # registries REQUIRE a filter_string of exactly the shape
    # ``catalog = 'X' AND schema = 'Y'`` — anything else (including
    # ``name LIKE 'X.Y.%'``) returns INVALID_PARAMETER_VALUE. We learned
    # this the hard way; see tests/unit/test_prompt_registry_probe.py::
    # test_read_probe_uses_uc_catalog_schema_filter_format.
    filter_string = _build_uc_filter_string(uc_schema)

    try:
        search_fn(filter_string=filter_string, max_results=1)
    except TypeError:
        # Older/newer SDK signatures may not accept filter_string or
        # max_results. Retry with no kwargs before giving up — we still
        # just want to see whether the feature gate lets us in.
        try:
            search_fn()
        except Exception as exc:  # noqa: BLE001
            classification = _classify_exception(exc, uc_schema)
            return _probe_result_from_classification(classification, exc, mode="read")
    except Exception as exc:  # noqa: BLE001 — fail-closed on any exception
        classification = _classify_exception(exc, uc_schema)
        return _probe_result_from_classification(classification, exc, mode="read")

    return ProbeResult(
        available=True,
        reason_code=REASON_OK,
        actionable_by=ACTIONABLE_BY_CUSTOMER,
        user_message="",
        raw_error=None,
        vendor_error_code=None,
        diagnostics={
            "mode": "read",
            "probe": "mlflow.genai.search_prompts",
            "scoped_to": uc_schema or "",
        },
    )


# ── Write probe (preflight, unchanged behaviour) ─────────────────────────


def _probe_write(uc_schema: str, probe_name_hint: str | None = None) -> ProbeResult:
    """Register a throwaway prompt under ``uc_schema`` then clean it up.

    Exercises the EXACT API path that ``register_judge_prompts`` uses during
    baseline evaluation, which is the real failure mode we are guarding
    against. Runs under whatever identity the caller's mlflow client uses
    (the job SP inside preflight). If register succeeds we clean up on the way
    out; if it fails we still attempt cleanup in case a partial write landed.
    """
    if not uc_schema or "." not in uc_schema:
        return ProbeResult(
            available=False,
            reason_code=REASON_PROBE_ERROR,
            actionable_by=ACTIONABLE_BY_PLATFORM,  # our bug: caller passed bad args
            user_message=(
                "Prompt Registry write probe requires a UC schema "
                "(catalog.schema); received empty or malformed value."
            ),
            raw_error=f"invalid uc_schema: {uc_schema!r}",
            diagnostics={"mode": "write"},
        )

    # Mirror the validation from ``_build_uc_filter_string`` (and the read
    # probe path) so both halves of ``uc_schema`` are SQL-identifier-safe
    # before we interpolate them into the ``DROP FUNCTION`` fallback inside
    # ``_cleanup_probe_prompt``. ``mlflow.genai.register_prompt`` will itself
    # reject a bad identifier, but the fallback cleanup runs on `finally`
    # and can still see the raw value — so we fail closed here.
    _catalog, _, _schema = uc_schema.partition(".")
    if not _UC_IDENT_RE.match(_catalog) or not _UC_IDENT_RE.match(_schema):
        return ProbeResult(
            available=False,
            reason_code=REASON_PROBE_ERROR,
            actionable_by=ACTIONABLE_BY_PLATFORM,
            user_message=(
                "Prompt Registry write probe received a UC schema with "
                "unexpected characters; refusing to proceed."
            ),
            raw_error=f"invalid uc_schema identifier: {uc_schema!r}",
            diagnostics={"mode": "write"},
        )

    import mlflow  # type: ignore

    suffix = probe_name_hint or uuid.uuid4().hex[:8]
    # Restrict the suffix to the same identifier charset so the final FQN
    # stays safe under the DROP FUNCTION fallback. UUID hex always passes.
    if not _UC_IDENT_RE.match(suffix):
        suffix = uuid.uuid4().hex[:8]
    probe_name = f"{uc_schema}.genie_opt_probe_{suffix}"

    try:
        version = mlflow.genai.register_prompt(
            name=probe_name,
            template="Genie auto-optimize probe prompt. Safe to delete.",
            commit_message="GSO preflight write-path probe",
            tags={"type": "probe", "transient": "true"},
        )
        logger.info(
            "Prompt Registry write probe succeeded: name=%s version=%s",
            probe_name,
            getattr(version, "version", "?"),
        )
        return ProbeResult(
            available=True,
            reason_code=REASON_OK,
            actionable_by=ACTIONABLE_BY_CUSTOMER,
            user_message="",
            raw_error=None,
            vendor_error_code=None,
            diagnostics={"mode": "write", "probe_name": probe_name},
        )
    except Exception as exc:  # noqa: BLE001 - fail-closed
        classification = _classify_exception(exc, uc_schema)
        return _probe_result_from_classification(
            classification, exc, mode="write", probe_name=probe_name,
        )
    finally:
        _cleanup_probe_prompt(probe_name)


def _cleanup_probe_prompt(probe_fqn: str) -> None:
    """Best-effort cleanup of the transient probe prompt.

    Tries ``mlflow.genai.delete_prompt`` first; if unavailable in the installed
    mlflow version, falls back to ``DROP FUNCTION IF EXISTS`` (UC-backed
    prompts are functions — see ``_try_drop_prompt`` in evaluation.py).
    Failures are logged at WARNING only; operators can sweep ``genie_opt_probe_*``.
    """
    try:
        import mlflow  # type: ignore

        delete_fn = getattr(getattr(mlflow, "genai", None), "delete_prompt", None)
        if callable(delete_fn):
            delete_fn(name=probe_fqn)
            logger.info("Cleaned up probe prompt: %s", probe_fqn)
            return
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "mlflow.genai.delete_prompt failed for probe %s; falling back to DROP FUNCTION. err=%s",
            probe_fqn,
            str(exc)[:200],
        )

    try:
        from pyspark.sql import SparkSession  # type: ignore

        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.sql(f"DROP FUNCTION IF EXISTS {probe_fqn}")
            logger.info("Dropped probe prompt via DROP FUNCTION: %s", probe_fqn)
            return
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not clean up probe prompt %s; may require manual cleanup. err=%s",
            probe_fqn,
            str(exc)[:200],
        )


# ── TTL cache ────────────────────────────────────────────────────────────
# The /permissions endpoint is polled by the UI; the /trigger endpoint calls
# this too. We cache per (sp_identity, uc_schema, mode) so repeated polls
# don't hammer the SDK. Callers can bypass with ``bypass_cache=True`` — the
# UI's "Re-check" button and the /trigger gate should always bypass.

_PROBE_CACHE: dict[tuple[str, str, str], tuple[float, ProbeResult]] = {}
_PROBE_TTL_SECONDS = int(os.environ.get("GSO_PROMPT_REGISTRY_PROBE_TTL_SECONDS", "300") or "300")


def _cache_key(sp_ws: Any, uc_schema: str | None, mode: ProbeMode) -> tuple[str, str, str]:
    client_id = ""
    if sp_ws is not None:
        cfg = getattr(sp_ws, "config", None)
        client_id = getattr(cfg, "client_id", "") or ""
    return (str(client_id), uc_schema or "", str(mode))


def _clear_probe_cache() -> None:
    """Test-only hook: flush the in-process probe cache."""
    _PROBE_CACHE.clear()


# ── Public entrypoint ────────────────────────────────────────────────────


def check_prompt_registry(
    sp_ws: Any = None,
    *,
    mode: ProbeMode = "read",
    uc_schema: str | None = None,
    probe_name_hint: str | None = None,
    bypass_cache: bool = False,
) -> ProbeResult:
    """Fail-closed probe of MLflow Prompt Registry availability.

    Args:
        sp_ws: ``WorkspaceClient`` for the read probe. Required for
            ``mode="read"`` so we can key the cache by identity and reject
            callers that accidentally omitted SP scoping. The SDK itself
            picks up auth from env; ``sp_ws`` is not passed to MLflow.
        mode: ``"read"`` performs ``mlflow.genai.search_prompts``;
            ``"write"`` registers and deletes a throwaway prompt under
            ``uc_schema``.
        uc_schema: Target ``catalog.schema``. Required for ``mode="write"``;
            used to scope the read probe when provided. When omitted the
            read probe runs unscoped (still a valid feature-availability
            check).
        probe_name_hint: Optional suffix for the probe prompt name (e.g.
            first 8 chars of a run_id). Defaults to a random hex suffix.
        bypass_cache: If True, ignore any cached result and re-probe.
            ``/trigger`` passes True; the UI's Re-check button should too.

    Returns:
        ``ProbeResult`` — ``available`` is True only on the positive path.
    """
    if mode == "read":
        if sp_ws is None:
            # Fail-closed: callers must pass an SP identity so cache keying
            # and future audit logging work. Also keeps the integration
            # contract with /permissions and /trigger explicit.
            return ProbeResult(
                available=False,
                reason_code=REASON_PROBE_ERROR,
                actionable_by=ACTIONABLE_BY_PLATFORM,
                user_message="Prompt Registry read probe requires a workspace client.",
                raw_error="sp_ws is None",
                diagnostics={"mode": "read"},
            )

        key = _cache_key(sp_ws, uc_schema, "read")
        now = time.monotonic()
        if not bypass_cache:
            cached = _PROBE_CACHE.get(key)
            if cached and (now - cached[0]) < _PROBE_TTL_SECONDS:
                return cached[1]

        result = _probe_read(uc_schema)
        _PROBE_CACHE[key] = (now, result)
        return result

    if mode == "write":
        if not uc_schema:
            return ProbeResult(
                available=False,
                reason_code=REASON_PROBE_ERROR,
                actionable_by=ACTIONABLE_BY_PLATFORM,
                user_message=(
                    "Prompt Registry write probe requires a UC schema (catalog.schema)."
                ),
                raw_error="uc_schema is empty",
                diagnostics={"mode": "write"},
            )
        # Write probe is never cached: it has side effects (creates + deletes
        # a UC function) and preflight calls it once per job run anyway.
        return _probe_write(uc_schema, probe_name_hint=probe_name_hint)

    return ProbeResult(
        available=False,
        reason_code=REASON_PROBE_ERROR,
        actionable_by=ACTIONABLE_BY_PLATFORM,
        user_message=f"Unknown probe mode: {mode!r}",
        raw_error=f"invalid mode: {mode!r}",
        diagnostics={"mode": str(mode)},
    )


__all__ = [
    "ProbeMode",
    "ProbeResult",
    "REASON_FEATURE_NOT_ENABLED",
    "REASON_MISSING_UC_PERMISSIONS",
    "REASON_REGISTRY_PATH_NOT_FOUND",
    "REASON_MISSING_SP_SCOPE",
    "REASON_VENDOR_BUG",
    "REASON_UNKNOWN",
    "REASON_OK",
    "REASON_PROBE_ERROR",
    "ACTIONABLE_BY_CUSTOMER",
    "ACTIONABLE_BY_PLATFORM",
    "check_prompt_registry",
]
