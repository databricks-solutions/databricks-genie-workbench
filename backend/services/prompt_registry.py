"""Thin re-export of the shared Prompt Registry probe.

The real implementation lives in ``genie_space_optimizer.common.prompt_registry``
so the GSO job (which does not have the Workbench ``backend`` package on its
Python path) and the Workbench webapp share one source of truth. Consumers in
``backend/routers/*`` should import from either path — they are the same.
"""

from __future__ import annotations

from genie_space_optimizer.common.prompt_registry import (  # noqa: F401
    ACTIONABLE_BY_CUSTOMER,
    ACTIONABLE_BY_PLATFORM,
    REASON_FEATURE_NOT_ENABLED,
    REASON_MISSING_SP_SCOPE,
    REASON_MISSING_UC_PERMISSIONS,
    REASON_OK,
    REASON_PROBE_ERROR,
    REASON_REGISTRY_PATH_NOT_FOUND,
    REASON_UNKNOWN,
    REASON_VENDOR_BUG,
    ProbeMode,
    ProbeResult,
    check_prompt_registry,
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
