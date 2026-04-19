"""Unit tests for the shared MLflow Prompt Registry probe.

Invariants we cannot let regress:
  1. Fail-closed default: any exception, missing argument, or unknown mode
     yields ``available=False``. We never "optimistically" allow optimization
     to proceed without an affirmative signal.
  2. Stable reason codes: UI messaging keys off these — they must match
     REASON_* constants exactly.
  3. Probe–workload parity: the read probe must call
     ``mlflow.genai.search_prompts`` (not a hand-rolled REST URL). This is
     the root cause from the Bug #1 follow-up — the previous probe hit
     ``/api/2.0/mlflow/unity-catalog/prompts`` which does not exist.
  4. Closed-world classifier: any unmapped vendor error_code becomes
     ``REASON_VENDOR_BUG`` with ``actionable_by = "platform"``. Never
     silently renders as a customer-actionable "go enable the toggle".
  5. Error classification: FEATURE_DISABLED -> feature_not_enabled,
     PERMISSION_DENIED -> missing_uc_permissions, RESOURCE_DOES_NOT_EXIST ->
     registry_path_not_found, ENDPOINT_NOT_FOUND / INVALID_PARAMETER_VALUE
     / INTERNAL_ERROR -> vendor_bug, UNAUTHENTICATED -> missing_sp_scope.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.common import prompt_registry as pr
from genie_space_optimizer.common.prompt_registry import (
    ACTIONABLE_BY_CUSTOMER,
    ACTIONABLE_BY_PLATFORM,
    REASON_FEATURE_NOT_ENABLED,
    REASON_MISSING_SP_SCOPE,
    REASON_MISSING_UC_PERMISSIONS,
    REASON_OK,
    REASON_PROBE_ERROR,
    REASON_REGISTRY_PATH_NOT_FOUND,
    REASON_VENDOR_BUG,
    ProbeResult,
    check_prompt_registry,
)


class _FakeDatabricksError(Exception):
    """Mimics ``databricks.sdk.errors.DatabricksError`` by carrying ``error_code``."""

    def __init__(self, error_code: str, message: str = "") -> None:
        super().__init__(message or error_code)
        self.error_code = error_code


def _mock_ws() -> MagicMock:
    """A workspace client stub with a stable client_id for cache keying."""
    ws = MagicMock()
    ws.config.client_id = "sp-client-xyz"
    return ws


@pytest.fixture(autouse=True)
def _flush_cache():
    """Keep tests independent of TTL cache state."""
    pr._clear_probe_cache()
    yield
    pr._clear_probe_cache()


def _patch_search_prompts(side_effect=None, return_value=None):
    """Patch ``mlflow.genai.search_prompts`` under the real module path.

    We install a ``mlflow.genai`` module with our mock so tests don't
    require the real mlflow package to be importable with all its deps.
    """
    mlflow_mod = sys.modules.get("mlflow")
    if mlflow_mod is None:
        mlflow_mod = types.ModuleType("mlflow")
        mlflow_mod.__version__ = "3.10.1"
        sys.modules["mlflow"] = mlflow_mod
    genai_mod = getattr(mlflow_mod, "genai", None)
    if genai_mod is None:
        genai_mod = types.ModuleType("mlflow.genai")
        mlflow_mod.genai = genai_mod
        sys.modules["mlflow.genai"] = genai_mod

    fake = MagicMock()
    if side_effect is not None:
        fake.side_effect = side_effect
    elif return_value is not None:
        fake.return_value = return_value
    return patch.object(genai_mod, "search_prompts", fake, create=True)


# ── Probe–workload parity (the root-cause invariant) ────────────────────


def test_read_probe_calls_mlflow_search_prompts_not_rest() -> None:
    """The probe MUST delegate to the SDK symbol the job uses.

    This test fails if someone re-introduces a hand-rolled REST call — the
    exact bug the Bug #1 follow-up is fixing.
    """
    with _patch_search_prompts(return_value=[]) as patched:
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is True
    assert result.reason_code == REASON_OK
    patched.assert_called_once()
    assert patched.call_args.kwargs.get("max_results") == 1


def test_read_probe_uses_uc_catalog_schema_filter_format() -> None:
    """Regression test for the "filter format wrong" production incident.

    Unity Catalog prompt registries require ``filter_string`` of exactly
    the shape ``catalog = 'X' AND schema = 'Y'``. A ``name LIKE 'X.Y.%'``
    filter returns INVALID_PARAMETER_VALUE, which the probe then
    classifies as a vendor_bug — falsely telling the customer that
    Prompt Registry is broken when in fact our probe sent the wrong
    filter.

    This test locks in the correct filter shape. If someone changes the
    filter syntax, this test must fail loudly rather than letting the
    probe ship a regression to prod.
    """
    with _patch_search_prompts(return_value=[]) as patched:
        check_prompt_registry(
            _mock_ws(),
            mode="read",
            uc_schema="serverless_stable_6t92c3_catalog.genie_space_optimizer",
        )
    filter_string = patched.call_args.kwargs.get("filter_string")
    assert filter_string == (
        "catalog = 'serverless_stable_6t92c3_catalog' "
        "AND schema = 'genie_space_optimizer'"
    ), f"UC prompt registry requires catalog/schema filter, got: {filter_string!r}"
    # Must never use LIKE — that's the bug we're guarding against.
    assert "LIKE" not in (filter_string or "")


def test_read_probe_unscoped_when_uc_schema_missing() -> None:
    """Without a uc_schema we can't build a legal UC filter; probe unscoped
    rather than sending a malformed filter."""
    with _patch_search_prompts(return_value=[]) as patched:
        check_prompt_registry(_mock_ws(), mode="read", uc_schema=None)
    assert patched.call_args.kwargs.get("filter_string") == ""


def test_read_probe_unscoped_on_suspicious_uc_schema() -> None:
    """If uc_schema contains quotes or other unexpected chars, we must
    NOT interpolate it into a filter (injection + malformed filter both).
    """
    with _patch_search_prompts(return_value=[]) as patched:
        check_prompt_registry(
            _mock_ws(), mode="read", uc_schema="main.gso'; DROP TABLE--",
        )
    assert patched.call_args.kwargs.get("filter_string") == ""


def test_read_probe_retries_without_kwargs_on_type_error() -> None:
    """Older/newer SDK signatures may not accept filter_string; the probe
    must fall back to a no-arg call rather than crashing."""
    call_count = {"n": 0}

    def _impl(*args, **kwargs):
        call_count["n"] += 1
        if kwargs:
            raise TypeError("unexpected kw")
        return []

    with _patch_search_prompts(side_effect=_impl):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is True
    assert call_count["n"] == 2


# ── Happy path ───────────────────────────────────────────────────────────


def test_read_probe_success_sets_available_true() -> None:
    with _patch_search_prompts(return_value=[]):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert isinstance(result, ProbeResult)
    assert result.available is True
    assert result.reason_code == REASON_OK
    assert result.actionable_by == ACTIONABLE_BY_CUSTOMER
    assert result.user_message == ""
    assert result.vendor_error_code is None


# ── Fail-closed invariants ──────────────────────────────────────────────


def test_read_probe_without_ws_is_fail_closed() -> None:
    result = check_prompt_registry(None, mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_PROBE_ERROR
    assert result.actionable_by == ACTIONABLE_BY_PLATFORM


def test_write_probe_without_schema_is_fail_closed() -> None:
    result = check_prompt_registry(mode="write", uc_schema=None)
    assert result.available is False
    assert result.reason_code == REASON_PROBE_ERROR


def test_write_probe_with_malformed_schema_is_fail_closed() -> None:
    result = check_prompt_registry(mode="write", uc_schema="just_a_name")
    assert result.available is False
    assert result.reason_code == REASON_PROBE_ERROR


def test_unknown_mode_is_fail_closed() -> None:
    result = check_prompt_registry(mode="wat", uc_schema="main.gso")  # type: ignore[arg-type]
    assert result.available is False
    assert result.reason_code == REASON_PROBE_ERROR


# ── Error classification (read probe) ───────────────────────────────────


def test_feature_disabled_maps_to_feature_not_enabled() -> None:
    err = _FakeDatabricksError("FEATURE_DISABLED", "Prompt registry not enabled")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_FEATURE_NOT_ENABLED
    assert result.actionable_by == ACTIONABLE_BY_CUSTOMER
    assert result.vendor_error_code == "FEATURE_DISABLED"
    assert "not enabled" in result.user_message.lower()


def test_permission_denied_maps_to_missing_uc_permissions() -> None:
    err = _FakeDatabricksError("PERMISSION_DENIED", "no EXECUTE")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_MISSING_UC_PERMISSIONS
    assert result.actionable_by == ACTIONABLE_BY_CUSTOMER
    assert result.missing_privileges, "UI needs a privilege list to render"


def test_insufficient_permissions_also_maps_to_missing_uc_permissions() -> None:
    err = _FakeDatabricksError("INSUFFICIENT_PERMISSIONS", "denied")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.reason_code == REASON_MISSING_UC_PERMISSIONS


def test_resource_does_not_exist_maps_to_registry_path_not_found() -> None:
    err = _FakeDatabricksError("RESOURCE_DOES_NOT_EXIST", "no schema")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_REGISTRY_PATH_NOT_FOUND


# ── Closed-world classifier: vendor-bug buckets ─────────────────────────


def test_endpoint_not_found_maps_to_vendor_bug_not_unknown() -> None:
    """Regression test for the exact production incident: a probe that hit a
    non-existent endpoint showed up in the UI as ``reason=unknown`` and
    therefore rendered the generic ``Enable MLflow Prompt Registry`` copy.

    With the closed-world classifier, ENDPOINT_NOT_FOUND is an explicit
    platform-actionable bug — and the UI has its own branch for it.
    """
    err = _FakeDatabricksError(
        "ENDPOINT_NOT_FOUND", "No API found for 'GET /mlflow/unity-catalog/prompts'"
    )
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_VENDOR_BUG
    assert result.actionable_by == ACTIONABLE_BY_PLATFORM
    assert result.vendor_error_code == "ENDPOINT_NOT_FOUND"
    # UI copy must mention the error code; the mono block shows it verbatim.
    assert "ENDPOINT_NOT_FOUND" in result.user_message


@pytest.mark.parametrize(
    "code",
    [
        "INVALID_PARAMETER_VALUE",
        "INVALID_STATE",
        "INTERNAL_ERROR",
        "IO_ERROR",
        "TEMPORARILY_UNAVAILABLE",
        "DEADLINE_EXCEEDED",
    ],
)
def test_other_vendor_bug_codes_classify_platform(code: str) -> None:
    err = _FakeDatabricksError(code, "boom")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.reason_code == REASON_VENDOR_BUG
    assert result.actionable_by == ACTIONABLE_BY_PLATFORM
    assert result.vendor_error_code == code


def test_unauthenticated_maps_to_missing_sp_scope() -> None:
    err = _FakeDatabricksError("UNAUTHENTICATED", "token lacks scope")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.reason_code == REASON_MISSING_SP_SCOPE
    assert result.actionable_by == ACTIONABLE_BY_CUSTOMER


def test_unmapped_vendor_code_defaults_to_vendor_bug() -> None:
    """A brand-new Databricks error code we haven't heard of must NOT
    render as ``Enable MLflow Prompt Registry`` — closed-world."""
    err = _FakeDatabricksError("SOMETHING_WE_HAVENT_SEEN_BEFORE", "surprise")
    with _patch_search_prompts(side_effect=err):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_VENDOR_BUG
    assert result.actionable_by == ACTIONABLE_BY_PLATFORM


def test_generic_exception_with_no_error_code_maps_to_vendor_bug() -> None:
    """No DatabricksError means no error_code; fallback classifier would
    return ``unknown`` — we must promote it to ``vendor_bug`` so the UI
    doesn't render admin-copy for a platform failure."""
    with _patch_search_prompts(side_effect=RuntimeError("connection reset")):
        result = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert result.available is False
    assert result.reason_code == REASON_VENDOR_BUG
    assert result.actionable_by == ACTIONABLE_BY_PLATFORM
    assert result.raw_error and "connection reset" in result.raw_error


def test_probe_result_diagnostics_include_mode() -> None:
    """Operators debugging the probe rely on diagnostics.mode."""
    with _patch_search_prompts(return_value=[]):
        ok = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert ok.diagnostics.get("mode") == "read"
    assert ok.diagnostics.get("probe") == "mlflow.genai.search_prompts"

    bad = check_prompt_registry(mode="write", uc_schema=None)
    assert bad.diagnostics.get("mode") == "write"


# ── TTL cache ────────────────────────────────────────────────────────────


def test_read_probe_caches_result_across_calls() -> None:
    """Repeated polls within the TTL window must not re-hit the SDK.

    The /permissions endpoint is polled by the UI; caching is how we keep
    that cheap without each poll making a Databricks round-trip.
    """
    with _patch_search_prompts(return_value=[]) as patched:
        first = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
        second = check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
    assert first.available is True
    assert second.available is True
    assert patched.call_count == 1  # second call served from cache


def test_bypass_cache_forces_reprobe() -> None:
    """/trigger and the UI's Re-check button pass bypass_cache=True."""
    with _patch_search_prompts(return_value=[]) as patched:
        check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
        check_prompt_registry(
            _mock_ws(), mode="read", uc_schema="main.gso", bypass_cache=True
        )
    assert patched.call_count == 2


def test_cache_key_includes_uc_schema() -> None:
    """Different target schemas must not share a cache entry, otherwise a
    PERMISSION_DENIED on schema A could leak into schema B."""
    with _patch_search_prompts(return_value=[]) as patched:
        check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.gso")
        check_prompt_registry(_mock_ws(), mode="read", uc_schema="main.other")
    assert patched.call_count == 2


def test_cache_key_includes_sp_identity() -> None:
    """Probing under a different SP identity must re-probe."""
    ws_a = MagicMock()
    ws_a.config.client_id = "sp-a"
    ws_b = MagicMock()
    ws_b.config.client_id = "sp-b"
    with _patch_search_prompts(return_value=[]) as patched:
        check_prompt_registry(ws_a, mode="read", uc_schema="main.gso")
        check_prompt_registry(ws_b, mode="read", uc_schema="main.gso")
    assert patched.call_count == 2


# ── Probe-level safety ──────────────────────────────────────────────────


def test_missing_sdk_symbol_classifies_as_vendor_bug() -> None:
    """If the installed mlflow predates search_prompts, the probe must
    declare unavailable with ``SDK_SYMBOL_MISSING`` rather than raising."""
    mlflow_mod = sys.modules.get("mlflow") or types.ModuleType("mlflow")
    sys.modules["mlflow"] = mlflow_mod
    genai_mod = types.ModuleType("mlflow.genai")
    # No search_prompts attribute.
    mlflow_mod.genai = genai_mod
    sys.modules["mlflow.genai"] = genai_mod
    try:
        result = check_prompt_registry(
            _mock_ws(), mode="read", uc_schema="main.gso", bypass_cache=True
        )
    finally:
        # Leave a sane mlflow.genai in place for subsequent tests.
        genai_mod.search_prompts = MagicMock(return_value=[])
    assert result.available is False
    assert result.reason_code == REASON_VENDOR_BUG
    assert result.vendor_error_code == "SDK_SYMBOL_MISSING"
    assert result.actionable_by == ACTIONABLE_BY_PLATFORM
