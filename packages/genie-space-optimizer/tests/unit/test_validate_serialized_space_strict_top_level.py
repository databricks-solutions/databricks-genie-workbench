"""Regression tests for strict top-level validation of ``serialized_space``.

If an unknown top-level key ever reaches the Genie PATCH endpoint the API
rejects the whole payload with ``Invalid serialized_space: Cannot find
field: <key>``. These tests ensure strict validation catches that class
of bug locally so the lever loop's payload sanitizer and runtime
annotation hygiene never silently regress.
"""

from __future__ import annotations

from genie_space_optimizer.common.genie_schema import (
    SERIALIZED_SPACE_TOP_LEVEL_KEYS,
    validate_serialized_space,
)


def _valid_config() -> dict:
    """Minimal strict-valid ``serialized_space`` config."""
    return {
        "version": 2,
        "data_sources": {"tables": [], "metric_views": []},
    }


def test_strict_accepts_minimal_config():
    ok, errors = validate_serialized_space(_valid_config(), strict=True)
    assert ok, f"Expected strict-valid config to pass, got errors: {errors}"


def test_strict_rejects_failure_clusters_top_level_key():
    config = _valid_config()
    config["failure_clusters"] = [{"cluster_id": "C001"}]
    ok, errors = validate_serialized_space(config, strict=True)
    assert not ok
    joined = "\n".join(errors)
    assert "unknown top-level keys" in joined
    assert "failure_clusters" in joined


def test_strict_rejects_multiple_unknown_top_level_keys():
    config = _valid_config()
    config["failure_clusters"] = []
    config["mystery_field"] = {"unexpected": True}
    ok, errors = validate_serialized_space(config, strict=True)
    assert not ok
    joined = "\n".join(errors)
    assert "failure_clusters" in joined
    assert "mystery_field" in joined


def test_strict_accepts_underscore_prefixed_runtime_key():
    """Phase 1.R1: underscore-prefixed top-level keys are the codebase's
    runtime-only convention (see ``common.config.is_runtime_key``). They
    are stripped by ``strip_non_exportable_fields`` before PATCH, so the
    strict validator must agree with that contract and NOT treat them as
    unknown-top-level-key errors. The applier now validates the stripped
    payload as belt-and-suspenders, but aligning the two layers on the
    same convention is the primary root-cause fix: a pre-existing
    runtime ``_data_profile`` on ``config`` used to crash the whole
    PATCH at this exact validator.
    """
    config = _valid_config()
    config["_space_id"] = "abc123"
    config["_data_profile"] = {"cat.sch.t": {}}
    config["_failure_clusters"] = []
    ok, errors = validate_serialized_space(config, strict=True)
    assert ok, f"runtime keys must not error strict validation; got: {errors}"


def test_lenient_mode_is_permissive_about_top_level_keys():
    """Lenient validation is used on configs fetched from the API and must
    remain permissive so legacy or forward-compatible fields don't break
    reads. Only strict mode enforces the allowlist.
    """
    config = _valid_config()
    config["failure_clusters"] = []
    ok, _ = validate_serialized_space(config, strict=False)
    assert ok


def test_allowlist_matches_client_constant():
    """Schema and client must agree on the allowed top-level keys."""
    from genie_space_optimizer.common.genie_client import (
        SERIALIZED_SPACE_TOP_LEVEL_KEYS as CLIENT_KEYS,
    )
    assert SERIALIZED_SPACE_TOP_LEVEL_KEYS == CLIENT_KEYS
