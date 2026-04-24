"""End-to-end regression guard for the runtime-key validator outage.

The lever loop outage at baseline 59.1% was caused by every post-patch
validation call rejecting the runtime config with:

    Genie config validation errors: ["root: unknown top-level keys
      ['_cluster_synthesis_count', '_data_profile', '_failure_clusters',
       '_original_instruction_sections', '_space_id']; allowed: [...]"]

Three coordinated fixes prevent it:

1. ``common.genie_schema._strict_validate`` defers to ``is_runtime_key``
   before flagging unknown top-level keys.
2. ``optimization.applier.apply_patch_set`` validates the
   ``strip_non_exportable_fields`` projection (belt-and-suspenders).
3. ``common.genie_client.strip_non_exportable_fields`` classifies dropped
   keys as ``known metadata``, ``unknown`` or ``undocumented runtime`` so
   operators notice new pollution without breaking the run.

This test reproduces the exact set of offending keys that hit production
and asserts the validator + stripper pipeline now tolerates all of them.
"""

from __future__ import annotations

from genie_space_optimizer.common.config import KNOWN_INTERNAL_RUNTIME_KEYS
from genie_space_optimizer.common.genie_client import (
    SERIALIZED_SPACE_TOP_LEVEL_KEYS,
    strip_non_exportable_fields,
)
from genie_space_optimizer.common.genie_schema import validate_serialized_space


OFFENDING_RUNTIME_KEYS = (
    "_cluster_synthesis_count",
    "_data_profile",
    "_failure_clusters",
    "_original_instruction_sections",
    "_space_id",
)


def _runtime_config_reproducing_outage() -> dict:
    """Build a config mirroring what the lever loop hands the validator.

    The shape is deliberately minimal on the PATCH-relevant side and
    deliberately polluted with every underscore-prefixed annotation that
    showed up in the outage log.
    """
    config = {
        "version": 2,
        "config": {"sample_questions": []},
        "data_sources": {"tables": [], "metric_views": []},
        "instructions": {"text_instructions": []},
        "benchmarks": {"questions": []},
        "_cluster_synthesis_count": 3,
        "_data_profile": {"cat.sch.t": {"row_count": 1000}},
        "_failure_clusters": [{"cluster_id": "C001"}],
        "_original_instruction_sections": {"PURPOSE": "demo"},
        "_space_id": "01ab23cd",
    }
    assert set(OFFENDING_RUNTIME_KEYS).issubset(config.keys())
    return config


def test_strict_validator_accepts_raw_runtime_config():
    """Fix #1: `_strict_validate` must skip `is_runtime_key` keys.

    Before the fix, calling ``validate_serialized_space(config, strict=True)``
    on the runtime dict tripped the unknown-top-level-keys rule for all
    five underscore keys and returned ``(False, [...])`` — which is what
    the lever loop saw on every iteration.
    """
    config = _runtime_config_reproducing_outage()
    ok, errors = validate_serialized_space(config, strict=True)
    assert ok, (
        f"strict validator still rejects runtime keys; errors: {errors}"
    )


def test_apply_patch_set_stripped_target_is_clean_and_valid():
    """Fix #2 + #3: validate the stripped projection, not the raw dict.

    This mirrors the call site in ``applier.apply_patch_set`` at
    line 3128: ``validate_serialized_space(strip_non_exportable_fields(
    copy.deepcopy(config)), strict=True)``. Two invariants:

    - The PATCH-ready payload has *no* underscore-prefixed keys left.
    - Strict validation of that payload passes.
    """
    config = _runtime_config_reproducing_outage()
    cleaned = strip_non_exportable_fields(config)

    leftover_runtime = [k for k in cleaned if k.startswith("_")]
    assert leftover_runtime == [], (
        f"strip_non_exportable_fields left runtime keys on PATCH payload: "
        f"{leftover_runtime}"
    )
    assert set(cleaned.keys()).issubset(SERIALIZED_SPACE_TOP_LEVEL_KEYS)

    ok, errors = validate_serialized_space(cleaned, strict=True)
    assert ok, f"stripped payload still fails strict validation: {errors}"


def test_all_outage_keys_classified_as_runtime():
    """``is_runtime_key`` is the contract both layers defer to.

    If any future refactor loosens the underscore convention (e.g. allows
    ``data_profile`` without the leading ``_``), both layers would drift
    and the outage could recur. This test fails loudly if that happens.
    """
    from genie_space_optimizer.common.config import is_runtime_key

    for key in OFFENDING_RUNTIME_KEYS:
        assert is_runtime_key(key), (
            f"{key!r} no longer classified as runtime; the validator "
            f"bypass will stop protecting it."
        )


def test_known_internal_runtime_keys_covers_outage_set():
    """Belt-and-suspenders: the curated list must cover the outage keys.

    ``is_runtime_key`` already tolerates the keys via the underscore
    prefix, but ``KNOWN_INTERNAL_RUNTIME_KEYS`` drives the stripper's
    logging classification — missing entries get logged as
    ``undocumented runtime`` which is noise.
    """
    missing = [k for k in OFFENDING_RUNTIME_KEYS if k not in KNOWN_INTERNAL_RUNTIME_KEYS]
    assert missing == [], (
        f"Known runtime-key inventory is missing outage keys: {missing}"
    )
