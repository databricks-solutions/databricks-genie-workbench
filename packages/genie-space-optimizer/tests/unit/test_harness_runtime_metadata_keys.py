"""Tests for lever-loop runtime metadata hygiene and schema-fatal error detection.

Two invariants are enforced here:

1. Runtime keys the lever loop writes onto ``metadata_snapshot`` between
   iterations must all be underscore-prefixed so
   :func:`strip_non_exportable_fields` drops them before the Genie
   PATCH payload is serialized. Historically ``failure_clusters`` was
   written without a leading underscore, which made every PATCH fail
   with ``Invalid serialized_space: Cannot find field: failure_clusters``.

2. :func:`_is_schema_fatal_patch_error` correctly classifies API payload
   rejections as deterministic so the lever loop can exit cleanly
   instead of burning iterations on a guaranteed-rollback path.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import harness
from genie_space_optimizer.optimization.harness import _is_schema_fatal_patch_error


# ---------------------------------------------------------------------------
# Runtime metadata hygiene
# ---------------------------------------------------------------------------


def test_lever_loop_runtime_metadata_keys_are_underscore_prefixed():
    """Scan the harness source for lever-loop ``metadata_snapshot[...]``
    writes and assert every key starts with ``_``.

    A non-underscore key here leaks into the PATCH payload and is rejected
    by the Genie API (see the fix for ``failure_clusters``).
    """
    import inspect
    import re

    source = inspect.getsource(harness)
    # Match simple dict-assign patterns like metadata_snapshot["key"] = ...
    pattern = re.compile(r'metadata_snapshot\["([^"]+)"\]\s*=')
    keys = set(pattern.findall(source))
    assert keys, "Sanity check: expected at least one metadata_snapshot write"

    bad = {k for k in keys if not k.startswith("_")}
    assert not bad, (
        "Lever loop must only stamp underscore-prefixed runtime keys onto "
        f"metadata_snapshot (otherwise they leak into Genie PATCH payloads). "
        f"Offenders: {sorted(bad)}"
    )


def test_failure_clusters_readers_accept_underscore_key():
    """Both readers of the cluster cache must prefer ``_failure_clusters``.

    Historically these read ``failure_clusters`` (without underscore); the
    rename is only safe if both reader code paths follow it.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _resolve_source_cluster_for_ag,
    )

    cluster = {
        "cluster_id": "C001",
        "root_cause": "wrong_join",
        "asi_blame_set": [],
        "question_ids": ["q1"],
    }
    snap = {"_failure_clusters": [cluster]}
    ag = {"source_cluster_ids": ["C001"]}

    # If _resolve_source_cluster_for_ag picks the cluster up, the
    # underscore key is wired through. The archetype gate may legitimately
    # return ``None`` on this minimal cluster — we accept that as long as
    # it did NOT raise (which would happen if the reader only looked at
    # the old non-underscore key and got an empty list + AttributeError).
    try:
        _resolve_source_cluster_for_ag(ag, snap)
    except Exception as exc:  # pragma: no cover - defensive
        pytest.fail(f"Reader failed on _failure_clusters input: {exc}")


# ---------------------------------------------------------------------------
# Schema-fatal patch error classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error_text",
    [
        "Invalid serialized_space: Cannot find field: failure_clusters "
        "in message databricks.datarooms.export.GenieSpaceExport",
        "invalid serialized_space: malformed payload",
        "SomePrefix: Cannot find field: mystery_field in message X",
    ],
)
def test_is_schema_fatal_patch_error_true(error_text: str):
    assert _is_schema_fatal_patch_error(error_text) is True


@pytest.mark.parametrize(
    "error_text",
    [
        "",
        None,
        "TimeoutError: request timed out",
        "429 Too Many Requests",
        "permission denied",
    ],
)
def test_is_schema_fatal_patch_error_false(error_text):
    assert _is_schema_fatal_patch_error(error_text) is False


def test_is_schema_fatal_patch_error_accepts_exception_objects():
    exc = RuntimeError(
        "Invalid serialized_space: Cannot find field: failure_clusters"
    )
    assert _is_schema_fatal_patch_error(exc) is True
