"""Simple in-workspace restore (CUJ-1 §4.5, Option 1).

Restore = write a stored historical ``serialized_space`` back onto the live Genie space
**as the logged-in user (OBO)**, guarded by an optimistic concurrency check, then record
the result as a new ``origin=restore`` version. This is the same trust model the rest of
the workbench uses to write spaces (the user's own edit rights are the enforcement) — it
does NOT use the governed mutation gate / approvals / Job (that heavyweight path stays
dormant behind ``FailClosedRestore`` for regulated/cross-workspace needs).

The two I/O seams are injected so the whole orchestration is offline-testable:

* ``live_reader(space_id) -> envelope`` — GET the live serialized space (OBO).
* ``live_writer(space_id, serialized_space, description)`` — PATCH it back (OBO; NO SP
  fallback, so a caller without edit rights is correctly rejected by the API).

Safety (the one real concern of a naive overwrite): before writing, we canonicalize the
live state and require it to still equal the version the user believed was current
(``expected_current_version_id``). If the space moved since they looked, we refuse with a
409 rather than clobbering the concurrent change.
"""

import logging

from backend.services.version_control import contracts as vc

logger = logging.getLogger(__name__)

# Machine-stable component keys (kept in ``details.mismatch``) mapped to human labels for
# the surfaced message. The UI already shows these three fingerprints, so naming which one
# drifted turns the old blanket "space changed" into an actionable, self-explaining error.
_COMPONENT_LABELS = {
    "config": "the configuration",
    "benchmark": "the benchmarks",
    "metadata": "the description",
}


class RestoreConflict(ValueError):
    """A restore precondition failed (still a ``ValueError`` so existing 409 mapping holds),
    but carries a stable ``vc_code`` and structured ``vc_details`` so the router surfaces a
    SPECIFIC 409 the UI can act on — instead of the generic ``request_conflict`` that made
    every failure read as an undiagnosable "the space changed"."""

    def __init__(self, code, message, *, details=None):
        super().__init__(message)
        self.vc_code = code
        self.vc_details = details


def _mismatched_components(live_snapshot, expected_snapshot):
    """Return the machine keys of the fingerprint components that differ. Defensive against
    snapshots without fingerprints (returns ``[]`` -> generic message) so a malformed
    observation can never mask the conflict behind an AttributeError."""
    live = getattr(live_snapshot, "fingerprints", None)
    expected = getattr(expected_snapshot, "fingerprints", None)
    if live is None or expected is None:
        return []
    diffs = []
    if live.config != expected.config:
        diffs.append("config")
    if live.benchmark != expected.benchmark:
        diffs.append("benchmark")
    if live.metadata != expected.metadata:
        diffs.append("metadata")
    return diffs


def _live_mismatch_message(comparison, mismatch):
    if comparison is vc.Comparison.UNKNOWN:
        return ("The live space was recorded under a different canonicalizer version, so it "
                "can't be safely compared. Refresh the history and try again.")
    if mismatch:
        labels = ", ".join(_COMPONENT_LABELS.get(name, name) for name in mismatch)
        return (f"The live space changed since you opened this view — {labels} differ. "
                "Refresh the history and try again.")
    return "The live space changed since you opened this view. Refresh the history and try again."


def _fingerprint_wire(snapshot):
    fingerprints = getattr(snapshot, "fingerprints", None)
    if fingerprints is None:
        return None
    return {
        "config": fingerprints.config,
        "benchmark": fingerprints.benchmark,
        "metadata": fingerprints.metadata,
        "canonicalizer_version": fingerprints.canonicalizer_version,
    }


def restore_space_version(runtime, *, space_id, version_id, expected_current_version_id,
                          actor, live_reader, live_writer) -> vc.ObservationResult:
    """Restore ``version_id`` onto ``space_id`` under ``actor`` (OBO). Returns the recorded
    ``ObservationResult`` (its ``captured_version`` is the new ``restore`` version, or the
    unchanged head when the live space already equals the requested version).

    Raises (mapped to HTTP by the router's ``_invoke``):
      * ``LookupError`` — space not enrolled / requested version unknown (404).
      * ``PermissionError`` — actor is not scoped to the binding (403).
      * ``ValueError`` — the client's view is stale or the space drifted (409).
    """
    binding = runtime.registry.find_active_by_space_key(space_id)
    if binding is None:
        raise LookupError("Space is not enrolled in version control")
    if (actor.workspace_id != binding.workspace_id
            or runtime.authorize_history(actor, binding) is not True):
        raise PermissionError("Binding history scope denied")

    # Restoring the version the caller already holds as current is a no-op (it would only
    # dedup to the head). Reject it up front with a specific, friendly 409 rather than doing
    # a pointless overwrite. Defense-in-depth: the UI also disables restore on the head.
    if version_id == expected_current_version_id:
        raise RestoreConflict(
            "restore_noop_current",
            "This version is already the live configuration; there's nothing to restore.")

    historical = runtime.ledger.get_version(binding, version_id)  # KeyError -> 404
    try:
        expected = runtime.ledger.get_version(binding, expected_current_version_id)
    except KeyError as error:
        logger.warning(
            "Restore precondition failed (stale_expected) for space %s: "
            "expected_current_version_id=%s not found in the ledger",
            space_id, expected_current_version_id)
        raise RestoreConflict(
            "restore_stale_expected",
            "Your history is out of date — the version you had as current no longer is. "
            "Refresh the history and try again.",
            details={"expected_current_version_id": expected_current_version_id},
        ) from error

    live = runtime.canonicalizer.observe(live_reader(space_id))
    comparison = runtime.canonicalizer.compare(live, expected.snapshot)
    if comparison is not vc.Comparison.EQUAL:
        mismatch = _mismatched_components(live, expected.snapshot)
        expected_fp, live_fp = _fingerprint_wire(expected.snapshot), _fingerprint_wire(live)
        logger.warning(
            "Restore precondition failed (live_mismatch) for space %s: comparison=%s "
            "mismatch=%s expected_fp=%s live_fp=%s",
            space_id, comparison.value, mismatch, expected_fp, live_fp)
        raise RestoreConflict(
            "restore_live_mismatch",
            _live_mismatch_message(comparison, mismatch),
            details={
                "comparison": comparison.value,
                "mismatch": mismatch,
                "expected_fingerprints": expected_fp,
                "live_fingerprints": live_fp,
            },
        )

    # Stored snapshots are frozen (contracts._freeze_json wraps them in MappingProxyType +
    # tuples for immutability), which json.dumps rejects ("Object of type mappingproxy is
    # not JSON serializable"). to_wire is the canonical thaw back to plain dict/list before
    # the OBO PATCH re-serializes it onto the live space.
    serialized_space = vc.to_wire(historical.snapshot.serialized_space)
    live_writer(space_id, serialized_space,
                historical.snapshot.restorable_metadata.get("description"))

    executor = runtime.identity.executor(runtime.reader_selection)
    # Record the human who clicked Restore as the ledger actor (the lease/append still run as
    # the SP executor); the restore was their deliberate, OBO-authorized write. The
    # post-write capture MUST read the live space under the SAME user (OBO) via live_reader:
    # the SP usually has no grant on a user-owned Genie space, so an SP-pinned read 403s and
    # the capture fails soft -- overwriting live but recording no version (mirrors the observe
    # path, which injects the OBO reader for exactly this reason).
    return runtime.observer.capture(binding, "restore", executor, origin=vc.Origin.RESTORE,
                                    restored_from_version_id=version_id, actor_override=actor,
                                    live_reader=lambda: live_reader(space_id))
