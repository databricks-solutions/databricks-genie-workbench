"""Trial 32 — structural-synthesis reachability fixes.

Mirror of :mod:`trial31_flags`. Same default-ON / OFF-vocabulary semantics.
Single emergency rollback knob is ``GSO_TRIAL32``; each sub-flag is forced OFF
when the master is OFF (byte-stable rollback to Trial 31).

W32.1 — column-FQN → owning-table blame resolution. Confirmed via the airline
W32.5 evidence bundle: Stage-1 blame_sets name 4-part column FQNs
(``catalog.schema.table.column``) but ``_resolve_asset_by_identifier`` only
resolved 3-part table/MV identifiers, so column-grained blame never grounded a
slice and the cluster declined with ``no_top_n_archetype``. The fix resolves a
column FQN to its owning table (3-part prefix). NOT a vocab change, NOT a
per-anchor hardcode — a general resolution fix for column-grained blame.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` / ``off``
(case insensitive) disables. Env unset, empty, or any other value enables.
"""
from __future__ import annotations

import os

_TRIAL32_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL32_FLAG_OFF_VALUES


def trial32_enabled() -> bool:
    """Trial 32 master flag. Default ON. ``GSO_TRIAL32=0`` forces every Trial 32
    sub-flag OFF (byte-stable rollback to Trial 31)."""
    return _flag_enabled("GSO_TRIAL32")


def trial32_column_fqn_resolution_enabled() -> bool:
    """W32.1 — resolve a 4-part column FQN blame entry to its owning table when
    it does not match a table/MV directly. Default ON; forced OFF when the
    master is OFF."""
    return trial32_enabled() and _flag_enabled("GSO_TRIAL32_COLUMN_FQN_RESOLUTION")


def trial32_phase_h_per_iter_selfwrite_enabled() -> bool:
    """W32.4 — teach the Phase-H strict validator that the 8 per-iteration
    contract artifacts are self-writes.

    ``_run_phase_h_strict_validation`` runs BEFORE
    ``_materialize_per_iter_contract_paths`` writes the per-iteration
    artifacts (same ``iterations`` / ``anchor_run_id``), yet pre-W32.4 the
    validator only registered 4 PARENT-level paths as ``self_write_paths``.
    Every per-iteration declared path was therefore false-flagged
    ``manifest_path_missing`` (airline live ``missing_count=29``, 7now
    ``=37``), and the post-upload completeness check then trusted an
    eventually-consistent MLflow re-listing that had not yet observed the
    just-written artifacts — together holding
    ``architecture_invariants_held=false`` on both anchors.

    When ON the validator treats the per-iteration paths as self-writes and
    the completeness check unions the materializer's own reported
    written-path set. Observability only — byte-stable to the decision path.
    Default ON; forced OFF when the master is OFF (Trial-31 behaviour: the
    per-iteration paths are excluded from ``self_write_paths`` and the
    materializer's written-paths are not unioned)."""
    return trial32_enabled() and _flag_enabled(
        "GSO_TRIAL32_PHASE_H_PER_ITER_SELFWRITE"
    )
