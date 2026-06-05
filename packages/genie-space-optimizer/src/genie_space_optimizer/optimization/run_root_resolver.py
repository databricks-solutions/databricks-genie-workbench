"""Centralized resolver for the per-run scratch ``run_root``.

Background — three callsites in the optimizer (``harness.py``,
``stages/diagnose.py``, and the canary path in ``optimizer.py``)
each had their own copy of the same resolution policy::

    Path(os.environ["GSO_PHASE_H_BUNDLE_ROOT"])
        if os.environ.get("GSO_PHASE_H_BUNDLE_ROOT")
        else Path(f"/tmp/gso/{run_id}")

Two problems with that pattern surfaced in production:

  1. Hardcoded ``/tmp/gso/<run_id>`` is non-portable — on macOS dev
     boxes ``$TMPDIR`` is the canonical writable temp area and
     ``/tmp`` is a symlink with restrictive ACLs.

  2. When multiple processes / containers / users on the same
     Databricks Apps node share ``/tmp/gso/<run_id>`` (because the
     run_id collides across replays or the SP rotated), the second
     process inherits a directory it cannot write into and
     :func:`pathlib.Path.mkdir` raises::

       PermissionError: [Errno 13] Permission denied:
       '/tmp/gso/<run_id>/iteration_1'

     The legacy code had no fallback — the lever-loop iteration
     died on the first ``mkdir`` and the entire run was lost.

This module exposes :func:`resolve_run_root` with a unified
resolution policy that handles both problems:

  * Honor ``GSO_PLAN_V3_RUN_ROOT`` (explicit operator override) and
    ``GSO_PHASE_H_BUNDLE_ROOT`` (postmortem bundle assembler hint)
    in that order — these are always honored, even if not writable.
    The deploy contract requires these paths to be writable when set.
  * Default to ``<tempfile.gettempdir()>/gso/<run_id>``. We use
    :func:`tempfile.gettempdir` rather than hardcoding ``/tmp`` so
    macOS / sandboxed runtimes pick the right writable area.
  * If the default path is not writable by the current process —
    either because the run-dir itself has restrictive perms OR
    because the ``<tempdir>/gso`` parent is non-searchable (the
    production traceback at ``Path.stat:842``) — fall back to a
    SIBLING root that lives directly under ``<tempfile.gettempdir()>``
    and is NOT parented under the possibly poisoned ``gso/``
    directory: ``<tempfile.gettempdir()>/gso_<run_id>__pid<PID>``.
    The legacy fallback (``<tempdir>/gso/<run_id>__pid<PID>``) was
    insufficient because it shared the same parent the EACCES came
    from in the first place; the second mkdir would die identically.

The resolver is side-effect free: it does NOT create the directory.
The caller (``write_qstate`` and friends) creates it lazily with
``mkdir(parents=True, exist_ok=True)`` so test fixtures that mock
the FS are unaffected.

Two layers of defense against ``OSError`` from the FS probe:
  * :func:`_is_writable_for_current_process` swallows ANY
    :class:`OSError` (including ``PermissionError``) from
    ``Path.exists`` / ``os.access`` and returns ``False``. The
    Python 3.12 contract is that ``Path.exists()`` propagates
    ``PermissionError`` from ``os.stat`` — earlier versions
    swallowed it — so a try/except is mandatory.
  * :func:`ensure_run_root` retries once under the sibling-root
    fallback if its own ``mkdir`` raises ``OSError``, ensuring
    end-to-end success even on environments where the resolver's
    first guess gets mkdir-denied.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path


def _env_override() -> str:
    """Return the explicit operator override, or empty string when
    none is set.

    Resolution order:
      1. ``GSO_PLAN_V3_RUN_ROOT`` — explicit per-run override.
      2. ``GSO_PHASE_H_BUNDLE_ROOT`` — bundle assembler hint.

    A non-empty value here SHORT-CIRCUITS the writability probe.
    The deploy contract requires these env vars to point at a
    writable path when set; we trust the operator over the heuristic.
    """
    explicit = os.environ.get("GSO_PLAN_V3_RUN_ROOT", "")
    if explicit:
        return explicit
    return os.environ.get("GSO_PHASE_H_BUNDLE_ROOT", "")


def _default_root_for(run_id: str) -> Path:
    """Compute the platform-portable default ``<tempdir>/gso/<run_id>``.
    Empty / falsy run_id collapses to the literal ``"unknown"`` so
    the path remains parseable by postmortems."""
    rid = str(run_id or "").strip() or "unknown"
    return Path(tempfile.gettempdir()) / "gso" / rid


def _sibling_fallback_root(run_id: str) -> Path:
    """Compute the SIBLING fallback root that lives directly under
    :func:`tempfile.gettempdir` and is NOT parented under the
    (possibly poisoned) ``<tempdir>/gso/`` directory.

    Production traceback ``PermissionError [Errno 13] Permission
    denied: '/tmp/gso/<run_id>'`` proved that the prior fallback
    ``<tempdir>/gso/<run_id>__pid<PID>`` was insufficient: it shared
    the same ``<tempdir>/gso`` parent that already returned EACCES,
    so the fallback's own mkdir would die identically. We sidestep
    that by minting ``<tempdir>/gso_<run_id>__pid<PID>`` (note the
    underscore replacing the slash) — a SIBLING of ``<tempdir>/gso``,
    not a child. The run_id remains in the path so postmortems can
    still recover it.
    """
    rid = str(run_id or "").strip() or "unknown"
    return Path(tempfile.gettempdir()) / f"gso_{rid}__pid{os.getpid()}"


def _is_writable_for_current_process(path: Path) -> bool:
    """Return ``True`` iff the current process can create files
    under ``path``. Returns ``True`` when ``path`` does not yet
    exist (the caller will create it) — that's the happy path on
    a clean node.

    The probe walks up to the first existing ancestor and checks
    write permission there. We avoid actually creating a tempfile
    inside ``path`` so the resolver remains side-effect free.

    Hardened against ``OSError`` (including the Python-3.12-only
    ``PermissionError`` propagation from ``Path.exists()``): any
    stat / access failure during the probe is treated as "not
    writable", so :func:`resolve_run_root` engages its fallback
    rather than the exception escaping to the lever loop.
    """
    cursor: Path = path
    while True:
        try:
            if cursor.exists():
                break
        except OSError:
            # PermissionError / EACCES from ``Path.stat`` — the
            # most common production cause is a poisoned
            # ``<tempdir>/gso`` parent owned by a prior SP. Treat
            # as "not writable" and let the caller fall back.
            return False
        parent = cursor.parent
        if parent == cursor:
            return False  # reached filesystem root; very abnormal
        cursor = parent
    try:
        return os.access(str(cursor), os.W_OK)
    except OSError:
        return False


def resolve_run_root(run_id: str) -> Path:
    """Return the directory the optimizer should use as ``run_root``.

    See module docstring for the resolution policy. The returned
    path may or may not exist; the caller MUST ``mkdir(parents=True,
    exist_ok=True)`` before writing.
    """
    explicit = _env_override()
    if explicit:
        return Path(explicit)

    candidate = _default_root_for(run_id)
    if _is_writable_for_current_process(candidate):
        return candidate

    # Fallback — mint a SIBLING root outside the possibly poisoned
    # ``<tempdir>/gso`` parent. See :func:`_sibling_fallback_root`
    # for the rationale; the prior in-tree fallback shared the same
    # parent and died on the same EACCES.
    return _sibling_fallback_root(run_id)


def ensure_run_root(run_id: str) -> Path:
    """Resolve and CREATE the run_root, returning the live path.

    Convenience helper for callers that do not want to interleave
    ``mkdir`` with the resolution logic. Equivalent to::

        root = resolve_run_root(run_id)
        root.mkdir(parents=True, exist_ok=True)
        return root

    Two-tier mkdir retry: if the first mkdir raises ``OSError``
    (e.g. the resolver's first guess was actually unwritable even
    though the probe said otherwise, or a TOCTOU race against the
    poisoner), retry once under :func:`_sibling_fallback_root`.
    Only after BOTH attempts fail do we re-raise — silently falling
    back further would mask a genuine filesystem misconfiguration.
    """
    root = resolve_run_root(run_id)
    try:
        root.mkdir(parents=True, exist_ok=True)
        return root
    except OSError:
        # TOCTOU / probe-vs-mkdir disagreement → take the sibling
        # fallback unconditionally. This is the second layer of
        # defense; the resolver's probe is the first.
        sibling = _sibling_fallback_root(run_id)
        if sibling == root:
            raise
        sibling.mkdir(parents=True, exist_ok=True)
        return sibling
