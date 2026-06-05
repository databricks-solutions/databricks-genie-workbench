"""Tests for ``run_root_resolver`` — the centralized policy that
resolved the production ``PermissionError: [Errno 13] Permission
denied: '/tmp/gso/<run_id>/iteration_1'`` failure mode."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.run_root_resolver import (
    ensure_run_root,
    resolve_run_root,
)


def test_explicit_plan_v3_override_short_circuits(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GSO_PLAN_V3_RUN_ROOT", str(tmp_path / "plan_v3"))
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path / "phase_h"))
    assert resolve_run_root("run_1") == tmp_path / "plan_v3"


def test_phase_h_override_used_when_plan_v3_absent(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path / "phase_h"))
    assert resolve_run_root("run_1") == tmp_path / "phase_h"


def test_default_path_uses_tempfile_gettempdir(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)
    resolved = resolve_run_root("abc-123")
    # The resolver MUST root under tempfile.gettempdir(), not
    # hardcoded "/tmp" — this is the macOS portability fix.
    assert str(resolved).startswith(tempfile.gettempdir())
    assert resolved.name == "abc-123"
    assert resolved.parent.name == "gso"


def test_empty_run_id_collapses_to_unknown(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)
    resolved = resolve_run_root("")
    assert resolved.name == "unknown"


def test_pid_fallback_when_default_path_not_writable(
    monkeypatch, tmp_path: Path,
) -> None:
    """The production failure mode: ``/tmp/gso/<run_id>`` exists
    but is owned by another user / SP. The resolver must detect
    this and fall back to a PID-suffixed directory.
    """
    # Stub gettempdir to point at our tmp_path so we can simulate
    # the collision deterministically.
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    # Create the existing collision dir and strip write perms.
    collision = tmp_path / "gso" / "shared-run"
    collision.mkdir(parents=True)
    os.chmod(collision, 0o500)  # read+exec, no write
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)

    resolved = resolve_run_root("shared-run")
    try:
        # Resolver MUST pick the SIBLING root that lives OUTSIDE the
        # possibly poisoned ``<tempdir>/gso`` parent (see
        # :func:`_sibling_fallback_root` for the production
        # rationale). The expected shape is
        # ``<tempdir>/gso_<run_id>__pid<PID>``.
        assert resolved != collision
        assert resolved.name.startswith("gso_shared-run__pid")
        assert str(os.getpid()) in resolved.name
        # Crucially the fallback MUST NOT be parented under the
        # poisoned ``<tempdir>/gso`` dir.
        assert (tmp_path / "gso") not in resolved.parents
    finally:
        # Restore perms so pytest can clean up.
        os.chmod(collision, 0o700)


def test_pid_fallback_is_writable(
    monkeypatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    collision = tmp_path / "gso" / "shared-run"
    collision.mkdir(parents=True)
    os.chmod(collision, 0o500)
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)
    try:
        # ensure_run_root MUST succeed without raising — the entire
        # point of the fallback.
        root = ensure_run_root("shared-run")
        assert root.exists()
        # The directory must be writable by us.
        probe = root / "iteration_1"
        probe.mkdir(exist_ok=True)
        assert probe.is_dir()
    finally:
        os.chmod(collision, 0o700)


def test_happy_path_creates_iteration_subdir(
    monkeypatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)
    root = ensure_run_root("clean-run")
    iter_dir = root / "iteration_1"
    iter_dir.mkdir()
    assert iter_dir.exists()


def test_resolver_is_side_effect_free(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)
    resolved = resolve_run_root("never-created")
    # Resolver MUST NOT create the directory — only ensure_run_root does.
    assert not resolved.exists()


# ── Regression tests for the production failure
# ``PermissionError [Errno 13] Permission denied: '/tmp/gso/<run_id>'``
# where the GSO PARENT (not the child run dir) is non-searchable.
# Three invariants the resolver MUST satisfy:
#   1. The writability probe MUST NOT raise on a stat-denied path.
#   2. The fallback MUST NOT be parented under the poisoned dir.
#   3. ensure_run_root() MUST succeed end-to-end.


def test_resolve_does_not_raise_when_gso_parent_is_non_searchable(
    monkeypatch, tmp_path: Path,
) -> None:
    """Reproduces the production traceback: ``/tmp/gso`` exists but
    has no execute/search bit, so ``Path('/tmp/gso/<run_id>').exists()``
    raises ``PermissionError`` from ``os.stat``. The resolver MUST
    catch that and fall back, not propagate."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)

    poisoned_parent = tmp_path / "gso"
    poisoned_parent.mkdir()
    # Strip execute/search bit — stat on any child now raises EACCES.
    os.chmod(poisoned_parent, 0o000)

    try:
        # MUST NOT raise.
        resolved = resolve_run_root("e94376a3")
        assert isinstance(resolved, Path)
    finally:
        os.chmod(poisoned_parent, 0o700)


def test_fallback_is_not_parented_under_poisoned_gso(
    monkeypatch, tmp_path: Path,
) -> None:
    """The fallback root MUST live OUTSIDE the poisoned ``<tempdir>/gso``
    parent — otherwise the same EACCES bites again on mkdir. The
    user's bug report identified this as the second flaw in the
    original hardening."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)

    poisoned_parent = tmp_path / "gso"
    poisoned_parent.mkdir()
    os.chmod(poisoned_parent, 0o000)

    try:
        resolved = resolve_run_root("e94376a3")
        # Critical invariant — the resolved fallback path MUST NOT
        # be a descendant of the poisoned ``<tempdir>/gso`` parent.
        assert poisoned_parent not in resolved.parents
        # The run_id MUST remain recoverable from the path so
        # postmortems can still grep for it.
        assert "e94376a3" in str(resolved)
    finally:
        os.chmod(poisoned_parent, 0o700)


def test_ensure_run_root_succeeds_when_gso_parent_non_searchable(
    monkeypatch, tmp_path: Path,
) -> None:
    """End-to-end: even with the GSO parent fully poisoned,
    ``ensure_run_root`` MUST return a writable directory the caller
    can mkdir iteration_<n> under without raising."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)

    poisoned_parent = tmp_path / "gso"
    poisoned_parent.mkdir()
    os.chmod(poisoned_parent, 0o000)

    try:
        root = ensure_run_root("e94376a3")
        # The returned path MUST exist and be writable enough to
        # mkdir an iteration subdir — that's what the lever loop
        # does immediately after this call.
        assert root.exists()
        (root / "iteration_1").mkdir()
        assert (root / "iteration_1").is_dir()
    finally:
        os.chmod(poisoned_parent, 0o700)


def test_probe_returns_false_on_oserror(
    monkeypatch, tmp_path: Path,
) -> None:
    """Defense-in-depth: any ``OSError`` from the writability probe
    (not just PermissionError) MUST collapse to ``False`` so the
    fallback engages rather than the exception propagating."""
    from genie_space_optimizer.optimization import run_root_resolver as rrr

    def _raise(self):  # type: ignore[no-untyped-def]
        raise OSError(13, "stat denied")

    # Monkeypatch Path.exists at the class level to raise — the
    # probe MUST swallow this and return False.
    monkeypatch.setattr(Path, "exists", _raise)
    assert rrr._is_writable_for_current_process(tmp_path / "nope") is False
