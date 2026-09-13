"""Offline composition test for the observe-only VC runtime.

Injects the same fake low-level ``adapters`` used by the promotion graph test into
``build_observe_runtime(config, adapters=...)`` and asserts the *real* leaf
constructors assemble into a working observe runtime: an enroll-capable registry, a
writes-enabled ledger, a coordination service, a Genie transport, and an Observer bound
to them. No live platform — only the lowest-level clients are faked — so this pins the
wiring and leaves credentials/DDL to the deploy step.
"""

from uuid import UUID

import pytest

from backend.services.config_fingerprint import Canonicalizer
from backend.services.genie_client import GenieTransport
from backend.services.version_control import contracts as vc
from backend.services.version_control.coordination.service import CoordinationService
from backend.services.version_control.ledger import DeltaVersionLedger
from backend.services.version_control.observer import Observer
from backend.services.version_control.platform.observe_seams import (
    build_observe_runtime,
    resolve_observe_runtime,
)
from backend.services.version_control.registry import DeltaRegistry
from backend.services.version_control.version_tags import DeltaVersionTagStore
from backend.tests.test_vc_live_seams import FakeAdapters, TARGET_HOST


def _config():
    return {
        "workspace_id": "target",
        "catalog": "sandbox_cat",
        "control_schema": "vc_ctl",
        "warehouse_id": "wh-123",
        "target_warehouse_id": "wh-123",
        "flags": {"vc_writes_enabled": True, "vc_history_enabled": True},
        "target_selection": {"workspace_id": "target", "host": TARGET_HOST,
                             "principal_id": "target-sp", "execution_ref": "job/123",
                             "profile": "target-profile"},
    }


def test_observe_runtime_assembles_with_real_leaves():
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    assert isinstance(runtime.observer, Observer)
    assert isinstance(runtime.ledger, DeltaVersionLedger)
    # Version-tag store is wired onto the runtime, qualified to the control schema
    # (backtick-quoted `cat`.`schema`.`genie_space_version_tags`).
    assert isinstance(runtime.tag_store, DeltaVersionTagStore)
    assert "genie_space_version_tags" in runtime.tag_store.table
    assert isinstance(runtime.registry, DeltaRegistry)
    assert isinstance(runtime.coordination, CoordinationService)
    assert isinstance(runtime.transport, GenieTransport)
    assert isinstance(runtime.canonicalizer, Canonicalizer)
    assert runtime.workspace_id == "target"


def test_registry_is_enroll_capable_and_ledger_writes_enabled():
    """Observe/capture writes: the registry must be able to auto-enroll (writes_enabled
    with an initializer that seeds the coordination row) and the ledger must accept
    observations. Both are still flag-gated at the call sites."""
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    assert runtime.registry.writes_enabled is True
    assert runtime.registry.initialize is not None  # enroll -> coordination row initializer
    assert runtime.ledger.writes_enabled is True
    # Capture provenance actor is the trusted target SP.
    assert runtime.actor.workspace_id == "target" and runtime.actor.actor_kind == "service"


def test_actor_trusts_app_entry_identity_without_scim():
    """Option A: the observe runtime resolves the actor from the app-entry (proxy-verified)
    identity, not a second SCIM lookup. The fake provider tags actors "service"; the
    entry-trusted wrapper returns a "human" actor built directly from the forwarded
    reference, proving the governed SCIM resolve_actor is bypassed for observe reads."""
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    actor = runtime.identity.actor(vc.AuthenticatedRequest("alice@corp.com", "target"))
    assert actor == vc.ActorContext("alice@corp.com", "target", "human")


def test_actor_strips_workspace_suffix_from_token_forwarded_reference():
    """Personal-token access forwards ``<subject>@<workspace_id>``; the workspace suffix is
    an access artifact and is stripped, while an email (a different @) is preserved."""
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    assert runtime.identity.actor(
        vc.AuthenticatedRequest("599900@target", "target")
    ) == vc.ActorContext("599900", "target", "human")


def test_actor_rejects_workspace_mismatch_or_empty_subject():
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    with pytest.raises(PermissionError, match="Server-authenticated"):
        runtime.identity.actor(vc.AuthenticatedRequest("alice@corp.com", "other-ws"))
    with pytest.raises(PermissionError, match="Server-authenticated"):
        runtime.identity.actor(vc.AuthenticatedRequest("", "target"))


def test_non_actor_identity_methods_delegate_to_governed_provider():
    """The wrapper overrides only actor(); executor() (and every other identity method)
    delegates to the underlying provider unchanged, so read/capture leaves are untouched."""
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    executor = runtime.identity.executor(runtime.reader_selection)
    assert executor.workspace_id == "target"


def test_authorize_history_is_target_local():
    runtime = build_observe_runtime(_config(), adapters=FakeAdapters())
    binding = vc.BindingRef(str(UUID(int=1)), 1, "sales", "target", "space-1", "dev")
    assert runtime.authorize_history(vc.ActorContext("sp", "target", "service"), binding) is True
    assert runtime.authorize_history(vc.ActorContext("sp", "other", "service"), binding) is False


def test_incomplete_config_stays_fail_closed():
    broken = {key: value for key, value in _config().items() if key != "catalog"}
    with pytest.raises(PermissionError, match="not integrated"):
        build_observe_runtime(broken, adapters=FakeAdapters())


@pytest.mark.parametrize("config", [None, {}, {"workspace_id": "target"}])
def test_resolve_returns_none_when_unconfigured(config):
    assert resolve_observe_runtime(config, adapters=FakeAdapters()) is None


def test_resolve_builds_runtime_when_fully_configured():
    runtime = resolve_observe_runtime(_config(), adapters=FakeAdapters())
    assert runtime is not None and isinstance(runtime.observer, Observer)
