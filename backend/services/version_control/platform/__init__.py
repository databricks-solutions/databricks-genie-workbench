"""Lazy, explicitly injected VC/1.0 adapters; no ambient credentials or clients."""

from collections.abc import Callable
from importlib import import_module
from threading import RLock
from typing import TypeVar, cast

from .feature_flags import WRITE_SWITCHES, FeatureFlags

Service = TypeVar("Service")
_EXPORTS = {
    name: module
    for module, names in {
        "bundles": ("bundle_detection_evidence", "deployment_inventory", "guard_bundle", "preflight_deployment"),
        "provisioning": ("OwnerMigrationRunner", "PROVISION_ROLES", "build_grant_matrix", "build_owner_manifests", "provision", "repeatable_sandbox_provision"),
        "permissions": ("verify_artifact_permissions", "verify_coordination_permissions", "verify_fact_permissions"),
        "capabilities": ("FIRST_WRITE_CAPABILITIES", "REQUIRED_WRITER_PATHS", "capabilities_ready", "storage_write_ready", "topology_read_ready"),
        "identity": ("PlatformIdentityProvider", "TrustedSnapshotReader", "verify_configured_job_run_as", "verify_job_run_as"),
        "termination": ("PlatformTerminationEvidenceProvider",),
    }.items()
    for name in names
}


def __getattr__(name):
    if name in {"contracts", "REQUIRED_WRITE_PORTS"}:
        contracts = import_module("..contracts", __name__)
        value = contracts if name == "contracts" else frozenset({
            contracts.Coordination, contracts.OperationFacts, contracts.MutationGate,
            contracts.IdentityProvider, contracts.Registry, contracts.VersionLedger,
            contracts.GenieTransport, contracts.ApprovalService, contracts.TerminationEvidenceProvider,
        })
    elif name in _EXPORTS:
        value = getattr(import_module(f".{_EXPORTS[name]}", __name__), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


class Composition:
    def __init__(self, flags: FeatureFlags | None = None, *, capability_probe=None, routed_writers=()) -> None:
        self._flags = flags if flags is not None else FeatureFlags()
        self._factories: dict[type, Callable[[], object]] = {}
        self._instances: dict[type, object] = {}
        self._lock = RLock()
        self._capability_probe = capability_probe
        self._routed_writers = frozenset(routed_writers)

    @property
    def flags(self) -> FeatureFlags:
        return self._flags

    def enabled(self, name: str) -> bool:
        if not self.flags.enabled(name):
            return False
        if name not in WRITE_SWITCHES:
            return True
        from . import REQUIRED_WRITE_PORTS
        from .capabilities import (
            FIRST_WRITE_CAPABILITIES,
            REQUIRED_WRITER_PATHS,
            capabilities_ready,
        )

        with self._lock:
            integrated = (REQUIRED_WRITE_PORTS <= self._factories.keys()
                          and REQUIRED_WRITER_PATHS <= self._routed_writers)
        return integrated and capabilities_ready(self._capability_probe, FIRST_WRITE_CAPABILITIES)

    def require_write(self, name: str) -> None:
        if name not in WRITE_SWITCHES or not self.enabled(name):
            raise PermissionError("VC write disabled: missing safety capability or integrated writer")

    def register(self, port: type[Service], factory: Callable[[], Service]) -> None:
        with self._lock:
            if port in self._factories:
                raise ValueError(f"{port.__name__} already registered")
            self._factories[port] = factory

    def resolve(self, port: type[Service]) -> Service:
        with self._lock:
            if port not in self._instances:
                self._instances[port] = self._factories[port]()
            return cast(Service, self._instances[port])


def compose(factories=None, *, flags: FeatureFlags | None = None,
            capability_probe=None, routed_writers=()) -> Composition:
    container = Composition(flags, capability_probe=capability_probe, routed_writers=routed_writers)
    for port, factory in (factories or {}).items():
        container.register(port, factory)
    return container
