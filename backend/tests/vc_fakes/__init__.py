"""Reusable deterministic VC doubles. Never import this package in runtime code."""

from .fixtures import (
    FakeCanonicalizer, FakeIdentityProvider, actor_fixture, binding_fixture, executor_fixture,
)
from .stores import (
    AmbiguousRows, CoordinationRow, CoordinationStateStore, CrashBoundary,
    FactRecord, FactState, FailureInjector, FakeCoordinationStore, FakeFactStore,
    InjectedCrash, ManualClock,
)

__all__ = [
    "AmbiguousRows", "CoordinationRow", "CoordinationStateStore", "CrashBoundary",
    "FactRecord", "FactState", "FailureInjector", "FakeCanonicalizer",
    "FakeCoordinationStore", "FakeFactStore", "FakeIdentityProvider", "InjectedCrash",
    "ManualClock", "actor_fixture", "binding_fixture", "executor_fixture",
]
