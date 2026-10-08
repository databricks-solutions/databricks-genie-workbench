"""Offline identity/canonicalizer fixtures, not production policy or normalization."""

import json

from backend.services.version_control.contracts import (
    ActorContext, AuthenticatedRequest, BindingRef, Comparison, DiffCategory,
    DiffChange, DiffItem, ExecutorContext, ExplicitExecutorSelection, Fingerprints,
    Snapshot, canonical_json_hash,
)


def binding_fixture() -> BindingRef:
    return BindingRef("00000000-0000-4000-8000-000000000001", 1, "sales", "123", "physical-space", "dev")


def actor_fixture() -> ActorContext:
    return ActorContext("reader@example.invalid", "123", "human")


def executor_fixture() -> ExecutorContext:
    return ExecutorContext("123", "https://example.invalid", "target-service", "service", object(), "job/456")


class FakeCanonicalizer:
    """Exact structured comparison only; M01 must test actual normalization."""

    def observe(self, envelope: dict, version: str = "vc-c14n/1") -> Snapshot:
        if version != "vc-c14n/1":
            raise ValueError("Unsupported fake canonicalizer version")
        if "serialized_space" not in envelope:
            raise ValueError("A full GET envelope with serialized_space is required")
        serialized = envelope["serialized_space"]
        if isinstance(serialized, str):
            serialized = json.loads(serialized)
        if not isinstance(serialized, dict):
            raise ValueError("serialized_space must be a parsed JSON object")
        metadata = {"description": envelope["description"]} if "description" in envelope else {}
        canonical = {
            "config": {key: value for key, value in serialized.items() if key != "benchmarks"},
            "benchmark": serialized.get("benchmarks", {}),
            "metadata": metadata,
        }
        fingerprints = Fingerprints(
            canonical_json_hash("vc-config/1", canonical["config"]),
            canonical_json_hash("vc-benchmark/1", canonical["benchmark"]),
            canonical_json_hash("vc-metadata/1", metadata), version,
        )
        return Snapshot(
            envelope, serialized, metadata, canonical_json_hash("vc-envelope/1", envelope),
            canonical_json_hash("vc-raw/1", {"serialized_space": serialized, "metadata": metadata}),
            canonical, fingerprints, fingerprints.state_digest,
        )

    def compare(self, left: Snapshot, right: Snapshot) -> Comparison:
        if any(snapshot.fingerprints.canonicalizer_version != "vc-c14n/1" for snapshot in (left, right)):
            return Comparison.UNKNOWN
        return Comparison.EQUAL if left.fingerprints == right.fingerprints else Comparison.DIFFERENT

    def semantic_diff(self, left: Snapshot, right: Snapshot) -> list[DiffItem]:
        if self.compare(left, right) == Comparison.UNKNOWN:
            raise ValueError("Cannot diff unsupported canonicalizer versions")
        return [DiffItem(category, f"/{key}", DiffChange.MODIFIED,
                         left.canonical_state[key], right.canonical_state[key], True)
                for key, category in (("config", DiffCategory.INSTRUCTIONS),
                                      ("benchmark", DiffCategory.BENCHMARKS),
                                      ("metadata", DiffCategory.METADATA))
                if left.canonical_state[key] != right.canonical_state[key]]


class FakeIdentityProvider:
    def __init__(self) -> None:
        self.actors: dict[str, ActorContext] = {}
        self.memberships: dict[tuple[str, str], frozenset[str]] = {}
        self.edit_rights: set[tuple[str, str, str, int]] = set()
        self.executors: dict[ExplicitExecutorSelection, ExecutorContext] = {}
        self.run_as: dict[str, str] = {}

    def actor(self, request: AuthenticatedRequest) -> ActorContext:
        actor = self.actors.get(request.authentication_reference)
        if actor is None or actor.workspace_id != request.workspace_id:
            raise PermissionError("No authenticated actor for this workspace")
        return actor

    def groups(self, subject_id: str, workspace_id: str) -> frozenset[str]:
        return self.memberships.get((subject_id, workspace_id), frozenset())

    def can_edit(self, subject_id: str, binding: BindingRef) -> bool:
        return (subject_id, binding.workspace_id, binding.binding_id, binding.binding_revision) in self.edit_rights

    def executor(self, selection: ExplicitExecutorSelection) -> ExecutorContext:
        executor = self.executors.get(selection)
        if executor is None or (executor.workspace_id, executor.host, executor.principal_id, executor.execution_ref) != (
                selection.workspace_id, selection.host, selection.principal_id, selection.execution_ref):
            raise PermissionError("Explicit executor selection is not configured or does not match")
        return executor

    def verify_run_as(self, execution_ref: str, expected_principal_id: str) -> None:
        if self.run_as.get(execution_ref) != expected_principal_id:
            raise PermissionError("Unexpected run_as; fake does not self-heal identities")
