# M02 — Immutable versions, identity and registry

## 1. Purpose and scope

Implement durable API-observed versions and stable logical identity, including provisional create, human rebind and immutable lineage. Supply shared typed contracts/test fixtures so all other modules can develop independently. Registry enrollment is serialized and distinct from mutation admission; this module never calls Genie or grants a mutation lease.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| New `backend/services/version_control/{__init__,contracts,ledger,registry,artifacts}.py` | M01 canonicalizer, M03 initializer Protocol implementation |
| New `backend/routers/vc_history.py` | M08 auth/SQL/Volume adapters, `backend/models.py` |
| `backend/version_control_ddl/01-versions.sql`, `02-registry.sql`, `03-snapshot-volume.sql`, `04-current-binding-view.sql` | M06 operation/create-intent facts |
| `backend/tests/test_vc_ledger.py`, `test_vc_registry.py`, `test_vc_contracts.py` | Existing Lakebase services (not reused as authority) |
| `backend/tests/vc_fakes/`, `backend/tests/fixtures/vc_contracts/` | Other modules' tests |

DDL ownership: `genie_space_versions`, `genie_space_registry`, current-binding view, `vc_snapshots` Volume. M08 executes migrations/grants. No ownership of operations/coordination DDL or shared `conftest.py`.

## 3. Public contract

`VersionLedger.append_observation(snapshot, context) -> ObservationRef`; `get_version(binding, version_id) -> Version`; `history(binding, cursor, limit) -> VersionPage`; `verify_committed(ref) -> bool`. `Registry.enroll(request, actor) -> BindingRef`, `bind_created(intent, space_id, evidence) -> BindingRef`, `tombstone/rebind(..., RebindAuthorization) -> BindingRef`. Expose history/version/diff GET routes in `contracts.md` §7; diff delegates to M01.

Normative Delta DDL: `contracts.md` §2 **versions and registry**, implemented byte-for-byte in migrations unless a reviewed VC revision is needed. Versions: immutable version_id, binding/revision, observation_key, exact/canonical forms, three fingerprints, lineage and operation/optimizer/release attribution. Registry: immutable registry_event_id, binding/revision/event_sequence, provisional/bound/tombstoned/rebound events and supersession. Active view rejects ambiguity rather than selecting a timestamp winner. Volume layout: §3 snapshot rows/pointers and digest verification.

## 4. Dependencies

Upstream contracts: M01 canonicalizer; M03 `Coordination.initialize` through injected `EnrollmentInitializer`; M06 `OperationFacts` for create-intent references; M08 authenticated artifact/SQL and enrollment serialization adapters. None is imported concretely in registry. Downstream: every module consumes DTOs; M03 verifies evidence refs, M04 captures, M05 projects, M06 approves immutable sources, M07 packages. M02's enrollment coordinator is the only caller authorized to request row INSERT; M03 owns its mechanics.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_vc_wire_contract_golden_roundtrip` in `test_vc_contracts.py`: IDs, nullable fields, enums, requests/receipts match VC/1.0 | Shared immutable dataclasses/Protocols and fixture JSON; no platform code |
| 2 | `test_append_requires_authoritative_observation` in `test_vc_ledger.py`: submitted desired JSON alone cannot become a version | Typed capture provenance and raw/canonical serialization |
| 3 | `test_envelope_pointer_is_published_only_after_digest_verified_upload` in same file | Snapshot artifact writer and commit ordering |
| 4 | `test_adjacent_duplicate_observation_reuses_but_a_b_a_retains_history` in same file | Serialized observation-key/idempotent append lookup, not global content dedup |
| 5 | `test_lost_append_response_resolves_by_event_key_without_duplicate` in same file: ambiguous write first queries durable key | Read-after-ambiguous-append path; inconsistent duplicate facts fail closed |
| 6 | `test_provisional_identity_precedes_create_and_has_no_physical_id` in `test_vc_registry.py` | Serialized enrollment creates stable binding_id/revision and records intent reference |
| 7 | `test_duplicate_enrollment_and_physical_binding_forks_are_quarantined` in same file: two attempts cannot return two valid owners | Enrollment serialization + validate registry chain and one coordination row; signal quarantine |
| 8 | `test_partial_enrollment_is_not_write_ready` in same file: crash after registry append/before row init blocks use | Explicit incomplete enrollment checks and audited repair result |
| 9 | `test_bind_created_requires_audited_identity_not_display_name` in same file | Append bound event linked to create intent; reject heuristic matching |
| 10 | `test_tombstone_and_human_rebind_invalidate_old_revision` in same file | Superseding revision events; current-binding view and old-token rejection |
| 11 | `test_restore_lineage_is_append_only_and_first_create_has_no_parent` in `test_vc_ledger.py` | Validate parent/restored_from linkage without rewriting history |
| 12 | `test_history_pagination_authorization_and_stale_coordination_reads` in same file: snapshot access scoped; cursor stable; read-only history works when coordination UPDATE fails | History/diff router with injected authorization and independent read path |
| 13 | `test_fact_tables_reject_update_delete_and_property_change` in `test_vc_ledger.py` (integration marker) | Owner DDL with append-only and hand off least-privilege grants to M08; no runtime ALTER capability |

## 6. Acceptance criteria

- [ ] FM-HISTORY/FM-RESTORE: immutable pre/post lineage, arbitrary historical retrieval and comparison.
- [ ] F-CREATE/F-ENROLLMENT/F-REBIND: provisional identity, no name-based orphan recovery, duplicate enrollment blocked.
- [ ] F-CRASH-STAGES: uncommitted/ambiguous evidence never reported as a committed preimage.
- [ ] FM-OUTAGE: coordination-write outage does not unnecessarily block available history reads.
- [ ] Canonical and raw data retained without credentials; source/target snapshot authorization enforced.

## 7. Parallel mocks and seam version

**VC/1.0**. M02 owns reusable fake append store, CAS-store test double interface, explicit clock, fault checkpoints, typed identity and canonicalizer fixtures under `vc_fakes/`. M03 owns CAS-specific test scenarios, not shared fake files. Mock M03 initializer, M06 facts, M08 artifact/SQL executors; contract tests run against real adapters after integration. Requests for shared fake changes go through M02.

## 8. Risks / verify at implementation time

Delta uniqueness is a protocol invariant, never a PK claim. Verify enrollment serialization survives multiple app/Job processes and partial failures. Validate append-only grants/ALTER denial and compatibility on actual compute with M08; broad MODIFY is not an equivalent default. Verify snapshot size/Volume threshold and permission policy. DDL namespace and timestamp choices are configuration, not production literals. Cross-binding physical ownership forks must disable both bindings, not just the newly observed one.
