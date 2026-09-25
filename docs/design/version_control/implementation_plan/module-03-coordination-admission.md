# M03 — Coordination, admission and quarantine

## 1. Purpose and scope

Provide the v1 Delta coordination authority with one unresolved attempt per binding, Serializable UPDATE-only CAS, unique attempt identity, durable request/approval claims and fail-closed recovery. Distinguish reserving from authorizing a write, preserve claims across crashes, and serialize observer head updates. This module does not send Genie mutations or decide who may approve.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| New `backend/services/version_control/coordination/` | M02 shared DTOs, registry/ledger and fakes |
| `backend/version_control_ddl/05-coordination.sql` | M06 OperationFacts implementation |
| `backend/tests/test_vc_coordination.py`, `test_vc_recovery.py` | M04 transport and worker lifecycle |
| `backend/tests/integration/test_vc_delta_cas.py` | M08 SQL adapter, grants and identity verification |

DDL ownership: only `genie_ops_coordination`; no Volumes. M08 owns any new shared integration-test configuration. M03 can author its own fixtures within its owned test files.

## 3. Public contract

VC/1.0 `Coordination.initialize`, `reserve`, `admit`, `renew`, `assert_owner`, `checkpoint`, `finish`, `quarantine`, `recover`, `observe_exclusively`, `advance_heads` signatures are frozen in `contracts.md` §4. `reserve` returns no PATCH capability. `admit(reservation, committed_preimage_or_create_intent, AuthorizationGrant) -> AdmissionClaim` is the sole write-authorizing transition. `FenceToken` contains binding revision, attempt_id, generation and row_version, never just SP identity.

Normative schema: `contracts.md` §2 coordination DDL. One existing row per binding_id, Serializable isolation, generation and row_version, lease/attempt/executor identity, active op/key/digest, consumed approval, committed preimage, stage/checkpoint, unresolved/quarantine state and three separate heads. No executor INSERT or automatic expired-row takeover. No HTTP route ownership; M06/M08 expose authorized operator recovery dispatch.

## 4. Dependencies

Upstream ports only: M02 committed evidence verification/current binding, `OperationFacts` Protocol (implemented M06), M08 SQL/clock/termination evidence adapter. M03 must not import M06 policy/fact implementation; this prevents an implementation cycle. Downstream: M04 writer/observer, M05 head-changing reconciliation through the gate, M06 policy commands/consumption, M07/M10 indirectly through M04.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_missing_or_duplicate_ownership_row_blocks_admission` in `test_vc_coordination.py` | Row cardinality/current-revision validation; initialization requires enrollment proof |
| 2 | `test_two_concurrent_reservations_have_exactly_one_winner` in same file | Predicate CAS at Serializable, update-only, loser conflict; acquisition read-back |
| 3 | `test_same_sp_stale_attempt_cannot_renew_or_patch` in same file | Fence by attempt_id+generation+revision, never principal alone |
| 4 | `test_reservation_is_not_admission_and_requires_committed_preimage` in same file | Separate state machine and evidence verification; create-intent exception typed explicitly |
| 5 | `test_admission_atomically_binds_key_digest_approval_and_preimage` in same file | One CAS updating all authorizing fields; verify exact claim by read-back |
| 6 | `test_same_key_different_digest_rejected_after_row_reuse` in same file | Durable OperationFacts lookup under reservation; distinct key/digest columns |
| 7 | `test_duplicate_completed_request_returns_receipt_without_admission` in same file | Stable same-digest completion lookup; preserve original operation_id |
| 8 | `test_consumed_approval_survives_row_reuse_and_crash` in same file | CAS claim plus append consumption; refuse row release until publish verified |
| 9 | `test_expired_lease_quarantines_without_takeover` in `test_vc_recovery.py` | Expiry transition retains unresolved claim and blocks all new attempts |
| 10 | `test_matching_get_after_timeout_cannot_resolve_attempt` in same file | Separate observed equality from trusted termination |
| 11 | `test_recovery_needs_exact_attempt_termination_and_three_post_termination_reads` in same file | Validate Job retries/children or app-worker proof, stability span > verified bound, then generation bump |
| 12 | `test_unknown_request_lifetime_disables_auto_clear` in same file | Capability gate; explicit audited human recovery path, never heartbeat-based clear |
| 13 | `test_crashes_between_evidence_cas_consumption_patch_and_release_remain_safe` in `test_vc_coordination.py`, parameterized | Durable checkpoints/resume classification; publication failure retains row, no replay |
| 14 | `test_observer_cannot_regress_or_approve_heads` in same file | Serialized observation lease and field-limited fenced head updates |
| 15 | `test_delta_outage_or_ambiguous_cas_never_returns_write_authority` in same file | Typed fail-closed errors; no memory/Lakebase fallback or retry acquisition |
| 16 | `test_real_delta_same_row_cas_one_winner` in `integration/test_vc_delta_cas.py`: independent clients race barrier; inspect row and facts | Validate SQL adapter/isolation on actual Delta, handle conflicts as losing attempts, not mutation retry |

## 6. Acceptance criteria

- [ ] FM-CONCURRENCY: one admitted sequence, losing request gets durable conflict/quarantine fact.
- [ ] FM-OUTAGE/FM-RECOVERY and F-APP-CRASH/F-DESCRIPTION: failure cannot authorize replay or takeover.
- [ ] F-ENROLLMENT/F-IDEMPOTENCY/F-APPROVAL-REUSE/F-CRASH-STAGES covered with real persisted claim recovery.
- [ ] F-OPENS: observed-only updates serialize with mutation reservations and cannot regress history.
- [ ] Real Delta tests prove the CAS and grants; an in-memory fake is not acceptance evidence.

## 7. Parallel mocks and seam version

**VC/1.0**. Mock `OperationFacts`, `VersionLedger.verify_committed`, current-binding resolver, explicit clock, trusted termination provider and SQL statements. Use M02's transactional fake with injected crash boundaries and a per-test execution trace. Consumers mock this Protocol, never duplicate CAS SQL. M08 integrates production adapter wiring.

## 8. Risks / verify at implementation time

Verify fine-grained DML and Serializable setting on actual compute; no unenforced unique key assumption. Verify SDK SQL execution ambiguity/read-back behavior. Determine trusted app-worker identity/termination mechanism and Job child/retry termination coverage. Genie maximum server request lifetime is unresolved: keep auto-clear disabled until measured/documented evidence is accepted. Ensure old-revision tokens cannot mutate a rebound row. Cross-table transactions and ETags are not prerequisites or shortcuts.
