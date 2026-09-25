# M06 — Native approvals, durable facts and audit

## 1. Purpose and scope

Implement the append-only operation/approval/release/receipt fact store and Workbench-native authorization policies. Bind approvals to immutable reviewed inputs, enforce requester/approver/deployer separation, re-resolve target-side group membership and expose audit evidence. Coordination owns single-use consumption mechanics; M06 persists the durable consumption facts and never creates an alternative lock or approval authority.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| New `backend/services/version_control/governance/` | M02 DTOs/ledger/registry, M03 coordination contract |
| New `backend/routers/{vc_approvals,vc_operations}.py` | M08 auth/group/rights provider and Job dispatcher |
| `backend/version_control_ddl/06-operations.sql`, `07-approval-evidence-volume.sql` | M04 gate, M07 preflight/receipt types |
| `backend/tests/{test_vc_operation_facts,test_vc_approvals,test_vc_audit}.py` | Existing auth routes and `system.access.audit` schema when verified |

DDL ownership: `genie_space_operations`, `vc_approval_evidence` Volume. Operations contains typed approval/release/receipt records; do not introduce fifth authoritative lock/table casually. M08 owns effective grants and any shared auth/composition changes.

## 3. Public contract

`OperationFacts.append`, `lookup_request`, `approval_use`, `get_request`, `publish_consumption`, `verify_flush` as in `contracts.md` §4. `ApprovalService.request(inputs, actor)`, `vote(approval_id, decision, actor)`, `authorize(request, executor) -> AuthorizationGrant`, `break_glass(request, actor)`; caller cannot submit an already-authorized grant. Approval/vote/get, operation/audit get and break-glass routes: §7.

Normative schema: §2 operations DDL, with event_id/event_key/transition_sequence, target binding revision, distinct idempotency_key/request_digest, attempt/generation, digests, references, status, actor and typed evidence. `fact_kind` distinguishes operations, create intents, approvals/votes/consumption/invalidation, releases, receipts, recovery and acknowledgements. Approval payload: §6, including raw/canonical source, rendered target, base+metadata+managed-permission policy, mapping/transformer/canonicalizer/test policies, expiry and recovery permission. Large private preflight evidence uses the separate Volume; authority remains Delta.

## 4. Dependencies

Upstream: M02 immutable versions/binding revision/DTOs, M08 server-derived identity/rights/groups/clock. Coordination consumption is invoked through M03 by the gate, not from an independent M06 CAS implementation. M03 consumes only the OperationFacts Protocol (no import cycle). Downstream: M04 all authorization/transition recording, M05 adopt/reapply, M07 target release approval and receipt publishing, M09 audit UI.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_operation_fact_schema_and_idempotent_append_survive_response_loss` in `test_vc_operation_facts.py` | Typed fact validation, deterministic event keys, durable lookup before append replay |
| 2 | `test_completed_request_and_consumed_approval_remain_after_coordination_reuse` in same file | Durable projections indexed logically by binding/revision/key and approval ID |
| 3 | `test_create_intent_is_durable_without_fake_pre_version` in same file | Explicit fact kind and intent evidence validation |
| 4 | `test_approval_digest_changes_for_every_bound_input` in `test_vc_approvals.py`, parameterized across §6 fields | Deterministic immutable approval/request digest construction |
| 5 | `test_vote_identity_is_server_derived_not_request_body` in same file | Authenticated actor binding and immutable vote fact |
| 6 | `test_production_requires_two_distinct_nonrequester_humans` in same file | Production policy validation; SP cannot masquerade as human vote |
| 7 | `test_target_membership_rechecked_by_target_executor_at_execution` in same file | Target-controlled group lookup, membership revocation blocks; source-only approval fails |
| 8 | `test_expiry_over_24_hours_and_stale_binding_revision_are_rejected` in same file | UTC expiry limit and current-binding verification |
| 9 | `test_dev_grant_requires_requester_edit_right_but_release_requires_policy_group` in same file | Separate self-service/edit and governed-release policy branches |
| 10 | `test_historical_approval_reuse_and_different_request_digest_are_rejected` in same file | Read durable consumption and return grant only for permissible exact same-operation recovery |
| 11 | `test_adopt_and_reapply_need_new_approval_for_newly_captured_base` in same file | Fresh request inputs; capture never implicitly approves |
| 12 | `test_break_glass_requires_group_reason_expiry_and_cannot_skip_evidence` in same file | Bound override fact, normal execution suspension, mandatory reconciliation flag |
| 13 | `test_consumption_publish_failure_retains_unresolved_claim` in `test_vc_operation_facts.py` | M03/M06 consumer contract test: finish refuses release until durable flush |
| 14 | `test_audit_correlation_is_optional_evidence_not_approval_or_lock` in `test_vc_audit.py` | Asynchronous/scoped platform-event correlation with unknown actor fallback |
| 15 | `test_untrusted_source_cannot_insert_target_approvals_or_receipts` in same file (integration marker) | Target-owned fact boundary and approved export-only surface via M08 grants |

## 6. Acceptance criteria

- [ ] FM-RESTORE/FM-DRIFT and F-FRESH-APPROVAL: local restore/reapply/adopt honor reviewed base.
- [ ] F-APPROVAL-REUSE/F-IDEMPOTENCY/F-CRASH-STAGES: durable single-use claims survive row reuse/crash.
- [ ] Production approval rules, target-side membership and ≤24h expiry enforced server-side.
- [ ] FM-OUTAGE: unavailable fact append or authorization evidence disables mutation.
- [ ] Approvals/releases/receipts live durably in Delta; audit logs and Volumes alone are not authority.

## 7. Parallel mocks and seam version

**VC/1.0**: mock identity/group/rights, immutable versions, clock, SQL append store and M03 claim/finalization. Publish an OperationFacts fake via M02-owned shared fixtures or keep module-local fixtures; M03 can develop before M06 adapter exists. Backend/UI use identical bound-input fixtures and error codes.

## 8. Risks / verify at implementation time

Verify target membership lookup under actual executor identity, stable human/SP distinction, group freshness and snapshot authorization. Fine-grained DML/property protection must be enforced by M08; application checks alone are insufficient against principals with broader privileges. Operations is one securable: don't claim JSON fact_kind or Volume prefixes isolate source and target roles. Test audit table availability/lag without placing it in the synchronous authorization path. Decide ACL representation explicitly before permitting managed-permission promotion.
