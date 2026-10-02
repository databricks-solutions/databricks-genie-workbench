# M10 — Champion-only optimizer integration

## 1. Purpose and scope

Preserve optimizer baseline/iteration/champion telemetry while moving the final live champion apply through the shared gate. Intermediate candidate evaluation must never mutate a managed binding, and an abandoned/unapplied run contributes no optimizer version. Replace in-process best-effort revert with the governed Job restore/compensation path, without turning optimizer history into the universal version ledger.

## 2. Exclusive ownership

Own runtime/source/test/script files under `packages/genie-space-optimizer/`, **except `packages/genie-space-optimizer/databricks.yml` (M08)**. Own new `backend/services/version_control/optimizer_adapter.py` and `backend/tests/test_vc_optimizer_adapter.py`. Proposed package tests: `tests/test_vc_champion_apply.py`, `test_vc_candidate_isolation.py`, `test_vc_revert_guard.py`. Verify exact source package layout and nested `AGENTS.md` before editing; discover and explicitly record the existing `revert.py` path in the ownership manifest.

Read only: `backend/routers/auto_optimize.py` and its tests (M04), M01 canonicalizer, M02 contracts/ledger, M03/M04 gate, M06 requester policy, M08 bundle/dispatch, existing Lakebase telemetry services. DDL/Volumes: **none**; existing optimizer telemetry remains in its current store and does not gain authority.

## 3. Public contract

`OptimizerChampionAdapter.apply(run_id, champion_id, binding, expected_base, executor) -> OperationResult` (VC/1.0). It builds a `MutationRequest` with `origin='optimizer'`, run/champion linkage, stable run+binding-revision idempotency scope and source payload digest, then calls M04. M04 owns backend auto-optimize adapter changes and routes this contract. Candidate evaluation takes an `EvaluationBinding` positively proven not equal to a managed binding; isolation cannot be a caller boolean.

Read schema: `genie_space_versions.optimizer_run_id/champion_id/origin` and operations idempotency/status in `contracts.md` §2. Only final authoritative champion post-state is optimizer-origin. Successful intermediate managed write checkpoints are operation evidence, not additional champion versions; distinct external preimages/recovery observations remain preserved with correct origin/reason.

## 4. Dependencies

M04 gate/Job restore interface, M01 canonicalizer (direct reusable packaging or injected adapter, no copy), M02 registry proof/DTOs, M06 self-service requester-edit/release approval policy, M08 target-local executor/Job dispatch and dependency packaging. Downstream: M04 existing auto-optimize routes and M09 optimizer history drill-through. M10 does not edit those consumers.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_unapplied_and_abandoned_runs_create_no_version` in package `tests/test_vc_champion_apply.py` | Separate iteration telemetry from apply adapter invocation |
| 2 | `test_candidate_evaluation_cannot_target_managed_binding` in package `tests/test_vc_candidate_isolation.py` | Positive isolated-resource validation and deny managed registry bindings |
| 3 | `test_every_candidate_mutation_uses_optimizer_owned_evaluation_resource` in same file | Route candidate exploration to isolated resources, never temporary live patches |
| 4 | `test_final_champion_applies_once_through_shared_gate` in `backend/tests/test_vc_optimizer_adapter.py` | Adapter constructs origin/run/champion request and invokes gate under Job executor |
| 5 | `test_retry_or_second_champion_in_same_run_cannot_append_second_optimizer_version` in same file | Stable run+binding-revision idempotency key; different champion/digest is conflict, not new key |
| 6 | `test_champion_version_uses_authoritative_readback_not_candidate_payload` in package `tests/test_vc_champion_apply.py` | Consume M04 verified final post-version; preserve normalization differences |
| 7 | `test_external_preimage_at_champion_apply_is_preserved_without_iteration_version` in same file | Allow additional external evidence linked to apply; count optimizer-origin rows separately |
| 8 | `test_noop_champion_has_receipt_but_no_new_optimizer_version` in same file | Respect gate no-op outcome and retain telemetry linkage |
| 9 | `test_partial_or_ambiguous_apply_never_retries_or_reverts_in_process` in package `tests/test_vc_revert_guard.py` | Retire direct best-effort revert; return unresolved status and Job recovery handle |
| 10 | `test_historical_revert_dispatches_governed_restore_job` in same file | Delegate selected historical version/base/approval to M04 Job operation contract |
| 11 | `test_telemetry_memory_fallback_never_authorizes_champion_apply` in adapter test | Mandatory coordination/authorization regardless of telemetry availability |
| 12 | `test_job_executor_verifies_requester_edit_or_release_policy` in adapter test | M06 grant check; no SP confused-deputy bypass |
| 13 | `test_package_install_exposes_shared_gate_without_importing_backend_app` in package champion test | M08-coordinated dependency/entry-point packaging, no FastAPI startup import |

## 6. Acceptance criteria

- [ ] F-CHAMPION: each run has at most one optimizer-origin champion version; no iteration/candidate versions.
- [ ] FM-HISTORY/FM-DRIFT: live champion mutation has durable preimage and authoritative post-state.
- [ ] FM-OUTAGE/FM-RECOVERY: optional telemetry fallback cannot bypass gate; ambiguous apply never blindly replays/reverts.
- [ ] F-COMPENSATION: obsolete in-process revert is retired; restore/compensation goes through Jobs.
- [ ] Existing optimizer telemetry/regression tests stay green and remain independently inspectable.

## 7. Parallel mocks and seam version

**VC/1.0**: fake `MutationGate`, registry isolation proof, M06 grant provider and M08 Job dispatcher. Candidate telemetry fakes stay package-local. M04 receives adapter request/response fixture requirements, not patches to its router or tests. M08 alone changes optimizer DAB manifest and root package dependencies.

## 8. Risks / verify at implementation time

Locate actual `revert.py` and all optimizer mutation utilities; this path is deliberately not invented. Inspect nested package `AGENTS.md`. Verify candidates do not currently patch the managed live agent; isolated resource lifecycle is required before enabling champion integration. Confirm shared gate import/deployment under the optimizer Job without duplicating canonicalization or backend server startup. A retry with a new execution attempt is not a new authorization; run-level at-most-one semantics outlive coordination row reuse.
