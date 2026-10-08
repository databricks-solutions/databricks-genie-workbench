# M04 — Shared mutation gate, observer and restore

## 1. Purpose and scope

Wrap every existing Workbench mutation in the shared gate, implement two-PATCH execution and read-back evidence, and provide serialized capture-on-open plus Job-based historical restore. Everyday create/edit retain OBO execution; governed restore/production/optimizer/promotion operations run target-locally in Jobs. This is the sole owner of the legacy mutation call sites, preventing several modules from editing the same routers.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| `backend/services/genie_client.py` | M01 canonicalizer and M02 ledger/registry/contracts |
| `backend/routers/{create,spaces,auto_optimize}.py` | M03 coordination, M06 authorization/facts |
| `backend/services/{create_agent,create_agent_session,create_agent_tools}.py`, `backend/genie_creator.py` | `backend/main.py`, `backend/services/auth.py` (M08) |
| New `backend/services/version_control/{mutation_gate,observer,restore}.py` | M05 classification/reconcile port, M10 champion adapter |
| New `backend/routers/vc_mutations.py`, `backend/jobs/vc_restore.py` | Optimizer package implementation and `revert.py` (M10) |
| Extend `backend/tests/{test_genie_client,test_current_version,test_auto_optimize_router,test_create_agent}.py` | Shared `backend/models.py`, dependency manifests |
| New `backend/tests/{test_vc_mutation_gate,test_vc_observer,test_vc_create,test_vc_restore}.py` | M08 integration scaffold |

DDL/Volumes: none. Reads/writes through M02/M03/M06 ports. All modifications within listed create helpers are limited to mutation integration; no unrelated agent changes. If another legacy write path is discovered, stop that bypass and obtain a reviewed ownership assignment before editing it.

## 3. Public contract

`MutationGate.execute(request, executor)`, `create(request, executor)`, `verify_only(operation_id, executor) -> OperationResult`; `Observer.capture_on_open(binding, viewer)`/`capture(binding, reason, executor) -> ObservationResult`. `GenieTransport` exposes GET with full serialized state and separately fenced `create_once`, `patch_config_once`, `patch_description_once`. No transport method permits caller-controlled retry.

M04 owns POST observe/restore routes in `contracts.md` §7; existing endpoints remain adapters. `backend/jobs/vc_restore.py` accepts **operation_id only**, loads immutable authorized request, invokes gate and tests/read-back. Consumed table schemas: versions/registry/coordination/operations in §2; M04 does not run ad-hoc SQL or author DDL. All persisted post versions include attempt/generation, parent and restored_from when applicable.

## 4. Dependencies

M01 normalization; M02 immutable evidence/identity; M03 admission/checkpoints/observer lease; M06 `ApprovalService` + `OperationFacts`; M08 explicit identity/Genie client/Job dispatcher/worker lifetime. M05 policy classification is consumed by old current-version adapters, not imported into the low-level gate. M07 and M10 invoke the gate; they never send direct managed PATCHes. M09 consumes observer/restore API responses.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_gate_has_no_transport_write_before_reserve_evidence_and_admit` in `test_vc_mutation_gate.py`: ordered spy trace | Gate orchestration with mandatory commit/admission ports |
| 2 | `test_noop_all_three_fingerprints_skips_both_patches` in same file | Compare normalized desired/live and durable no-op finalization |
| 3 | `test_reviewed_base_mismatch_preserves_external_preimage_and_blocks` in same file | Fresh GET before admission; explicit conflict, not comparison only to cached head |
| 4 | `test_two_patch_happy_path_checkpoints_and_reasserts_each_send` in same file | Config PATCH → GET/checkpoint → conditional description PATCH → final GET/version |
| 5 | `test_description_requires_expected_config_and_approved_metadata_preimage` in same file | Stop partial/conflicted; field-independent completion only under explicit bound policy |
| 6 | `test_config_timeout_matching_readback_never_replays_or_sends_description` in same file | Disable write retries, quarantine unresolved; verification-only follow-up |
| 7 | `test_description_timeout_and_5xx_quarantine_without_replay` in same file (parameterized) | Apply same fencing/ambiguity rules to second PATCH |
| 8 | `test_successful_patch_then_failed_readback_is_applied_unverified` in same file | Persist status/quarantine; no success from API response alone |
| 9 | `test_worker_crash_at_every_send_boundary_keeps_attempt_unresolved` in same file | Record send intent before call; resume reads/evidence only after ambiguous stage |
| 10 | `test_capture_on_open_commits_before_display_and_advances_only_observed` in `test_vc_observer.py` | Trusted reader, serialized capture and observed-only projection |
| 11 | `test_concurrent_opens_deduplicate_without_regressing_or_mislabeling_managed_checkpoint` in same file | Acquire observer lease before GET; busy active mutation stays managed |
| 12 | `test_capture_persistence_failure_returns_stale_and_disables_actions` in same file | Stale/busy response; no approved/deployed updates or false realignment |
| 13 | `test_create_lost_response_keeps_provisional_binding_without_replay` in `test_vc_create.py` | Durable create-intent admission before POST; quarantine possible orphan |
| 14 | `test_create_persists_physical_binding_before_follow_on_edit` in same file: first GET version parent null; bind failure stops edits | Bind returned identity, commit authoritative first version, only then gated edits |
| 15 | `test_restore_is_job_executed_and_appends_historical_lineage` in `test_vc_restore.py` | Validate historical schema/deps, enqueue local Job and use gate |
| 16 | `test_compensation_requires_preauthorization_and_unchanged_post_stage` in same file | New guarded compensation operation; external drift blocks rollback |
| 17 | `test_all_existing_mutation_entrypoints_use_gate_and_fail_closed` across existing test files above: create helpers/edit/auto-optimize/current-version adapters cannot bypass | Replace direct sends with injected gate; keep route compatibility and telemetry separation |
| 18 | `test_transport_disables_hidden_sdk_retries_for_post_and_both_patches` in `test_genie_client.py` | Explicit non-retrying mutation transport; GET retries bounded independently |

## 6. Acceptance criteria

- [ ] FM-HISTORY/FM-RESTORE/FM-DRIFT: pre/post evidence, arbitrary-version Job restore, fresh-base conflict.
- [ ] FM-CONCURRENCY: concurrent route requests produce one admitted two-PATCH sequence.
- [ ] FM-OUTAGE/FM-RECOVERY: every create/edit/restore path is fail-closed, no ambiguous replay.
- [ ] F-DESCRIPTION/F-APP-CRASH/F-CREATE/F-OPENS/F-CRASH-STAGES tests all pass.
- [ ] F-CHAMPION seam supplied for M10; capture-on-open changes observed only, not policy drift.

## 7. Parallel mocks and seam version

**VC/1.0**: mock M01–03/M06 ports and M08 identity/dispatch. Fake Genie is stateful with independent config/description persistence, delayed completion, normalizing GET and transport response loss. M05 supplies status fixture expectations; M10 supplies champion request fixtures. All legacy router edits stay with M04; M08 mounts new routers.

## 8. Risks / verify at implementation time

Inventory actual GET/PATCH/POST call sites, including tool-invoked follow-on create writes and `auto_optimize.py`. Verify SDK retry defaults and request metadata semantics. Keep v1 two-PATCH behavior regardless of ETag preview. Confirm trusted production full-read permission and app-worker termination evidence. Locate actual optimizer `revert.py` with M10; M10 retires implementation, M04 changes backend callers. Unexpected create response and possible orphan need operator evidence, never display-name lookup. Do not claim API atomicity against external editors.
