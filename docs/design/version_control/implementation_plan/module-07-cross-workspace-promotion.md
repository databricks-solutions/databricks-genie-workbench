# M07 — Target-local cross-workspace promotion

## 1. Purpose and scope

Build content-addressed portable packages, immutable schema-aware mappings, target-local preflight/dispatch/execution and durable receipts. Promotion pulls immutable source inputs into a pre-enrolled target and invokes the same gate as local writes; it never gains source-held target Genie credentials or creates an agent opportunistically during deployment. Implement offline pieces in parallel, but enable promotion only after local version/restore acceptance.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| New `backend/services/version_control/promotion/` | M01 canonicalization and M02 artifact/ledger ports |
| New `backend/routers/vc_releases.py`, `backend/jobs/vc_promote.py` | M03 coordination, M04 gate, M05 drift |
| `backend/version_control_ddl/08-promotion-volumes.sql` | M06 approval/operation facts and M08 identity/dispatch/provisioning |
| `backend/tests/{test_vc_packages,test_vc_mapping,test_vc_promotion,test_vc_dispatch}.py` | Existing UC/Genie clients; do not modify them |
| `backend/tests/integration/test_vc_two_workspace_promotion.py` | M08 integration fixtures |

DDL ownership: `vc_outbound_packages` and `vc_target_receipts` Volumes only. Receipt/release **fact schema** is M06 operations DDL; M07 owns typed payload semantics in §6 via reviewed M02 DTO changes. No new command queue table or separate orchestrator.

## 3. Public contract

`PromotionService.package(version_id, mapping, policy) -> PackageRef`, `preflight(operation_id, executor) -> PreflightEvidence`, `dispatch_pending(target_workspace_id, cursor) -> DispatchPage`, `execute(operation_id, executor) -> DeploymentReceipt`. Internal `MappingTransformer.render(artifact, mapping, target_binding) -> RenderedPayload` is pure and versioned. Target Job accepts operation_id only; release/validate/promote/receipt routes are specified in `contracts.md` §7.

Schema/layout: §3 separate package/receipt Volumes; §6 PackageManifest/DeploymentReceipt; M06 operations release/receipt fact kinds in §2. Target-local durable requested operation facts are the dispatch source; inbound published package manifests are inputs, not write commands. Target service validates package intent, local binding and approval before appending an executable request. Repeated poll/Job triggers resolve by idempotency key; Job scheduling is execution, Delta is admission authority.

## 4. Dependencies

Upstream: M02 immutable source version/artifacts; M01 canonicalizer; M06 target-approved inputs/fact writer; M05 target drift; M04 gate and compensation; M08 target-local authentication, Jobs and verified transport grants. M07 imports no Lakebase or Git API. Downstream: M09 promotion UI; M08 provisions Job handlers/Volumes using supplied manifests. M06 accepts preflight evidence through a port, not a concrete promotion import.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_package_pins_immutable_source_and_excludes_source_environment_ids` in `test_vc_packages.py` | Version-to-portable extraction with manifest and all file digests |
| 2 | `test_existing_digest_path_is_never_overwritten_and_incomplete_package_not_published` in same file | Publish-after-upload verification; equal-bytes reuse, unequal-bytes fail |
| 3 | `test_structured_tables_metric_views_catalogs_schemas_map_exactly` in `test_vc_mapping.py` | Allowlisted structured mapping traversal |
| 4 | `test_sql_identifier_mapping_preserves_literals_comments_and_instruction_prose` in same file | Supported qualified-identifier tokenizer/parser path; no global replace |
| 5 | `test_unsupported_sql_or_unresolved_source_identifier_fails_closed` in same file | Executable-field scan; ambiguous cases demand reviewed exact overrides |
| 6 | `test_warehouse_folder_binding_and_principals_remain_target_local` in same file | Bind target environment separately from portable artifact |
| 7 | `test_preflight_checks_executor_and_consumer_dependencies_and_permissions` in `test_vc_promotion.py` | Target tables/metric views/warehouse/folder/permission checks, hashed evidence |
| 8 | `test_executor_recomputes_artifact_mapping_render_and_policy_digests` in same file | Reload immutable inputs by operation_id and invalidate substituted/changed approvals |
| 9 | `test_source_only_approval_or_wrong_target_host_cannot_deploy` in same file | Target-local identity and M06 authorization recheck |
| 10 | `test_target_drift_or_missing_pre_enrollment_blocks_without_create` in same file | Observer/preflight conflict and registry lookup before gate |
| 11 | `test_polling_discovers_only_local_approved_pending_operations` in `test_vc_dispatch.py` | Cursor-based local operations polling; explicit dispatcher, not vague “receive ID” |
| 12 | `test_duplicate_dispatch_or_job_retry_does_not_replay_mutation` in same file | Stable key/digest and gate resume/verification semantics |
| 13 | `test_receipt_binds_intended_rendered_observed_and_pre_post_versions` in `test_vc_promotion.py` | Target Delta receipt after GET/tests with operation/fence/approval/job lineage |
| 14 | `test_receipt_export_failure_retries_export_not_patch` in same file | Read durable receipt, repair reverse export only |
| 15 | `test_failed_smoke_compensation_needs_preauthorization_and_matching_post_stage` in same file | Invoke guarded new M04 compensation; drift/ambiguity blocks |
| 16 | `test_cross_metastore_transport_unsupported_is_explicitly_disabled` in same file | Verified capability selection; never external bus or target credentials fallback |
| 17 | `test_target_writes_source_cannot_and_receipt_is_reverse_read_only` in `integration/test_vc_two_workspace_promotion.py` | Real separate principals/securables/topology with negative permission tests |

## 6. Acceptance criteria

- [ ] Promotion preserves FM-CONCURRENCY/FM-DRIFT/FM-OUTAGE/FM-RECOVERY and all local gate guarantees.
- [ ] F-PROMOTION: immutable pinning, mapping/render digest equality, target-side authorization and target preimage.
- [ ] F-COMPENSATION: no rollback over unrelated edits or unresolved requests.
- [ ] F-DISPATCH: local discovery, retry deduplication and reverse receipt transport exercised.
- [ ] No operational Git/GitHub Actions; no target creation in v1 promotion; no unrestricted string replacement.

## 7. Parallel mocks and seam version

**VC/1.0**: fake package transport, local operations poller, M04 gate, M05 drift, M06 approvals, M08 identity/dependency validators. Pure mapping golden fixtures live inside M07 tests, not M01 directories. Contract fixtures cover source and target fingerprints that intentionally differ. M09 can build release forms against frozen APIs before actual transport exists.

## 8. Risks / verify at implementation time

Verify actual Delta Sharing Volume/artifact support per metastore/region; disable unsupported topology pending an approved native transport representation. Confirm target receipt grants are separate securables, not prefixes. Verify SQL parser coverage and all executable fields; fail closed on unsupported syntax. Decide benchmark thresholds and ACL representation with policy owners. Explicit source/target profiles must resolve host/workspace IDs; source SP must never hold broad target Genie write permissions. A per-workspace Job concurrency limit is a throttle, not correctness.
