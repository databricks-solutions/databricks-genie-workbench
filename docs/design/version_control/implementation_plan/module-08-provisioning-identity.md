# M08 — DAB provisioning, identities and integration owner

## 1. Purpose and scope

Provision the native execution/governance machinery, verify effective permissions and run identities fail-closed, and own the few unavoidable root composition/build changes. DABs provision app, Jobs, schemas, tables, Volumes and grants, but never manage Genie content. M08 is the integration owner for shared files, not a second implementation of ledger, authorization, gate or promotion logic.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| Root `databricks.yml`, `app.yaml`, `pyproject.toml`, `requirements.txt` | All module service/router/job implementations |
| `packages/genie-space-optimizer/databricks.yml` (exception to M10 subtree) | Optimizer runtime/package configuration owned M10 |
| `scripts/deploy.sh`, `scripts/deploy_lib/`, `scripts/preflight.sh`, `scripts/grant_permissions.py` | Existing `scripts/setup_lakebase.py`, `scripts/setup_synced_tables.py` (unchanged) |
| New `resources/version-control/`, `scripts/version_control/` | M02/M03/M06/M07 DDL files |
| `backend/main.py`, `backend/models.py`, `backend/services/auth.py` | Existing auth router/_validators; do not duplicate authorization policy |
| New `backend/services/version_control/platform/`, `backend/jobs/__init__.py` | M02 shared Protocols; M06 policy implementation |
| `backend/tests/{test_vc_provisioning,test_vc_identity,test_vc_composition}.py` | Existing regression suites |
| `backend/tests/integration/conftest.py`, `test_vc_permissions.py` if new; root test configuration only when required | Module-owned integration tests |

DDL authorship: **none of the four tables/Volumes**. Own deployment execution/order/grants and schema/catalog configuration, invoking owner migrations without editing them. Existing `backend/models.py` changes should be avoided by module-local request models. No shared root `conftest.py` edits by other modules.

## 3. Public contract

`IdentityProvider.actor/groups/can_edit/executor/verify_run_as` and `JobDispatcher.submit_local(operation_id, job_kind)` in `contracts.md` §4. Platform adapter also provides explicit-credential SQL/Volume clients and `TerminationEvidenceProvider.for_attempt(execution_ref, attempt_id) -> TerminationEvidence | None`. M08 verifies identity; M06 decides policy. No alternate public API routes.

Provisioned handlers: M04 restore, M05 reconcile, M07 target promotion/polling, M10 optimizer champion apply, plus serialized M02 enrollment and authorized M03 recovery wrappers wired by platform integration. All governed mutation handlers load durable request by operation_id. Data schema deployment: exactly §2's four logical tables plus §3's separate Volumes; root DAB includes resource files and a narrowly privileged provisioning task where a resource is not directly declarative. No `resources.genie_spaces` content under management.

## 4. Dependencies

Can start with contract/scaffold and platform verification in parallel with M01/M02. Consumes DDL manifests and handler interfaces from owners as they arrive. Supplies SQL/artifact/identity/dispatch adapters to M02–07/M10 and composition to M09 endpoints. M08 owns dependency addition requests from all modules; modules submit required import/version/test evidence, not root manifest patches.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_deployment_contains_no_governed_genie_content_resource` in `test_vc_provisioning.py` | Static resolved-resource guard including nested bundle includes |
| 2 | `test_existing_optimizer_bundle_and_app_deploy_paths_remain_accounted_for` in same file | Inventory actual root/package bundles and script deployment; explicit DAB transition plan |
| 3 | `test_modules_are_wired_once_with_write_switch_default_off` in `test_vc_composition.py` | Shared package/composition scaffold, lazy dependency injection, no accidental writer enablement |
| 4 | `test_ddl_migrations_are_owned_ordered_idempotent_and_not_content_deploys` in `test_vc_provisioning.py` | Provisioning Job runs validated owner SQL/Volume specs and grants |
| 5 | `test_runtime_fact_writes_are_append_guarded_and_tamper_evident` (+ `test_fact_table_tampering_is_detectable_in_delta_history`) in `integration/test_vc_permissions.py` | Non-owner runtime (SELECT+MODIFY): INSERT succeeds, UPDATE/DELETE rejected by delta.appendOnly, DROP denied by non-ownership, each append attributed in Delta history. **Validated platform reality:** UC MODIFY also confers ALTER/SET TBLPROPERTIES/CREATE OR REPLACE, so a runtime appendOnly flip *succeeds* but is durably recorded/attributed in Delta history — writer-immutability is by detection, not prevention. UC has no INSERT/UPDATE privilege — see contracts.md deviation |
| 6 | `test_coordination_writers_are_distinct_serialized_sps` in same file | Executor + enrollment are distinct SPs both with SELECT+MODIFY; UPDATE-only/INSERT-only separation is protocol-enforced (Serializable + serialized enrollment Job + CAS), not grant-enforced |
| 7 | `test_missing_fine_grained_dml_or_serializable_disables_writes` in `test_vc_provisioning.py` | Capability checks fail closed; no broad MODIFY fallback |
| 8 | `test_wrong_run_as_fails_startup_without_self_healing` in `test_vc_identity.py` | Replace discovered startup mutation with verify/refuse behavior; no Job identity edits |
| 9 | `test_explicit_profile_host_workspace_mismatch_is_rejected` in same file | Explicit source/target selection validation and OAuth M2M/OBO resolution; no default profile |
| 10 | `test_production_viewer_capture_uses_trusted_reader_without_elevation` in same file | Separate snapshot-view authorization from target-local full-config reader |
| 11 | `test_jobs_take_operation_id_and_retry_verification_not_mutation` in `test_vc_composition.py` | DAB handlers and retry policy consistent with M03/M04 unresolved state |
| 12 | `test_worker_termination_evidence_is_positive_and_attempt_specific` in `test_vc_identity.py` | Verify actual Job children/retries or specified app-worker termination; missing proof returns None |
| 13 | `test_bundle_inventory_guard_rejects_governed_content_and_exports_detection_evidence` in `test_vc_provisioning.py` | Sanctioned preflight guard and M05 read-only inventory adapter |
| 14 | `test_package_approval_receipt_securables_have_separate_writers` in `integration/test_vc_permissions.py` | Separate Volume grants and target-owned operation table, source denied target writes |
| 15 | `test_first_write_enable_requires_every_safety_capability` in `test_vc_composition.py` | Gate on coordination/evidence/authorization/identity/routed-write coverage; unintegrated paths disabled |
| 16 | `test_dab_validate_and_sandbox_provision_are_repeatable` in `test_vc_provisioning.py` (integration marker) | Run actual CLI validate/provision twice with explicit nonprod target/profile; no Genie content changes |

## 6. Acceptance criteria

- [ ] FM-OUTAGE: degraded coordination/evidence disables **all** version-changing flows; telemetry memory fallback irrelevant.
- [ ] FM-DUAL-AUTHORITY: sanctioned deploy content guard plus inventory feed for one-cycle detection.
- [ ] F-IDENTITY: no startup authority repair, no default production profile, no viewer elevation.
- [ ] F-ENROLLMENT and FM-CONCURRENCY have verified grants/Serializable, not just mocked SQL.
- [ ] First write-enabled milestone includes M03+M04 and all FM/F safety cases; unsupported writers fail explicitly.
- [ ] App and operational machinery provision via DAB; no external CI/CD control plane or operational Git ledger.

## 7. Parallel mocks and seam version

**VC/1.0**: fake SDK identity/permissions/group/Jobs endpoints and resolved bundle files; module handler stubs during scaffold. Other modules mock platform ports. Integration tests use explicitly configured test principals/workspaces, never auto-selected accounts. M08 alone updates root dependencies/package includes and router mounts after each subtree lands.

## 8. Risks / verify at implementation time

Verify current DAB support for app/tables/grants and use provisioning-only native tasks where needed; do not assume every securable is a direct bundle resource. The package optimizer manifest is the only current DAB Job per supplied map; inspect root includes rather than duplicating the Job. Locate actual startup run_as self-healing code and seek explicit file ownership transfer if outside listed paths. Verify fine-grained DML rollout, compute/isolation support, full-read permissions and live source/target identity. Never add Lakebase coordination or implicit in-memory authorization. Document maximum-lifetime uncertainty and disable auto-clear rather than treating job termination alone as sufficient.
