# Strict TDD and acceptance strategy

Contract: **VC/1.0**. Tests prove behavior; mock success, Job success or HTTP 200 is not proof of authoritative live state. The detailed test-first steps are in each module file. This index defines shared conventions, adversarial fakes and the integration/release gate.

## Red → green → refactor

1. Select the next module test; write its failing assertion **before** implementation. Run it and record command plus the expected failure reason in the implementation handoff. An import/syntax/environment error is not a sufficient behavioral red once scaffolding exists.
2. Make the smallest change that satisfies the assertion. Run the focused test, then its owning module suite. Refactor only with green tests; preserve old exported functions and router compatibility unless a reviewed contract change requires migration.
3. Add the next boundary/failure test. Do not write the whole module and retrofit tests. A bugfix begins with a reproducer.
4. Before integration, run consumer/provider contract fixtures and existing touched-path regressions. Before enabling writes, run real Delta/identity tests and the full first-milestone failure matrix. Never describe skipped integration tests as passed.
5. Keep each module's implementation/tests within its ownership manifest. New shared fakes/DTOs go through M02; root config/dependencies through M08; existing mutation callers through M04; frontend mounting through M09.

## Test layout and commands

| Layer | Location / owner | Typical command (verify repository runner first) |
|---|---|---|
| Canonicalization regressions | Existing `backend/tests/test_config_fingerprint.py` / M01 | `python -m pytest backend/tests/test_config_fingerprint.py -q` |
| Backend module unit/contract | `backend/tests/test_vc_*.py` / respective module | `python -m pytest backend/tests/test_vc_coordination.py -q` |
| Existing mutation regressions | `test_genie_client.py`, `test_current_version.py`, `test_auto_optimize_router.py`, `test_create_agent.py` / M04 | `python -m pytest backend/tests/test_genie_client.py backend/tests/test_current_version.py backend/tests/test_auto_optimize_router.py backend/tests/test_create_agent.py -q` |
| Real Delta/security | `backend/tests/integration/` / M03, M07, M08 | `python -m pytest backend/tests/integration -m integration -q` |
| Optimizer | `packages/genie-space-optimizer/tests/` / M10 | Run package's existing pytest runner after installing its declared dependencies |
| Frontend unit/components | New colocated vitest tests / M09 | From `frontend/`, run the existing package-manager test script in nonwatch mode |
| Broad regression | Backend + optimizer + frontend existing suites | `python -m pytest backend/tests -q` plus package/frontend runners |
| Bundle machinery | Resolved root/package bundles / M08 | `databricks bundle validate -t <test-target> --profile <explicit-test-profile>` from each applicable bundle root |

Register an `integration` marker using M08-owned test configuration; require explicitly configured nonproduction workspace(s), schema(s), identities and target agents. No credentials in fixtures or test output. Use a dedicated disposable test catalog/schema; destructive teardown only there and only by provisioning/test owner, never runtime principals or production data. Do not invent package-manager commands without reading package scripts/lockfile.

## Shared deterministic fakes (M02-owned)

### Fake Genie service

- Model GET envelope and separate whole-document config/description writes, with M01-normalization hooks and exact GET observations.
- Queue outcomes independently: rejected-before-send; accepted-and-response-lost; delayed server completion after client timeout; applied config/failed description; unrelated direct edit between stages; GET unreachable; create accepted with lost identity response.
- Mutation retry counter must stay zero for ambiguous calls. Distinguish server acceptance from client response and from eventual termination. A matching GET does not cancel a queued old request.
- Expose an ordered trace: `reserve`, `get`, `append_preimage`, `admit_cas`, `publish_consumption`, `checkpoint_send_intent`, `assert_owner`, `patch_config`, `get_checkpoint`, `patch_description`, `get_post`, `append_post`, `publish_terminal`, `finish`. Assert forbidden orderings, not only return values.
- Simulate late writes from an old attempt after lease expiry; ensure no replacement attempt is admitted. Simulate create orphan explicitly; never “find by name” in the fake.

### Fake Delta / fact store

- Immutable append facts with deterministic event keys and commit-visible state. Inject failures before commit, after commit/before response, and during follow-up read.
- Mutable coordination row supports predicate CAS with row_version/generation and single-row Serializable behavior. Barrier-based scheduling yields deterministic races; don't use sleeps as the concurrency proof.
- Allow deliberately seeded duplicate enrollment rows, conflicting registry chains and old consumed-approval facts. Fake uniqueness must not conceal Delta's unenforced key risk.
- Separate table commits. There is **no** fake transaction across versions, operations and coordination. A test that commits preimage/admission together would conceal the central crash window.
- Restart fake clients/workers while preserving durable stores; retry through a new process identity/attempt and assert unresolved old claims still block.

### Fake coordination and authorization consumers

- Consumer unit tests inject explicit `Reservation`, `AdmissionClaim` or `Quarantined` outcomes. The real M03 adapter gets its own provider tests; consumers do not reproduce CAS logic.
- Controllable UTC clock, exact executor/attempt refs, terminal Job children/retries, positive app-worker termination evidence, server lifetime bound and ≥3 post-termination read samples.
- Target-specific group memberships and edit rights; revoke membership between approval and execution. A supplied approver name is ignored/rejected in favor of authenticated identity.
- Package/receipt transport treats Volume objects as separate securables; a path prefix alone must not pass a role-denial test.

## Real-platform integration tests (cannot be replaced by mocks)

- **Delta CAS:** two independent connections race the same existing row under verified Serializable. Assert one winning attempt by row read-back, one admitted mutation trace and a losing conflict/quarantine fact. Repeat with the same SP but different attempts, expired leases, and seeded duplicate rows.
- **Permissions:** runtime fact writer cannot UPDATE/DELETE/ALTER/TRUNCATE/DROP; executor cannot INSERT coordination; enrollment authority is narrowly isolated. Verify effective inherited grants as well as direct grants. If unavailable on deployment compute, writes remain disabled.
- **Evidence ordering:** terminate executor after preimage commit, admission CAS, consumption publication and each send boundary; a separate recovery worker must derive the safe action from durable state alone.
- **Genie compatibility:** disposable target verifies full GET reader permissions, current normalization, two PATCHes and final GET. Do not rely on preview ETag behavior. Test timeout/late-completion behavior in controlled transport where actual network fault injection cannot establish reliable server outcomes.
- **Identity:** wrong Job run_as prevents startup rather than editing configuration. Production viewer can access authorized history through trusted observer without gaining edit permission. Requester rights/target-group policy validated by executor.
- **Recovery:** use known termination evidence; no automatic exit while maximum request lifetime remains unverified. A sandbox human recovery drill records actor/reason/checkpoint classification and residual risk; it is not presented as automatic safety proof.
- **Promotion:** independent source/target identities; target-only writes, source denied; tampered package/map/approval denied; target drift preserved; receipt export failure does not reapply. Test same-metastore transport first, then cross-metastore only when capability is verified.

## First milestone mapping

The README's first milestone is **write-enabled**, not merely the earlier observation-only checkpoint. All rows below are required before enabling any legacy or new writer; unsupported flows remain disabled. First milestone includes in-workspace restore, baseline approvals/consumption, dual-authority detection, and champion gating. Later promotion extends the same gates.

| ID | Required behavior | Primary backend tests | Frontend / end-to-end assertion |
|---|---|---|---|
| FM-HISTORY | Durable pre/post history from API reads, exact state and lineage | `test_vc_ledger.py`, `test_vc_mutation_gate.py` | History displays current/new versions and origin/actor |
| FM-RESTORE | Restore any captured version via Job; append, never rewrite | `test_vc_restore.py` | Select old version, compare, approve, restore, poll new lineage |
| FM-DRIFT | Preserve unexpected state and block write against reviewed base | `test_vc_mutation_gate.py`, `test_vc_reconcile.py` | Captured external version visible before adopt/reapply |
| FM-CONCURRENCY | Two requests → one admitted sequence, one conflict/quarantine | `test_vc_coordination.py`, `integration/test_vc_delta_cas.py` | Losing request shows conflict, not a success toast |
| FM-DUAL-AUTHORITY | Governed bundle content flagged within one cycle | `test_vc_dual_authority.py`, `test_vc_provisioning.py` | Drift reason identifies dual authority; no inferred actor |
| FM-OUTAGE | All mutations stop without coordination/evidence; available history reads continue | Gate/create/optimizer/reconcile suites + `test_vc_composition.py` | Stale state disables commands but displays readable history |
| FM-RECOVERY | Timeout/crash quarantines, no replay; exact termination + stable reads or audited human action | `test_vc_recovery.py`, `test_vc_mutation_gate.py` | Quarantine/partial/unverified distinguishable, no retry-PATCH control |

## Failure-coverage matrix

Every required README failure appears explicitly; additional rows cover linked architectural guarantees. `Required local` rows run for first write-enabled milestone. `Promotion` rows run before promotion enablement.

| ID / gate | Fault or adversarial case | Expected invariant | Tests / owner |
|---|---|---|---|
| F-DESCRIPTION / Required local | Second PATCH timeout, 5xx or wrong metadata preimage | No second-PATCH replay; partial/unverified and quarantine preserved | `test_vc_mutation_gate.py` / M04; `test_vc_recovery.py` / M03 |
| F-APP-CRASH / Required local | App-worker crash mid-write | Lease/heartbeat not termination; unresolved blocks successor | `test_vc_mutation_gate.py`, `test_vc_recovery.py`, `test_vc_identity.py` / M04/M03/M08 |
| F-CREATE / Required local | Lost create response; crash after create before binding | Provisional intent remains; no replay/name match/follow-on edit | `test_vc_create.py`, `test_vc_registry.py` / M04/M02 |
| F-OPENS / Required local | Concurrent opens, slow GET, active managed checkpoint, persistence failure | No head regression/duplicate observation/mislabeling; observed only; stale on failure | `test_vc_observer.py`, frontend tab tests / M04/M09 |
| F-ENROLLMENT / Required local | Duplicate physical/logical enrollment or coordination rows | Reject and quarantine ambiguity; no unenforced-key assumption | `test_vc_registry.py`, `integration/test_vc_delta_cas.py` / M02/M03 |
| F-APPROVAL-REUSE / Required local | Historical approval after row reuse, or crash before consumption publish | Old approval cannot authorize a new operation | `test_vc_coordination.py`, `test_vc_operation_facts.py`, `test_vc_approvals.py` / M03/M06 |
| F-CRASH-STAGES / Required local | Crash between evidence/CAS/PATCH; lost CAS/append response; failed final fact publish | No PATCH without committed bound evidence, no premature release/replay | Gate/coordination/ledger/facts tests / M02/M03/M04/M06 |
| F-IDEMPOTENCY / Required local | Same key same digest; same key different digest; same SP different attempt | Existing receipt vs hard reject; stale attempt cannot act | `test_vc_coordination.py` / M03 |
| F-FRESH-APPROVAL / Required local | Capture followed by adopt/reapply with old approval | Adopt requests approval; reapply binds new captured base | `test_vc_reconcile.py`, `test_vc_approvals.py`, frontend reconcile tests / M05/M06/M09 |
| F-CHAMPION / Required local | Abandoned run, candidate iterations, retry/different champion, external preimage | ≤1 optimizer-origin final version; no managed candidate PATCH | Package VC tests, `test_vc_optimizer_adapter.py` / M10 |
| F-CANONICALIZER / Required local | Metadata-only drift, meaningful array order, unsupported canonicalizer version | Distinct fingerprints; unknown instead of false equality | Fingerprint/semantic-diff/drift tests / M01/M05 |
| F-REBIND / Required local | Delete/recreate same name; rebind after approval | Explicit human identity evidence, revised binding invalidates old token | `test_vc_registry.py`, `test_vc_approvals.py` / M02/M06 |
| F-IDENTITY / Required local | Wrong run_as, default profile, viewer export, SP acting for unauthorized requester | Fail-closed identity; trusted reader; rights/policy checked server-side | `test_vc_identity.py`, `test_vc_approvals.py` / M08/M06 |
| F-COMPENSATION / Required local + promotion extension | Post-test failure plus unrelated direct edit or ambiguous original call | No unconditional rollback; preauthorization and fresh checkpoint equality required | `test_vc_restore.py`, `test_vc_promotion.py` / M04/M07 |
| F-PROMOTION / Promotion | Changed artifact/map/renderer/base/policy or source-only approvers | Target rehashes and rejects; captures drift; no opportunistic target create | Package/mapping/promotion/two-workspace tests / M07 |
| F-DISPATCH / Promotion | Duplicate polling/Job retries, wrong workspace, failed receipt export | Local ID discovery; no replay; repair receipt only | `test_vc_dispatch.py`, `test_vc_promotion.py` / M07 |

## End-to-end scenarios

Use existing browser test infrastructure if present; otherwise execute/document these as a manual sandbox checklist and cover interactions with vitest plus backend HTTP integration tests. Do not claim an unbuilt E2E runner passed.

1. Open managed agent → capture initial version → external description/config edit → reopen → external version saved, observed advances, badge remains drifted → compare → adoption request → required approval → deliberate policy head update.
2. Select arbitrary historical version → show semantic diff → authorize against current base → execute restore Job → inspect authoritative new version with restored_from linkage. Race an external edit before admission and verify preserved conflict instead.
3. Launch two edits concurrently → one sequence only. Drop coordination UPDATE access → both new edit and restore disabled while available SELECT history remains usable; also test full UC outage honestly failing reads.
4. Delay config/description PATCH beyond client timeout → matching GET still quarantined → Job/app termination proof + stability evidence verified (or audited human resolution if bound unknown) → no old approval replay.
5. Lose create response → pending provisional binding, no second POST → audited orphan identity resolution → first authoritative version → follow-on edit only after physical binding persisted.
6. Run optimizer candidates on isolated resources → abandon yields no version; applied champion yields one optimizer version with run/champion links, external preimage separately preserved when encountered.
7. Publish source package → target-local preflight/approval → pull Job executes → receipt returns. Tamper mapping, move target base, revoke target approver membership and fail receipt export in separate runs; all fail safely without duplicate mutation.

## Definition of done

- Every module acceptance checklist and applicable FM/F row passes; all touched existing suites and contract fixtures are green.
- Real-platform capability/permission/CAS tests have recorded environment and results; unavailable integrations are explicit blockers to affected feature enablement, not silent test skips.
- Repository mutation inventory contains no unmanaged bypass; disabled paths visibly refuse writes. No preview ETag dependency, cross-table transaction assumption, Git operational ledger, GitHub Actions, DAB-owned content or Lakebase/memory authorization fallback.
- Operational runbooks explain quarantine, create orphan, partial PATCH, rebind and receipt repair, including automatic recovery disabled until request-lifetime verification.
- M08 integrates modules without ownership overlap; M09 surfaces honest status and policy authority. Application source stays in Git; runtime facts stay Databricks-native.
