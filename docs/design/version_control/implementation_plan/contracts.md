# Shared contracts — VC/1.0

This document freezes proposed seams, not existing repository symbols. M02 owns `backend/services/version_control/contracts.py` and shared contract fixtures; each implementation owner below consumes those Protocols. New HTTP models live in the owning router/module, not in shared `backend/models.py`. M08 alone changes composition and dependencies. Breaking changes require a version bump and consumer test updates.

## 1. Values, fingerprints, identity

- IDs are UUID strings, timestamps timezone-aware UTC, digests lowercase SHA-256 hex. Hash versioned, deterministic UTF-8 JSON; exclude the digest's own field. Never hash concatenated ambiguous strings.
- `BindingRef(binding_id, binding_revision, space_key, workspace_id, space_id: str | None, environment)`. `binding_id` exists before create and stays stable when its provisional binding is resolved. Human rebind increments `binding_revision`; all tokens, approvals and queries include that revision. Workspace+physical-space ownership must be unique across active bindings.
- `Fingerprints(config, benchmark, metadata, canonicalizer_version)`; `state_digest = sha256({domain:'vc-state/1', config, benchmark, metadata, canonicalizer_version})`. Metadata includes every governed updateable field (description in v1). Managed permissions, if supported, carry a separate permission-policy digest in approval and preflight, never disappear from the reviewed base.
- `Snapshot` contains full GET envelope, exact parsed serialized document, allowlisted restorable metadata, raw digests, canonical representations and fingerprints. Submitted payloads are never observations. Unknown canonicalizer comparison produces `unknown` unless both sides can be recomputed under one supported version.
- `ObservationRef(version_id, binding_id, binding_revision, state_digest, envelope_digest)` points to already committed evidence; an upload alone is insufficient. Create uses `CreateIntentRef`, explicitly not a fictitious preimage.

## 2. Delta DDL (four logical tables)

Provision in a **workspace-owned control schema**. Replace `${catalog}` and `${control_schema}` only with validated, quoted configuration identifiers, never caller input. This is a schema specification to execute/test on approved compute at implementation time, not a claim the DDL has already been run. Each DDL owner authors migrations; M08's provisioning task invokes them.

### M02 — versions

```sql
CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_versions (
  version_id STRING NOT NULL,
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  space_key STRING NOT NULL,
  workspace_id STRING NOT NULL,
  space_id STRING NOT NULL,
  environment STRING NOT NULL,
  observation_key STRING NOT NULL,
  observed_at TIMESTAMP NOT NULL,
  observation_reason STRING NOT NULL,
  observed_by STRING NOT NULL,
  actor_kind STRING NOT NULL,
  origin STRING NOT NULL,
  parent_version_id STRING,
  restored_from_version_id STRING,
  operation_id STRING,
  attempt_id STRING,
  generation BIGINT,
  api_update_time TIMESTAMP,
  response_envelope_json STRING,
  response_envelope_uri STRING,
  response_envelope_digest STRING NOT NULL,
  serialized_space_json STRING NOT NULL,
  restorable_metadata_json STRING NOT NULL,
  raw_state_digest STRING NOT NULL,
  canonical_state_json STRING NOT NULL,
  config_fingerprint STRING NOT NULL,
  benchmark_fingerprint STRING NOT NULL,
  metadata_fingerprint STRING NOT NULL,
  state_digest STRING NOT NULL,
  canonicalizer_version STRING NOT NULL,
  optimizer_run_id STRING,
  champion_id STRING,
  release_id STRING,
  CONSTRAINT valid_origin CHECK (origin IN
    ('workbench','external','optimizer','restore','promotion','unknown')),
  CONSTRAINT valid_revision CHECK (binding_revision > 0),
  CONSTRAINT envelope_present CHECK (
    (response_envelope_json IS NOT NULL AND response_envelope_uri IS NULL) OR
    (response_envelope_json IS NULL AND response_envelope_uri IS NOT NULL))
) USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');
```

`observation_key` is an idempotent capture identity (binding/revision + operation/stage, or serialized observer sequence), **not content hash alone**: an A→B→A history must retain the final A. Exact adjacent unchanged reads may reuse an observation. Successful optimizer apply emits one optimizer-origin final version; its intermediate checkpoint uses operation evidence. Divergent external and recovery observations remain separately capturable.

### M02 — append-only registry

```sql
CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_registry (
  registry_event_id STRING NOT NULL,
  space_key STRING NOT NULL,
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  event_sequence BIGINT NOT NULL,
  event_type STRING NOT NULL,
  supersedes_event_id STRING,
  workspace_id STRING NOT NULL,
  space_id STRING,
  environment STRING NOT NULL,
  governed_by STRING NOT NULL,
  binding_reason STRING NOT NULL,
  bound_by STRING NOT NULL,
  bound_at TIMESTAMP NOT NULL,
  create_operation_id STRING,
  evidence_json STRING,
  CONSTRAINT registry_governance CHECK (governed_by = 'workbench'),
  CONSTRAINT registry_event CHECK (event_type IN
    ('provisional','bound','tombstoned','rebound')),
  CONSTRAINT registry_revision CHECK (binding_revision > 0 AND event_sequence > 0)
) USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');
```

M02 owns a current-binding **view**, not a fifth mutable authority. Project validated supersession chains, not `MAX(timestamp)`/name matching; concurrent forks, duplicate current physical ownership or duplicate coordination rows return `ambiguous` and quarantine. A tombstone removes the current physical binding; a human rebind starts a new revision. Serialized enrollment is the sole path that coordinates these facts with M03's row initializer. On partial enrollment, disable the binding and repair explicitly; never infer success from one table alone.

### M06 — operations, approvals, releases and receipt facts

```sql
CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_operations (
  event_id STRING NOT NULL,
  fact_kind STRING NOT NULL,
  operation_id STRING NOT NULL,
  transition_sequence BIGINT NOT NULL,
  event_key STRING NOT NULL,
  space_key STRING NOT NULL,
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  target_workspace STRING NOT NULL,
  idempotency_key STRING NOT NULL,
  request_digest STRING NOT NULL,
  operation_type STRING NOT NULL,
  attempt_id STRING,
  generation BIGINT,
  coordination_backend STRING NOT NULL,
  requested_base_fingerprint STRING,
  desired_artifact_fingerprint STRING,
  mapping_fingerprint STRING,
  transformer_version STRING,
  test_policy_fingerprint STRING,
  pre_version_id STRING,
  post_version_id STRING,
  approval_id STRING,
  approval_digest STRING,
  approval_reference STRING,
  release_id STRING,
  job_run_id STRING,
  requester_id STRING NOT NULL,
  actor_id STRING NOT NULL,
  actor_kind STRING NOT NULL,
  status STRING NOT NULL,
  drift_override BOOLEAN NOT NULL,
  evidence_json STRING NOT NULL,
  evidence_uri STRING,
  evidence_digest STRING,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT operations_backend CHECK (coordination_backend = 'delta'),
  CONSTRAINT operation_sequence CHECK (transition_sequence >= 0),
  CONSTRAINT operation_fact CHECK (fact_kind IN
    ('operation','create_intent','approval_request','approval_vote',
     'approval_granted','approval_consumed','approval_invalidated',
     'release','receipt','acknowledgement','break_glass','recovery'))
) USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');
```

Typed, schema-versioned `evidence_json` carries approvals and release/receipt payloads, not unvalidated arbitrary dictionaries. Fact-specific required fields are validated by M06. Unknown schemas fail closed. Facts reference immutable input digests; approvals and receipts are durable Delta facts even when large attachments are in Volumes.

Statuses: `requested`, `preimage_captured`, `apply_attempted`, `applied_unverified`, `applied_partial`, `confirmed`, `conflicted`, `quarantined`, `failed`, `compensation_attempted`, `compensated`; additionally `noop`, `approved`, `consumed`, `invalidated`, `acknowledged` for their appropriate fact kinds. `applied_unverified`/`applied_partial` end automatic mutation execution, **not** the unresolved-attempt safety obligation.

### M03 — mutable coordination

```sql
CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_ops_coordination (
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  space_key STRING NOT NULL,
  workspace_id STRING NOT NULL,
  space_id STRING,
  generation BIGINT NOT NULL,
  row_version BIGINT NOT NULL,
  state STRING NOT NULL,
  holder STRING,
  attempt_id STRING,
  executor_kind STRING,
  executor_ref STRING,
  lease_expires_at TIMESTAMP,
  active_operation_id STRING,
  idempotency_key STRING,
  request_digest STRING,
  approval_id STRING,
  approval_digest STRING,
  approval_consumption_published BOOLEAN NOT NULL,
  pre_version_id STRING,
  preimage_digest STRING,
  create_intent_event_id STRING,
  expected_base_fingerprint STRING,
  admitted_at TIMESTAMP,
  mutation_stage STRING,
  checkpoint_json STRING,
  unresolved BOOLEAN NOT NULL,
  quarantine_reason STRING,
  termination_evidence_json STRING,
  observed_head_version_id STRING,
  approved_head_version_id STRING,
  deployed_head_version_id STRING,
  observed_sequence BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT coordination_state CHECK (state IN
    ('idle','observing','reserved','admitted','quarantined')),
  CONSTRAINT nonnegative_fence CHECK (generation >= 0 AND row_version >= 0)
) USING DELTA TBLPROPERTIES ('delta.isolationLevel'='Serializable');
```

Exactly **one row per `binding_id`**, updated for an explicitly authorized revision change; a new physical identity does not inherit old approvals. No reliance on informational PK/UNIQUE constraints. Enrollment alone INSERTs under serialized provisioning/enrollment authority, and detects duplicate logical/physical ownership. Runtime CAS is UPDATE-only `MERGE` with **no NOT MATCHED INSERT branch**. Verify Serializable and effective grants on startup and in integration tests.

### Grant boundaries and append semantics

- Runtime principals are not table owners. Fact writers append only; destructive `DROP` requires ownership (`MANAGE`). Coordination executor updates existing rows only; enrollment service alone inserts. No runtime DELETE. *(Intent — see the platform-forced deviation below. On Unity Catalog `MODIFY` also confers `ALTER`/`CREATE OR REPLACE`, so writer-immutability is provided by durable **detection**, not prevention.)*
- Fine-grained INSERT/UPDATE/DELETE availability and compatible compute are deployment gates, not assumptions. If intended privileges cannot be enforced, keep writes disabled; do not silently grant broad `MODIFY` or claim equivalent isolation.

> **Platform-forced deviation (VC/1.0, validated on Databricks serverless UC).** Unity Catalog tables expose only `SELECT` and `MODIFY` — there is **no** `INSERT`, `UPDATE`, or `DELETE` privilege (`GRANT SELECT, INSERT …` fails with `PRIVILEGE_NOT_APPLICABLE_TO_ENTITY`). The DML-verb least-privilege above cannot be expressed as grants. It is instead enforced as follows, which preserves the *effective* guarantees:
> - **Fact tables** are granted `SELECT, MODIFY` to the runtime and carry `delta.appendOnly='true'`. Delta rejects UPDATE/DELETE while the property is set, so `MODIFY` is append-only *in effect* for ordinary data DML. **Validated platform reality (serverless UC):** `MODIFY` *also* confers `ALTER TABLE … SET TBLPROPERTIES` (including `delta.appendOnly='false'`), `ADD COLUMN`, and `CREATE OR REPLACE TABLE`; only `DROP` requires `MANAGE`/ownership. **No UC grant configuration makes a table immutable to its own writer** (the owner can always alter/drop too), so `delta.appendOnly` is a guard against *accidental* UPDATE/DELETE, not a barrier against a malicious writer. Writer-immutability is instead achieved by **tamper-evident detection**: every commit — including a `SET TBLPROPERTIES` appendOnly flip or a `CREATE OR REPLACE` — is durably recorded and attributed to the acting principal in Delta transaction history (`DESCRIBE HISTORY`, synchronous, non-owner-truncatable) and in `system.access.audit` (which GenieWatch already reads); reconstruction uses Delta time-travel/history retention. The writers are non-interactive Job service principals with no interactive login. This is *not* a "broad MODIFY fallback"; it is `MODIFY` bounded by the append-only property for data DML plus durable audit of any schema/property mutation.
> - **Coordination** (mutable, `Serializable`) is granted `SELECT, MODIFY` to the executor **and** to the separate enrollment SP. UC cannot split UPDATE-only vs INSERT-only, so the executor-updates / enrollment-inserts separation is enforced by the **coordination protocol** — distinct service principals, separate governed Jobs, `max_concurrent_runs=1` + queue, Serializable isolation, and UPDATE-only `MERGE` CAS with no NOT-MATCHED-INSERT branch — not by DML-verb grants.
> - Grants never include `ALL PRIVILEGES`, `MANAGE`, or ownership. `USE CATALOG`/`USE SCHEMA` are issued by the admin/catalog owner during namespace setup, never by the provision job. A fuller least-privilege split (an append-only enrollment-intent table so enrollment holds `MODIFY` only on an append-only securable while the executor is the sole writer of the mutable row) is recorded as future hardening; adopting it requires a reviewed schema/protocol revision.
> - `verify_fact_permissions` / `verify_coordination_permissions` assert the effective proofs as `{SELECT, MODIFY}` accordingly. Integration tests (`backend/tests/integration/test_vc_permissions.py`) confirm on live tables: append-only rejection of UPDATE/DELETE, `DROP` denied to the non-owner runtime (ownership boundary), each append attributed to the runtime principal in Delta history, and — on a disposable owner-created table — that a runtime appendOnly flip *succeeds* but is recorded and attributed in Delta history (`test_fact_table_tampering_is_detectable_in_delta_history`), plus Serializable isolation and distinct serialized coordination writers.
- Logical uniqueness of event/version/approval identifiers is enforced by serialized protocol plus read-back and deterministic event keys, not unenforced Delta keys. Retrying an ambiguous append first queries its key; inconsistent duplicates block admission. Read views may deduplicate byte-identical retry facts without mutating history.
- An operations table is a single grant boundary. Source principals never write the target operations table. Target approvers submit to the authenticated target service; executor re-resolves policy. Export only dedicated receipt artifacts/approved views, never the whole table as a public receipt channel. If roles require isolation beyond the target trusted service, physical copies/splits of the logical fact schema need an explicit reviewed contract revision; don't invent path-level table grants.

## 3. UC Volumes and immutable artifacts

Separate **Volume securables**, not prefixes, for different writers/readers. Replace names below per environment; M08 provisions from owner DDL/specs.

| Owner | Volume | Layout | Access intent |
|---|---|---|---|
| M02 | `vc_snapshots` | `envelopes/sha256/<digest>.json`, `evidence/sha256/<digest>.json` | Trusted observer/fact writer; authorized history readers through app |
| M06 | `vc_approval_evidence` | `sha256/<digest>/preflight.json`, `policy.json` | Target-local approval service; no source write authority |
| M07 | `vc_outbound_packages` | `sha256/<package-digest>/manifest.json`, `artifact.json`, `mapping.json`, `validation.json` | Source package writer; target read-only |
| M07 | `vc_target_receipts` | `sha256/<receipt-digest>/receipt.json`, `tests.json` | Target-only writer; source read-only/reverse share |

Each uses `CREATE VOLUME IF NOT EXISTS ${catalog}.${control_schema}.<name>` through provisioning; UC grants attach to the Volume. Never overwrite an existing digest path: equal bytes return existing artifact, unequal bytes reject and alert. Verify bytes on every consumer read; a digest detects substitution but is not WORM storage. Publish the manifest/Delta reference only after all referenced uploads succeed; orphan cleanup cannot delete referenced evidence. Retention/access policy is explicit and separately authorized.

Same-metastore transport uses shared UC read grants. Cross-metastore transport uses verified Databricks-to-Databricks Delta Sharing support; if Volume sharing is unsupported, define a reviewed shareable read-only artifact representation or disable that topology. Do not add an external command bus or cross-workspace write credentials as a workaround.

## 4. Python ports

All signatures below are synchronous domain Protocols; async routers wrap blocking work using existing repository conventions. Executors receive explicit `ExecutorContext(workspace_id, host, principal_id, actor_kind, credential_handle, execution_ref)`; never serialize credentials into Delta/Jobs or resolve default profiles. Shared DTO definitions and Protocols: M02 `contracts.py`; adapters in owner directories.

```python
class Canonicalizer:  # M01, existing config_fingerprint.py remains entry point
    def observe(self, envelope: dict, version: str = "vc-c14n/1") -> Snapshot: ...
    def compare(self, left: Snapshot, right: Snapshot) -> Comparison: ...
    def semantic_diff(self, left: Snapshot, right: Snapshot) -> list[DiffItem]: ...

class VersionLedger:  # M02
    def append_observation(self, snapshot: Snapshot, context: CaptureContext) -> ObservationRef: ...
    def get_version(self, binding: BindingRef, version_id: str) -> Version: ...
    def history(self, binding: BindingRef, cursor: str | None, limit: int) -> VersionPage: ...
    def verify_committed(self, observation: ObservationRef) -> bool: ...

class Registry:  # M02; initializer injected, not imported from M03
    def enroll(self, request: EnrollmentRequest, actor: ActorContext) -> BindingRef: ...
    def resolve(self, binding_id: str) -> BindingRef: ...
    def bind_created(self, intent: CreateIntentRef, space_id: str, evidence: IdentityEvidence) -> BindingRef: ...
    def tombstone(self, binding: BindingRef, approval: RebindAuthorization) -> BindingRef: ...
    def rebind(self, binding: BindingRef, new_space_id: str, approval: RebindAuthorization) -> BindingRef: ...

class OperationFacts:  # M06 implementation; protocol owned M02
    def append(self, fact: OperationFact) -> FactRef: ...
    def lookup_request(self, binding: BindingRef, idempotency_key: str) -> RequestHistory: ...
    def approval_use(self, binding: BindingRef, approval_id: str) -> ApprovalUse: ...
    def get_request(self, operation_id: str) -> ApprovedOperation: ...
    def publish_consumption(self, claim: AdmissionClaim) -> FactRef: ...
    def verify_flush(self, claim: AdmissionClaim) -> bool: ...

class Coordination:  # M03
    def initialize(self, binding: BindingRef, enrollment: EnrollmentProof) -> None: ...
    def reserve(self, binding: BindingRef, operation: RequestIdentity, executor: ExecutorContext) -> Reservation: ...
    def admit(self, reservation: Reservation, preimage: ObservationRef | CreateIntentRef,
              authorization: AuthorizationGrant) -> AdmissionClaim: ...
    def renew(self, claim: FenceToken) -> FenceToken: ...
    def assert_owner(self, claim: FenceToken) -> None: ...
    def checkpoint(self, claim: AdmissionClaim, stage: PatchStage, evidence: StageEvidence) -> None: ...
    def finish(self, claim: AdmissionClaim, result: OperationResult) -> None: ...
    # Definite pre-send rejection of a RESERVED (never-admitted) attempt: publishes the
    # terminal CONFLICTED fact and releases the binding to IDLE. Fails closed if the row is
    # not a proven pre-send RESERVED row owned by this reservation (must NOT bypass the
    # possible-send quarantine obligation). Counterpart to finish for the pre-admission
    # reviewed-base-conflict path (VC/1.0 addition; see ADR decision 8 / M03+M04 integration).
    def reject_reservation(self, reservation: Reservation, message: str) -> None: ...
    def quarantine(self, claim: FenceToken, reason: str) -> None: ...
    def recover(self, binding: BindingRef, evidence: RecoveryEvidence) -> RecoveryResult: ...
    def observe_exclusively(self, binding: BindingRef, executor: ExecutorContext) -> ObservationLease: ...
    def advance_heads(self, fence: FenceToken, update: HeadUpdate) -> Heads: ...

class GenieTransport:  # M04, wrapper around genie_client.py
    def get(self, binding: BindingRef, executor: ExecutorContext) -> dict: ...
    def create_once(self, payload: CreatePayload, claim: AdmissionClaim) -> CreateResponse: ...
    def patch_config_once(self, binding: BindingRef, serialized_space: dict, claim: AdmissionClaim) -> None: ...
    def patch_description_once(self, binding: BindingRef, description: str, claim: AdmissionClaim) -> None: ...

class MutationGate:  # M04
    def execute(self, request: MutationRequest, executor: ExecutorContext) -> OperationResult: ...
    def create(self, request: CreateRequest, executor: ExecutorContext) -> OperationResult: ...
    def verify_only(self, operation_id: str, executor: ExecutorContext) -> OperationResult: ...

class Observer:  # M04
    def capture_on_open(self, binding: BindingRef, viewer: ActorContext) -> ObservationResult: ...
    def capture(self, binding: BindingRef, reason: str, executor: ExecutorContext) -> ObservationResult: ...

class DriftService:  # M05
    def classify(self, heads: Heads, versions: VersionLookup, reachability: str) -> DriftResult: ...
    def reconcile(self, binding: BindingRef, action: ReconcileRequest, actor: ActorContext) -> OperationHandle: ...
    def scan(self, workspace_id: str, cursor: str | None) -> ReconcileBatch: ...

class ApprovalService:  # M06
    def request(self, inputs: ApprovalInputs, actor: ActorContext) -> ApprovalRequest: ...
    def vote(self, approval_id: str, decision: str, actor: ActorContext) -> ApprovalRecord: ...
    def authorize(self, request: MutationRequest, executor: ExecutorContext) -> AuthorizationGrant: ...
    def break_glass(self, request: BreakGlassRequest, actor: ActorContext) -> AuthorizationGrant: ...

class PromotionService:  # M07
    def package(self, version_id: str, mapping: MappingSpec, policy: TestPolicy) -> PackageRef: ...
    def preflight(self, operation_id: str, executor: ExecutorContext) -> PreflightEvidence: ...
    def dispatch_pending(self, target_workspace_id: str, cursor: str | None) -> DispatchPage: ...
    def execute(self, operation_id: str, executor: ExecutorContext) -> DeploymentReceipt: ...

class IdentityProvider:  # M08; authorization decisions owned M06
    def actor(self, request: AuthenticatedRequest) -> ActorContext: ...
    def groups(self, subject_id: str, workspace_id: str) -> frozenset[str]: ...
    def can_edit(self, subject_id: str, binding: BindingRef) -> bool: ...
    def executor(self, selection: ExplicitExecutorSelection) -> ExecutorContext: ...
    def verify_run_as(self, execution_ref: str, expected_principal_id: str) -> None: ...

class JobDispatcher:  # M08 target-local adapter; runtime handlers owned M04/05/07
    def submit_local(self, operation_id: str, job_kind: str) -> OperationHandle: ...
```

DTO details: `RequestIdentity(operation_id, idempotency_key, request_digest)`; `FenceToken(binding_id, binding_revision, attempt_id, generation, row_version)`; `AdmissionClaim` adds committed preimage/intent and optional consumed approval; `HeadUpdate(observed?, approved?, deployed?, authorization_reference)`; `OperationHandle(operation_id, status, job_run_id?)`. `PatchStage` distinguishes config pending/in-flight/observed and description pending/in-flight/observed. `Comparison` distinguishes equal/different/unknown, not a misleading boolean.

`RecoveryEvidence` includes exact attempt identity, trusted termination source/time, all retry/child execution terminal evidence or positive app-worker termination proof, post-termination GET timestamps/digests, verified request-lifetime bound/reference, checkpoint classification, and optional audited human authorization/reason. `AuthorizationGrant` is an internal validated value, never a client-supplied assertion.

## 5. Admission, execution and recovery rules

1. Resolve unique current binding/revision; reject duplicate ownership. `reserve` CASes only idle/unresolved=false with expected row revision, increments generation and binds a unique attempt. CAS losers return conflict; an expired occupied row becomes quarantined, never idle. Read back exact holder/attempt/generation, not just SQL success.
2. While exclusively reserved, check durable `(binding, revision, idempotency_key)` history: same key/different digest → hard conflict; same key/same completed digest → existing receipt, no mutation. Recheck old approval consumption. Initial request facts may be persisted before reservation; only admission authorizes writes.
3. Fresh GET, canonicalize, persist immutable preimage. If reviewed base differs, preserve external state and publish `conflicted`; no PATCH. Persist no-op receipt when all three fingerprints match; no write-authorizing admission necessary, but authorization, idempotency and guarded finalization still apply.
4. `admit` verifies committed observation/intent, revalidates authorization, and performs one UPDATE-only CAS binding the separate operation/key/digest, approval ID/digest, preimage ID/digest, base, generation and attempt. Confirm acquisition via read-back. CAS expiry/ambiguous result → no PATCH until proven exact admission; no blind reacquisition.
5. Persist approval consumption through `OperationFacts` before allowing row reuse (prefer before PATCH). On fact-publish failure, keep claim unresolved, fail closed. Old consumption survives later row reuse; same-operation recovery can finish publication but does not grant a fresh mutation attempt.
6. Before **each** PATCH persist stage intent/checkpoint and reassert exact fence. Use no SDK/HTTP automatic retry on mutations. Record config read-back before deciding whether description is safe: config must match normalized intended content and description its approved preimage. Default: no field-independent drift completion. An explicit approved policy may permit description-only completion but final status remains conflicted.
7. Timeout, 5xx, disconnect, app crash after send, or post-write GET failure → unresolved quarantine; matching GET is observation only, **not** permission to replay or run the second PATCH. If a crash occurred after recording send intent, assume possible send. Same-operation resume is restricted to reads/evidence/finalization unless durable proof shows that stage was never sent and ownership is still valid.
8. Final GET produces authoritative version and verification/test evidence. Generation-conditioned head updates: observer changes observed only; approved requires policy authorization; deployed requires verified deployment. Publish all required terminal/consumption facts and confirm them before clearing active claim. If projection/fact publication fails, keep unresolved; never erase the claim.
9. Recovery requires trusted termination of the exact prior attempt plus ≥3 stable GETs **after** termination, spanning strictly longer than the verified maximum Genie server request lifetime; classify against checkpoint, record recovery facts, bump generation. Lease expiry, heartbeat silence, job cancellation request, or base equality alone are insufficient. Until the bound is verified, no auto-clear. Audited human resolution is explicit break-glass with evidence and residual-risk acknowledgement, never a blind retry button.
10. Compensation is a new governed operation linked to the original, permitted only with bound preauthorization and fresh live equality to the recorded post-stage. If it drifted, preserve and block. No in-process unconditional rollback.

Create exception: serialized provisional enrollment + coordination reservation and durable create-intent precede `create_once`. Lost response quarantines provisional binding; never replay or match by name. Resolve possible orphan using audited identity evidence; persist physical binding before follow-on writes, then authoritative first GET/version with no parent. Rebind/tombstone invalidates old-revision approvals. A returned create followed by binding/evidence failure remains unresolved, not permission to recreate.

Observer protocol: acquire a non-mutating observation lease before GET and append/head advancement. Concurrent opens serialize; delayed observations cannot regress a newer head. Active managed attempts yield their checkpoint/busy result rather than being labeled external. If capture fails, return stale and disable reconcile actions. Observation-only deployments may use this coordination subset while write admission is feature-disabled.

## 6. Approval, package and receipt payloads

`ApprovalInputs(schema_version, operation_id, operation_type, source_version_id, raw_source_digest, source_fingerprints, artifact_digest?, mapping_digest?, rendered_target_digest, transformer_version?, canonicalizer_version, target_binding, expected_base_fingerprints, permission_policy_digest?, validation_policy_digest, benchmark_policy_digest, preflight_evidence_digest, thresholds, requester_id, recovery_policy, expires_at)`. Approval digest additionally binds server-derived approver identities and approval timestamps. Mutable binding revision, policy, target, base or any rendering input invalidates approval. Self-service edits use an explicit rights-checked grant; they are not implicit approvals.

Production: two distinct human approvers, neither requester/deployer; at least one target-group eligible as re-resolved by the target SP at execution; expiry ≤24 hours. Adoption files a new request; reapply needs fresh approval for newly captured base. Acknowledge records scoped divergence without pretending heads agree. Break-glass is separate-group, reason-required, time-limited, suspends ordinary execution, keeps evidence mandatory and requires later reconciliation.

`PackageManifest(schema_version, space_key, source_version_id, raw_source_digest, source_fingerprints, artifact_digest, mapping_digest, transformer_version, required_sources, validation_policy_digest, benchmark_policy_digest, ownership, file_digests, package_digest)`. Portable content excludes source workspace/space/warehouse IDs and paths; immutable mapping supplies target identifiers/principals outside portable content. Target pre-enrollment is required.

`DeploymentReceipt(schema_version, release_id, operation_id, target_binding, attempt_id, generation, coordination_backend='delta', approval_id, approval_digest, source_version_id, package_digest, mapping_digest, intended_fingerprints, rendered_fingerprints, observed_fingerprints, pre_version_id, post_version_id?, transformer_version, canonicalizer_version, executor_id, job_run_id, validation_evidence_digest, benchmark_evidence_digest, status, compensation_operation_id?, recorded_at)`. Target writes Delta receipt fact first; export to target receipt Volume is repairable and cannot cause another PATCH.

## 7. HTTP contracts and owners

New prefix: `/api/version-control`. Existing create/edit URLs remain compatible adapters owned M04; inspect repository route names rather than invent replacements. Every route authenticates server-side, resolves binding and history/snapshot authorization. `Idempotency-Key` is required on commands; body includes reviewed base/binding revision where relevant. Never accept credentials or an approver identity as authorization.

| Owner | Method/path | Request → response |
|---|---|---|
| M02 | `GET /bindings/{binding_id}/versions` | `cursor?, limit≤100` → `VersionPage` |
| M02 | `GET /bindings/{binding_id}/versions/{version_id}` | → authorized `VersionDetail` |
| M02 | `GET /bindings/{binding_id}/diff` | `left, right` → `SemanticDiff` (M01) |
| M04 | `POST /bindings/{binding_id}/observe` | reason=`open/history/refresh/return` → `ObservationResult` |
| M04 | `POST /bindings/{binding_id}/restore` | `version_id, binding_revision, expected_base, approval_id` → 202 `OperationHandle`; Job only |
| M05 | `GET /overview` | `cursor?, limit≤100` → projected `OverviewPage`, no live GET per row |
| M05 | `GET /bindings/{binding_id}/status` | → `BindingStatus` |
| M05 | `POST /bindings/{binding_id}/reconcile` | action=`adopt/reapply/acknowledge`, base, revision, policy inputs → 202 handle |
| M06 | `POST /approvals` | immutable requested inputs → `ApprovalRequest` |
| M06 | `POST /approvals/{approval_id}/votes` | decision only → `ApprovalRecord`; actor from auth |
| M06 | `GET /approvals/{approval_id}` | → `ApprovalRecord` |
| M06 | `GET /operations/{operation_id}` | → operation, status, checkpoints, audit/receipt references |
| M06 | `POST /break-glass` | scoped request, reason, expiration → audited handle |
| M07 | `POST /releases` | source version, mapping, policy, explicit source/target selection → handle |
| M07 | `POST /releases/{release_id}/validate` | → 202 preflight handle |
| M07 | `POST /releases/{release_id}/promote` | target-local approval reference → 202 pending operation |
| M07 | `GET /releases/{release_id}/receipt` | → receipt or 202 pending |

Enrollment/rebind initially run through M02/M08 audited administrative Job contract, not an unauthenticated generic create-table endpoint. Recovery runs through M03/M06 authorized Job/operator contract; UI displays evidence/status via operations. Comparison is a GET, manual merge is a reviewed new desired payload through the existing gated edit flow; v1 has no auto-merge API.

Errors: `ApiError(code, message, operation_id?, retryable, stale, details?)`; 401/403 authentication/authorization; 404 scoped missing resource; 409 conflict/key mismatch/drift/ambiguous binding; 423 unresolved/quarantine; 422 invalid mapping/schema; 503 coordination/evidence unavailable. A 503 does **not** invite mutation replay; clients poll the operation id. History browsing can remain available when only coordination writes fail; full UC outage can also stop reads. Snapshot persistence failure returns explicit stale/busy result or 503, never success with a silently advanced head.

## 8. Frontend types (M09)

```typescript
type DriftState = 'clean' | 'external_ahead' | 'desired_ahead' | 'diverged'
  | 'unknown' | 'unreachable' | 'applied_unverified' | 'conflicted';
type OperationStatus = 'requested' | 'preimage_captured' | 'apply_attempted'
  | 'applied_unverified' | 'applied_partial' | 'confirmed' | 'conflicted'
  | 'quarantined' | 'failed' | 'compensation_attempted' | 'compensated' | 'noop';
type Origin = 'workbench' | 'external' | 'optimizer' | 'restore' | 'promotion' | 'unknown';
interface Heads { observed: string | null; approved: string | null; deployed: string | null }
interface Fingerprints { config: string; benchmark: string; metadata: string; canonicalizer_version: string }
interface BindingStatus {
  binding_id: string; binding_revision: number; heads: Heads; drift: DriftState;
  quarantined: boolean; unresolved_operation_id: string | null;
  observed_at: string | null; projection_as_of: string; stale: boolean;
  allowed_actions: string[]; reasons: string[];
}
interface VersionSummary {
  version_id: string; binding_id: string; observed_at: string; origin: Origin;
  observed_by: string; parent_version_id: string | null; restored_from_version_id: string | null;
  fingerprints: Fingerprints; optimizer_run_id: string | null; champion_id: string | null;
}
interface Page<T> { items: T[]; next_cursor: string | null }
interface OperationHandle { operation_id: string; status: OperationStatus; job_run_id: string | null }
interface DiffItem {
  category: 'sources' | 'columns' | 'instructions' | 'joins' | 'filters' | 'parameters'
    | 'sql' | 'benchmarks' | 'questions' | 'metadata' | 'bindings';
  path: string; change: 'added' | 'removed' | 'modified'; before: unknown; after: unknown;
  review_required: boolean;
}
interface ObservationResult { status: BindingStatus; captured_version: VersionSummary | null; busy: boolean }
interface ApiError { code: string; message: string; operation_id?: string; retryable: boolean; stale: boolean }
```

M02 supplies JSON fixtures for nullable fields, stale status, all drift/operation enums, fingerprints, full approval and receipt payloads. M09 models full receipt/approval types from section 6 (no `any`); tests check Python serialization against TypeScript fixture consumption. Short IDs are display labels, never monotonic version numbers or authoritative ordering. Badge `In sync` derives from approved/deployed policy equality, never simply live==observed.
