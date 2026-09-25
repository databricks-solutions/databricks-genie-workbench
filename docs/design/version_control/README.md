# Genie Agent Version Control and Promotion Design

**Status:** Draft
**Last updated:** September 6, 2026

## Purpose

This document proposes an architecture for two related but separate capabilities in Genie Workbench:

1. **Version control of live Genie Agents (formerly Genie Spaces)** — save immutable versions, compare them, restore older versions, and reconcile changes made outside Workbench.
2. **Governed cross-workspace promotion** — promote an approved version to another Databricks workspace with validation, approvals, rollback, and audit evidence.

The primary design priority is reliable version control and reconciliation. Cross-workspace promotion should be built only after the system can capture, compare, and safely restore live state in one workspace.

## Fully Databricks-Native Constraint

This design is **fully Databricks-native**. Operational version control, approvals, versions, deployments, and reconciliation are managed entirely inside Genie Workbench and governed Databricks resources. A user never has to leave Genie Workbench to approve, restore, reconcile, or promote a Genie Agent.

The system does **not** use, and must not reintroduce:

- GitHub Actions or any external CI/CD runner.
- GitHub environments (or any external environment gate) as the deployment-approval system.
- An external release control plane or release orchestrator.
- Git or GitHub as the authoritative version ledger for Genie Agents.
- Any workflow that forces a user to leave Genie Workbench to approve, restore, reconcile, or promote.

Git and GitHub may continue to store the Genie Workbench **application source code and documentation**. They must not be required for operational version control or promotion of Genie Agents. All operational state — approvals, versions, deployments, and reconciliation — lives in Genie Workbench and governed Databricks resources.

The architecture is built from:

- **Databricks Declarative Automation Bundles (DABs)** — provisioning only.
- **Databricks Jobs and Workflows** — the execution plane for governed operations (restore, promotion, reconciliation, optimizer champion apply).
- **Genie Workbench application** — executes everyday create/edit as the user, through the same shared version-capture and coordination gate.
- **Databricks CLI and Python SDK** — used by both the app and Jobs with explicit executor authentication; never an auto-selected profile.
- **Genie REST APIs** — the authoritative source of live state via read-back.
- **Unity Catalog Delta tables** — durable, append-only version and operation facts, and the v1 coordination authority.
- **Lakebase** — an optional future transactional coordination backend (not required for the first release).
- **Databricks identities, groups, permissions, OAuth, and service principals** — the authorization plane.
- **Workbench-native user interfaces** — history, comparison, restore, approval, promotion, and audit.

## Executive Decision

Adopt the **native hybrid** architecture (Option 3) with explicit authority boundaries:

- **Genie API read-back** determines what is actually live.
- **Unity Catalog Delta tables** store durable, append-only version and operation facts, and act as the **v1 coordination authority** through a per-binding compare-and-swap row.
- **Every write path is version-controlled through the same gate.** Everyday Workbench actions (create-agent, edit) execute in the application as the user, but are wrapped in the same version-capture and coordination gate as everything else. High-stakes governed operations (restore, production change, cross-workspace promotion) additionally run through **Databricks Jobs** for separation of duties, retries, and audit. The discipline is identical for all writers — acquire the coordination lease, capture the pre-state, apply, read back, capture the post-state — and no write bypasses version capture.
- **Genie Workbench** is the sole control plane and user interface for history, comparison, restore, drift/reconcile, releases, approvals, and audit.
- **DABs** provision the Workbench, Jobs, tables, permissions, identities, and environment configuration. **DABs never manage Genie Agent content.**
- **Lakebase** is not required for the first release. It is a legitimate *future* coordination backend (enforced keys, multi-record transactions), never merely a cache and never an automatic failover target.

Neither Git nor a DAB alone can meet the version-control requirement: changes made directly through the Databricks UI or Genie API may never enter Git, and a bundle that "owns" Genie content will silently re-assert stale configuration on the next deploy, destroying the out-of-band-edit-preservation guarantee.

### Why not DAB-first

DABs now expose `resources.genie_spaces` (with `file_path` or inline `serialized_space`) and `databricks bundle generate genie-space --existing-id ... --watch --force` (CLI >= 1.3.0, `engine: direct`). Rejecting DAB-first is therefore an **authority choice, not a capability claim**:

- Bundle state would believe it owns `serialized_space` and re-assert a stale document on the next `bundle deploy`, overwriting out-of-band edits the system is required to preserve.
- Driving promotion through `bundle deploy` places release control in a bundle/CLI deployment path rather than inside Workbench, and tends to centralize cross-workspace credentials — conflicting with the no-leave-Workbench and least-privilege constraints. (This is an authority objection, not a claim that bundles must be run by a human.)
- `bundle generate genie-space --watch --force` is a shipped command that overwrites local outputs from remote state; a subsequent deploy is the competing write. Bundle-managed Genie content is therefore an active dual-authority hazard, not a hypothetical one.

The registry marks each managed space `governed_by=workbench`, and the reconciler flags any governed space that also appears in bundle state. See **Dual-Authority Guard** below.

### Why not Workbench SDK-only

Everything except *provisioning* is correct in an SDK-only model. But target-local Jobs, the control schema, table grants, and service-principal entitlements must be created once per workspace, idempotently and reviewably, by a human holding that workspace's profile — which is exactly what DABs are good at, and the repository already has the pattern (the optimizer job in `databricks.yml`). Hand-rolling Job creation through the SDK produces drift in the very machinery that detects drift.

## Repository Foundations

Genie Workbench already has several components that should be generalized rather than replaced:

- `backend/services/config_fingerprint.py` canonicalizes Genie configuration and benchmarks independently and calculates stable fingerprints. It already handles Genie's read-back normalization (metric views flattening into `tables`, boundary quotes, fragment-array reordering), which makes read-back-as-authoritative viable.
- `backend/services/genie_client.py` can retrieve complete Genie Agent state, including `serialized_space`. The write path is `PATCH /api/2.0/genie/spaces/{space_id}` with `{"serialized_space": json.dumps(...)}` — effectively a **whole-document replace (last-writer-wins)** — plus a **separate** top-level `description` PATCH.
- `backend/routers/auto_optimize.py` has conservative current-version and drift checks that avoid declaring drift when history is incomplete.
- `packages/genie-space-optimizer/` stores optimization snapshots and distinguishes submitted configuration from authoritative post-write observations. Its `revert.py` performs best-effort, **non-atomic**, in-process compensation — which is the source of the unrecoverable partial-write behavior this design replaces with a guarded Job path.
- `databricks.yml` provisions the optimization Job and supporting deployment resources, but not the complete Genie Agent lifecycle. The application itself is deployed by `scripts/deploy.sh`, not the bundle; the transition from script-based deployment to DAB provisioning is an implementation task.
- Lakebase support already exists in `backend/services/lakebase.py` and `backend/services/gso_lakebase.py`; it is optional and may fall back to in-memory state. Retained telemetry/session use is fine, but that fallback must never authorize version-changing operations.

The create flow to integrate lives in `backend/routers/create.py` and `backend/services/create_agent.py` (with `create_agent_session.py`, `create_agent_tools.py`, and `genie_creator.py`); follow-on writes from these paths must also pass through the gate. The exact location of the optimizer `revert.py` and the startup `run_as` self-healing behavior are asserted from prior inspection and must be re-verified at implementation time.

Existing optimizer history is useful but is not a universal version ledger. It captures optimization-specific baselines, iterations, and champions rather than every managed, external, optimizer, restore, or promotion change.

### No conditional-update API

V1 assumes **no usable conditional-update capability in its supported deployment matrix** and deliberately retains the existing two-PATCH client protocol. (A request-body `etag` and combined `serialized_space`+`description` update are documented in Public Preview; this design does not rely on preview behavior and treats optimistic concurrency as unavailable until verified in the target deployments.) Safety is therefore built in this control plane. See **Safe Write and Concurrency Model**.

## Requirements

### Version Control

The system must:

- Assign each managed Genie Agent a stable logical identity independent of workspace-specific IDs.
- Save immutable, inspectable API-observed versions.
- Preserve exact restorable state as well as canonical state used for comparisons.
- Produce semantic diffs instead of relying only on raw JSON diffs.
- Restore an arbitrary historical version without deleting or rewriting history.
- Capture state before and after every managed mutation.
- Detect externally made changes before Workbench updates, restores, or deployments.
- Preserve newly observed external state before asking the user to adopt, reapply, or reconcile it.
- Avoid silently overwriting unknown live state.
- Record actors, origins, approvals, operation IDs, validation results, and failure states.
- Keep version state and history accessible through Genie Workbench.

### Governed Cross-Workspace Promotion

The system must:

- Promote a selected immutable source version rather than an unpinned mutable file.
- Separate portable configuration from workspace-specific bindings.
- Resolve catalog, schema, table, metric-view, warehouse, folder, Space ID, and principal mappings.
- Validate target dependencies and permissions before applying changes.
- Require approval inside Workbench (or another Databricks-native governance surface).
- Block deployment when unexplained target drift exists.
- Capture the target pre-deployment state.
- Verify the applied configuration through an authoritative API read-back.
- Run smoke and benchmark tests.
- Produce a durable deployment receipt and support guarded compensation (rollback).

## Non-Goals and Guarantees

The system can guarantee preservation of:

- Every managed pre-mutation and post-mutation boundary.
- Every distinct external state observed by reconciliation.

It cannot guarantee recovery of every intermediate external edit. If a user makes two direct UI edits between reconciliation runs, only the state visible at the next API read may be recoverable unless Databricks exposes a complete configuration event stream.

**"Never silently overwrite" is a governed-writer policy, not an unconditional API guarantee.** Because Genie provides no conditional update, the honest guarantee is three-part:

- **Prevention by ACL where ACLs are enforced.** In production, humans receive `CAN_RUN`/`CAN_VIEW` and editing is reserved to the Workbench/executor service principal, so routine writes flow only through governed, version-captured paths.
- **Serialization of governed writers always.** Every writer we control — Workbench, optimizer, restore, reconciliation, promotion — is serialized per binding by a generation-fenced admission row.
- **Detect, preserve, attribute, and recover everywhere else.** Against a human editing in the Genie UI, a workspace admin, or a break-glass path, an irreducible time-of-check-to-time-of-use window remains. The product detects, preserves, attributes, and recovers; it does not claim to prevent.

Restore and deployment are verified compensating operations, not atomic distributed transactions.

## Authority Model

| Plane | Authority | Responsibility |
|---|---|---|
| Live state | Genie API read-back | Proves what configuration is currently applied, at a point in time |
| Durable facts | Unity Catalog Delta | Append-only versions, operations, registry/bindings, approvals, releases, and receipts |
| Coordination (v1) | Unity Catalog Delta | One mutable per-binding compare-and-swap row (lease, generation, heads) under Serializable isolation |
| Artifacts and evidence | Unity Catalog Volumes | Content-addressed promotion packages and large evidence artifacts, hashed from Delta |
| Approval | Workbench + Databricks groups | Approval records bound to immutable digests; group membership resolved server-side |
| Execution | Workbench app (everyday create/edit) and Databricks Jobs (governed operations) | All mutation paths, each version-captured through the same gate; Jobs carry restore, promotion, reconciliation, validation, verification, and tests |
| Provisioning | DABs | App, Jobs, schemas, permissions, identities, and environment configuration |
| Authorization | Databricks identities and groups | Requester, approver, and deployer separation, resolved server-side |

No cached head, submitted JSON document, or successful API response should be treated as proof of live state without a follow-up API read. Every operation row stamps `coordination_backend` and `generation` so that a future dual-authority bug is visible in the ledger rather than latent.

## Canonical Representation Model

One JSON representation should not serve every purpose.

### Response Envelope

The complete Genie API GET response retained for audit and compatibility. It may contain metadata that is not valid as an update payload. Once median row size is large, envelopes should be written to a UC Volume with a pointer and fingerprints retained in Delta.

### Restorable State

The exact parsed `serialized_space` plus governed top-level metadata that can be reapplied to the same physical Genie Agent. Restore uses this representation.

### Canonical State

A stable comparison representation that:

- Removes transient and non-restorable fields.
- Normalizes historical wrapper shapes.
- Concatenates Genie fragment arrays where appropriate.
- Sorts object keys and ID-addressed unordered collections.
- Preserves meaningful order.
- Calculates separate configuration, benchmark, and metadata fingerprints.
- Records the canonicalizer version.

The current fingerprint implementation should be extracted into a reusable versioning service and extended with governed top-level metadata.

### Portable Artifact

An environment-neutral representation generated for adoption or promotion. It excludes source workspace IDs, physical Genie Space IDs, warehouse IDs, and workspace paths.

Portable artifacts and their mappings are stored as **content-addressed packages in Unity Catalog Volumes** (governed by UC grants), not in Git. A promotion package manifest includes the logical `space_key`, artifact schema version, required data sources, validation policy, benchmark policy, ownership, and artifact digest.

## Minimal Authoritative Data Model

The initial implementation avoids a large event-sourced schema. It uses **four logical grant-domains** in Unity Catalog Delta plus a UC Volume for envelopes, packages, and evidence. Physical table count is an implementation choice, but mutable coordination must never share a securable with append-only facts, and approval and release facts must be durable in Delta.

Append-only tables are enforced by **both grants and Delta table properties/constraints**, not by naming convention.

### `genie_space_versions`

Append-only authoritative observations:

| Field | Purpose |
|---|---|
| `version_id` | Immutable version UUID |
| `space_key` | Stable logical Genie Agent identity |
| `workspace_id` | Origin workspace |
| `space_id` | Physical Genie Agent ID in that workspace |
| `environment` | Development, test, stage, or production |
| `observed_at` | Observation timestamp |
| `observation_reason` | Managed write, reconciliation, restore, deployment, or manual refresh |
| `observed_by` | User, application principal, deployment principal, or unknown |
| `origin` | `workbench`, `external`, `optimizer`, `restore`, `promotion`, or `unknown` (distinct from `observed_by`, which is the observer identity, not the inferred change author) |
| `parent_version_id` | Previous observed or reconciliation-base version |
| `restored_from_version_id` | Historical source when created by restore |
| `operation_id` | Correlated governed operation |
| `api_update_time` | Genie-reported update timestamp |
| `response_envelope_json` | Complete API observation (or UC Volume pointer) |
| `serialized_space_json` | Exact parsed restorable content |
| `restorable_metadata_json` | Governed updateable metadata |
| `config_fingerprint` | Canonical non-benchmark configuration identity |
| `benchmark_fingerprint` | Canonical benchmark identity |
| `metadata_fingerprint` | Governed top-level metadata identity |
| `canonicalizer_version` | Version of comparison rules |
| `optimizer_run_id` | Optimizer run that produced this version, when `origin` is `optimizer` |
| `champion_id` | Optimizer champion applied to the live agent, when applicable |
| `release_id` | Optional promotion release association |

Application and deployment principals should not receive `UPDATE` or `DELETE` privileges on this table; enrollment is the only `INSERT` path for coordination rows (see below).

### `genie_space_operations`

Append-only durable operation transitions:

| Field | Purpose |
|---|---|
| `operation_id` | Stable operation UUID |
| `idempotency_key` | Bound to the full request digest; prevents duplicate governed operations |
| `operation_type` | Update, restore, adopt, deploy, or compensate |
| `space_key` | Logical Genie Agent |
| `target_workspace` | Workspace receiving the operation |
| `attempt_id` | Unique per-execution attempt identity (not the shared SP identity) |
| `generation` | Monotonic coordination generation at admission |
| `coordination_backend` | Coordination authority used (Delta in v1) |
| `requested_base_fingerprint` | Live state the approver expected |
| `desired_artifact_fingerprint` | Approved desired source |
| `mapping_fingerprint` | Exact environment transformation rules |
| `transformer_version` | Rendering implementation version |
| `test_policy_fingerprint` | Approved validation rules |
| `pre_version_id` | Durable pre-operation observation |
| `post_version_id` | Durable post-operation observation |
| `approval_reference` | Workbench approval record or break-glass record |
| `job_run_id` | Databricks Job execution |
| `status` | Current durable saga boundary |
| `evidence_json` | Validation, benchmark, and diagnostic evidence |
| `recorded_at` | Transition timestamp |

Meaningful operation states include:

- `requested`
- `preimage_captured`
- `apply_attempted`
- `applied_unverified`
- `applied_partial`
- `confirmed`
- `conflicted`
- `quarantined`
- `failed`
- `compensation_attempted`
- `compensated`

Detailed execution logs should remain in Databricks Jobs rather than producing a durable event for every function call.

### `genie_space_registry`

The **logical identity** table. Genie spaces are not Unity Catalog securables, so there is nowhere on the live object to stamp `space_key` — **the registry is the identity**:

| Field | Purpose |
|---|---|
| `space_key` | Stable logical identity |
| `workspace_id` + `space_id` | Physical binding |
| `environment` | Binding environment |
| `governed_by` | `workbench` for managed spaces (drives the dual-authority guard) |
| `binding_reason` | Why the binding was created or changed |
| `bound_by` | Actor who created the binding |
| `bound_at` | Timestamp |

Append-only with a current-binding view. A deleted-and-recreated space requires an **explicit, audited human rebind** — never an automatic match by display name. Any binding with more than one active ownership row is quarantined by the reconciler.

### `genie_ops_coordination` (mutable)

One row per binding, the **v1 coordination authority**, kept in a securable separate from the append-only facts:

- Lease holder, monotonic `generation`, and unique `attempt_id`.
- `active_operation_id`, `request_digest`, `approval_digest`, `expected_base_fingerprint`.
- Projected **observed**, **approved**, and **deployed** heads, tracked separately and never compressed into a single "current version."

## Safe Write and Concurrency Model

Because Genie exposes no conditional-update API, advisory leases and fencing tokens serialize **our** writers and nothing else. The one native prevention mechanism is **Genie space permissions**: in production, grant humans `CAN_RUN`/`CAN_VIEW` and reserve editing to the executor service principal, so production is deploy-only by construction. This removes ordinary human competitors, but not a stale authorized executor. The shared safety invariant is therefore:

> **One unresolved mutation attempt per binding — not merely one unexpired lease.**

### Delta compare-and-swap admission

Coordination uses Delta optimistic concurrency: a `MERGE` against a single per-binding row conflicts on same-row writes under Serializable/WriteSerializable isolation, so a losing concurrent writer fails or no-ops. This is a genuine compare-and-swap, subject to three invariants:

1. **Exactly one row per binding.** Delta primary/unique constraints are informational and do not prevent concurrent duplicate enrollment, so uniqueness is an explicit protocol invariant: enrollment is **serialized** and is the *only* `INSERT` path, the admission `MERGE` requires exactly one matching ownership row and aborts otherwise, and a reconcile check quarantines any binding with more than one row. Enforcing insert-only vs update-only authority requires Unity Catalog fine-grained DML privileges (separate INSERT/UPDATE/DELETE, in Beta as of August 2026) on compatible compute; without them, `MODIFY` is broader than the intended authority. Append-only fact tables set `delta.appendOnly=true`, and runtime principals must not be able to alter that property.
2. **Generation, not identity.** Acquisition increments `generation` and binds a unique `attempt_id`. Renewal and the pre-PATCH re-assert require the same `(attempt_id, generation)`. A predicate keyed on the shared deployer SP identity would let a stale worker pass its own ownership check and must not be used.
3. **One write-authorizing admission transition, at Serializable isolation** (pin Serializable; do not leave it as "Serializable/WriteSerializable"). A single `MERGE` on the existing ownership row (no `INSERT` branch in executor admission) binds `active_operation_id`, a distinct `idempotency_key` and `request_digest` (scoped to the target binding — keep them as separate columns, not one value), the consumed `approval_id`/`approval_digest`, the committed preimage version id, `expected_base_fingerprint`, and `generation`. The same `idempotency_key` with a *different* `request_digest` is a hard rejection, never a dedup. The authoritative acquisition test is reading `holder`/`generation`/`attempt_id` back, not the statement exit code.
4. **Durable approval and idempotency consumption must survive row reuse.** Because the mutable coordination row is overwritten by the next operation, completed-operation and consumed-approval facts live in the append-only `genie_space_operations`/approval records, not only in the coordination row. Before authorizing, check durable completed/consumed records under exclusive binding ownership; CAS-bind an approval to exactly one operation and durably publish its consumption before the row is reused. A crash must preserve the unresolved claim; define safe *same-operation* resume versus forbidden approval reuse.

Admission does not assume a cross-table transaction. Because `genie_ops_coordination` and `genie_space_versions` are separate securables, the protocol is: (a) **reserve** the binding without authorizing any Genie write; (b) fetch and **commit the immutable preimage evidence** to the version table; (c) perform **one write-authorizing compare-and-swap** on the coordination row that binds the operation id, generation, consumed approval, and the committed preimage version id/digest. No PATCH precedes that CAS. The rule is "admission atomically binds already-committed evidence," not "evidence and admission are one commit." Databricks does support multi-table transactions under explicit catalog-commit/compute prerequisites, but this design does not depend on that.

### The two-PATCH write

A Genie write is two non-atomic operations: the `serialized_space` PATCH (whole-document replace) and a separate top-level `description` PATCH. There is no single-mutation state.

1. **No-op gate.** Canonicalize desired vs. live. If configuration, benchmark, and metadata fingerprints all match, do not PATCH; write a no-op receipt. An unnecessary whole-document replace is pure race exposure.
2. **Admit atomically** (per the invariants above), binding op-id, idempotency key, approval digest, expected base, generation, and attempt-id.
3. **Capture pre-state durably** (config + description) before any write. This is the "preserve external state before overwrite" guarantee and runs unconditionally.
4. **Compare against the base the approver actually reviewed**, not merely the ledger head. On mismatch, preserve the observed state and block (`conflicted`); offer adopt / reapply / compare / acknowledge; no override without break-glass.
5. **Re-assert `(attempt_id, generation)`** immediately before writing; abort if lost.
6. **PATCH `serialized_space`.** On timeout or 5xx, do **not** blindly retry. A matching read-back records an *observed* match but does not prove the timed-out request has terminated, so it does not by itself resolve the attempt or authorize another writer. Keep the attempt unresolved and block subsequent PATCHes until the recovery condition (trusted termination evidence) is satisfied; classify `applied_unverified`. Base-equality alone never authorizes replay.
7. **Checkpoint, then handle `description`.** Record the intervening observation. Complete the description PATCH only when the observed configuration matches the expected normalized result and the description still matches its approved preimage; if unrelated configuration drift appeared, completion is permitted only under an explicitly approved field-independent policy and the operation is still classified `conflicted`. Otherwise stop at `applied_partial`. Because configuration, benchmark, and metadata are fingerprinted separately, a partial application is precisely diagnosable. Fencing, re-assertion, ambiguous-outcome handling, and retry suppression apply to the description PATCH exactly as to the `serialized_space` PATCH.
8. **GET again** and persist the authoritative post version with `operation_id`, `attempt_id`, `generation`, `parent_version_id`, and `restored_from_version_id`.
9. **Verify and test.** Compare observed vs. intended fingerprints (expect normalization), run smoke/benchmark checks, and record a terminal state conditioned on the fencing generation.

### Recovery

- **Expired lease does not authorize takeover.** Expiry transitions the binding to `quarantined`; it never grants ownership.
- **Ambiguous outcome quarantines the binding.** No blind retry, no unconditional compensation. Auto-compensation is allowed only when preauthorized *and* live state still matches this operation's recorded post-stage state.
- **Quarantine exit requires trusted termination evidence for the *exact* prior attempt, regardless of executor type.** Signal 1 is proof the prior attempt can issue no further writes: a terminal Job execution (including retries and child runs) for Job-executed operations, or a specified app-worker termination mechanism for everyday Workbench writes. Lease expiry or a missing heartbeat is **not** termination proof. Signal 2 is live-state stability across at least three consecutive GETs spanning longer than Genie's maximum server-side request lifetime, measured from *after* established termination. Both signals plus a recorded classification against the last checkpoint clear quarantine and bump the generation; either missing keeps it quarantined and pages an operator. Automatic clearance depends on the request-lifetime bound being verified (see Open Questions); **until it is verified, automatic clearance is unavailable and quarantine clears only by audited human action.**
- **Per-workspace serialization is a throttle, not a safety property.** Per-binding admission is the only correctness mechanism; any per-workspace serialization must carry an explicit "not a safety mechanism" note so it can be relaxed later.

### Fail-closed rule

No mutation occurs without the **coordination authority** and the **mandatory pre-write evidence commit** — and in v1 those are one Delta commit. If the coordination authority (UC Delta in v1) or the pre-write evidence commit is unavailable, all version-changing operations stop. In-memory fallback must never authorize restore, promotion, deployment, merge, or reconciliation. Coordination backends are never switched automatically.

## Dual-Authority Guard

To prevent bundle-managed content from competing with Workbench-governed content:

- The registry marks each managed space `governed_by=workbench`.
- The reconciler flags any governed space that also appears in bundle state or is otherwise deployed through an ungoverned path, and attributes the drift from available evidence.
- Ownership is enforced during sanctioned bundle validation/deployment so that bundles do not deploy governed agents' content.

This reduces dual-authority risk but does not fully neutralize workspace administrators or entirely ungoverned deployment paths; those remain covered by detect/preserve/attribute/recover.

## Core Workflows

### Create Agent (Workbench)

Creating a Genie Agent through the Workbench create-agent flow is a first-class mutation and **initiates version control immediately**. There is no prior state to preserve, so the flow:

1. Registers a stable logical `space_key` in `genie_space_registry` with a **provisional binding** (no `space_id` yet) and marks it `governed_by=workbench`, plus a durable create-intent record. The physical binding cannot be acquired first because `space_id` only exists after the create response.
2. Creates the agent through the Genie API. On a **lost/ambiguous response**, do not blindly replay the create and do not match by display name; resolve the possible orphan through an audited step, then continue.
3. Appends the physical `(workspace_id, space_id)` binding to the registry and persists the returned identity **before any follow-on edit**.
4. Reads the agent back and persists the authoritative read-back as the **first immutable version** (`origin: workbench`, no `parent_version_id`).
5. Records binding revision/tombstone events so approvals cannot outlive a delete-and-human-rebind.

From that point the agent is under version control and every subsequent change — in-app edit, optimizer, restore, promotion, or an externally observed change — appends a new version.

### Managed Snapshot and Update

Follow the two-PATCH safe write protocol above: admit atomically, capture pre-state, compare against the reviewed base, no-op gate, re-assert generation, PATCH config, checkpoint, conditionally PATCH description, read back, verify, record a terminal transition, update projections conditioned on the generation, and release the lease. If the update call returns but read-back fails, transition to `applied_unverified` and quarantine; retry only the read-back, never the mutation.

### Semantic Diff

The Workbench should present categorized differences such as:

- Data sources added or removed.
- Included columns changed.
- Instructions added, removed, or modified.
- Join specifications changed.
- Filters and parameters changed.
- Example SQL changed.
- Benchmarks added, deleted, or modified.
- Sample questions changed.
- Title, description, or governed metadata changed.
- Environment bindings changed separately from portable content.

Arrays with stable IDs compare by ID rather than list position. SQL-bearing changes default to manual review. The canonicalizer is versioned; on a version mismatch, dual-compute during migration and mark cross-version mismatches `unknown` rather than fabricating a diff.

### Capture on Open (Align to the Live Source of Truth)

The live Genie Agent is the source of truth. Whenever a user **opens a Genie Agent in Workbench**, the app reads the live state and compares it to the latest recorded version:

1. Fetch live state and canonicalize.
2. If its fingerprints differ from the latest recorded version, **immediately persist the observed live state as a new version** tagged `origin: external`, with `parent_version_id` pointing at the previous recorded version — **before** presenting anything to the user. Preserving the external state must never depend on a user decision.
3. Only then surface the change and offer the reconciliation choices (adopt / compare / reapply / acknowledge).

Capturing external state realigns only the **observed** head; it must **not** update the approved or deployed heads or clear drift — an unapproved external change stays visibly drifted against policy. Concurrent opens must be serialized so they do not regress the head, duplicate the same observation, or mislabel an active managed-PATCH checkpoint as `external`. If persistence fails, show history as stale and disable reconciliation actions rather than claiming a successful realignment.

### External Drift Reconciliation

Reconcile:

- When a user opens a Genie Agent (capture-on-open; see above).
- Before every governed mutation.
- When a user opens version history.
- After returning from the Genie UI.
- On a scheduled Databricks Job.
- Before target deployment.

Use `update_time` as a fetch optimization, not as the authoritative equality check. Periodically perform a full API fetch and fingerprint comparison.

Maintain three projected heads:

- **Observed:** Latest API-observed live version.
- **Approved:** Workbench-approved desired version.
- **Deployed:** Last verified deployment result.

Suggested drift states:

| State | Meaning |
|---|---|
| `clean` | Observed, approved, and deployed states agree |
| `external_ahead` | Live state changed outside the approved deployment path |
| `desired_ahead` | A release is approved but not yet deployed |
| `diverged` | Live and approved state both changed from the common base |
| `unknown` | Authoritative baseline or history is incomplete |
| `unreachable` | Current live state cannot be fetched |
| `applied_unverified` | A write may have succeeded, but read-back is incomplete |
| `conflicted` | Reconciliation requires explicit human resolution |

When a new external state is found, save it before offering:

- **Adopt:** Capture the observed version and file an *adoption request*; recording it as the approved desired state must satisfy the same approval policy (in production, full approval — a capture is not an approval).
- **Reapply:** Reapplying the approved or deployed version is a governed write that obtains **fresh authorization bound to the newly captured target base**; a confirmation button must never resurrect an approval invalidated by the drift.
- **Compare:** Review semantic differences.
- **Merge manually:** Required for overlapping instructions, joins, SQL, or benchmark changes.
- **Acknowledge:** Record intentional environment-specific divergence where policy permits it.

Do not implement general automatic semantic merging in the first release.

### Historical Restore

Restore is a governed operation that runs through a Databricks Job (never the in-process `revert.py`). It creates a new version and never mutates history:

1. Display the historical-to-current semantic diff.
2. Admit the operation and acquire the coordination row (generation, attempt-id).
3. Fetch and durably capture the current live state.
4. Confirm the current fingerprint still matches what the user reviewed.
5. Validate the historical state against the current Genie schema and dependencies.
6. Apply the historical restorable state to the stable physical `space_id`.
7. Fetch authoritative state from the Genie API.
8. Persist the resulting version linked by `restored_from_version_id`.
9. Verify configuration, benchmark, and metadata fingerprints.
10. Run smoke or selected benchmark tests.
11. Mark the operation confirmed, conflicted, quarantined, or failed.

Never automatically compensate after an ambiguous result if doing so could overwrite an unrelated direct edit. Preserve the observed state and require reconciliation.

### Optimizer Reconciliation

The Genie Space Optimizer explores many iterations and selects a champion. Its detailed iteration, baseline, and champion telemetry stays in the optimizer's own history and is **not** promoted into the version ledger. Only the **final applied state** becomes a version:

- When the optimizer **applies a champion** to the live agent, that apply flows through the shared version-capture and coordination gate and produces **one immutable version**, tagged `origin: optimizer`, with `optimizer_run_id` and `champion_id` linking back to the optimizer's own record.
- Intermediate iterations and unapplied candidates are never written to `genie_space_versions`. A run that is abandoned, or whose champion is not applied, creates no version.
- Net: a run produces **at most one champion version** (`origin: optimizer`). Any external preimage observation discovered at apply time, or recovery evidence, is an *additional* version and is not an optimizer iteration version — so "capture every managed write" and "never capture intermediate iterations" both hold. Candidate evaluation must not mutate the managed binding; use isolated optimizer-owned evaluation resources. Reviewers drill into run internals through the linked `optimizer_run_id`.

## Cross-Workspace Promotion

Promotion is **pull-based**: `immutable version -> portable package -> immutable mapping -> target-local preflight -> approval -> target-local execution -> receipt`. An application service principal in one workspace has no implicit rights in another, and this design never fixes that by giving one workspace broad credentials into another.

### Workspace Identity and Authentication

- Deploy Workbench per workspace (already true via `deploy.sh`); each instance drives only its own workspace's Genie API. A single central Workbench UI may present multi-workspace history and approvals, but each mutation executes target-locally. Target authority is a security requirement; a second browser location is not.
- Give each target its own least-privilege deployment service principal. The source holds no broad cross-workspace Genie-write credentials.
- Use account/workspace service principals with OAuth machine-to-machine authentication and governed credential storage. No long-lived PATs.
- When CLI profiles are used, **never auto-select a profile**. Require the user to select explicit **source and target** profiles in Workbench, verify the resolved host and workspace ID, and never rely on a default profile for production promotion.

### Package Transport

- When workspaces share a Unity Catalog metastore, the promotion package lands in a content-addressed UC Volume/Delta location in a governed catalog; the source SP writes the outbound path and the target SP reads it. The **receipt** table is written by the target and read by the source. This uses zero cross-workspace API calls and expresses all authorization as UC grants.
- Across metastores or regions, use Databricks-to-Databricks Delta Sharing (one share outbound for packages, one inbound for receipts). Sharing is a transport, never a writable command bus.

### Target-Local Databricks Job Responsibilities

- **Dispatch:** target-local Jobs discover pending operation ids for their own `workspace_id` (e.g. target-local polling of an inbound table/Volume, or a file-arrival trigger). "Receive an operation id" is a payload contract, not a dispatch mechanism.
- Fetch the approved package and recompute its digest itself (nothing in transit can be substituted).
- Treat outbound packages, target approvals, and receipts as **separate securables** (Volume path prefixes are not independent grant boundaries); target receipts live in target-owned storage and are reverse-shared. Content-addressing detects substitution but does not prevent deletion or overwrite.
- Load environment mappings and verify their fingerprint against the approval.
- Fetch target live state and block unexplained drift.
- Capture the target pre-deployment version.
- Validate tables, metric views, warehouse access, folders, and permissions — testing both executor access and intended-consumer access.
- Render the target payload.
- Apply the Genie update to a **pre-enrolled** target agent (v1 does not create targets inside a promotion; target creation is a separate, partially-recoverable operation).
- Fetch authoritative target state.
- Run smoke questions and benchmark policy.
- Persist the target version and deployment receipt.
- Perform guarded compensation only when post-deployment gates fail and compensation is preauthorized.

### Mapping Policy

Do not perform unrestricted text replacement across the serialized document. Mappings are immutable release inputs covering catalogs, schemas, tables, metric views, warehouses, folders, target Space bindings, and principals/permissions. Use an incremental, schema-aware strategy:

1. Rewrite structured table and metric-view identifiers.
2. Rewrite known catalog and schema fields.
3. Bind warehouse, parent path, physical Space ID, and principals outside portable content.
4. Replace exact qualified identifiers inside supported SQL fields.
5. Add parser-assisted SQL rewriting where reliable.
6. Scan executable fields for unresolved source identifiers.
7. Fail closed on ambiguous or unsupported transformations.
8. Require reviewed explicit overrides when necessary.

## Workbench UI and Front-End

The entire experience lives inside Genie Workbench. Two surfaces are added.

### Overview Page (All Agents)

Each managed agent row shows:

- **Version indicator** — current recorded version number / short id.
- **Drift badge**, computed from the projected heads (cheap, not a live API call per row, with an on-demand refresh):

| Badge | Meaning |
|---|---|
| In sync | Live fingerprint matches the **approved/deployed** policy-relevant state (not merely the latest observed head, which capture-on-open keeps equal to live) |
| Drifted | Live state diverges from the approved/deployed state; links to the captured `origin: external` version |
| Pending release | A version is approved but not yet deployed |
| Unknown / unreachable | History is incomplete or live state cannot be fetched |

### Agent Page — "Version Control and Promotion" Tab

A dedicated tab on the Genie Agent page provides:

- **History timeline** — every version with version number, actor, origin (`workbench`, `optimizer`, `external`, `restore`, `promotion`), and timestamp.
- **Diff** — semantic comparison between any two versions (not raw JSON).
- **Restore / forward** — revert to any historical version or move forward to a newer captured version; both create a new version and never rewrite history.
- **Reconcile** — shown when drifted; offers adopt / compare / reapply / acknowledge.
- **Validate** — check target dependencies and permissions and run smoke / benchmark tests before pushing.
- **Promote** — select a version, resolve the target mapping, validate, request approval, push to a target workspace, and view the returned receipt.
- **Approvals and audit** — approval records, bound digests, approver identities, and expiry.

These surfaces are read-mostly projections of the durable ledger; all mutations they trigger go through the same version-capture and coordination gate described above.

## Native Approval Workflow

Approvals live inside Workbench. There is no external environment gate. Workbench provides History, Compare, Restore, Drift/Reconcile, Releases, Approvals, and Audit surfaces.

Bind each approval to one immutable digest containing at least:

- Source version ID and raw + canonical source fingerprints.
- Portable artifact, mapping, and resolved target-payload fingerprints.
- Transformer and canonicalizer versions.
- Target workspace and agent binding.
- Expected target-base fingerprint, including description and managed permission changes.
- Validation policy and benchmark policy versions, thresholds, and preflight evidence.
- Requester and approver identities, approval timestamp, and expiration.
- Permitted recovery behavior.

Changing any bound input voids the approval.

Rules:

- Separate requester, approver, and deployer service principal; enforce identity and group checks **server-side** (a caller cannot submit an approver name as evidence).
- Production requires two distinct authorized human approvers, neither of whom is the requester.
- At least one production approver must be authorized **target-side**, evaluated by the target SP against a target-controlled release-approver group at execution time. A source-workspace-only approval is a governance illusion. (Genie `CAN_MANAGE` may be one acceptable eligibility criterion but is not required, because it also grants edit and permission administration.)
- For self-service, same-workspace, unapproved edits (dev iteration, optimizer apply on a space the user owns), the executor verifies the requester actually holds edit rights on that space, so the service principal does only what the human could do directly. For governed release operations, the requester needs release-policy group membership only — never direct Genie edit rights.
- Executors accept an operation id and fetch the approved payload themselves.
- Approvals are single-use via CAS consumption, production expiry <= 24 hours, and re-approval is mandatory if the target base fingerprint moved.
- Break-glass is a separate group, time-limited, reason-required, visible in Workbench; it suspends normal execution, requires reconciliation afterward, and records the drift override as a first-class field — it never bypasses evidence capture.
- Correlate application facts with `system.access.audit` for platform evidence. Audit tables are evidence, never the approval ledger or a synchronous write lock.

## Decision Matrix

Scores prioritize live-state versioning and reconciliation under the Databricks-native constraint.

| Criterion | DAB-first | Workbench SDK-only | Native hybrid |
|---|---:|---:|---:|
| Represents Genie lifecycle safely | 2/5 | 4/5 | 5/5 |
| Out-of-band-edit preservation | 1/5 | 5/5 | 5/5 |
| No-leave-Workbench governance | 2/5 | 5/5 | 5/5 |
| Provisioning repeatability | 5/5 | 2/5 | 5/5 |
| Every write path version-captured | 2/5 | 4/5 | 5/5 |
| Separation of duties and audit | 3/5 | 4/5 | 5/5 |
| Cross-workspace safety | 2/5 | 4/5 | 5/5 |
| Dual-authority risk (higher is safer) | 1/5 | 5/5 | 5/5 |
| Maintenance simplicity | 4/5 | 3/5 | 3/5 |
| Repository fit | 3/5 | 4/5 | 5/5 |

### DAB-first

Rejected as the primary mechanism. `resources.genie_spaces` exists, so this is a capability that could be used — but bundle-managed content re-asserts stale configuration, and `bundle deploy` centralizes cross-workspace credentials and forces a CLI act outside Workbench. It fails the out-of-band-preservation and no-leave-Workbench requirements.

### Workbench SDK-only

Rejected as the complete architecture only because provisioning belongs in DABs. Everything else about it is correct, and it collapses into the native hybrid once provisioning is factored out.

### Native hybrid

Accepted. DABs provision; the Workbench app performs everyday create/edit and Databricks Jobs perform governed operations, all through one shared version-capture and coordination gate; Workbench governs history, approvals, and promotion; Delta is the durable and coordination authority; Genie read-back proves live state.

## Security Model

- **Capture-on-open in production needs a trusted reader.** Exporting `serialized_space` requires `include_serialized_space=true` and at least `CAN_EDIT`; production humans limited to `CAN_VIEW`/`CAN_RUN` cannot perform full capture through OBO alone. Assign production configuration reads to a trusted target-local service identity (which therefore holds edit-capable Genie permission), authorize history/snapshot viewing separately in Workbench, and do not elevate ordinary viewers merely to enable capture.
- Use separate observer and deployer service principals where practical; the deployer SP is strictly more powerful than any human user, so Jobs must re-verify requester authorization server-side.
- Grant the observer only the permissions required to retrieve tracked configurations.
- Grant the deployer update access only to managed target Spaces and required workspace folders.
- Continue using OBO identity for user-sensitive interactive operations.
- In production, reserve Genie editing to the executor SP (humans `CAN_RUN`/`CAN_VIEW`).
- Use OAuth M2M and governed credential storage; no Databricks PATs.
- Replace the startup `run_as` self-healing behavior with **fail-closed verification**: verify the configured run identity and refuse to start if it is wrong. Self-healing identity is self-healing authority.
- Restrict access to raw snapshots because they may contain business logic, SQL, and metadata; ensure snapshots contain no credentials or secrets.
- Record whether an actor was a human OBO user, Workbench service principal, optimization principal, or deployment principal.
- Require explicit, audited break-glass approval to override drift gates.

## Key Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Stale authorized executor writes after takeover | Generation + unique attempt-id fence; expiry quarantines; terminal Job-run signal for exit |
| Ambiguous PATCH outcome retried destructively | No replay on base-equality; `applied_unverified`/`applied_partial` as terminal; two-signal quarantine clearance |
| Direct UI edit races with restore or deployment | Pre/post reads, generation fence, expected-base checks, conflict state, ACL lockdown where enforceable |
| API normalizes submitted JSON | Treat only API read-back as authoritative; versioned canonicalizer |
| Dual authority via bundle-managed Genie content | `governed_by=workbench` + reconciler detection; ownership enforced at sanctioned deploy; treat `--watch --force` as a detection target |
| Canonicalizer changes create false drift | Version canonicalizers, retain raw observations, dual-compute on migration, mismatch -> `unknown` |
| Catalog mapping changes SQL literals or instructions | Structured schema-aware mapping, exact identifier replacement, post-render source-identifier scan, fail closed |
| Promotion overwrites legitimate target drift | Require the expected target base fingerprint; block on unexplained drift |
| Approval does not match rendered deployment | Bind approval to artifact, mapping, transformer, target, base, and policy digests; executor recomputes |
| Source-only approval is a governance illusion | Require at least one target-side approver resolved by the target SP at execution |
| Quarantine backlog (crashed worker parks a space) | Operable two-signal auto-clear; page only on genuine ambiguity |
| Coordination authority outage | Keep history in Delta; fail closed on all governed mutations; never fall back to memory |
| Initial implementation becomes too broad | Start with local capture, diff, restore, and reconciliation before cross-workspace promotion |

## Phased Implementation Plan

### Phase 1: Trustworthy In-Workspace Versions

- Extract shared canonicalization and fingerprint services.
- Add metadata fingerprinting.
- Create `genie_space_versions` and `genie_space_registry`.
- **Phase 1 is observation-only:** record versions from reads (including capture-on-open, which realigns only the observed head) without enabling any governed write path yet.
- Add history and semantic comparison APIs.
- Add initial version-history UI plus overview-page version indicators and drift badges.

### Phase 2: Guarded Mutation and Coordination (first write-enabled milestone)

- Create `genie_space_operations` and `genie_ops_coordination`. **No write path is enabled until coordination, mandatory pre-write drift checks, and fail-closed behavior exist.**
- Implement Delta compare-and-swap admission (serialized enrollment, unique row, generation, atomic write-authorizing transition binding already-committed preimage evidence).
- Route every mutation through the shared version-capture + coordination gate: everyday Workbench create/edit in-app, and governed operations (optimizer champion apply, restore, promotion) through Jobs; retire in-process `revert.py`.
- Add authoritative read-back verification, `applied_unverified`/`applied_partial` handling, and quarantine with the two-signal exit.
- Replace `run_as` startup self-healing with fail-closed verification.

### Phase 3: External Drift Reconciliation

- Add a scheduled observation Job.
- Reconcile before managed writes and on user refresh.
- Track observed, approved, and deployed heads separately.
- Add drift classification, the dual-authority guard, and notifications.
- Implement adopt, reapply, compare, and manual resolution workflows, surfaced in the agent-page reconcile panel.

### Phase 4: Native Approvals

- Add Workbench approval records bound to immutable digests.
- Implement requester/approver/deployer separation, two-person production approval, single-use CAS consumption, expiry, and break-glass.
- Correlate approval and operation facts with `system.access.audit` evidence.

### Phase 5: Governed Cross-Workspace Promotion

- Add DAB-provisioned target-local deployment Jobs and least-privilege target SPs.
- Add content-addressed UC Volume packages (same metastore) or Delta Sharing (across metastores) and receipts.
- Add environment mapping, target preflight, target drift gates, and target-side approval.
- Apply, read back, test, and record deployment evidence with guarded compensation.

### Phase 5b: Workbench Front-End

- Build the agent-page "Version Control and Promotion" tab: history timeline, semantic diff, restore/forward, reconcile, validate, promote, and approvals/audit.
- Wire overview-page version indicators and drift badges to the projected heads with on-demand refresh.

### Phase 6: Hardening

- Add reconciliation lag, failure, and ambiguity metrics.
- Add bounded concurrency, pagination, backoff, and per-Space failure isolation.
- Add retention policies, recovery drills, and compliance exports.
- Consider Lakebase as a transactional coordination backend where its enforced keys and multi-record transactions add value — never as an automatic failover or cache-only afterthought.

## First Milestone Acceptance Criteria

The first milestone demonstrates durable pre/post history, arbitrary-version restore, drift blocking, duplicate suppression, outage fail-closed behavior, and crash/timeout quarantine. Specifically:

- **Concurrency:** two concurrent governed writers on one `space_key` produce exactly one admitted mutation sequence (which may legitimately contain both a config and a description PATCH) plus one `conflicted`/`quarantined` row — never two independent overwrites.
- **Dual authority:** a `bundle deploy` of a `governed_by=workbench` space is detected and flagged within one reconcile cycle.
- **Fail-closed:** when the coordination authority is unavailable, all version-changing operations stop; history browsing continues when only coordination writes fail, though a shared Delta/UC outage may also affect reads.
- **Recovery:** an ambiguous PATCH outcome results in quarantine, not a blind retry, and clears only on trusted-termination + stability signals (or audited human action until the request-lifetime bound is verified).
- **Failure coverage:** description-PATCH timeout, app-worker crash mid-write, lost create response, concurrent opens, duplicate enrollment, historical approval reuse, and crashes between the evidence, CAS, and PATCH stages are each exercised.

## Open Questions

- What is Genie's maximum server-side request lifetime (needed to size the quarantine stability window)?
- Does the Genie API expose a reliable conditional-update token or ETag in any supported workspace?
- Which top-level Space fields are consistently updateable and should be included in `metadata_fingerprint`?
- Can system audit tables reliably identify direct Genie UI/API actors and operations?
- What is the level of support for Delta multi-table transactions in the target metastore, and does admission need to stay single-row?
- Does Databricks-to-Databricks Delta Sharing support UC Volumes in the specific metastore/region topology in use?
- Is there a Genie edit-without-manage grant so target-side approvers can be authorized without `CAN_MANAGE`?
- What maximum reconciliation lag is acceptable by environment?
- Should production allow direct UI editing, or should policy restrict it operationally?
- Should raw snapshots be retained indefinitely, encrypted separately, or subject to a shorter retention policy?
- Which benchmark threshold and smoke questions are mandatory for promotion?
- How should ACLs be represented: in the portable artifact, a separate policy artifact, or infrastructure configuration?

## Related Decision Record

See [ADR-001: API-Observed Versioning and Governed Promotion](ADR-001-api-observed-versioning.md).
