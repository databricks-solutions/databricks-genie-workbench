# ADR-001: API-Observed Versioning and Governed Promotion

**Status:** Proposed
**Date:** September 6, 2026

## Context

Databricks Genie Agents are mutable workspace resources. They may be changed through Genie Workbench, Genie Space Optimizer, the Databricks UI, direct APIs, or automation.

This design is **fully Databricks-native**. Operational version control, approvals, versions, deployments, and reconciliation are managed inside Genie Workbench and governed Databricks resources. Git and GitHub may store the application source code and documentation, but they must not be the authoritative version ledger for Genie Agents, the deployment-approval system, or a required part of the operational promotion path. No workflow may force a user to leave Genie Workbench to approve, restore, reconcile, or promote a Genie Agent.

Databricks Declarative Automation Bundles can now express `resources.genie_spaces`, but managing live Genie content through a bundle is rejected on authority grounds: bundle state would re-assert stale configuration on the next deploy, and `bundle deploy` centralizes cross-workspace credentials in a CLI act outside Workbench.

The Genie API may normalize submitted configuration and exposes **no ETag, If-Match, or conditional-update token**. A successful update request or submitted JSON document therefore does not prove that the same representation is currently live, and server-side optimistic concurrency is unavailable. Authoritative state requires a follow-up Genie API read, and concurrency safety must be built in this control plane.

The organization requires two distinct capabilities:

1. Recoverable and inspectable version history for live Genie Agents, including externally observed changes.
2. Governed promotion of approved configurations between workspaces.

## Decision

1. Assign each managed Genie Agent a stable logical `space_key`, recorded in a durable `genie_space_registry`. Genie spaces are not Unity Catalog securables, so the registry is the identity; a deleted-and-recreated space requires an explicit, audited human rebind.
2. Store every managed pre-mutation and post-mutation state, and every externally observed state, as an immutable version in governed Unity Catalog Delta tables. **Every write path initiates version control through the same gate** — including the Workbench create-agent flow (which registers the identity and persists the first version) and everyday in-app edits — and no write bypasses version capture.
3. Treat authoritative Genie API read-back as evidence of live applied state.
4. Preserve four representations where appropriate:
   - Complete API response envelope for audit.
   - Exact restorable state for same-workspace recovery.
   - Canonical state and versioned fingerprints for semantic comparison.
   - Portable artifacts for governed cross-workspace promotion.
5. Extend the repository's existing configuration and benchmark fingerprint logic and add governed metadata fingerprinting.
6. Store durable operation and deployment transitions in Unity Catalog Delta.
7. Use **Unity Catalog Delta as the coordination authority for the first release**, through a single mutable per-binding compare-and-swap row (`genie_ops_coordination`) under Serializable isolation, providing idempotency, generation-fenced leases, current saga state, and observed/approved/deployed head projections. Lakebase is **not required for v1**; it is a legitimate future coordination backend (enforced keys, multi-record transactions), never merely a cache and never an automatic failover target.
8. **Fail closed when the coordination authority or the mandatory pre-write evidence is unavailable.** Do not assume a cross-table transaction: first commit the immutable preimage evidence to the version table, then perform one write-authorizing compare-and-swap on the coordination row that binds that already-committed evidence (its version id/digest), the operation, generation, and consumed approval. No PATCH precedes that CAS. Stop restore, deployment, adoption, merge, Workbench mutation, and optimizer apply when durable coordination or the pre-write evidence is unavailable. In-memory fallback must not authorize these operations, and coordination backends must never be switched automatically.
9. Do not use Git or GitHub as the Genie Agent version ledger or the operational promotion path. Store approved portable artifacts and mappings as content-addressed packages in Unity Catalog Volumes governed by UC grants.
10. Maintain observed, approved, and deployed heads separately; never compress them into a single "current version."
11. Capture and persist unexpected live state before asking a user to adopt, reapply, compare, acknowledge, or manually reconcile it. In particular, when a user opens a Genie Agent in Workbench, read the live state and, if it differs from the recorded head, persist it immediately as a new `origin: external` version to realign to the source of truth **before** presenting any choice. Preserving external state must never depend on a user decision.
12. Implement approvals inside Genie Workbench, bound to immutable digests, with requester/approver/deployer separation, two-person production approval, and group membership resolved server-side. There is no external environment gate.
13. Execute everyday Workbench mutations (create-agent, edit) in the application as the user, wrapped in the shared version-capture and coordination gate; execute high-stakes governed operations (restore, production change, cross-workspace promotion, reconciliation, optimizer apply) through DAB-provisioned, target-local Databricks Jobs for separation of duties, retries, authoritative read-back, tests, and deployment evidence. All paths use the same discipline: acquire the lease, capture the pre-state, apply, read back, capture the post-state.
14. Use DABs to provision infrastructure and workflows only, never as the authoritative Genie Agent content manager. Mark managed spaces `governed_by=workbench` and detect any governed space that also appears in bundle state (the dual-authority guard).
15. Bind production approval to at least: source version ID, raw and canonical source fingerprints, portable artifact fingerprint, mapping fingerprint, transformer version, canonicalizer version, target workspace, target agent binding, expected target-base fingerprint, validation policy, benchmark policy, approver identity, approval timestamp, and expiration. Changing any bound input voids the approval.
16. Implement restore and deployment as idempotent operations with explicit ambiguous, conflict, and quarantine states.
17. Treat historical restore as a verified compensating operation, not an atomic rollback.
18. Block governed mutations on unexpected live drift unless an authorized user adopts or explicitly overrides the drift through audited break-glass.
19. Use structured and exact-identifier environment mappings. Fail closed on ambiguous transformations rather than globally replacing arbitrary text.
20. Implement reliable in-workspace capture, comparison, reconciliation, and restore before building cross-workspace promotion automation.
21. Enforce the safety invariant **one unresolved mutation attempt per binding — not merely one unexpired lease**: serialized enrollment with a unique per-binding coordination row, monotonic generation with a unique per-attempt identity (not the shared service-principal identity), and a single write-authorizing admission transition (at Serializable isolation) binding operation, separate idempotency key and request digest, consumed approval, committed preimage id, expected base, and generation. Completed-operation and consumed-approval facts are durable in append-only records so they survive reuse of the mutable coordination row; an approval CAS-binds to exactly one operation and its consumption is published before row reuse.
22. Treat an expired lease as suspected failure, not permission to take over: expiry quarantines the binding. Quarantine auto-clears only on trusted termination evidence for the *exact* prior attempt — a terminal Job execution (including retries/children) for Job-executed operations, or a specified app-worker termination mechanism for everyday Workbench writes; lease expiry or a missing heartbeat is not termination proof — followed by live-state stability across repeated reads spanning longer than Genie's maximum server-side request lifetime, measured after established termination. The request-lifetime bound is a release prerequisite: until it is verified, automatic clearance is unavailable and quarantine clears only by audited human action. Never authorize replay from base-equality alone, and a matching read-back after a timeout does not resolve the in-flight request. Never auto-compensate unless preauthorized and live state still matches the recorded post-stage.
23. Reserve production Genie editing to the executor service principal (humans receive `CAN_RUN`/`CAN_VIEW`) as the one native prevention mechanism, and replace startup `run_as` self-healing with fail-closed verification.
24. Require at least one production approver to be authorized target-side, evaluated by the target service principal against a target-controlled release-approver group at execution time. A source-workspace-only approval is insufficient.
25. Capture only the Genie Space Optimizer's **final applied champion** as a version (`origin: optimizer`, with `optimizer_run_id` and `champion_id`); never promote intermediate iterations or unapplied candidates into the version ledger. The optimizer's own iteration and champion telemetry remains separate. One optimizer run yields at most one new version.
26. Deliver the experience through Workbench-native front-end surfaces: overview-page version indicators and drift badges computed from the projected heads, and a dedicated agent-page "Version Control and Promotion" tab for history, semantic diff, restore/forward, reconcile, validate, promote, and approvals/audit. These surfaces are read-mostly projections of the durable ledger; any mutation they trigger passes through the shared version-capture and coordination gate.

## Consequences

### Positive

- Direct UI or API changes are not silently overwritten after they have been observed.
- Every managed mutation has a durable preimage and verified post-operation observation.
- Historical restore preserves lineage rather than rewriting history.
- All operational governance stays inside Databricks and Workbench; there is no external CI/CD control plane to secure or correlate.
- Target deployments are verified against actual target state and require target-side approval.
- Deployment evidence includes intended, rendered, and observed identities.
- DAB limitations do not constrain the version model, and bundle-managed content cannot silently overwrite governed spaces undetected.
- The v1 coordination authority is a single system (Delta), so command admission and pre-write evidence are one commit.

### Negative

- The system coordinates Genie API operations, Delta, and Databricks Jobs without a single distributed transaction; multi-table writes require a deliberately checkpointed protocol.
- Restore and deployment cannot be described as strictly atomic.
- Scheduled reconciliation cannot recover external intermediate states that are never observed.
- Versioned canonicalization, idempotency, reconciliation, and recovery add implementation complexity.
- Concurrency safety depends on a control-plane protocol rather than a server-side conditional update, so an irreducible time-of-check-to-time-of-use window remains outside ACL-locked production.

### Neutral

- Direct UI editing remains technically possible unless separate organizational policy or production ACL lockdown restricts it.
- Existing optimizer history remains optimization-specific and should not become the universal version ledger.
- General automatic semantic merging is deferred. SQL-bearing and overlapping changes require human review.
- Lakebase may later replace Delta coordination where its transactional features add value, but only as a deliberate, non-automatic migration.

## Alternatives Considered

### DAB-first (bundle-managed Genie content)

Rejected as the primary mechanism. `resources.genie_spaces` exists, so this is a capability, not an impossibility — but bundle state re-asserts stale configuration on the next deploy, destroying out-of-band-edit preservation, and `bundle deploy` centralizes cross-workspace credentials in a CLI act outside Workbench. It fails the out-of-band-preservation and no-leave-Workbench requirements.

### Git and GitHub Actions as the governance plane

Rejected. Using GitHub Actions for release orchestration, GitHub environments for deployment approval, or Git as the authoritative version ledger violates the fully-Databricks-native constraint, forces users out of Workbench to approve and promote, and cannot observe out-of-band live changes. Git and GitHub are retained only for application source code and documentation.

### Workbench SDK-only

Rejected as the complete architecture only because provisioning belongs in DABs. Hand-rolling per-workspace Job creation, schema, grants, and entitlements through the SDK produces drift in the machinery that detects drift. Once provisioning is factored into DABs, this collapses into the native hybrid.

### Native hybrid

Accepted. DABs provision; the Workbench app performs everyday create/edit mutations and Databricks Jobs perform governed operations, all through one shared version-capture and coordination gate; Genie Workbench governs history, approvals, and promotion; Unity Catalog Delta is the durable and coordination authority; Genie API read-back proves live state.

## Implementation Constraints

- Start with `genie_space_versions` and `genie_space_registry`, then `genie_space_operations` and `genie_ops_coordination`; add specialized ledgers only when needed. Physical table count is an implementation choice, but mutable coordination must not share a securable with append-only facts, and approval and release facts must be durable in Delta.
- Enforce append-only via grants and Delta table properties/constraints, not naming convention.
- Use Databricks Jobs rather than any second workflow engine as the execution queue and retry engine.
- Fetch live state before every governed write even when a current-head projection exists.
- Persist the pre-operation observation before submitting a mutation, in the same admission commit.
- When a write returns but read-back fails, transition to `applied_unverified`, quarantine, retry verification only, and block further governed mutation until reconciled.
- Do not implement a universal SQL rewrite engine in the first release.
- Do not use unrestricted string replacement across `serialized_space`.
- Do not claim that every transient direct UI edit can be recovered.
- Route every mutation through the shared version-capture + coordination gate. Everyday Workbench create/edit run in-app; governed operations (optimizer apply, restore, promotion, reconciliation) run through Jobs. Retire the in-process, non-atomic `revert.py`.
- Require explicit user-selected source and target CLI profiles; never auto-select or rely on a default profile for production promotion.

## Follow-Up Decisions

- Snapshot retention and access policy.
- ACL representation and promotion strategy.
- Reconciliation schedules and service-level objectives.
- Production direct-edit policy and ACL lockdown.
- Canonicalizer migration policy.
- Benchmark and smoke-test promotion thresholds.
- Whether and when to adopt Lakebase as a transactional coordination backend.
- Retention and linkage policy between optimizer telemetry and the version ledger.
- Front-end refresh strategy for drift badges (projection cadence vs. on-demand live checks) at scale.
