# Debby Handoff: Genie Agent Version Control Debate

**Prepared:** September 6, 2026
**Updated:** September 6, 2026 — reflects the completed two-round debate and the fully Databricks-native decision.

## Repository

```text
databricks-genie-workbench
```

Branch:

```text
version-control-cicd
```

## Documents to Review

- `docs/design/version_control/README.md`
- `docs/design/version_control/ADR-001-api-observed-versioning.md`

## Current Decision (Post-Debate)

The design is **fully Databricks-native**. No GitHub Actions, no GitHub environments as a deployment-approval system, no external CI/CD runner, no external release control plane, and no Git as the authoritative version ledger. Git and GitHub retain the application source code and documentation only; a user never leaves Genie Workbench to approve, restore, reconcile, or promote a Genie Agent.

- Adopt the **native hybrid** architecture: DABs provision only; Databricks Jobs are the single guarded mutation path; Genie Workbench is the sole control plane and UI; Unity Catalog Delta is the durable and v1 coordination authority; Genie API read-back proves live state.
- Use API-observed state rather than Git as the authoritative live version source.
- Store durable versions, operations, registry/bindings, approvals, and receipts in append-only Unity Catalog Delta tables; store packages and large evidence in content-addressed UC Volumes.
- Use Unity Catalog Delta as the v1 coordination authority through a single mutable per-binding compare-and-swap row. **Lakebase is not required for v1**; it is a legitimate future coordination backend, never a cache-only afterthought and never an automatic failover target.
- Fail governed mutations closed when the coordination authority or the mandatory pre-write evidence commit is unavailable; in v1 these are one Delta commit. Never fall back to in-memory authorization; never switch backends automatically.
- Enforce the safety invariant **one unresolved mutation attempt per binding — not merely one unexpired lease**: unique per-binding row, monotonic generation with a unique per-attempt identity, atomic admission transition, expiry quarantines (no takeover), and an operable two-signal quarantine exit.
- Reserve production Genie editing to the executor service principal (humans `CAN_RUN`/`CAN_VIEW`) as the one native prevention mechanism, and replace startup `run_as` self-healing with fail-closed verification.
- Promote pull-based to pre-enrolled targets: immutable version → portable package → immutable mapping → target-local preflight → approval → target-local execution → receipt. Require at least one target-side approver resolved by the target SP at execution.
- Use target-local Databricks Jobs for validation, deployment, authoritative read-back, and testing.
- Use DABs to provision the machinery only; mark managed spaces `governed_by=workbench` and detect bundle-managed Genie content (the dual-authority guard).
- Implement local snapshot, diff, restore, and external reconciliation before cross-workspace deployment.

## Debate Outcome Summary

The two-round debate (Claude Opus vs. GPT astra) converged on the native hybrid with Delta-only v1 coordination and Lakebase deferred. Key converged points:

- Delta optimistic concurrency on a single per-binding row is a genuine compare-and-swap, so Lakebase is not required for writable v1.
- Genie space ACLs are the mechanism for exclusive routine write access; they remove human competitors but not stale authorized executors.
- Expired lease must quarantine, not take over; ambiguous PATCH outcomes must not replay from base-equality.
- Executors receive an operation id and fetch the approved payload themselves.
- Four logical grant-domains (versions, operations, registry, coordination); physical table count is an implementation choice.
- Target authority is a security requirement; a second Workbench instance is not.

Remaining, narrow disagreements recorded for implementation:

1. Whether the description PATCH completes conditionally or is completed with record-don't-block semantics.
2. Whether target `CAN_MANAGE` specifically is required, versus a target-controlled release-approver group.
3. How strongly the reconciler can detect every competing bundle path.
4. Precise conditions for auto-clearing quarantine.
5. Ensuring per-workspace serialization never becomes load-bearing for correctness.

## Questions Requiring Further Validation

- What is Genie's maximum server-side request lifetime (needed to size the quarantine stability window)?
- Does the Genie API expose an update token, ETag, or other conditional-write mechanism in any supported workspace?
- What is the support level for Delta multi-table transactions in the target metastore, and must admission stay single-row?
- Does Databricks-to-Databricks Delta Sharing support UC Volumes in the metastore/region topology in use?
- Is there a Genie edit-without-manage grant so target-side approvers can be authorized without `CAN_MANAGE`?
- Which fields belong in restorable state versus environment-specific bindings?
- How should generated Genie IDs be matched across versions without producing noisy diffs?
- Can audit logs reliably attribute external Genie changes to a user and operation?
- What reconciliation interval is operationally and financially acceptable?
- Which changes can ever be automatically merged safely?
- Should production direct editing be prohibited by policy even though reconciliation supports it?
- What evidence should a deployment receipt retain for governance and rollback?
