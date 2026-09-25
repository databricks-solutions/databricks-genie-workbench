# CUJ 2 — Per-Agent Cross-Workspace Promotion

**Status:** Draft (locked scope)
**Last updated:** September 11, 2026
**Scope:** Per-agent. Promote a chosen version of *this* agent to another (prod) workspace
the signed-in user is entitled to, and always show what is currently live there.
In-workspace restore/publish is a separate journey — see [CUJ 1](cuj-1-version-management.md).
**Architecture reference:** [README](README.md) · [ADR-001](ADR-001-api-observed-versioning.md)

## 1. Purpose

From a single Genie Agent, let an entitled user promote a selected immutable source version
to a target (prod) workspace through the existing governed
`release → preflight → approval → promote → receipt` engine, and **coherently track which
version is live in prod right now**.

The promotion **engine** is built and was live-validated end-to-end across two real
workspaces (the D2.6 two-workspace smoke). This CUJ designs the **per-agent product
surface** and the **user-access targeting layer** the operator-authored topology does not
yet have.

## 2. Surface

`SpaceDetail → Version Control → Promote` sub-tab (the second segment of the
`Versions | Promote` control introduced in [CUJ 1](cuj-1-version-management.md)).

Layout, **one selected target at a time**:

```
Promote  ─────────────────────────────────────────────
 Target:  [ ▼ entitled target ]        (only targets the user may promote to)
 ┌── Currently in production (selected target) ──────┐
 │ Live: source vX  →  target version Y              │
 │ Promoted by U · 2026-09-10 · receipt r-123        │
 │ Status: confirmed   ·   Drift: none / DRIFTED     │
 └───────────────────────────────────────────────────┘
 [ Promote a version … ]        ← opens the stepper
 ── Promotion history (this agent → selected target) ──
 r-123 confirmed · r-118 failed · r-101 compensated …
```

A per-target selector (not an all-targets matrix) keeps the prod-state panel, drift, and
history unambiguous for the chosen destination. Targets are usually few; the selector
defaults to the most-recently-promoted target.

## 3. Access model — targets are gated by *user* entitlement (LOCKED)

**`selectable targets = operator-authored topology ∩ user entitlement`, fail-closed.**

1. **Candidate targets** come from the operator-authored promotion topology
   (`scripts/version_control/author_two_workspace_config.py`): each candidate is a target
   workspace + target space/binding + mapping + policy. Bounded and operator-controlled.
2. **Per-user entitlement = `CAN_MANAGE` on the target space** (LOCKED). Checked with the
   existing `user_can_manage_space(target_ws, target_space, user_email, user_groups,
   acl_client=<target SP>)` mechanism (already used by `mv_entitlement._space_manage_row`):
   a **target-workspace SP reads the target space ACL** and matches the promoting user's
   **account identity** (email + groups). This works cross-workspace without requiring the
   user's OBO token to be valid in the target.
3. **Verdicts** (borrowed from the `mv_entitlement` probe pattern):
   - `GRANTED` → target selectable.
   - `DENIED` → shown greyed with a "request access" affordance.
   - `UNKNOWN` (ACL unreadable) → **fail-closed** (hidden/blocked), never silently enabled.
4. **Governance is unchanged.** Entitlement only opens the door; the actual write is still
   gated by the existing **target-local two-person approval** (two distinct humans,
   target-group-eligible, neither requester nor deployer, ≤24h expiry). "User can initiate"
   ≠ "user can deploy alone."
5. **Re-verify at execution, downgrade-only** (mirror `mv_entitlement.verify`): re-probe
   the user's target entitlement immediately before `promote`. A revocation between
   selection and promote **blocks**; a stale verdict never authorizes the write.

Why probe the user and not the SP: the app SP is structurally the wrong identity to ask
"may this person promote?" — see the same reasoning in `mv_entitlement.py`. Entitlement is
the **user's** privilege in the **target**, resolved server-side.

## 4. Current-prod tracking — the coherence spine (LOCKED)

"Which version is live in prod right now" is **already durable** and does not need a new
store; it is a read projection over existing facts.

**Source of truth = the latest `CONFIRMED` `RECEIPT` fact** for `(this binding → selected
target)` (`promotion/receipts.py`):

- `source_version_id` — the version that was promoted (what you shipped).
- `post_version_id` + `observed_fingerprints` — the target-ledger version and what is
  actually running.
- `rendered_fingerprints` — what the promotion intended to write.
- `status`, `compensation_operation_id`, promoted-by, promoted-at, `release_id`.

Promotion also imports the pinned `source_version_id` into the **target-owned ledger** and
advances the **target binding's approved head** to it, so the receipt and the target head
agree after a confirmed promotion.

**Derived signals shown in the panel:**

- **Live version** = `source_version_id` of the latest confirmed receipt (empty state:
  "No version promoted to `<target>` yet").
- **Drift** = target's *current* observed fingerprints vs the receipt's
  `rendered_fingerprints` (prod changed out-of-band since promotion).
- **Promotion history** = the receipt/release list for this pair (confirmed / failed /
  compensated), each drillable to its audit trail.

This state is **target-side**, read via the target-workspace SP (same read path as the
entitlement ACL check). The only new backend piece is a **read projection**:
"prod head + history for `(binding, target)`" over receipts (+ target approved head).

## 5. The flow (stepper) → existing endpoints

| Step | UX | Endpoint (built) |
|---|---|---|
| 1. Source version | Prefilled from the `Versions` selection (`source_version_id`) | — |
| 2. Target + inputs | Choose an **entitled** target; mapping + policy | *(new: entitled-targets read)* |
| 3. Preflight / validate | Create release; run dependency + mapping probes | `POST /releases`, `POST /releases/{id}/validate` |
| 4. Approval | Request target-local approval; two reviewers vote | `POST /api/vc/approvals`, `.../votes`, `GET .../{id}` |
| 5. Promote | Atomic apply under the consumed approval | `POST /releases/{id}/promote` |
| 6. Receipt | Durable, verified receipt | `GET /releases/{id}/receipt` |
| Audit / recovery | Operation audit; break-glass | `GET /api/vc/operations/{id}[/audit]`, `POST /break-glass` |

On a **CONFIRMED** receipt, the "Currently in production" panel updates to the promoted
version.

## 6. States (design all, not just the happy path)

- **No entitled targets** — "You don't have promote access (`CAN_MANAGE`) to any configured
  target — request access."
- **Preflight failed** — show the probe/dependency evidence; do not advance the stepper.
- **Approval pending** — poll; show reviewers and "not yet approved"; requester/deployer
  excluded.
- **Promote in-flight** — status; no replay on ambiguous outcomes.
- **Receipt confirmed vs unverified** — never say "deployed" without a confirmed receipt.
- **Quarantined / unresolved** — surface operator evidence; no mutation replay.
- **Entitlement downgraded at execution** — block with the downgrade reason.

## 7. New vs reuse (scope for implementation)

- **Reuse (built + D2.6-validated):** the release / approval / receipt / compensation
  engine and its routers (`vc_releases`, `vc_approvals`, `vc_reconcile`, `vc_operations`);
  the `mv_entitlement` probe/verify pattern and `user_can_manage_space`; receipts as the
  durable prod-state record; the operator topology authoring scripts.
- **New:**
  1. Mount the governed routers behind `VC_PROMOTION_ENABLED`.
  2. **Entitled-targets read** — topology ∩ `CAN_MANAGE` (cross-workspace, target-SP ACL
     read), fail-closed.
  3. **Prod-head + history read projection** for `(binding, target)` (§4).
  4. **Execution-time re-verify** of user entitlement (downgrade-only).
  5. The real `Promote` UI — target selector + prod panel + stepper + history — replacing
     the `demoApi` demo panels.
  6. Wire the frontend promotion client off `demoApi` to the real `VersionControlApi`.

## 8. Open questions

1. Entitlement check timing: gate **target selection** only, or also **preflight
   initiation**? (Recommend: gate selection; re-verify at promote.)
2. Is `CAN_MANAGE` the sole promoter criterion, or is an additional release-policy group
   required to *initiate* (separate from the two approvers)?
3. Prod-head read cadence: on sub-tab open + on receipt confirm, or also a periodic drift
   refresh?
4. Cross-metastore transport (Delta Sharing) vs shared-metastore Volume — does the target
   topology affect how the prod-head projection is read back?

## 9. Non-goals

- No in-workspace restore/publish (that is [CUJ 1](cuj-1-version-management.md)).
- No account-wide overview across agents (per-agent only).
- No change to promotion governance semantics (approvals, receipts, compensation stay as
  built and validated).
