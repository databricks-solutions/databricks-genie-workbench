# CUJ 1 — Per-Agent Version Management

**Status:** Draft (locked scope)
**Last updated:** September 11, 2026
**Scope:** Per-agent, single-workspace only. Cross-workspace promotion is a separate journey — see [CUJ 2](cuj-2-cross-workspace-promotion.md).
**Architecture reference:** [README](README.md) · [ADR-001](ADR-001-api-observed-versioning.md)

## 1. Purpose

Give a user working on one Genie Agent a coherent, in-workspace surface to:

1. **See** the agent's captured configuration history (manual captures + the before/after
   snapshots around optimizer runs).
2. **Understand** what changed (inspect a version; diff two versions).
3. **Act** in-workspace — **capture** the current live state on demand, and **publish**
   (restore / roll-forward) a chosen version as a new reviewed write.

"Publish" in this CUJ means **restore/roll-forward within the same workspace** — making a
chosen historical (or newer captured) version the live configuration of *this* agent. It
never crosses a workspace boundary. All dev→prod movement lives in CUJ 2.

## 2. Surface

`SpaceDetail → Version Control` tab, presented as a segmented control with two sub-tabs:

```
Version Control
├─ Versions   ← this CUJ (default)
└─ Promote    ← CUJ 2 (cross-workspace)
```

Rationale for sub-tabs (not a modal/drawer): version history and promotion both have
durable, resumable state a user returns to; a persistent surface fits better than a
transient dialog. Keeping both under one per-agent tab preserves shared context (the same
binding; the version a user selects here becomes the `source_version_id` in CUJ 2).

## 3. Current state (what is built today)

| Capability | Backend | Status in running app |
|---|---|---|
| List versions | `GET /api/version-control/spaces/{id}/versions` → `ledger.history` | **Mounted, live** (native) |
| Capture current state | `POST /api/version-control/spaces/{id}/observe` → `Observer.capture_on_open` | **Mounted, live** (native) |
| Version detail | `GET /api/version-control/bindings/{id}/versions/{versionId}` → `ledger.get_version` | **Mounted, live** (native) |
| Semantic diff | `GET /api/version-control/bindings/{id}/diff` → `canonical_diff.py` + `SemanticDiffView` | **Mounted, live** (native) |
| Publish (= in-workspace restore) | `POST /api/version-control/spaces/{id}/restore` → `restore_local.restore_space_version` | **Mounted, live** (native) — simple OBO path |
| Deployment flags | `GET /api/version-control/config` | **Mounted, live** (reports capabilities; always on when the surface is wired) |

CUJ-1 is a **native, baked-in feature**: history, capture, and in-workspace restore are
live whenever the observe surface is wired (all `VC_OBSERVE_*` resolved). There is no
per-capability switch to turn them off — `app_observe.py` hardwires
`vc_history`/`vc_writes`/`vc_restore` on for the app surface. The `VC_*_ENABLED` feature
switches now govern only the CUJ-2+ **governed** writes (promotion / optimizer-apply /
reconcile), which stay fail-closed.

The **governed** restore path (`POST .../restore` with `RestoreCommand` → mutation gate /
approvals / Job) remains present but **dormant** (`FailClosedRestore()`), reserved for
regulated / shared-production needs. CUJ 1 ships the lightweight in-workspace restore
below.

The tab shell (`SpaceVersionControlTab.tsx`) is already on the design system; the body
(`history.tsx`) is a raw-HTML prototype and is the visual debt to replace.

## 4. The journey

### 4.1 View history (read; always available when VC is enabled)

A timeline/list of versions, newest first, paginated. Each row:

- **Short, copyable version id** (monospace), full id on hover.
- **Origin badge**, color-coded: `manual`, `optimizer`, `restore`, `promotion`, `external`.
- **Relative time** ("2h ago") with absolute timestamp on hover.
- **Actor**, resolved to a friendly label (the app SP GUID renders as
  "Workbench service principal", not `20d58fdb-…`).
- **Provenance chips** only when present: optimizer-run link + champion (`optimizer_run_id`,
  `champion_id`); lineage (`parent_version_id`, `restored_from_version_id`) shown only when
  non-null.
- **Optimizer before/after pairs** are visually grouped (a bracket/connector), because the
  headline value is "what the optimizer changed."

### 4.2 Capture current state (governed write; passive + on-demand)

- A **Capture** action reads live state and appends a new version **only if fingerprints
  changed** ("No change since the last captured version" otherwise).
- **Origin semantics fix:** a user-initiated capture must surface as `origin=manual`, not
  `external`. `external` is reserved for drift observed by capture-on-open. (This is a
  labeling change from today's behavior, where manual capture records `external`.)
- Always available when the VC surface is wired (native — no capture switch).

### 4.3 Inspect a version (read)

Row → detail: fingerprints (config / benchmark / metadata), canonicalizer version,
provenance, lineage, and the restorable snapshot. Requires mounting a `get_version` detail
route.

### 4.4 Diff two versions (read)

Pick A and B → categorized, field-level diff (data sources, columns, instructions, joins,
filters, example SQL, benchmarks, metadata) rather than raw JSON. Engine (`canonical_diff`)
and view (`SemanticDiffView`) exist; needs a mounted diff endpoint. **Open:** ship now vs
defer (see §7).

### 4.5 Publish = in-workspace restore / roll-forward (governed write)

Publishing a selected version applies it to *this* agent as a **new reviewed write**,
never rewinding history. This CUJ ships the **simple in-workspace path**
(`restore_local.restore_space_version`): the workbench writes the stored snapshot back to
the live space **as the logged-in user (OBO)** — the same trust model the Create Agent
uses — guarded by an optimistic concurrency check, then records the result:

1. Show the current→selected semantic diff (reuses the diff endpoint).
2. Guard: canonicalize the live space and require it to still equal the version the user
   believed was current (`expected_current_version_id`); if it moved, refuse with a 409
   ("the space changed — refresh and retry") rather than clobbering the change.
3. `POST /api/version-control/spaces/{id}/restore {version_id, expected_current_version_id}`
   PATCHes `serialized_space` (+ description) via the OBO client — the user's own edit
   rights are the authorization (a non-editor is rejected by the API; no SP fallback).
4. Record the resulting state as a new `origin=restore` version linked by
   `restored_from_version_id` (via `Observer.capture`, which advances the observed head).

Restore is native — always available when the VC surface is wired. `GET .../config`
still reports capabilities so the UI keeps a defensive disabled-with-explainer state, but
in a wired deployment it always resolves enabled.

What the simple path trades away vs. the dormant **governed** gate (coordination lease,
two-person approval, target-local Job, durable audit — see the [README](README.md)) is
concurrency/approval/audit strength; the `expected_current_version_id` guard covers the
clobber risk, which is the material concern for a human restoring their own space. The
governed gate stays available to switch on for regulated / shared-production spaces.

## 5. States (must all be designed, not just the happy path)

- **VC surface not wired on this deployment** (`VC_OBSERVE_*` unset → no routes mounted) —
  distinct empty state (not "no versions").
- **Agent not enrolled yet** — distinct empty state.
- **No captures yet** — distinct empty state with a Capture call-to-action.
- **Loading** — skeleton rows, not blank.
- **Capturing** — inline status; success ("captured") vs no-op ("no change") notices.
- **Error** — inline alert (fail-closed; history shown stale, actions disabled).
- **Restore disabled** — explainer, not a dead button.

## 6. New vs reuse (scope for implementation)

- **Reuse (live):** versions list + capture endpoints; `ledger`, `Observer`,
  `canonical_diff`; `Observer.capture` (extended with `origin` + `restored_from_version_id`).
- **Built:** version-detail route; diff endpoint; the origin labeling fix (`workbench`
  for user captures, `optimizer` for run pairs, `restore` for restores); the redesigned
  `history.tsx` + `Versions` sub-tab; the simple in-workspace restore
  (`restore_local.py` + `POST .../restore`) and the `GET .../config` flag surface.
- **Dormant (opt-in):** the governed restore path (mutation gate / approvals / Job) stays
  fail-closed behind `FailClosedRestore()` for regulated / shared-production needs.

## 7. Resolved questions

1. **Diff:** shipped now (the engine existed; the read surface is low-risk).
2. **Origin relabel:** done as a **backend change at capture time** — the recorded
   `origin` is authoritative (`WORKBENCH`/`OPTIMIZER`/`RESTORE`), not a display mapping.
   (The §4.1 badge label "manual" is the `workbench` origin — the code/UI use "Workbench".)
3. **Publish/restore:** shipped as the **simple in-workspace OBO path** (§4.5), native
   (on whenever the VC surface is wired) with a defensive disabled-with-explainer
   fallback. The governed gate is left dormant for later opt-in rather than being the
   CUJ-1 path.

## 8. Non-goals

- No cross-workspace movement (CUJ 2).
- No account-wide / all-agents overview surface (both CUJs are per-agent).
- No automatic semantic merge.
