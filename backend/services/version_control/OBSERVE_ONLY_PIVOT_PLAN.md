# Observe-Only Optimizer Pivot — Execution Plan

**Status:** In progress (Step 1 complete) · **Branch:** `vc/observe-pivot` (off `vc/unblock`)
**Supersedes:** `packages/genie-space-optimizer/M10_IMPLEMENTATION.md` (M10 governed-optimizer path — to be removed)
**Target merge:** `feature/version-control-ci-cd`, based off `origin/feature/metric-view-advisor`

## 1. Decision

Stop *governing* the optimizer. Make the optimizer's code **pristine** (identical to
`feature/metric-view-advisor`), let it run its own self-healing flow, and have Version
Control **observe** it (snapshot before/after) for history + restore. **Keep**
cross-workspace promotion (M07 / D2.6) and the shared governance core it depends on.

Rationale: the optimizer is already self-safe — non-improving iterations are rolled back
(`unified_loop.py` calls `rollback(...)` at the current-config snapshot). The invasive
candidate-sandbox / champion-gate model (M10) required editing the optimizer's internals,
which is out of this feature's purview. An observe-only model keeps the optimizer untouched
and is a fraction of the remaining work, while promotion (the actual dev→prod safety
boundary) stays fully governed.

## 2. Principles / constraints

- **Do not touch optimizer surface.** End state: `packages/genie-space-optimizer/src` is
  byte-identical to `origin/feature/metric-view-advisor`.
- **No regression** to promotion (M07 / D2.6 stays green) or to optimizer behavior on
  `metric-view-advisor`.
- The optimizer is trusted (self-rollback); VC records but does not gate it.
  **Accepted tradeoff:** a crashed run can leave a space mid-changed — VC captures the
  result but does not prevent it. Governance is enforced at the *promotion* boundary.

## 3. Target architecture

- **Optimizer:** runs in dev, writes directly, unmodified. The MV-advisor `/trigger`
  features (`operator_guidance`, join-advice seeds, MV attach hook) return for free with
  the revert to base.
- **VC observe:** capture pre-run base + post-run result as immutable ledger versions
  (`origin=optimizer`, `optimizer_run_id` stamped) → history + restore.
- **Promotion:** unchanged — governed, approved, atomic, receipted (dev→prod).

## 4. Scope — REMOVE (optimizer-governance / M10 slice only)

- Delete `backend/services/version_control/optimizer_adapter.py` (isolated — nothing
  outside the optimizer and its own tests imports it).
- Delete `packages/genie-space-optimizer/src/genie_space_optimizer/integration/version_control.py`
  (candidate/champion model).
- Revert to base: the 5 guard files (`optimization/applier.py`, `optimization/unified_loop.py`,
  `optimization/preflight.py`, `optimization/space_quality_enrichment.py`,
  `common/genie_client.py`), the 2 job blockers (`jobs/run_optimize.py`,
  `jobs/run_benchmark_qc_and_repair.py`), and the 3 gutted integration files
  (`integration/apply.py`, `integration/discard.py`, `integration/revert.py`).
- Revert to base: `backend/routers/auto_optimize.py` + `backend/routers/create.py`
  (restore functional endpoints).
- Surgically remove the `optimizer_apply` handler/seam/kind-switch in
  `backend/jobs/__init__.py`, the `optimizer` field in `GovernedSeams`, and any
  optimizer-only type in `contracts.py`. (`platform/live_seams.py` already passes
  `optimizer=None`, so the live graph never used it — low risk.)
- Delete optimizer VC tests (candidate isolation/lifecycle, champion apply, revert guard,
  `tests/integration/test_vc_optimizer_platform.py`); revert the optimizer unit tests that
  were rewritten for candidate clients; revert `test_auto_optimize_router.py` /
  `test_create_agent.py` fail-close assertions to base.
- Delete `packages/genie-space-optimizer/M10_IMPLEMENTATION.md`.

## 5. Scope — KEEP (shared; promotion needs it)

`governance/`, `coordination/`, `mutation_gate.py`, `ledger.py`, `registry.py`,
`canonical_diff.py`, `contracts.py` (shared types), `observer.py`, `drift/`, `restore.py`,
all `promotion/`, `platform/` (minus the optimizer seam), `backend/jobs` (minus the
optimizer handler), `vc_mutations.py`, and the promotion scripts + **D2.6 fixtures**.

## 6. Scope — ADD (observe-only) — larger than "reuse observer.py"

The observe path is **not currently wired into the running app**:

- `append_observation` is gated: requires `writes_enabled=True` **and** the version +
  observer identity to belong to the **trusted target workspace** (`ledger.py`).
- `backend/main.py` composes `app.state.version_control` but does **not** mount the
  `vc_mutations` observe/restore router, and `spaces.py` history is the *scan* history
  (`get_score_history`), **not** the VC ledger. Nothing captures today.

So the add is a real (bounded) integration:

1. Compose a live observer (ledger + transport + coordination + identity + status_reader)
   as the **trusted target-workspace SP**, with the versions-table write path enabled.
2. **Capture-before** in the restored `/trigger` (records the base) and **capture-after**
   (see §11 open decision), stamping `origin=optimizer` + `optimizer_run_id`.
3. Expose a **history / restore read surface** (mount a read endpoint or extend `spaces`
   history) if the UI should show VC versions.
4. Gate behind `vc_history_enabled` (+ the ledger write flag) in the target config.

## 7. Non-goals (explicitly out of scope)

No M10 / candidate sandbox / governed optimizer apply. No change to promotion semantics.
No optimizer code changes beyond reverting to base. Not enabling any Genie-space *mutation*
governance for the optimizer.

## 8. Risks & regression areas + mitigations

- **R1 Dangling refs** after deleting the candidate model/adapter → grep must return zero
  for `assert_candidate_write` / `require_candidate_session_for_job` / `optimizer_adapter` /
  `integration.version_control`; offline tests + typecheck.
- **R2 Dual-touched files** (`applier`, `unified_loop`, `genie_client`, `run_optimize`):
  vc/unblock's edits there are purely guards, so `checkout base` is clean and loses no
  non-VC work. Confirm per-file diff before committing.
- **R3 Job-graph surgery** breaking promotion/restore → `live_seams` already `optimizer=None`;
  `test_vc_runtime_assembly` / `test_vc_composition` / `test_vc_governed_ports` must stay green.
- **R4 Test reconciliation** — fork fail-close tests will fail once endpoints are functional;
  revert them to base and delete candidate/champion tests.
- **R5 Observe wiring (highest)** — capture is a gated write with a trusted-identity
  requirement and isn't wired today; treat it as the main build item, not a snippet.
  Decide capture-after mechanism + identity + flag.
- **R6 Merge (positive)** — reverting optimizer files to base eliminates the
  `auto_optimize.py` + `unified_loop.py` merge conflicts; likely only `pyproject.toml`
  pytest markers remain.
- **R7 create-agent** — reverting restores functional create; confirm create-agent is
  intentionally *not* governed (recommended: yes — it creates a new space, not a
  managed-binding mutation).
- **R8 MV-advisor tests** — the base's `operator_guidance` 200-tests must pass against the
  restored functional router.
- **R9 Baseline reset** — record new green counts for backend (`./scripts/test.sh`) and
  GSO (`uv run pytest`).

## 9. Validation (prove no regression)

- `git diff origin/feature/metric-view-advisor -- packages/genie-space-optimizer/src` →
  **empty** (optimizer truly pristine).
- Grep-clean for all candidate/adapter symbols.
- Backend + GSO offline suites green; promotion tests (`test_vc_promotion*`,
  `test_vc_runtime_assembly`) green.
- Lint + typecheck on touched files.
- Merge dry-run into `feature/version-control-ci-cd` (off `metric-view-advisor`) → expect
  ≤1 trivial conflict.
- Live (nonprod): one optimizer run writes exactly a before + after version; one promotion
  smoke (D2.6 already validated).

## 10. Sequence

1. Work on a fresh `vc/observe-pivot` branch. **(done)**
2. Revert the optimizer slice (§4) → offline green + grep-clean.
3. De-wire the optimizer seam (jobs/seams/contracts) → promotion tests green.
4. Reconcile tests → full offline baseline green.
5. Build observe capture (§6) → offline tests for capture.
6. Live nonprod validation of capture (+ promotion smoke).
7. Seamless merge into `feature/version-control-ci-cd`.

## 11. Open decisions — RESOLVED

- **Capture-after mechanism:** **BOTH** — `capture_on_open` as the always-on passive
  baseline, plus job-poll of the optimizer run id for a tight before/after bracket.
- **UI:** **Include a history/restore read surface** — mount a read endpoint + a minimal
  history/restore UI tab (the fork already ships `frontend/src/components/version-control/`).
- `create-agent` stays functional / ungoverned. **(done — reverted to base.)**
- Flag + identity for capture: `vc_history_enabled` + ledger writes enabled, app SP as the
  trusted target identity.

## 12. Rough effort

Revert + de-wire + test reconcile + merge: ~2–3 days. Observe-capture wiring (composition +
hooks + flag + identity, optional history UI + live check): ~3–6 days. Total ≈ 1–1.5 weeks —
vs. ~3–4 weeks to finish M10.

## 13. Mechanism refinement (discovered during Step 2)

**Finding:** the fork branch (`vc/unblock` @ `8c3da097`) diverged from base at merge-base
`ae8c4367` and **never took base's 83 commits** — it lacks base's entire MV-advisor feature
(13 base-only backend files: `mv_create.py`, `mv_entitlement.py`, `mv_suggest.py`,
`join_advisor.py`, their tests; plus the optimizer-package MV modules `mv_advisor.py`,
`mv_attach.py`, …). Therefore a **file-by-file revert of the optimizer slice on the fork
branch** does not just "undo guards" — reverting `auto_optimize.py` to base pulls in base's
whole MV feature by hand (import cascade). That is fragile and effectively re-builds base's
branch manually.

**Conflict surface is tiny.** Only **10 files are dual-touched** (changed by both base and
fork since `ae8c4367`). Non-test dual-touched files:

| File | Resolution |
|---|---|
| `backend/routers/auto_optimize.py` | take **base** (functional trigger + MV features); re-add observe-before hook in Step 5 |
| `packages/.../optimization/unified_loop.py` | take **base** |
| `packages/.../optimization/applier.py` | take **base** |
| `packages/.../common/genie_client.py` | take **base** |
| `packages/.../jobs/run_optimize.py` | take **base** |
| `pyproject.toml` | combine (markers + addopts; both small/additive) |
| `databricks.yml` | combine (base +24 / fork +40, additive) |
| `scripts/deploy_lib/gso_job.py` | combine (base +13 / fork +3, additive) |
| `frontend/src/lib/api.ts` | combine (base +282 MV UI / fork +17 VC, disjoint sections) |

Everything else is a clean one-sided add: base's MV feature comes in automatically, and the
fork's VC-keep files (`backend/services/version_control/**`, `vc_*` routers/jobs) add cleanly.

**Refined mechanism (supersedes §10 steps 2–4 & 7):** construct the target by **merging fork
into a base-derived `feature/version-control-ci-cd`** with the resolution policy above,
instead of reverting file-by-file on the fork branch. This is why the earlier merge felt
non-seamless — it had no resolution policy for the optimizer `/trigger`; we now do
("take base for optimizer-governance; combine for VC-shared; add VC-keep").

**Refined sequence:**
1. `vc/observe-pivot` off `vc/unblock` holds this plan doc. **(done)**
2. Create `feature/version-control-ci-cd` off `origin/feature/metric-view-advisor`.
3. `git merge` the fork (`vc/unblock`) into it; resolve the 10 conflicts per the table.
4. **Post-merge cleanup** = the rest of §4's REMOVE that isn't covered by conflict
   resolution (these are fork-only adds the merge keeps): delete
   `backend/services/version_control/optimizer_adapter.py`,
   `packages/.../integration/version_control.py`, the GSO candidate/champion tests,
   `backend/tests/test_vc_optimizer_adapter.py`, `M10_IMPLEMENTATION.md`; de-wire the
   optimizer seam in `backend/jobs/__init__.py`; revert to base any fork-only GSO/router
   tests written for the candidate model (`test_auto_optimize_router.py`,
   `test_create_agent.py`, GSO `conftest.py` candidate fixture + rewritten unit tests).
5. Grep-clean + full offline green (backend + GSO).
6. Build observe capture (§6); live nonprod validation.
7. Branch **is** the target — open PR / land.

**Validation the merge is correct:** after resolution + cleanup,
`git diff origin/feature/metric-view-advisor -- packages/genie-space-optimizer/src` is
empty, and grep for `assert_candidate_write` / `require_candidate_session_for_job` /
`optimizer_adapter` / `integration.version_control` is zero.

## 14. Progress log

- **Steps 1–5 complete.** `feature/version-control-ci-cd` created off base and the fork
  merged in (`815c27e3`, 2-parent: base `923a78be` + fork `b9732a0d`). 226 files differ
  from base = the net VC surface.
- Resolutions applied per §13 table: optimizer package == base (empty diff verified);
  `auto_optimize.py`/`create.py` == base; `pyproject.toml` combined. Deleted
  `optimizer_adapter.py`, GSO `integration/version_control.py`, candidate/champion GSO
  tests, `M10_IMPLEMENTATION.md`, `test_vc_optimizer_adapter.py`. De-wired the optimizer
  seam (`jobs/__init__.py`, `live_seams.py`, `GovernedSeams.optimizer`,
  `contracts.OptimizerChampionAdapter` + its spec entry in
  `docs/design/.../contracts.md`). Reverted `test_auto_optimize_router.py` /
  `test_create_agent.py` to base; adjusted `test_vc_runtime_assembly.py` /
  `test_vc_governed_ports.py` for the removed optimizer handler.
- **Decision taken (minimal de-wire):** the inert `optimizer_apply` kind + flag +
  capability vocabulary is retained (reserved/unwired/fail-closed), matching the
  codebase's existing pattern for other not-yet-wired kinds. Full excision of that
  vocabulary is a possible follow-up.
- **Offline baseline:** `./scripts/test.sh` → **3237 passed**. Remaining 3 failed + 2
  errored are base's MV-advisor doc-parity GSO tests (`test_gap_report_counts`,
  `test_rules_parity`, `test_exposure_matrix`) that read **gitignored** local files
  (`docs/design/mv-advisor-{gap-report,playbook}.md`) absent in this clone — identical on
  base, not a regression.
- **Not yet done:** frontend build/lint check (`api.ts` auto-merged), Step 6 observe
  capture (blocked on §11 capture-after decision), Step 7 live nonprod validation + PR.
- **Open cleanup (optional):** the M10 design docs
  (`docs/design/version_control/implementation_plan/module-10-optimizer-integration.md`,
  `testing-strategy.md` optimizer_adapter refs) still describe the removed governed-
  optimizer model — decide whether to prune/annotate them.

## 15. Step 6 detailed build plan (post-reconnaissance)

Both the backend and frontend VC surfaces were mapped. Step 6 is a real platform
integration, larger than §6 first implied, and its live path is **deploy-only-validated**
(no local server). Offline we build wiring + fake-based tests; live capture is Step 7.

### 15.1 What already exists (fork-built, dormant)
- **Backend routers (unmounted):** `vc_mutations` (POST observe, POST restore),
  `vc_reconcile` (GET overview, GET status, POST reconcile), `vc_operations`
  (GET operation, audit), `vc_releases`, `vc_approvals`. `main.py` composes an empty,
  all-flags-off container and mounts none of them.
- **Capture/ledger:** `Observer.capture_on_open/capture`; `ledger.history()`/`get_version()`
  are reads (no write flag); `append_observation` needs `writes_enabled` **and** binding +
  observer identity in the trusted target workspace.
- **Enrollment:** `registry.enroll(EnrollmentRequest, actor)` creates a provisional binding
  for a `space_key` (a write; needs the M03 initializer + `writes_enabled`).
  `registry.resolve(binding_id)` reads it back. **Observing a space requires a binding.**
- **Frontend (demo-mocked):** `frontend/src/components/version-control/` has real
  presentational `History` + `RestorePanel` + `VersionControlTab`, a real
  `lib/version-control-api.ts` client, but everything defaults to `demoApi` (in-memory
  mock) and is only reachable via `?view=version-control`
  (`pages/VersionControlWorkbench.tsx`). Not on SpaceDetail/AdminDashboard.

### 15.2 Backend gaps to close
1. **Live compose** (`platform/` — new builder, fail-closed until configured, mirroring
   `live_seams.build_governed_seams`): construct Delta registry + ledger + coordination +
   canonicalizer + transport + identity + status_reader + Volume artifact adapters as the
   trusted target-workspace SP, register them into the `Composition`, `writes_enabled` +
   `vc_history_enabled` from config. Build an `Observer` + restore service from these.
2. **History-list endpoint** — the frontend calls `versions()`/`diff()` which have **no
   backend route**. Add `GET /api/version-control/bindings/{id}/versions` (→ `ledger.history`)
   and optionally `/versions/{vid}` + `/diff` (→ `get_version` + canonicalizer compare).
3. **Mount routers** in `main.py`, supplying ports from `app.state.version_control`, gated on
   `vc_history_enabled` (reads) and the write flags (observe/restore/enroll).
4. **`/trigger` hooks** in `auto_optimize.py`: on trigger, resolve-or-**enroll** the space →
   **capture-before**; then **capture-after** via BOTH `capture_on_open` (passive) and a
   job-poll of the optimizer `run_id`. Stamp `optimizer_run_id` (see next).
5. **Observer run-id stamping** — `Observer.capture` sets `Origin.EXTERNAL` and no
   `optimizer_run_id`; add a capture variant that accepts `optimizer_run_id`/`champion_id`
   so after-run versions carry provenance.
6. **`request.state.vc_auth`** — the observe/restore routers read it; the OBO middleware (or
   a small dependency) must populate the authenticated VC actor.

### 15.3 Frontend changes
- Swap `demoApi` → `new VersionControlApi(fetch)` in `VersionControlWorkbench.tsx`,
  `use-version-control.ts`, `version-control-tab.tsx` (keep `demoApi` for Vitest only).
- Fix `version-control-api.ts`: operations → `/api/vc/operations/{id}`; restore body
  `expected_base` as 64-hex digest string (not `Fingerprints`); align approvals route.
- Source `binding_id` from the space's binding (via overview/status) rather than the demo
  shell; decide the mount location (see decisions).

### 15.4 Decisions (Step 6) — RESOLVED
- **UI mount: BOTH** — a per-agent "Version Control" tab in `SpaceDetail` **and** the
  top-level overview (promote `VersionControlWorkbench` to a real nav destination).
- **Binding lifecycle: AUTO-enroll on first optimizer trigger** — `/trigger` resolves-or-
  enrolls the space's binding, then captures. Enrollment writes are gated behind the VC
  write flag (no-op until enabled at deploy).
- **Live config/identity:** catalog/control-schema/warehouse for the versions table +
  Volume, and the trusted SP identity — provided at deploy (Step 7); offline stays
  fail-closed.
- **History/diff endpoints:** add the history-list endpoint now; **defer semantic `diff`**
  (drive history/detail from `ledger.history`/`get_version`).

### 15.5 Sequence (Step 6)
a. Observer run-id capture variant + offline test.
b. History-list endpoint (`GET .../versions` → `ledger.history`) + offline tests. DONE
   (`backend/routers/vc_history.py`, gated on `vc_history_enabled`). Version **detail**
   and **diff** deferred: the frontend's `VersionDetail` is a flat summary+snapshot and
   restore's `expected_base` is a digest string, so those shapes are reconciled with the
   frontend in step (f) rather than shipping a mismatched backend contract now.
c. Live-compose builder (fail-closed) + register ports + offline fake tests.
d. Mount routers + `vc_auth` population + flag gating.
e. `/trigger` enroll + capture-before/after (both) + offline tests with fakes.
f. Frontend de-mock + route/body fixes + SpaceDetail tab.
g. (Step 7) deploy to nonprod: provision versions table/Volume, set flags + SP, validate
   one optimizer run writes before+after versions and restore works.

## 16. CUJ-1 persistence: inline-Delta, size-guarded, no Volumes

**Status:** Decided (design) · **Supersedes:** the Volume-fork path in §15.1 / §15.2 /
§15.5g for the CUJ-1 (in-workspace history + restore) surface.

### 16.1 Decision

Scope the persistence design to **CUJ-1 only** (in-workspace capture / history / restore).
CUJ-2 (cross-workspace promotion) may be deprecated, so it does not get a vote in this
design. For CUJ-1 we **mimic the optimizer**: store the whole Genie config **inline in the
Delta `genie_space_versions` table** and **remove the UC Volume fork entirely**. When a
config is ever too large to write inline, fail **loudly** with an explicit error — never
silently divert to a Volume. One write path, one atomic `INSERT`, no second storage system.

Rationale: the optimizer (GSO) already proves the inline pattern at the same payload scale —
it stores full configs as plain Delta `STRING` columns (`config_snapshot`,
`config_json`, `observed_config_json` in `optimization/ddl.py`) with no Volume and no size
cap. The ledger's own row is already 95% inline: `serialized_space_json`,
`canonical_state_json`, and `restorable_metadata_json` are inline `STRING` today
(`ledger.py:167-173`). Only the raw `response_envelope` is subject to the 32 KB cap
(`ledger.py:82` `inline_max_bytes=32768`) and forks to `/Volumes/.../vc_snapshots/`
(`ledger.py:126-138`). That fork is the sole reason CUJ-1 needs a Volume — and it is the
exact step that failed in nonprod (see §16.6).

### 16.2 Column type — keep `STRING`, do not switch to `VARIANT`

All payload columns are `STRING` today (`01-versions.sql:21-27`) and stay `STRING`.

| Option | Per-value ceiling | Fit for the ledger |
|---|---|---|
| `STRING` (current) | Arbitrary length; practically bounded only by Spark's ~2 GB single-value ceiling | **Chosen** — matches the optimizer; compresses well; no query restrictions |
| `VARIANT` | Hard **16 MiB** (128 MiB only on DBR ≥17.1) → `VARIANT_SIZE_LIMIT` error | Rejected — hard cliff **and** can't `GROUP BY`/`ORDER BY`/`DISTINCT`/set-op/partition/cluster on it |

`VARIANT`'s only advantage is native `:`/path querying inside the JSON — which we don't
need, because everything we filter or dedupe on is already extracted into typed sidecar
columns (`config_fingerprint`, `state_digest`, `origin`, `response_envelope_digest`, …).
Refs: [VARIANT limitations](https://docs.databricks.com/aws/en/tables/features/variant),
[VARIANT_SIZE_LIMIT report](https://community.databricks.com/t5/data-engineering/variant-size-limit-cannot-build-variant-bigger-than-16-0-mib-in/td-p/133563).

### 16.3 The real ceiling is the write path, not Delta storage — the 16 MiB anchor

The app has no Spark; it writes via the **SQL Statement Execution API**
(`POST /api/2.0/sql/statements`, `platform/live_seams.py`), passing each JSON payload as a
**bound parameter**, not spliced into SQL text. So the binding constraint is the API request,
not the Delta cell:

| Limit | Value | Binds CUJ-1? |
|---|---|---|
| Delta `STRING` cell | ~2 GB (Spark) | No — orders of magnitude above any config |
| `VARIANT` value | 16 MiB (128 MiB on DBR ≥17.1) | Only if we used VARIANT (we don't) |
| **SQL Statement Execution API** | **"maximum query text size is 16 MiB"**; ≤256 bound params | **Yes — the real one** |

The **16 MiB** figure is the non-arbitrary anchor (it happens to coincide with the legacy
VARIANT cap). Source:
[Execute a SQL statement — API reference](https://docs.databricks.com/api/workspace/statementexecution/executestatement).
Note the 25 MiB / 100 GiB numbers in that doc are **result-set** limits (reads), not write
limits.

### 16.4 Derived size-guard (replaces the 32 KB Volume trigger)

Replace `inline_max_bytes=32768`-and-fork with a **loud tripwire** derived from the 16 MiB
request budget, divided by the number of large JSON copies one `INSERT` carries:

- **Variant A — always inline the envelope (recommended now):** one row binds 3 large JSON
  params (`serialized_space_json` + `canonical_state_json` + `response_envelope_json`) plus
  `restorable_metadata_json`. Guard ≈ 16 MiB ÷ ~3 − overhead ≈ **~4–5 MiB per config**.
  Smallest change: schema and read path are untouched; the `envelope_present` CHECK
  (`01-versions.sql:44`) is satisfied because `response_envelope_json` is non-null and
  `response_envelope_uri` is null.
- **Variant B — stop persisting the raw envelope (optional later):** keep only
  `response_envelope_digest`; store 2 large copies. Guard rises to **~7 MiB**. Requires a
  Snapshot-contract + read-path change and dropping/relaxing the `envelope_present` CHECK.

The guard is a **defensive tripwire, not an operating limit.** Genie's own validity rules
(`schema.md:207-213`: ≤100 instructions, individual strings ≤25,000 chars, array items
≤10,000) cap a realistic config at **5–50 KB typical, 50–300 KB for a large space** — 1–2
orders of magnitude below the guard. If the guard ever trips, that is a signal (a malformed
or pathological config), and the right response is a clear error, not a silent Volume divert.

### 16.5 On-disk footprint

Two distinct numbers: **logical** (`octet_length(...)`, what counts against the 16 MiB
request budget) vs **on-disk** (Delta/Parquet, Snappy/ZSTD). Genie config JSON is highly
repetitive, so on-disk is typically **3–10× smaller** than the raw JSON (a 100 KB config is
~10–30 KB on disk). Storage is a non-concern; only the write-request size is worth guarding.
Measure with:
```sql
SELECT version_id,
       octet_length(serialized_space_json)  AS ss_bytes,
       octet_length(canonical_state_json)   AS canon_bytes,
       octet_length(response_envelope_json) AS env_bytes,
       response_envelope_uri
FROM   `${catalog}`.`${control_schema}`.`genie_space_versions`
ORDER BY observed_at DESC LIMIT 25;
```

### 16.6 Concrete changes (when implemented — one PLAN→VERIFY slice)

1. `ledger.py:126-138` — replace `_publish_envelope`'s Volume fork with a size-guard: encode
   the envelope, and if it exceeds the guard, raise a loud `ValueError` naming the space and
   the measured/allowed bytes; otherwise return `(inline_json, None)`. Never call
   `write_artifact`/`read_artifact` on the CUJ-1 path.
2. `ledger.py:80-96` — retire `inline_max_bytes=32768`; introduce the guard constant
   (~4–5 MiB, Variant A). Stop constructing `volume_prefix` for CUJ-1; drop the
   `write_artifact`/`read_artifact` seams from the CUJ-1 wiring
   (`platform/observe_seams.py:169-173`).
3. Add a ledger test: an envelope **> 32 KB** round-trips **inline** with no `write_artifact`
   call; an envelope **> guard** raises the loud error.
4. Refresh live-claim comments and the `vc_snapshots` mentions in this doc (§15.1, §15.2 #1,
   §15.5g) and in `scripts/version_control/provision_observe_nonprod.py`; decide whether to
   drop `vc_snapshots` Volume provisioning for a CUJ-1-only deploy.
5. Fixes the nonprod failure directly: the Aircraft agent's `response_envelope` exceeded the
   32 KB cap → Volume fork → `write_artifact` wrote a **relative** (non-`/Volumes/`) path
   (`ledger.py:132`) → `files.upload` failed, masked by an SDK logger bug (`len(BytesIO)`).
   Removing the fork removes both the relative-path bug and the SDK-logger crash for CUJ-1.

### 16.6.1 Landed (Variant A slice)

`_publish_envelope` now always inlines the envelope and raises a loud
`ValueError` above a `_INLINE_ENVELOPE_GUARD_BYTES = 4 MiB` guard
(`ledger.py`); the default `inline_max_bytes` moved from 32 KB to that guard. The
two Volume-fork ledger tests were replaced by an inline-over-32-KB regression
test and a loud-guard test (`test_vc_ledger.py`). Schema and the `_response_envelope`
read path are untouched; `write_artifact`/`read_artifact`/`volume_prefix` remain on
the ledger (dormant on the write path, still serving legacy `_uri` reads).
**Deferred** to a follow-up slice (kept out to minimise this diff): dropping the
now-unused artifact seams from the CUJ-1 wiring (`observe_seams.py:169-173`) and
removing `vc_snapshots` Volume provisioning
(`scripts/version_control/provision_observe_nonprod.py`) for a CUJ-1-only deploy.

### 16.7 CUJ-2 note

If cross-workspace promotion is kept, its large-artifact handling (`promotion/store.py`,
`releases.py` absolute `/Volumes/...` paths) is a **separate** path and may retain a Volume —
but it must not be reached from the CUJ-1 capture/restore code. This decision governs CUJ-1
inline persistence only.

## 17. Space "Version Control" tab — UX improvement plan

**Status:** Planned, not implemented (decided to write down first) · **Surface:**
`frontend/src/components/version-control/SpaceVersionControlTab.tsx` and its children
(`history.tsx`, `version-detail-panel.tsx`, `config-view.tsx`, `diff.tsx`).

### 17.1 Problems observed (with root cause in code)

1. **Capture lag with no feedback.** On open, `syncOnOpen` (`SpaceVersionControlTab.tsx:69`)
   calls `api.spaceObserve` — the slow part is the OBO live GET of the Genie space inside
   `capture_on_open` — and **swallows everything silently** (no spinner/message) until
   `load()` returns. The manual `capture()` (`:186`) only branches on `captured_version`
   truthy, conflating *no-change*, *busy*, and *permission error* into one message.
2. **Config view too narrow + text overflow.** The tab grid is inverted:
   `grid-cols-[minmax(0,1fr)_minmax(320px,380px)]` (`:267`) makes the **history list wide**
   and crams the **config/detail into a 320–380px sticky rail**. `ConfigView` is fine — it is
   starved of width. Long UC join identifiers overflow (`config-view.tsx:160-161` lack
   `break-all`); `SqlCodeBlock` `overflow-x-auto` scrolls sideways, painful at 320px.
3. **No list multi-select for diff.** Diff is dropdown-driven (`history.tsx:186-202` → two
   `<select>`s → `SemanticDiffView` below the grid).

Backend already distinguishes the outcomes we need: `ObservationResult`
(`types/version-control.ts:30`) carries **`captured_version`** *and* **`busy`**. Observe is a
single blocking POST (`vc_spaces.py:105` `space_observe`) — no SSE today.

### 17.2 Direction — master–detail (list-detail) pattern

Adopt the pattern used by mature version-history UIs (VS Code Timeline, GitKraken, Google
Docs "Version history", Notion, Figma): a **narrow selectable list rail** + a **wide detail
pane with a sticky metadata header over an independently scrollable body**. This is the
inverse of today's layout and directly fixes #2/#3.

### 17.3 Slices (each its own PLAN → VERIFY → commit)

- **Slice A — capture status & staged messaging (fixes #1). Decided: client-driven. LANDED.**
  Make on-open capture visible (status line + rail skeleton). Distinct terminal states from
  `ObservationResult` + errors (Nielsen "visibility of system status"): *saved new version
  (changes detected)* · *no changes since last version* · *capture already in progress*
  (`busy`) · *capture not enabled* (503 `vc_writes_disabled`) · *no access to read this
  space* (permission). Auto-dismiss success/no-change; keep errors sticky. Honest single
  "reading & comparing…" phase now (observe is one blocking call); **no SSE**. Absorbs the
  previously-pending messaging slice. Implemented: `capture-notice.ts`
  (`describeObservation`/`describeCaptureError`), visible `syncing` status +
  `loading || syncing` rail skeleton and tone-styled auto-dismissing notices in
  `SpaceVersionControlTab.tsx`, Slice D folded in (`version-format.ts` `workbench` →
  "Auto-captured", header auto-capture copy, softer empty state). Tests:
  `SpaceVersionControlTab.test.tsx`.
- **Slice B — layout inversion + overflow (fixes #2 & #3). LANDED.** Flipped to
  `lg:grid-cols-[320px_minmax(0,1fr)]`: narrow versions rail left, wide detail right. Detail
  (`version-detail-panel.tsx`) is now `flex h-full flex-col` — a `shrink-0` header (origin
  badge, short id/copy, captured time, actor, fingerprints, lineage, Restore) over a
  `flex-1 min-h-0 overflow-auto` body (sectioned `ConfigView`). Both panes `lg:h-[70vh]` and
  scroll on their own; stack below `lg`. Join identifiers got `break-all`/`min-w-0`
  (`config-view.tsx`) to kill the overflow. `SqlCodeBlock` keeps its horizontal scroll
  (comfortable at the new width).
- **Slice C — multi-select compare in the rail (fixes #4). LANDED.** Checkbox per rail row
  (`history.tsx`, additive `compareIds`/`onToggleCompare` — legacy dropdown kept only for the
  M02 demo). Single-click still opens detail; checkboxes drive compare (avoids the click-nav
  vs multi-select ambiguity — GitHub/Google Docs pattern). The space tab holds a rolling
  max-two `compareIds`; at exactly two selected the right pane switches from version detail to
  a compare panel (`Comparing X ↔ Y`, Clear) rendering the existing `SemanticDiffView`.
  Restore-preview diff is a separate state, untouched.
- **Slice D — labels & copy (fold into A).** Option A relabel `workbench` origin →
  **"Auto-captured"** in `version-format.ts` `ORIGIN_META`; explanatory copy (auto-capture
  points = optimizer runs + entering the workbench; direct-in-Genie edits captured on next
  open; no background watcher); soften the empty state.
- **Slice E (optional, deferred) — true server-side stages via SSE.** Stream
  `reading → canonicalizing → comparing → captured/no-change` from a new `observe/stream`
  endpoint reusing the create-agent SSE infra. Only if Slice A's client-side status is
  insufficient. **Not chosen now.**

**Suggested order:** A → B → C, D folded into A. Recommended when implementation resumes.

---

## 18. ConfigView redesign — navigable, clearly-labeled Genie config

The right-pane `ConfigView` (`frontend/src/components/version-control/config-view.tsx`,
rendered by `version-detail-panel.tsx`) is the surface a reviewer reads to understand a
captured version. Today it stacks a few flat sections; for a real space (e.g. the airline
demo: 7 data sources, ~20 columns each) it becomes a wall of text. Authoritative shape:
`backend/references/schema.md` (serialized_space v2).

**Problems (grounded in current code):**
- Tables and metric views are merged and unlabeled (`config-view.tsx` `[...tables,
  ...metricViews].map`).
- Columns are always a flat inline list with none of the schema's per-column metadata
  (`synonyms`, `exclude`, `enable_entity_matching`, `enable_format_assistance`,
  `get_example_values`, `build_value_dictionary`).
- Distinct concepts collapse into one "Sample SQL" bucket (example SQLs + expressions +
  measures + sql_functions); example SQL `parameters`/`usage_guidance` are dropped.
- Instructions are a raw `<pre>`; join `--rt=…--` relationship annotations leak into the SQL
  body; `benchmarks.questions` are not surfaced; there is no in-config navigation.

**Nuance:** in serialized_space a metric view is a data source referenced by `identifier` +
`column_configs` (its dimensions/measures surface as columns). Space-level
measures/filters/expressions live under `instructions.sql_snippets`, NOT inside the metric
view — so "Metric views" is labeled distinctly but its body still shows columns; Measures is
a separate section.

**UI direction (researched):** schema-browser tree + progressive disclosure (VS Code
Explorer, DBeaver, UC schema browser) for entities→columns; detail-on-the-side peek
drawer/popover (Linear/Notion) for a column's full metadata; anchored TOC / jump-nav (docs
sites, Stripe API ref) for navigation; attribute badges + tooltips for booleans. Prefer
stacked sections + jump-nav over tabs for a *review* surface. Reuse existing primitives:
`ui/accordion.tsx` (`AccordionItem`), `ui/collapsible.tsx`, `ui/tabs.tsx`, `ui/tooltip.tsx`,
`ui/table.tsx`, `ui/badge.tsx`, `SqlCodeBlock`. Keep the `unknown`-safe reads
(`asObject`/`asArray`/`text`), empty-section omission, unknown-key "Other", and raw-JSON
disclosure.

**Slices (each PLAN → VERIFY → commit):**
- **Slice 1 — split & label (this slice).** Separate **Tables** and **Metric views**; break
  "Sample SQL" into **Measures**, **Expressions**, **Filters**, **SQL functions**, and
  **Example SQL** (with `parameters` + `usage_guidance`); add **Benchmarks**
  (`benchmarks.questions` + expected SQL answer); surface `sample_questions` from
  `config.sample_questions` too. Snippet sections show `synonyms`/`instruction`/`comment`.
  Columns stay flat for now. Pure restructuring, lowest risk. Tests extend
  `config-view.test.tsx` with a full schema-shaped fixture.
- **Slice 2 — collapsible entities + column attribute badges. LANDED.** Data source entities
  are collapsible cards (`AccordionItem`): header = identifier + description + `N cols · M
  excl`; body reveals columns with per-column attribute badges (`Excluded` / `Entity match` /
  `Format assist` / `Example values` / `Value dict`, explained via native `title`) plus full
  description + synonyms. `defaultOpen` when `1 ≤ columns ≤ 8` so big tables (e.g. airline
  `certifying_staff`) collapse; the body stays in the DOM when collapsed so it remains
  searchable. The full-detail "column peek" was realized as this in-card expansion (inline
  badges + detail) rather than a separate side drawer — chosen for robustness (no portal/
  overlay); a true side drawer remains an option if deeper per-column metadata is needed.
- **Slice 3 — in-config section nav. LANDED.** `ConfigView`'s return is now driven by a
  declarative `defs[]` (the single source of truth for BOTH the nav and the rendered
  sections, so they cannot drift). A jump-nav (shown only when >1 section) is a wrapping chip
  bar that becomes a sticky left index in a two-column `xl:grid-cols-[168px_minmax(0,1fr)]`
  layout; each `Section` carries an `id={vc-cfg-<id>}` and clicking a chip
  `scrollIntoView`s it (optional-chained for jsdom). Empty state / raw-JSON disclosure
  unchanged.
- **Slice 4 (join polish) — LANDED.** `config-view.tsx`: `JoinEntry` component renders short
  table names (mono, full identifier on hover-`title`), the shared `catalog.schema` once, a
  relationship **badge** lifted from the `--rt=FROM_RELATIONSHIP_TYPE_…--` marker via
  `parseJoinSql`, and the ON condition as a compact inline snippet (backticks stripped) instead
  of the oversized `SqlCodeBlock`. The raw `--rt=…--` marker no longer leaks (test asserts
  `not.toContain('--rt=')` / `'FROM_RELATIONSHIP_TYPE'` on the rendered surface). GSL instruction
  header parsing (## PURPOSE / DISAMBIGUATION) is **dropped by decision** — parsing the
  serialized JSON and laying it out is sufficient; instructions stay a faithful `<pre>` of the
  text. Shipped alongside the non-blocking tab open (`SpaceVersionControlTab.tsx`: history read
  is painted before the background OBO live-config observe; rail skeletons decoupled from the
  sync banner).

---

## §19 — Version tagging + diff-direction correctness

> **For agentic workers:** written with the `writing-plans` discipline — bite-sized
> TDD tasks, real code, frequent commits. Each Task ends with an independently testable
> deliverable and its own PLAN → VERIFY → commit slice (mark the Task **LANDED** here in
> the same commit, MV-D9). Steps use `- [ ]` for tracking.

**Goal:** Let a user label ("tag") any captured version to remember it, and fix the
checkbox-compare diff so Before/After and added/removed read correctly.

**Architecture:** Tags are mutable app-UX metadata, so they live in **Lakebase** (the
app's own mutable Postgres, `backend/services/lakebase.py`), NOT the append-only, governed
VC Delta ledger (`ledger.py`; provisioning `OWNER_SPECS` in `platform/provisioning.py`).
The space-keyed router exposes tag CRUD plus a per-space tag map; the frontend fetches
versions (ledger) and tags (Lakebase) **separately** and merges by `version_id`. So
`contracts.py`, `ledger.py`, the governed provisioning/grant matrix, and every fingerprint
are untouched. The diff fix orders the compare pair chronologically before `api.diff`.

**Tech Stack:** FastAPI + asyncpg (backend); React 19 + TS + Tailwind + vitest (frontend).
Tests: backend `./scripts/test.sh`; frontend `cd frontend && npm run test` / `npm run lint`.

**Spec:** this doc's CUJ-1 sections + §18, and the design Q&A that motivated §19 —
tagging is a version-keyed annotation **decoupled** from the fingerprint-dedup capture path
(capture appends nothing when the config is unchanged, so a tag cannot ride a capture);
benchmarks are already versioned via `Fingerprints.benchmark` (`contracts.py:290`,
`config_fingerprint.py:202-207`) so the optional spotlight is presentation-only.

### Global Constraints
- Frontend-only Tasks keep vitest green (**baseline 540**) and lint clean. Backend Tasks
  run `./scripts/test.sh` (**baseline 3317**), print the import path + resolved sqlglot
  version, and leave `git status -- uv.lock` clean.
- Tags NEVER enter any fingerprint (config/benchmark/metadata) and NEVER affect dedup or
  drift. Tags do NOT touch the governed Delta store, `OWNER_SPECS`, the grant matrix, or
  any `contracts.py` wire value.
- Lakebase is app-instance scoped (AGENTS.md gotcha): tags are in-workspace UX and do not
  travel via promotion. Accepted for CUJ-1.

### Design decisions
- **VC-D-tag1 (store):** _SUPERSEDED by VC-D-tag1′ in §20 — the store moved from Lakebase to
  the governed Delta control table `genie_space_version_tags`._ Lakebase table `genie.vc_version_tags`, mutable CRUD
  (`INSERT … ON CONFLICT DO UPDATE` / `DELETE`), modeled on `star_space`
  (`lakebase.py:562-592`). Rationale: a mutable tag row in the VC Delta ledger would force a
  non-append-only fact table through the strict provisioning `OWNER_SPECS`
  (`provisioning.py:30-39`) and grant matrix (which grants only `SELECT, MODIFY` and relies
  on `delta.appendOnly='true'`, `provisioning.py:228-233`). Tags are workbench UX, not
  governed facts.
- **VC-D-tag2 (shape):** a tag is `{label: str (1–60 chars), note: str|None}`, keyed by
  `version_id` (globally-unique UUID) with a `space_id` scope column, authored under OBO and
  stamped with the human actor. Returned as a separate per-space map
  (`GET /spaces/{id}/tags` → `{version_id: {label, note, author}}`) and merged client-side.
  `VersionSummary` and the ledger are untouched.
- **VC-D-tag3 (auth):** tag writes reuse `actor_for` + `authorize_history(actor, binding)`
  (the same gate as history reads); the binding is resolved via
  `registry.find_active_by_space_key` (read-only) — tagging never enrolls.
- **VC-D-diff (direction):** the checkbox-compare effect orders the two selected ids by
  `observed_at` ascending (older→`left`/Before, newer→`right`/After) before `api.diff`. The
  restore preview's intentional `current→target` direction (`SpaceVersionControlTab.tsx`
  `beginRestore`) is unchanged.

### File structure
- `frontend/src/components/version-control/compare-order.ts` — **new**; pure `chronoPair`
  helper (kept out of the component file to satisfy `react-refresh/only-export-components`).
- `frontend/src/components/version-control/SpaceVersionControlTab.tsx` — use `chronoPair`
  for the compare effect + header; hold tag state; wire tag handlers.
- `frontend/src/components/version-control/diff.tsx` — hide the split toggle when no
  `modified` items.
- `backend/services/lakebase.py` — tag table + `get_version_tags` / `set_version_tag` /
  `delete_version_tag`.
- `backend/routers/vc_spaces.py` — `GET /spaces/{id}/tags`, `PUT`/`DELETE`
  `/spaces/{id}/versions/{version_id}/tag`.
- `frontend/src/lib/version-control-api.ts` + `frontend/src/types/version-control.ts` —
  client methods + `VersionTag` type.
- `frontend/src/components/version-control/history.tsx`,
  `version-detail-panel.tsx` — render the tag badge + tag editor.

---

### Task 1 — Diff pair ordered chronologically (fixes bugs #2/#3)

**LANDED:** `chronoPair` helper added in its own module `compare-order.ts` (older `version_id` first, input-order fallback on missing/equal timestamps); wired into the checkbox-compare effect (`api.diff` now passes older→newer) and the "Comparing" header (reads `before → after`). Restore-preview direction untouched. TDD followed (RED module-not-found → GREEN). Frontend suite 542/542, lint clean.

**Files:**
- Create: `frontend/src/components/version-control/compare-order.ts`
- Create: `frontend/src/components/version-control/compare-order.test.ts`
- Modify: `SpaceVersionControlTab.tsx` (compare effect ~L160-174; header ~L356)

**Interfaces — Produces:** `chronoPair(ids: string[], items: VersionSummary[]): [string, string]`
(older `version_id` first; falls back to input order when timestamps are missing/equal).

- [x] **Step 1: Write the failing test** (`compare-order.test.ts`)

```ts
import { describe, it, expect } from 'vitest'
import { chronoPair } from './compare-order'
import type { VersionSummary } from '@/types/version-control'

const v = (id: string, iso: string) => ({ version_id: id, observed_at: iso } as VersionSummary)

describe('chronoPair', () => {
  it('returns older id first regardless of selection order', () => {
    const items = [v('new', '2026-09-12T10:00:00Z'), v('old', '2026-09-12T08:00:00Z')]
    expect(chronoPair(['new', 'old'], items)).toEqual(['old', 'new'])
    expect(chronoPair(['old', 'new'], items)).toEqual(['old', 'new'])
  })
})
```

- [x] **Step 2: Run it, confirm it fails** — `cd frontend && npm run test -- compare-order` → FAIL (module not found).

- [x] **Step 3: Implement** (`compare-order.ts`)

```ts
import type { VersionSummary } from '@/types/version-control'

// Order two selected version ids chronologically (older first) using the loaded page, so a
// diff reads Before(older) → After(newer). Unknown/equal timestamps keep the given order.
export function chronoPair(ids: string[], items: VersionSummary[]): [string, string] {
  const at = (id: string) => Date.parse(items.find(v => v.version_id === id)?.observed_at ?? '')
  const [a, b] = ids
  return at(a) <= at(b) ? [a, b] : [b, a]
}
```

- [x] **Step 4: Wire it into the effect** (`SpaceVersionControlTab.tsx`) — replace the
  `api.diff(bindingId, compareIds[0], compareIds[1])` call:

```tsx
import { chronoPair } from './compare-order'
// …inside the compareIds effect, after the bindingId guard:
const [olderId, newerId] = chronoPair(compareIds, page.items)
api.diff(bindingId, olderId, newerId)
  .then(d => { if (live) setCompareDiff(d) })
```

  And make the "Comparing" header read older → newer instead of the ambiguous `↔`:

```tsx
{compareIds.length === 2 && (() => {
  const [beforeId, afterId] = chronoPair(compareIds, page.items)
  return <span className="font-mono text-xs text-secondary">{shortId(beforeId)} → {shortId(afterId)}</span>
})()}
```

- [x] **Step 5: Run tests + lint** — `npm run test` (541) and `npm run lint` (clean).
- [x] **Step 6: Commit** — `git add frontend/src/components/version-control/compare-order.ts compare-order.test.ts SpaceVersionControlTab.tsx` → `vc(diff): order compare pair chronologically (Before=older, After=newer)`. Mark Task 1 LANDED.

---

### Task 2 — Split/unified toggle only shows when it does something (fixes bug #4)

> LANDED: `SemanticDiffView` now gates the split/unified toggle behind
> `hasModified = diff.items.some(item => item.change === 'modified')` — added-only
> and removed-only diffs no longer show a toggle that does nothing. New TDD test
> `frontend/src/components/version-control/diff.test.tsx` covers both cases (545/545
> frontend tests pass, lint clean).

**Files:**
- Modify: `frontend/src/components/version-control/diff.tsx` (toggle at ~L120-130)
- Test: `frontend/src/components/version-control/diff.test.tsx` (create if absent)

Rationale: the toggle passes `split` only to `ValueDiff`, which renders solely for
`modified` items (`diff.tsx:75-79`); `added`/`removed` use the single-column `ValueBlock`
and have no second side to lay out. Showing the toggle on an add/remove-only diff makes it
look broken. Hide it unless there is at least one `modified` item.

- [x] **Step 1: Write the failing test** (`diff.test.tsx`)

```tsx
import { describe, it, expect } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { SemanticDiffView } from './diff'
import type { SemanticDiff } from '@/types/version-control'

const added: SemanticDiff = { comparison: 'different', items: [
  { category: 'questions', path: 'benchmarks.questions[0]', change: 'added', before: null, after: { id: 'q' }, review_required: false },
]}
const modified: SemanticDiff = { comparison: 'different', items: [
  { category: 'instructions', path: 'instructions', change: 'modified', before: 'a', after: 'b', review_required: false },
]}

describe('SemanticDiffView toggle', () => {
  it('hides the split toggle when there are no modified items', () => {
    expect(renderToStaticMarkup(<SemanticDiffView diff={added} />)).not.toContain('Switch to unified diff')
  })
  it('shows the split toggle when a modified item is present', () => {
    expect(renderToStaticMarkup(<SemanticDiffView diff={modified} />)).toContain('Switch to unified diff')
  })
})
```

- [x] **Step 2: Run it, confirm the first case fails** — `npm run test -- diff` → FAIL (toggle currently always renders).

- [x] **Step 3: Implement** — in `diff.tsx`, compute and gate:

```tsx
const hasModified = diff.items.some(item => item.change === 'modified')
// …in the header, change the guard:
{diff.items.length > 0 && hasModified && (
  <button type="button" onClick={() => setSplit(value => !value)} …>
    <ToggleIcon className="w-3 h-3" />
    {split ? 'Unified' : 'Side-by-side'}
  </button>
)}
```

- [x] **Step 4: Run tests + lint** — `npm run test` (543) / `npm run lint`.
- [x] **Step 5: Commit** — `vc(diff): hide split toggle when no modified items`. Mark Task 2 LANDED.

> Note (deferred, not this Task): a richer option is to route `added`/`removed` through
> `ValueDiff` with the opposite side blank so every change kind obeys the toggle. Skipped
> here to avoid a jsdom-flaky assertion on `react-diff-viewer` internals; revisit only if
> users ask to see adds/removes side-by-side against an empty column.

---

### Task 3 — Lakebase tag store

**LANDED:** `genie.vc_version_tags` table + `idx_vc_version_tags_space` index added to
`_ensure_schema` (next to `starred_spaces`), a `vc_version_tags` bucket added to the
in-memory `_memory_store`, and `set_version_tag` / `delete_version_tag` / `get_version_tags`
added near `star_space` (branch on `_lakebase_available`/`_pool`, in-memory fallback; `get`
calls `_maybe_retry_schema` like the other read paths). Mutable UX metadata only — never a
governed VC Delta fact, never in any fingerprint; `contracts.py`/`OWNER_SPECS`/grant matrix
untouched. TDD followed (RED `AttributeError: no attribute 'set_version_tag'` → GREEN 1
passed). `git status -- uv.lock` clean; import path resolves in-repo; sqlglot 30.0.3.

**Files:**
- Modify: `backend/services/lakebase.py` (memory store ~L20-33; `_ensure_schema` ~L182-193; new functions near `star_space` ~L562)
- Test: `backend/tests/test_vc_version_tags.py` (create)

**Interfaces — Produces (all `async`, in-memory fallback when Lakebase is down):**
- `get_version_tags(space_id: str) -> dict[str, dict]` → `{version_id: {label, note, author}}`
- `set_version_tag(space_id, version_id, label, note, author) -> None`
- `delete_version_tag(space_id, version_id) -> None`

- [x] **Step 1: Write the failing test** (`test_vc_version_tags.py`)

```python
import pytest
from backend.services import lakebase


@pytest.mark.asyncio
async def test_set_get_delete_version_tag_in_memory():
    sid, vid = "space-tags-1", "11111111-1111-1111-1111-111111111111"
    await lakebase.set_version_tag(sid, vid, "Golden baseline", "before rollout", "amy@x.io")
    tags = await lakebase.get_version_tags(sid)
    assert tags[vid] == {"label": "Golden baseline", "note": "before rollout", "author": "amy@x.io"}
    # scoped by space
    assert await lakebase.get_version_tags("other-space") == {}
    # upsert overwrites
    await lakebase.set_version_tag(sid, vid, "Renamed", None, "amy@x.io")
    assert (await lakebase.get_version_tags(sid))[vid]["label"] == "Renamed"
    # delete
    await lakebase.delete_version_tag(sid, vid)
    assert vid not in await lakebase.get_version_tags(sid)
```

- [x] **Step 2: Run it, confirm it fails** — `./scripts/test.sh backend/tests/test_vc_version_tags.py` → FAIL (functions undefined).

- [x] **Step 3: Implement** — add the memory bucket to `_memory_store`:

```python
    "vc_version_tags": {},  # version_id -> {space_id, label, note, author}
```

  add the table to `_ensure_schema` (next to `starred_spaces`):

```python
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.vc_version_tags (
                    version_id VARCHAR(64)  PRIMARY KEY,
                    space_id   VARCHAR(128) NOT NULL,
                    label      VARCHAR(60)  NOT NULL,
                    note       TEXT,
                    author     TEXT,
                    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_vc_version_tags_space ON genie.vc_version_tags(space_id)"
            )
```

  and the CRUD functions (near `star_space`):

```python
async def set_version_tag(space_id: str, version_id: str, label: str,
                          note: str | None, author: str | None) -> None:
    """Create or update the tag on one version (mutable UX metadata; never a VC fact)."""
    if not _lakebase_available or _pool is None:
        _memory_store["vc_version_tags"][version_id] = {
            "space_id": space_id, "label": label, "note": note, "author": author}
        return
    async with _pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO genie.vc_version_tags (version_id, space_id, label, note, author, updated_at)
               VALUES ($1, $2, $3, $4, $5, NOW())
               ON CONFLICT (version_id) DO UPDATE
                 SET label = EXCLUDED.label, note = EXCLUDED.note,
                     author = EXCLUDED.author, updated_at = NOW()""",
            version_id, space_id, label, note, author)


async def delete_version_tag(space_id: str, version_id: str) -> None:
    if not _lakebase_available or _pool is None:
        _memory_store["vc_version_tags"].pop(version_id, None)
        return
    async with _pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM genie.vc_version_tags WHERE version_id = $1 AND space_id = $2",
            version_id, space_id)


async def get_version_tags(space_id: str) -> dict:
    """Return {version_id: {label, note, author}} for one space."""
    await _maybe_retry_schema()
    if not _lakebase_available or _pool is None:
        return {vid: {"label": t["label"], "note": t.get("note"), "author": t.get("author")}
                for vid, t in _memory_store["vc_version_tags"].items()
                if t["space_id"] == space_id}
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT version_id, label, note, author FROM genie.vc_version_tags WHERE space_id = $1",
            space_id)
        return {r["version_id"]: {"label": r["label"], "note": r["note"], "author": r["author"]}
                for r in rows}
```

- [x] **Step 4: Run it, confirm it passes** — `./scripts/test.sh backend/tests/test_vc_version_tags.py`.
- [x] **Step 5: Commit** — `vc(tags): Lakebase vc_version_tags store (mutable UX, in-memory fallback)`. Mark Task 3 LANDED.

---

### Task 4 — Space-keyed tag endpoints

**LANDED:** Added `TagBody` (label 1–60, strip; `note?`), a module-scope async error-mapper `_ainvoke` (mirrors `_invoke`'s exception→HTTP mapping, no `to_wire`), and three routes inside `build_router` reusing the DI closures — `actor_for`/`registry.find_active_by_space_key`/`authorize_history`/`flags` — via a shared `resolve_readable`. GET `/spaces/{space_id}/tags` (gate `vc_history_enabled`, `{}` when not enrolled), PUT/DELETE `…/versions/{version_id}/tag` (gate `vc_writes_enabled`, 404 when not enrolled; author = `actor.subject_id`). Full suite 3321 passed; `test_spaces_router_exposes_exact_routes` extended for the 3 new routes.

**Files:**
- Modify: `backend/routers/vc_spaces.py` (imports; `_ainvoke` helper; three routes inside `build_router`)
- Test: `backend/tests/test_vc_spaces_router.py` (extend)

**Interfaces — Consumes:** Task 3's `lakebase.*` functions. **Produces:**
- `GET /api/version-control/spaces/{space_id}/tags` → `{version_id: {label, note, author}}`
- `PUT …/spaces/{space_id}/versions/{version_id}/tag` body `{label, note?}` → the saved tag
- `DELETE …/spaces/{space_id}/versions/{version_id}/tag` → `{version_id, deleted: true}`

- [x] **Step 1: Write the failing test** (extend `test_vc_spaces_router.py`, reusing the file's existing router/client fixture)

```python
def test_put_get_delete_version_tag(client):  # `client` = the module's TestClient fixture
    space, vid = "sp_tag", "22222222-2222-2222-2222-222222222222"
    put = client.put(f"/api/version-control/spaces/{space}/versions/{vid}/tag",
                     json={"label": "Golden", "note": "keep"})
    assert put.status_code == 200 and put.json()["author"]  # stamped with the actor
    got = client.get(f"/api/version-control/spaces/{space}/tags").json()
    assert got[vid]["label"] == "Golden"
    assert client.delete(f"/api/version-control/spaces/{space}/versions/{vid}/tag").status_code == 200
    assert vid not in client.get(f"/api/version-control/spaces/{space}/tags").json()
```

- [x] **Step 2: Run it, confirm it fails** — `./scripts/test.sh backend/tests/test_vc_spaces_router.py` → 404 (routes missing).

- [x] **Step 3: Implement** — imports at top of `vc_spaces.py`:

```python
from pydantic import BaseModel, ConfigDict, StringConstraints
```

  add a body model near `RestoreSpaceBody`:

```python
class TagBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: Annotated[str, StringConstraints(min_length=1, max_length=60, strip_whitespace=True)]
    note: str | None = None
```

  add an async error-mapper mirroring `_invoke` (module scope, next to `_invoke`):

```python
async def _ainvoke(op):
    try:
        return await op()
    except HTTPException:
        raise
    except PermissionError as error:
        raise _error(403, "scope_denied", str(error)) from error
    except LookupError as error:
        raise _error(404, "resource_not_found", "Scoped resource not found") from error
    except (ValueError, TypeError) as error:
        raise _error(409, "request_conflict", str(error)) from error
    except Exception as error:
        logger.exception("VC space request failed")
        raise _error(503, "evidence_unavailable", "Evidence unavailable", stale=True) from error
```

  inside `build_router`, add a binding resolver + the three routes:

```python
    def resolve_readable(space_id, request):
        actor = actor_for(request)
        binding = registry.find_active_by_space_key(space_id)
        if binding is not None and (actor.workspace_id != binding.workspace_id
                                    or authorize_history(actor, binding) is not True):
            raise PermissionError("Binding history scope denied")
        return binding, actor

    @router.get("/spaces/{space_id}/tags")
    async def space_tags(space_id: SpaceId, request: Request):
        from backend.services import lakebase
        async def op():
            if flags.enabled("vc_history_enabled") is not True:
                raise _error(503, "vc_history_disabled", "VC history reads disabled", stale=True)
            binding, _ = resolve_readable(space_id, request)
            if binding is None:
                return {}  # not enrolled yet -> no tags
            return await lakebase.get_version_tags(space_id)
        return await _ainvoke(op)

    @router.put("/spaces/{space_id}/versions/{version_id}/tag")
    async def set_tag(space_id: SpaceId, version_id: UUID, body: TagBody, request: Request):
        from backend.services import lakebase
        async def op():
            if flags.enabled("vc_writes_enabled") is not True:
                raise _error(503, "vc_writes_disabled", "VC writes disabled", stale=True)
            binding, actor = resolve_readable(space_id, request)
            if binding is None:
                raise _error(404, "resource_not_found", "Space is not enrolled in version control")
            await lakebase.set_version_tag(space_id, str(version_id), body.label, body.note, actor.subject_id)
            return {"version_id": str(version_id), "label": body.label,
                    "note": body.note, "author": actor.subject_id}
        return await _ainvoke(op)

    @router.delete("/spaces/{space_id}/versions/{version_id}/tag")
    async def delete_tag(space_id: SpaceId, version_id: UUID, request: Request):
        from backend.services import lakebase
        async def op():
            if flags.enabled("vc_writes_enabled") is not True:
                raise _error(503, "vc_writes_disabled", "VC writes disabled", stale=True)
            binding, _ = resolve_readable(space_id, request)
            if binding is None:
                raise _error(404, "resource_not_found", "Space is not enrolled in version control")
            await lakebase.delete_version_tag(space_id, str(version_id))
            return {"version_id": str(version_id), "deleted": True}
        return await _ainvoke(op)
```

- [x] **Step 4: Run it, confirm it passes** — `./scripts/test.sh backend/tests/test_vc_spaces_router.py`; print import path + sqlglot version; `git status -- uv.lock` clean.
- [x] **Step 5: Commit** — `vc(tags): space-keyed tag GET/PUT/DELETE endpoints`. Mark Task 4 LANDED.

---

### Task 5 — Tag UI (badge in the rail + editor in the detail panel)

LANDED: `VersionTag`/`VersionTagMap` types + `spaceTags`/`setVersionTag`/`deleteVersionTag`
API methods; rail tag Badge in `history.tsx`; sticky-header tag editor (label input + Save +
Remove tag) in `version-detail-panel.tsx`; tag state/handlers wired into
`SpaceVersionControlTab.tsx`. Tests appended to `history.test.tsx` and
`version-detail-panel.test.tsx` (renderToStaticMarkup markup assertions — no new deps).
Full vitest suite 552 passed; lint clean.

**Files:**
- Modify: `frontend/src/types/version-control.ts` (add `VersionTag`)
- Modify: `frontend/src/lib/version-control-api.ts` (client methods)
- Modify: `SpaceVersionControlTab.tsx` (tag state + handlers, pass down)
- Modify: `history.tsx` (badge), `version-detail-panel.tsx` (editor)
- Test: `history.test.tsx` and `version-detail-panel.test.tsx` (create/extend)

**Interfaces — Consumes:** Task 4 endpoints. **Produces:** `VersionTag`,
`api.spaceTags/setVersionTag/deleteVersionTag`; `History` gains optional `tags?: VersionTagMap`;
`VersionDetailPanel` gains optional `tag`, `onSetTag`, `onRemoveTag`.

- [ ] **Step 1: Types** (`types/version-control.ts`)

```ts
export interface VersionTag { label: string; note: string | null; author?: string | null }
export type VersionTagMap = Record<string, VersionTag>
```

- [ ] **Step 2: API client** (`version-control-api.ts`, add `VersionTag` to the import list, then methods)

```ts
  spaceTags(spaceId: string, signal?: AbortSignal) {
    return this.get<Record<string, VersionTag>>(`/spaces/${encodeURIComponent(spaceId)}/tags`, signal)
  }
  setVersionTag(spaceId: string, versionId: string, body: { label: string; note?: string | null }) {
    return this.request<VersionTag & { version_id: string }>(
      `/spaces/${encodeURIComponent(spaceId)}/versions/${encodeURIComponent(versionId)}/tag`,
      { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  }
  deleteVersionTag(spaceId: string, versionId: string) {
    return this.request<{ version_id: string; deleted: boolean }>(
      `/spaces/${encodeURIComponent(spaceId)}/versions/${encodeURIComponent(versionId)}/tag`,
      { method: 'DELETE' })
  }
```

- [ ] **Step 3: Failing test — rail badge** (`history.test.tsx`)

```tsx
import { describe, it, expect } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { History } from './history'
import type { VersionPage } from '@/types/version-control'

const page: VersionPage = { items: [{
  version_id: 'v1', binding_id: 'b1', observed_at: '2026-09-12T10:00:00Z', origin: 'workbench',
  observed_by: 'amy@x.io', parent_version_id: null, restored_from_version_id: null,
  fingerprints: { config: 'c', benchmark: 'bm', metadata: 'm', canonicalizer_version: 'vc-c14n/1' },
  optimizer_run_id: null, champion_id: null,
}], next_cursor: null }

describe('History tags', () => {
  it('renders the tag label when a tag exists for the version', () => {
    const html = renderToStaticMarkup(
      <History page={page} onNext={() => {}} onSelect={() => {}} tags={{ v1: { label: 'Golden', note: null } }} />)
    expect(html).toContain('Golden')
  })
})
```

- [ ] **Step 4: Implement — `history.tsx`** — extend props and render the badge in `VersionRow`:

```tsx
// HistoryProps: add `tags?: VersionTagMap`; VersionRowProps: add `tag?: VersionTag`.
// In VersionRow, after the id/copy controls (inside the top flex row):
{tag && (
  <Badge variant="secondary" className="gap-1" title={tag.note ?? tag.label}>
    <Tag className="w-3 h-3" />{tag.label}
  </Badge>
)}
// import { Tag } from 'lucide-react'; pass tag={tags?.[version.version_id]} from History.
```

- [ ] **Step 5: Failing test — detail editor** (`version-detail-panel.test.tsx`)

```tsx
// Render VersionDetailPanel with tag={{ label: 'Golden', note: null }} + onSetTag/onRemoveTag noops.
// Assert the input shows the existing label and a "Remove tag" control is present.
import { render, screen } from '@testing-library/react'
// …assert screen.getByDisplayValue('Golden') and screen.getByRole('button', { name: /remove tag/i })
```

- [ ] **Step 6: Implement — `version-detail-panel.tsx`** — add a small editor in the sticky
  header (below Fingerprints), controlled by local state seeded from `tag`:

```tsx
// Props: tag?: VersionTag; onSetTag?: (label: string, note: string | null) => void; onRemoveTag?: () => void
// A labeled <input maxLength={60}> + Save button (calls onSetTag) and, when tag exists, a
// "Remove tag" button (calls onRemoveTag). Gate the whole block on `onSetTag` being provided.
```

- [ ] **Step 7: Wire `SpaceVersionControlTab.tsx`** — add `const [tags, setTags] = useState<VersionTagMap>({})`;
  fetch after the initial `load()` in `syncOnOpen` (and on `spaceId` change) via
  `api.spaceTags(spaceId).then(setTags).catch(() => {})`; add handlers:

```tsx
const setTag = useCallback(async (versionId: string, label: string, note: string | null) => {
  await api.setVersionTag(spaceId, versionId, { label, note })
  setTags(await api.spaceTags(spaceId))
}, [spaceId])
const removeTag = useCallback(async (versionId: string) => {
  await api.deleteVersionTag(spaceId, versionId)
  setTags(await api.spaceTags(spaceId))
}, [spaceId])
// pass tags={tags} to <History/>; pass tag/onSetTag/onRemoveTag to <VersionDetailPanel/>.
```

- [ ] **Step 8: Run tests + lint** — `npm run test` / `npm run lint`.
- [x] **Step 9: Commit** — `vc(tags): tag badge in rail + tag editor in detail panel`. Mark Task 5 LANDED.

---

### Task 6 (optional) — Benchmark-changed spotlight

**LANDED:** `benchmarkChanged` helper (`benchmark-change.ts` + `.test.ts`) added; `history.tsx` computes `byId` from `page.items` and renders a `Badge variant="info"` "Benchmarks changed" on rows whose benchmark fingerprint differs from their parent, alongside the Task 5 tag badge (untouched). Presentation-only, no backend change. Full vitest 557 passed, lint clean. Commit `vc(tags): benchmark-changed chip on the timeline`.

**Files:**
- Create: `frontend/src/components/version-control/benchmark-change.ts` (+ `.test.ts`)
- Modify: `history.tsx` (chip on rows whose benchmark fingerprint differs from their parent)

Benchmarks are already versioned (their own `fingerprints.benchmark`); this only surfaces
"benchmarks changed in this version" — presentation-only, no backend change.

**Interfaces — Produces:** `benchmarkChanged(version: VersionSummary, byId: (id: string) => VersionSummary | undefined): boolean`.

- [x] **Step 1: Failing test** (`benchmark-change.test.ts`)

```ts
import { describe, it, expect } from 'vitest'
import { benchmarkChanged } from './benchmark-change'
const mk = (id: string, bm: string, parent: string | null) => ({
  version_id: id, parent_version_id: parent,
  fingerprints: { config: 'c', benchmark: bm, metadata: 'm', canonicalizer_version: 'v' },
} as any)

describe('benchmarkChanged', () => {
  const parent = mk('p', 'BM1', null)
  const byId = (id: string) => (id === 'p' ? parent : undefined)
  it('true when the benchmark fingerprint differs from the parent', () => {
    expect(benchmarkChanged(mk('c', 'BM2', 'p'), byId)).toBe(true)
  })
  it('false when unchanged or no parent', () => {
    expect(benchmarkChanged(mk('c', 'BM1', 'p'), byId)).toBe(false)
    expect(benchmarkChanged(mk('c', 'BM2', null), byId)).toBe(false)
  })
})
```

- [x] **Step 2: Implement** (`benchmark-change.ts`)

```ts
import type { VersionSummary } from '@/types/version-control'

// True when this version's benchmark fingerprint differs from its parent's — i.e. the
// benchmark question set changed here. No parent (or unknown parent) => not a change.
export function benchmarkChanged(
  version: VersionSummary,
  byId: (id: string) => VersionSummary | undefined,
): boolean {
  if (!version.parent_version_id) return false
  const parent = byId(version.parent_version_id)
  return !!parent && parent.fingerprints.benchmark !== version.fingerprints.benchmark
}
```

- [x] **Step 3: Wire the chip** — in `History`, build `byId` from `page.items` and render a
  `Badge variant="info"` "Benchmarks changed" on rows where `benchmarkChanged(version, byId)`.
- [x] **Step 4: Run tests + lint; Commit** — `vc(tags): benchmark-changed chip on the timeline`. Mark Task 6 LANDED.

---

### Self-review (writing-plans)
- **Spec coverage:** #2/#3 → Task 1; #4 → Task 2; tagging (store/endpoints/UI) → Tasks 3-5;
  optional benchmark spotlight → Task 6. All covered.
- **Type consistency:** `chronoPair`, `VersionTag`/`VersionTagMap`, `get_version_tags`/
  `set_version_tag`/`delete_version_tag`, `spaceTags`/`setVersionTag`/`deleteVersionTag`,
  `benchmarkChanged` are named identically across producer and consumer tasks.
- **No governed-store contact:** confirmed — tags are Lakebase-only; the Delta ledger,
  `contracts.py`, `OWNER_SPECS`, and every fingerprint are untouched.

### Suggested order
Tasks 1-2 first (fast, frontend-only bug fixes, high payoff), then 3 → 4 → 5 (tagging
end-to-end), then 6 if wanted.

---

## §20 — Tagging store pivot: Lakebase → governed Delta control table (LANDED)

Supersedes **VC-D-tag1** (the Lakebase decision) and the §19 file-structure/self-review
claims that named `lakebase.py` as the tag store. Motivation (from review): the module is
otherwise a self-contained GSO control plane keyed by the GSO Delta schema; a Lakebase
side-table split tag state away from that plane and depended on app-instance-scoped
Postgres that the deployed app does not provision. Tags now live in the SAME governed
schema as the rest of the VC control plane, one row per version (1:1/0..1 mapping to
`genie_space_versions`).

### Decisions (amending §19)
- **VC-D-tag1′ (store, REPLACES VC-D-tag1):** mutable Delta control table
  `genie_space_version_tags`, keyed by `version_id` (PRIMARY KEY) with a `space_id` scope
  column. Written in place (`MERGE` upsert / `DELETE`) by the observe runtime's executor
  identity — the same pattern as `genie_ops_coordination` (`DeltaCoordinationStore`), which
  established that a mutable, non-`appendOnly` Delta table with `SELECT, MODIFY` is the
  sanctioned shape for control (non-fact) state. Not an `appendOnly` fact table, so it is
  intentionally excluded from `_FACT_TABLES`.
- **VC-D-tag2 (shape):** unchanged — one `{label (1–60), note?}` per version, author-stamped.
- **VC-D-tag4 (comment field, §19-followup):** the single per-version tag carries an
  optional free-text `note` (comment). The detail-panel editor surfaces a `note` textarea
  plus quick-pick preset chips (`Champion`, `Challenger`, `Baseline`, `v1`).
- **VC-D-sql (formatting):** example SQL, expected-SQL benchmark answers, and sql_snippets
  render through `SqlCodeBlock` with `format` (pretty-print via the already-vendored
  `sql-formatter`, `spark` dialect, parse-failure falls back to raw) and a contextual header
  `label`.

### Landed changes
- `backend/version_control_ddl/09-version-tags.sql` — **new** owner DDL: `CREATE TABLE …
  genie_space_version_tags` (PK `version_id`, `Serializable`, NOT `appendOnly`) + idempotent
  `label` length `CHECK` via `ALTER`.
- `backend/services/version_control/platform/provisioning.py` — `("M09","table",
  "genie_space_version_tags")` added to `OWNER_SPECS`; grant matrix adds `GRANT SELECT,
  MODIFY … genie_space_version_tags TO <executor>`.
- `backend/services/version_control/version_tags.py` — **new** `DeltaVersionTagStore`
  (`set_tag`/`delete_tag`/`get_tags`, `current_timestamp()`-stamped, space-scoped).
- `backend/services/version_control/platform/observe_seams.py` — `tag_store` added to
  `ObserveRuntime`; constructed in `build_observe_runtime` from the executor `sql` seam.
- `backend/routers/vc_spaces.py` — tag routes now **sync**, backed by `runtime.tag_store`;
  the async `_ainvoke` helper is removed (all routes go through `_invoke` → `to_wire`).
- `backend/services/lakebase.py` — `vc_version_tags` table, index, in-memory bucket, and the
  three async tag functions **removed**.
- `scripts/version_control/provision_observe_nonprod.py` — `09-version-tags.sql` +
  `genie_space_version_tags` added to the observe DDL/table lists.
- `frontend/src/components/SqlCodeBlock.tsx` — optional `format`/`language`/`label` props.
- `frontend/src/components/version-control/config-view.tsx` — formatted SQL for
  example/benchmark/snippet blocks with contextual headers.
- `frontend/src/components/version-control/version-detail-panel.tsx` — `note` textarea +
  preset chips.

### Verify (this session)
- Backend: `./scripts/test.sh` — **3326 passed**, 17 deselected, 0 failed (import resolves
  inside this checkout; sqlglot 30.0.3 under `--frozen`).
- Frontend: touched vitest files green (13 passed); `eslint` clean; `tsc --noEmit` clean.
- Grep-clean: no `set_version_tag`/`delete_version_tag`/`get_version_tags`/`vc_version_tags`/
  `_ainvoke` in code (only descriptive test names + this doc's frozen §19 history remain).

## §21 — Initial-capture hook for the Create flow (PLAN)

> **For agentic workers:** written with the `writing-plans` discipline — bite-sized
> TDD tasks, real code, frequent commits. Each Task ends with an independently testable
> deliverable and its own PLAN → VERIFY → commit slice (mark the Task **LANDED** here in
> the same commit, MV-D9). Steps use `- [ ]` for tracking.
>
> **Numbering note:** §20 is already taken (the LANDED tagging-store pivot), so this
> forward-looking plan is §21.

**Goal:** When a Genie space is born inside the workbench (REST wizard or the agentic
Create flow), append its **first** VC version automatically, so the version timeline starts
at "created here" instead of lazily at "first tab open." The capture is **best-effort**:
space creation must never fail because VC is off, unintegrated, or hiccuped.

**Architecture:** The `POST /spaces/{space_id}/observe` handler
(`vc_spaces.py:112`) already does exactly "enroll-if-needed + capture live config":
`resolve_or_enroll_bound` auto-enrolls a brand-new space and `capture_on_open` writes the
first version. The create hook does **not** re-implement any of that — it invokes the same
seam once, right after `create_genie_space` returns, from inside the OBO request context.
A single fail-soft helper (`capture_initial_version(request, space_id)`) reads the runtime
off `app.state.vc_observe` (`main.py:167`) and the actor off `request.state.vc_auth`
(`main.py:124`), so no create-path code needs to know the binding id or the observer
internals. The observer's fingerprint dedup makes a later tab-open capture idempotent (it
returns the existing head, exactly the "no changes" branch §19/§20 already rely on), so the
hook is safe to add without touching the tab path.

**Tech Stack:** FastAPI (backend); pytest via `./scripts/test.sh`. No frontend change (the
new version simply appears in the existing history list).

**Spec:** this doc's CUJ-1 capture semantics + §19/§20. The hook reuses the observe seam
verbatim; it introduces no new fingerprint, wire value, or governed table.

### Global Constraints
- Backend Tasks run `./scripts/test.sh` (**baseline 3326**, combined suites), print the
  import path + resolved sqlglot version, and leave `git status -- uv.lock` clean.
- **Creation is never blocked by VC.** Every failure path in the helper is swallowed and
  logged; `create_space_endpoint` and the agentic `created` event behave identically when
  VC is unintegrated (`vc_observe is None`), disabled (`vc_writes_enabled` not true), or
  when `capture_on_open` raises.
- The hook runs **inside the OBO request context** (never a detached background task) so the
  `live_reader` reads the live serialized space under the creator's token — the app SP has
  no grant on a just-created user-owned space and would 403 (same reason the observe route
  injects `live_reader`, `vc_spaces.py:127-129`).
- No change to the observer, ledger, registry, `OWNER_SPECS`, or the grant matrix. The ONE
  contract change is **purely additive**: a new `Origin.CREATE` enum member (Task 1). No
  existing wire value, fingerprint, or governed table is altered.
- Frontend Tasks keep vitest green and `npx tsc --noEmit` clean — adding `Origin.CREATE`
  forces a new `ORIGIN_META` entry (exhaustive `Record<Origin, …>`, tsc-enforced).

### Design decisions
- **VC-D-init1 (hook shape):** a single fail-soft helper
  `capture_initial_version(request, space_id) -> None`. It no-ops when
  `getattr(request.app.state, "vc_observe", None) is None`, when
  `runtime.flags.enabled("vc_writes_enabled") is not True`, or when
  `getattr(request.state, "vc_auth", None) is None`; otherwise it resolves the actor via
  `runtime.identity.actor(auth)` and calls the observe seam. All exceptions are caught and
  logged at `warning`. Lives next to the observe app glue in
  `platform/app_observe.py` (the module that already owns `vc_auth_from_request` and router
  mounting), keeping the create routers free of VC internals.
- **VC-D-init2 (identity/OBO):** the ledger actor is the authenticated human
  (`actor_override=actor`, mirroring `space_observe`), and the live read runs under OBO via
  `live_reader=lambda: get_genie_space(space_id)`. The lease/status/append still run as the
  SP executor inside `capture_on_open`.
- **VC-D-init3 (dedup/idempotency):** the hook does **not** guard against a later tab-open
  capture. `capture_on_open` fingerprints the config; a redundant call returns the existing
  head instead of appending a duplicate (the §19/§20 "no changes since the last captured
  version" branch). So double-capture is harmless by construction.
- **VC-D-init4 (origin) — LOCKED: add `Origin.CREATE`.** `Origin` (`contracts.py:82`) today
  is `WORKBENCH/EXTERNAL/OPTIMIZER/RESTORE/PROMOTION/UNKNOWN`. We add a new member
  `CREATE = "create"` so the timeline reads "born in the workbench" distinctly from a
  tab-open auto-capture (`WORKBENCH` → "Auto-captured"). This is a **purely additive**
  contract change landed FIRST (Task 1) because two surfaces enforce enum totality:
  the golden round-trip asserts the enum's ordered values equal the fixture
  (`test_vc_contracts.py:32`, fixture `enums.json:26`), and the frontend `ORIGIN_META` is an
  exhaustive `Record<Origin, …>` (`version-format.ts:9`) that fails `tsc` on a missing key.
  The member is APPENDED (never reordered) so the ordered-list assertion stays a pure add.
  The helper (VC-D-init2) then passes `origin=vc.Origin.CREATE`.
- **VC-D-init5 (agentic threading):** the agentic Create flow emits `created` deep inside an
  async SSE generator (`create_agent.py:344`) with no `Request` handle. Rather than couple
  the agent to VC, `agent_chat` (`create.py:184`, which already has `request`) builds a
  bound callback `vc_capture: Callable[[str], None]` and threads it into the agent run;
  `create_agent` invokes it via `loop.run_in_executor(...)` (the precedent at
  `create_agent.py:826`) right after `session.space_id` is set. The parameter defaults to
  `None`, so existing tests and non-VC deployments are unaffected.

### File structure
- `backend/services/version_control/contracts.py` — append `Origin.CREATE = "create"`
  (`Origin`, `contracts.py:82`).
- `backend/tests/fixtures/vc_contracts/enums.json` — append `"create"` to the `Origin` list
  (`enums.json:26`; golden test `test_vc_contracts.py:32`).
- `frontend/src/types/version-control.ts` — add `'create'` to the `Origin` union
  (`version-control.ts:4`).
- `frontend/src/components/version-control/version-format.ts` — add a `create:` entry to
  `ORIGIN_META` (`version-format.ts:9`).
- `backend/services/version_control/platform/app_observe.py` — **new** helper
  `capture_initial_version(request, space_id)` (VC-D-init1/2/3/4).
- `backend/routers/create.py` — `create_space_endpoint` gains `request: Request` and calls
  the helper after creation; `agent_chat` builds and threads the `vc_capture` callback.
- `backend/services/create_agent.py` — the run/session entrypoint accepts an optional
  `vc_capture` callback and invokes it (via `run_in_executor`) after the `created` event.
- `backend/tests/test_vc_create_hook.py` — **new** unit tests for the helper (no-op guards +
  positive path + swallow-on-raise).
- `backend/tests/test_create_*.py` — extend the create-endpoint tests to assert creation is
  unaffected when the hook is present and to assert the hook is invoked once with the new
  `space_id`.

---

### Task 1 — Introduce `Origin.CREATE` across the wire (LANDS FIRST)

**LANDED** (commit `9c0429cb`): `Origin.CREATE = "create"` appended (no reorder) in
`contracts.py` + `enums.json`; `'create'` added to the TS union; `ORIGIN_META.create =
{ label: 'Created', variant: 'success', Icon: FilePlus }`. Verify: golden round-trip green
(`1 passed`), frontend `tsc --noEmit` + `eslint` clean.

**Files:**
- Modify: `backend/services/version_control/contracts.py` (`Origin`, `contracts.py:82`)
- Modify: `backend/tests/fixtures/vc_contracts/enums.json` (`Origin` list, `enums.json:26`)
- Modify: `frontend/src/types/version-control.ts` (`Origin` union, `version-control.ts:4`)
- Modify: `frontend/src/components/version-control/version-format.ts` (`ORIGIN_META`, `version-format.ts:9`)

**Why first:** the golden round-trip test asserts the enum's ordered values equal the
fixture (`test_vc_contracts.py:32` — `[member.value for member in Origin] == values`), and
`ORIGIN_META` is an exhaustive `Record<Origin, …>` (tsc fails on a missing key). Landing the
enum before the helper keeps every later Task green.

- [x] **Step 1: APPEND** `CREATE = "create"` to `Origin` (do **not** reorder existing members
  — the assertion is order-sensitive, so it must be a pure trailing add):

```python
class Origin(str, Enum):
    WORKBENCH = "workbench"
    EXTERNAL = "external"
    OPTIMIZER = "optimizer"
    RESTORE = "restore"
    PROMOTION = "promotion"
    UNKNOWN = "unknown"
    CREATE = "create"
```

- [x] **Step 2:** Append `"create"` (same trailing position) to the `Origin` array in
  `enums.json`, then `./scripts/test.sh -k test_vc_wire_contract_golden_roundtrip` → green.

- [x] **Step 3:** Add `'create'` to the `Origin` union (`version-control.ts:4`) and a
  `create:` entry to `ORIGIN_META` (`version-format.ts:9`) — landed as
  `create: { label: 'Created', variant: 'success', Icon: FilePlus }`.
  `npm run lint` + `npx tsc --noEmit` green (tsc enforces the exhaustive record).

- [x] **Step 4:** `./scripts/test.sh -k test_vc_wire_contract_golden_roundtrip` green; `git status -- uv.lock` clean. **LANDED**.

---

### Task 2 — Fail-soft `capture_initial_version` helper

**LANDED:** `capture_initial_version(request, space_id)` added to `app_observe.py` (module
`logger`; lazy imports of `resolve_or_enroll_bound`/`get_genie_space` inside the try;
`origin=vc.Origin.CREATE`, `actor_override=actor`, OBO `live_reader`). `test_vc_create_hook.py`
covers all five behaviors (3 no-op guards, positive CREATE-origin capture incl. the
`live_reader` lambda, swallow-on-raise). Verify: `5 passed`.

**Files:**
- Modify: `backend/services/version_control/platform/app_observe.py`
- Create: `backend/tests/test_vc_create_hook.py`

**Interfaces — Produces:** `capture_initial_version(request, space_id: str) -> None`
(best-effort; returns `None` always; never raises).

- [x] **Step 1: Write failing tests** covering the four behaviors — (a) no-op when
  `request.app.state.vc_observe is None`; (b) no-op when `vc_writes_enabled` is not true;
  (c) no-op when `request.state.vc_auth is None`; (d) positive path calls
  `resolve_or_enroll_bound(runtime, space_id=...)` then `runtime.observer.capture_on_open`
  once with `actor_override` set and a `live_reader`; (e) swallows an exception raised by
  `capture_on_open` (still returns `None`). Use fakes for `runtime`/`request` (mirror the
  `_FakeTagStore`/`_runtime` shape in `test_vc_spaces_router.py`).

- [x] **Step 2: Run it, confirm it fails** — `./scripts/test.sh backend/tests/test_vc_create_hook.py`.

- [x] **Step 3: Implement** the helper:

```python
# backend/services/version_control/platform/app_observe.py
import logging

logger = logging.getLogger(__name__)

def capture_initial_version(request, space_id: str) -> None:
    """Best-effort first VC capture for a freshly created space. Never raises —
    creation must succeed even when VC is off/unintegrated or the capture fails."""
    runtime = getattr(request.app.state, "vc_observe", None)
    if runtime is None:
        return
    if runtime.flags.enabled("vc_writes_enabled") is not True:
        return
    auth = getattr(request.state, "vc_auth", None)
    if auth is None:
        return
    try:
        from backend.services.version_control import contracts as vc
        from backend.services.version_control.observe_optimizer import resolve_or_enroll_bound
        from backend.services.genie_client import get_genie_space
        actor = runtime.identity.actor(auth)
        binding = resolve_or_enroll_bound(runtime, space_id=space_id)
        runtime.observer.capture_on_open(
            binding, actor, origin=vc.Origin.CREATE, actor_override=actor,
            live_reader=lambda: get_genie_space(space_id))
    except Exception:
        logger.warning("initial VC capture failed for %s", space_id, exc_info=True)
```

- [x] **Step 4:** `./scripts/test.sh backend/tests/test_vc_create_hook.py` green (`5 passed`); import resolves inside this checkout; `git status -- uv.lock` clean. **LANDED**.

---

### Task 3 — Wire the REST wizard (`create_space_endpoint`)

**LANDED:** top-level import of `capture_initial_version`; `create_space_endpoint` gains
`request: Request` and calls the hook after successful creation (best-effort, never raises).
New test `backend/tests/test_create_space_hook.py` (2 passed): hook invoked once with the new
space id; creation returns 200 with the real helper when no `vc_observe` runtime is set
(default/unintegrated no-op). No regression in `test_create_agent_model_selection.py`.

**Files:**
- Modify: `backend/routers/create.py` (`create_space_endpoint`, currently `create.py:143`)
- Create: `backend/tests/test_create_space_hook.py`

**Current code:**

```python
@router.post("", response_model=CreateSpaceResponse)
async def create_space_endpoint(body: CreateSpaceRequest):
    try:
        result = create_genie_space(...)
    ...
    return CreateSpaceResponse(space_id=result["genie_space_id"], ...)
```

- [x] **Step 1:** Endpoint test (`test_create_space_hook.py`): hook invoked once with the new
  `genie_space_id`; and creation returns 200 via the REAL helper when the app has no
  `vc_observe` runtime (the fail-soft guarantee lives in the helper, so the default no-op is
  the correct thing to assert at the call site).

- [x] **Step 2:** Add `request: Request` to the signature and call the helper after creation:

```python
async def create_space_endpoint(body: CreateSpaceRequest, request: Request):
    ...
    space_id = result["genie_space_id"]
    capture_initial_version(request, space_id)   # best-effort, never raises
    return CreateSpaceResponse(space_id=space_id, ...)
```

- [x] **Step 3:** `./scripts/test.sh backend/tests/test_create_space_hook.py` green (`2 passed`). **LANDED**.

---

### Task 4 — Wire the agentic Create flow (`created` event)

**LANDED:** `vc_capture` threaded `chat → _fast_create → _create_space_with_repair` (all
optional, default `None`); new `_maybe_capture(session, vc_capture)` fires the hook off the
event loop via `run_in_executor(None, run_in_context(vc_capture, space_id))` — `run_in_context`
propagates the OBO ContextVar into the worker thread so the helper's live read runs under the
creator's token. Fired at BOTH genuine `created` sites (main loop + `_create_space_with_repair`
success branch); the idempotent-guard branch (`already_existed`) deliberately does NOT capture.
Router binds `vc_capture = lambda sid: capture_initial_version(request, sid)`. New
`test_create_agent_hook.py` (8 passed) covers both paths, the guard, all `_maybe_capture` edge
cases, and default-None safety; one `FakeAgent.chat` stub updated to accept the new kwarg.

**Files:**
- Modify: `backend/routers/create.py` (`agent_chat`, `create.py:184`)
- Modify: `backend/services/create_agent.py` (entrypoint `chat`, `create_agent.py:103`; the
  TWO `created` emission sites, `create_agent.py:345` and `create_agent.py:850`)
- Create: `backend/tests/test_create_agent_hook.py`
- Modify: `backend/tests/test_create_agent_model_selection.py` (FakeAgent stub kwarg)

**⚠ Two creation sites — both must fire the hook.** `session.space_id` is set and a `created`
event emitted in **both** the main tool-result loop (`create_agent.py:345`) **and**
`_create_space_with_repair` (`create_agent.py:850`, used by the auto-chain and by
`_fast_create`, `create_agent.py:860`). A hook at only one site silently misses spaces made
via the repair/fast path. Prefer a single private helper (e.g. `self._emit_created(...)` or a
`_maybe_capture(session, vc_capture, loop)`) called from both sites so the capture and the
`created` event stay in lockstep by construction.

- [x] **Step 1:** Threaded `vc_capture: Callable[[str], None] | None = None` from `chat` down
  through `_fast_create` and `_create_space_with_repair` (all call sites updated); not stored
  on the shared agent instance.

- [x] **Step 2:** Added `_maybe_capture`; fired after the `created` emission at both genuine
  sites. OBO is preserved with `run_in_context` (not a bare `run_in_executor(None, vc_capture,
  id)` — the ContextVar would not reach the thread otherwise); wrapped in try/except.

- [x] **Step 3:** `agent_chat` binds `vc_capture = lambda sid: capture_initial_version(request,
  sid)` and passes it to `agent.chat(...)` (OBO token is already set inside `event_stream`
  before the call, so `run_in_context` snapshots it).

- [x] **Step 4:** `test_create_agent_hook.py` (8 passed): both paths capture once with the new
  id; the idempotent-guard branch does NOT capture; default `None` path unchanged.

- [x] **Step 5:** `./scripts/test.sh` green (see §21 Verify). **LANDED**.

---

### Verify (whole §21, on completion) — DONE
- `./scripts/test.sh` — **3341 passed**, 17 deselected (baseline 3326 + 15 new §21 tests:
  5 `test_vc_create_hook` + 2 `test_create_space_hook` + 8 `test_create_agent_hook`; Task 1
  added no test, only fixture parity). Import resolves inside this checkout
  (`packages/genie-space-optimizer/.../__init__.py`); sqlglot 30.0.3 under `--frozen`.
- `git status -- uv.lock` clean.
- **Enum parity (Task 1):** `test_vc_wire_contract_golden_roundtrip` green — the `Origin`
  enum (`contracts.py:82`) and the fixture (`enums.json:26`) both end in `create`, appended
  (not reordered). Frontend `npx tsc --noEmit` green (the exhaustive `ORIGIN_META` record now
  has a `create` key), vitest + eslint green.
- Grep sweep (paste output): `capture_initial_version` is called from **both** create paths
  (`create_space_endpoint` and `agent_chat`) and nowhere re-implements enroll/capture — the
  only `resolve_or_enroll_bound` + `capture_on_open` call sites remain `vc_spaces.py` and the
  new helper. The only `Origin.CREATE` producer is the helper.
- MV-D9: no gap-report sites apply (VC plane, not the mv-advisor surface).
