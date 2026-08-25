# MV write-to-read exposure matrix

Every column the metric-view feature writes, and how it reaches (or deliberately
does not reach) a client. Built once at Prompt 14 and **pinned** by
`packages/genie-space-optimizer/tests/unit/test_exposure_matrix.py`, which walks
the live `CREATE TABLE` bodies + `ADDITIVE_COLUMN_MIGRATIONS` in
`optimization/ddl.py` and fails if a column is missing here or carries an empty
classification. A column added to a covered table without classifying its
exposure fails that test — which is the moment the Prompt 11 / Prompt 13 class of
defect ("wire to a read the writing side never exposed") is caught, one PLAN
phase earlier than it was on this branch.

## Scope

Twice on this branch a "wire to existing endpoints" instruction assumed a read
the writing side never exposed: the space-scoped proposals read (caught at
Prompt 11) and the created-objects/lift read (caught at the Prompt 13 PLAN).
Both are now SERVED (routes below); the matrix exists so the *next* one is caught
by a test, not a PLAN.

Scope is **MV columns wherever they live**, not only the three `genie_opt_mv_*`
tables the Prompt 14 body names. The one MV column outside those tables —
`run_kind` on `genie_opt_runs` (MV-D23) — is included, because leaving it out
would let the sentinel discriminator drift unclassified. The pin therefore walks
the three MV tables plus that single migration entry.

## Legend

| Token | Meaning |
|---|---|
| **SERVED** | Returned to a client by a named HTTP route (the route serving it is named in the last column). |
| **DELIBERATELY INTERNAL** | Written and consumed server-side by design; never surfaced. The reason is named (audit ledger, dedup key, consent-verification plumbing, sentinel discriminator, replay source). Not a gap. |
| **GAP** | Written, but no read serves it and no reason justifies withholding it. A finding to raise (a row in `mv-advisor-gap-report.md`), **not** to fix in this prompt. |

Routes are under the `/api/auto-optimize` prefix (`backend/routers/auto_optimize.py`,
included in `backend/main.py`). The ten MV routes:

1. `POST mv/probe` — entitlement probe (writes consents)
2. `GET runs/{run_id}/mv-proposals` — run-scoped proposals
3. `GET spaces/{space_id}/mv-proposals` — space-scoped proposals (Prompt 11)
4. `POST spaces/{space_id}/mv/suggest` — on-demand suggest (MV-D23)
5. `POST spaces/{space_id}/mv/register` — bring-your-own registration (MV-D24)
6. `GET spaces/{space_id}/semantic-graph` — semantic model (re-reads proposals)
7. `GET runs/{run_id}/mv-ddl` — rendered DDL artifact (not the MV tables)
8. `POST mv/proposals/{suggestion_id}/decision` — approve/reject (writes decision)
9. `POST mv/created/{suggestion_id}/drop` — detach-never-drop (reads gates)
10. `GET runs/{run_id}/mv-created` — created objects + lift (Prompt 13 step 0)

## Matrix

### `genie_opt_mv_candidates`

| Table | Column | Exposure | Route / Reason |
|---|---|---|---|
| genie_opt_mv_candidates | dedup_fingerprint | SERVED | routes 2/3/4/6 (`MvProposal.dedup_fingerprint`) |
| genie_opt_mv_candidates | target_space_id | SERVED | route 3 (space-scoped key) |
| genie_opt_mv_candidates | suggestion_id | SERVED | routes 2/3/4/6/8 |
| genie_opt_mv_candidates | run_id | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | candidate_type | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | confidence_score | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | tier | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | proposed_object | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | score_components_json | SERVED | routes 2/3/4/6 (`score_components`) |
| genie_opt_mv_candidates | evidence_json | SERVED | routes 2/3/4/6 (`evidence`) |
| genie_opt_mv_candidates | provenance_json | SERVED | routes 2/3/4/6 (`provenance`) |
| genie_opt_mv_candidates | alternatives_json | SERVED | routes 2/3/4/6 (`alternatives`) |
| genie_opt_mv_candidates | conflicts_json | SERVED | routes 2/3/4/6 (`conflicts`) |
| genie_opt_mv_candidates | requested_mode | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | effective_mode | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | decision | SERVED | read routes 2/3; written by route 8 |
| genie_opt_mv_candidates | decided_by | SERVED | routes 2/3 |
| genie_opt_mv_candidates | decided_at | SERVED | routes 2/3 |
| genie_opt_mv_candidates | suppressed_until | SERVED | routes 2/3 (`MvProposal.suppressed_until`) |
| genie_opt_mv_candidates | approved_for_rerun | SERVED | routes 2/3 (also route-3 query filter) |
| genie_opt_mv_candidates | created_at | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | updated_at | SERVED | routes 2/3/4/6 |
| genie_opt_mv_candidates | yaml_text | SERVED | route 7 (`mv-ddl`) returns it verbatim as `MvDdlArtifact.yaml_text` (models.py:476), and `create_ddl` embeds the same body inside the `AS $$…$$` fence. The prior "DELIBERATELY INTERNAL / never this raw column" token was false twice over: route 7 served the raw column directly for artifact-backed runs even before the fallback, and advice runs (which write no artifact) 404'd instead — the exact gap the matrix exists to surface. Prompt 15.1 added the candidate-row fallback so advice runs serve it too. Still the MV-D23 replay source for `mv_create._load_ddl_artifact`. |

### `genie_opt_mv_consents`

| Table | Column | Exposure | Route / Reason |
|---|---|---|---|
| genie_opt_mv_consents | probe_id | SERVED | route 1 (`MvProbeResult.probe_id`) |
| genie_opt_mv_consents | run_id | DELIBERATELY INTERNAL | consent→run linkage; the audit answer to "which run used this consent", not a client field |
| genie_opt_mv_consents | granted_by | DELIBERATELY INTERNAL | consent audit ledger (the OBO identity every write ran under) |
| genie_opt_mv_consents | granted_at | DELIBERATELY INTERNAL | consent audit ledger |
| genie_opt_mv_consents | target_catalog | DELIBERATELY INTERNAL | consent record; route 1 echoes the *requested* target, not a read of this stored column |
| genie_opt_mv_consents | target_schema | DELIBERATELY INTERNAL | consent record (as above) |
| genie_opt_mv_consents | materialize_consented | SERVED | route 1 (echoed on `MvProbeResult`) |
| genie_opt_mv_consents | probe_results_json | DELIBERATELY INTERNAL | audit copy of the probe verdict; route 1 returns the live probe payload, not this stored blob |
| genie_opt_mv_consents | verdict | SERVED | route 1 (`MvProbeResult.verdict`) |
| genie_opt_mv_consents | reverified_at_trigger | DELIBERATELY INTERNAL | consent-verification plumbing: the job refuses to attach without a fresh re-verify; never a client field |
| genie_opt_mv_consents | downgrade_reason | SERVED | route 10 (surfaced alongside created objects) |
| genie_opt_mv_consents | updated_at | DELIBERATELY INTERNAL | consent audit-ledger timestamp |

### `genie_opt_mv_created_objects`

| Table | Column | Exposure | Route / Reason |
|---|---|---|---|
| genie_opt_mv_created_objects | run_id | SERVED | route 10 |
| genie_opt_mv_created_objects | suggestion_id | SERVED | route 10 |
| genie_opt_mv_created_objects | full_name | SERVED | routes 10/9 |
| genie_opt_mv_created_objects | created_by | SERVED | route 10 |
| genie_opt_mv_created_objects | created_at | SERVED | route 10 |
| genie_opt_mv_created_objects | attach_patch_id | SERVED | route 10 |
| genie_opt_mv_created_objects | baseline_eval_run_id | SERVED | route 10 |
| genie_opt_mv_created_objects | post_attach_eval_run_id | SERVED | route 10 |
| genie_opt_mv_created_objects | status | SERVED | routes 10/9 |
| genie_opt_mv_created_objects | on_regression_action | SERVED | route 10 |
| genie_opt_mv_created_objects | updated_at | DELIBERATELY INTERNAL | lifecycle audit timestamp (moves with every status transition); the transitions themselves are served via `status` |
| genie_opt_mv_created_objects | lift_report_json | SERVED | route 10 (`lift_report`) |
| genie_opt_mv_created_objects | provenance | SERVED | route 10 (`MvCreatedObject.provenance`, Prompt 14.1). MV-D24 create-path discriminator: written by route 5 (register, `USER_CREATED`), gates route 9 (drop refuses `USER_CREATED`) and attach's identity relaxation server-side. Route 10 now returns it (NULL → `OBO_CREATED`), so a reloaded UI hides the Drop affordance the mockups (frame 8b) omit. Closed the exposure sweep's first GAP. |

### `genie_opt_mv_suppressions`

Per-measure suppression ledger (MV-D30 as-implemented, Prompt 15.3). Every column
is **DELIBERATELY INTERNAL — until a surface explains suppression to the user.**
The ledger exists solely so the advisor's consolidation
(`mv_state.load_mv_suppressed_fingerprints` / warehouse twin) can drop rejected
members before bundling. No route serves a suppression row — the client only ever
sees its *effect* (a rejected measure absent from future bundles). It is written
by route 8's fan-out (`wh_suppress_mv_measures`), never read by a client.

The classification is correct today but conditional by design: a user who rejects
a bundle and later wonders why a measure never reappears has no surface that
explains it. If 15.4's hydrated panel grows a "suppressed measures" disclosure,
`measure_fingerprint`, `suppressed_until`, `originating_suggestion_id`, and
`reason` flip to **SERVED** — and that flip is planned, not a correction. This
note is the matrix doing its job: making the eventual flip visible in advance.

| Table | Column | Exposure | Route / Reason |
|---|---|---|---|
| genie_opt_mv_suppressions | target_space_id | DELIBERATELY INTERNAL | suppression-ledger key; scopes the internal decisions-reader, never a client field |
| genie_opt_mv_suppressions | measure_fingerprint | DELIBERATELY INTERNAL | the per-measure identity suppression is enforced at; consumed only by the advisor consolidation to drop rejected members before bundling |
| genie_opt_mv_suppressions | suppressed_until | DELIBERATELY INTERNAL | decay window read by the internal reader (NULL = indefinite); the served copy is the bundle row's own `suppressed_until` |
| genie_opt_mv_suppressions | originating_suggestion_id | DELIBERATELY INTERNAL | audit link back to the bundle rejection that fanned out to this row |
| genie_opt_mv_suppressions | reason | DELIBERATELY INTERNAL | suppression provenance (e.g. `bundle_rejected`); audit-only |
| genie_opt_mv_suppressions | created_at | DELIBERATELY INTERNAL | ledger audit timestamp |
| genie_opt_mv_suppressions | updated_at | DELIBERATELY INTERNAL | ledger audit timestamp (a re-rejection refreshes the window) |

### `genie_opt_runs` (MV columns only)

| Table | Column | Exposure | Route / Reason |
|---|---|---|---|
| genie_opt_runs | run_kind | DELIBERATELY INTERNAL | MV-D23 sentinel discriminator; consumed by the pinned `MV_ADVICE_RUN_EXCLUSION` predicate at every listing site to *exclude* advice runs — never surfaced as a client field |
