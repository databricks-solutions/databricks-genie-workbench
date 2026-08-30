# Ontology — Phase 2 Goal-Mode driver

Copy-paste launcher for building **Phase 2 of the Ontology page** (batch
materialization + Lakebase mirror) with a long-running agent (Claude Code / Cursor
Goal Mode). Run it on the **`ontology`** branch, on top of the shipped Phase-1 spine.
The prompt is bounded so the agent builds only the **offline-verifiable** slice and
stops at the deploy gate.

- **Spec (source of truth):** `docs/design/ontology-phase2-build.md`
- **Phase-1 baseline (already shipped):** `docs/design/ontology-phase1-build.md`
- **Design context:** `docs/design/ontology-engine-architecture.md` (§2 thin-page,
  §7 data model, the **L7 persistence** + **L8 serving** subsections)
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (Prompt 17c; MV-D39 /
  D41 / D42 / D43 / D45)
- **Visual contract:** reuse `docs/design/mockups/17.0b` (taxonomy) + `17.0c` (tags
  lens) — **no new frames**; add only a freshness chip + "Refresh ontology" button
- **Project rules:** `AGENTS.md`

Unlike Phase 1, Phase-2 acceptance is **offline for the code** but **deploy-gated for
verification**: the batch job, DABs schedule, live SP reads, Delta write, and
synced-table mirror can only be validated in a deployed Databricks App. The agent
builds and green-tests the offline slice, then **stops**; a human runs the deploy +
first materialize.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build the OFFLINE slice of Phase 2 of the Ontology page — batch
materialization + Lakebase mirror + reader swap — exactly as specified. Work only on
the current branch (ontology), on top of the shipped Phase-1 spine.

SPEC (source of truth, follow section-by-section):
  docs/design/ontology-phase2-build.md   ← §1 scope, §2 decisions, §3 layout,
  §4 contracts, §5 TS mirrors, §6 routes, §7 DDL+idempotency, §8 job, §9 frontend,
  §10 grants/deploy, §11 tests, §12 DoD
BASELINE: docs/design/ontology-phase1-build.md (do NOT regress it)
DESIGN CONTEXT: docs/design/ontology-engine-architecture.md (§2, §7, L7, L8)
DECISIONS: docs/design/mv-advisor-playbook.md (Prompt 17c; MV-D39/D41/D42/D43/D45)
VISUAL CONTRACT: reuse mockups 17.0b + 17.0c; add ONLY a freshness chip + a
  "Refresh ontology" button with zero-burden copy (no job/Delta/synced-table jargon).
PROJECT RULES: AGENTS.md (read it first).

REUSE, DON'T FORK:
  genie_space_optimizer/optimization/ddl.py          (Delta-DDL pattern for ontology/ddl.py)
  genie_space_optimizer/jobs/run_intake_and_snapshot.py (job-task shape: notebook-source,
                                                     make_workspace_client, params in / Delta out)
  genie_space_optimizer/integration/trigger.py + backend job_launcher (jobs.run_now → GSO_ONT_JOB_ID)
  backend/services/gso_lakebase.py                   (synced-table reads from Lakebase → mirror.py)
  scripts/setup_synced_tables.py + scripts/deploy_lib/ (register the new genie_ont_* synced tables)
  backend/ontology/services/{tag_graph,taxonomy,dedupe}.py (extract PURE transforms to the wheel;
                                                     backend imports them back — contracts UNCHANGED)
  backend/watch/services/system_tables.py            (SP reads + TTL cache for the live fallback)

HARD GUARDRAILS (do not violate):
  - Do NOT change any Phase-1 route RESPONSE contract (§4). The ONLY new model is
    OntologyRefreshStatus; the ONLY new routes are GET/POST /api/ontology/refresh.
  - Write ONLY genie_ont_tag_graph + genie_ont_taxonomy_snapshot + genie_ont_runs.
    Create the Phase-3 tables (domains/members/pages/consents/suppressions) EMPTY
    (schema only) and write NOTHING to them.
  - The only UC writes are the genie_ont_* Delta snapshot MERGEs. NO SET TAG,
    NO CREATE GOVERNED TAG, NO manage_uc_tags — anywhere (backend OR the wheel).
  - Idempotent: derived keys + MERGE (incl. NOT MATCHED BY SOURCE DELETE, scoped to
    workspace_id). A re-run MUST NOT duplicate rows.
  - Reader swap degrades, never blocks: mirror when fresh, else the Phase-1 live-SP
    path (MV-D43). Never block a request on the job.
  - Do NOT pull forward §12: no proposals, no embeddings, no Lakebase Search
    (do NOT enable it — irreversible), no clustering/Louvain, no external context /
    web search, no SET TAG apply.
  - NO DEPLOY. Do NOT run scripts/deploy.sh, databricks bundle deploy, uvicorn, or
    npm run dev. Do NOT run the job on a real workspace. Do NOT enable Lakebase Search.
  - Keep Pydantic (§4) and TypeScript (§5) mirrors 1:1.

ACCEPTANCE (all must be true before you declare done):
  - ./scripts/test.sh is green, including: the contract-frozen guard (Phase-1 models
    byte-identical), mirror-vs-live PARITY on a fixture, idempotent re-run (no dup
    rows, NOT-MATCHED-BY-SOURCE delete), reader-swap fallback, freshness states, and
    the EXTENDED read-only firewall that also scans the wheel's ontology/ package.
  - The taxonomy (17.0b) + tags lens (17.0c) render mirror-backed data with a
    zero-burden freshness chip; the admin "Refresh ontology" button calls POST
    /refresh and polls GET /refresh.
  - cd frontend && npm run lint passes; tsc is clean.
  - The §12 "Offline done" checklist reads true.

WORKFLOW: extract shared transforms → wheel ontology/{transforms,ddl,graph,materialize}
  → job task run_ontology_materialize.py → backend mirror.py + refresh.py → reader
  swap in tag_graph.py/taxonomy.py → refresh router + main.py wiring → frontend
  types/api → freshness chip + button → tests. Run ./scripts/test.sh after each slice.
  Stop and ask if a spec detail is ambiguous or if any guardrail would have to be crossed.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../genie_space_optimizer/ontology/, jobs/,
                           #   backend/ontology/{services,routers,models}, frontend/src/ontology/,
                           #   databricks.yml, scripts/setup_synced_tables.py, scripts/deploy_lib/, backend/tests/
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..

git add packages/genie-space-optimizer backend/ontology frontend/src/ontology \
        databricks.yml scripts/setup_synced_tables.py scripts/deploy_lib backend/tests
git commit -m "feat(ontology): Phase 2 batch materialization + Lakebase mirror + reader swap (MV-D41)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update      # builds the wheel, registers the ontology job + synced tables,
                                  # injects GSO_ONT_JOB_ID, redeploys the app
# In the live app: click "Refresh ontology" → confirm the run succeeds, the synced
# tables populate Lakebase, and the taxonomy/tags panels flip from "Live view"
# (source=live) to "Updated …" (source=mirror) with a real as_of. Confirm the nightly
# schedule is set. Re-run once to confirm idempotency (no duplicate rows).
```

The batch job, DABs schedule, SP system-table reads, the Delta write, and the
synced-table mirror can only be validated in a deployed app — that boundary is why
Phase 2's offline slice stops here.
