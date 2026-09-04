# Ontology — Re-grain to metastore (MV-D49) Goal-Mode driver

Copy-paste launcher for the **metastore re-grain refactor** (MV-D49) with a long-running
agent (Claude Code / Cursor Goal Mode). Run it on the **`ontology`** branch, on top of
the shipped Phase-1/2/3a/3b spine and **before** Phase 3c (17f). It is a **pure re-grain**
— it changes the partition key of every `genie_ont_*` table from `workspace_id` to
`metastore_id`, re-scopes the idempotent MERGE, runs the batch once per metastore, and
demotes `workspace_id` to provenance. **No feature, no new table, no dependency, no
response-contract change.**

- **Spec (source of truth):** `docs/design/implemented/ontology-regrain-build.md`
- **Design context:** `docs/design/ontology-engine-architecture.md` §7 (grain) + §8
- **Decision:** `docs/design/mv-advisor-playbook.md` (MV-D49; also MV-D42/D41/D26)
- **Baseline (no regress):** `ontology-phase3b-build.md` + `3a` + `2` + `1`
- **Project rules:** `AGENTS.md`

Acceptance is **offline for the code, deploy-gated for verification** (run the job once
per metastore, confirm one ontology, no per-workspace duplicates). No dependency changes,
so `uv.lock` is untouched.

---

## Driver prompt (paste verbatim)

```text
GOAL: Re-grain the ontology from workspace to METASTORE (MV-D49). Change every genie_ont_*
partition key workspace_id->metastore_id, re-scope the idempotent MERGE delete to
metastore_id, run the batch once per metastore, demote workspace_id to provenance. Pure
refactor — NO feature/new table/dependency/response-contract change. Branch: ontology, atop
shipped Phase-1/2/3a/3b, BEFORE 17f.

SPEC (§1-§12): ontology-regrain-build.md. DESIGN: ontology-engine-architecture.md §7+§8.
DECISION: mv-advisor-playbook.md MV-D49 (+MV-D42/D41/D26). BASELINE (no regress):
phase3b+3a+2+1. RULES: AGENTS.md.

REUSE, DON'T FORK:
  - ddl.build_snapshot_merge_sql — SAME MERGE builder; only its delete-predicate arg changes
    workspace_id->metastore_id. No new persistence path.
  - materialize.run_materialize + SparkSnapshotWriter — rename the scope param through the
    reader/writer seam; keep them injectable (offline tests).
  - WorkspaceClient().metastores.current().metastore_id — the resolver (job AND backend);
    already available, no new grant.

HARD GUARDRAILS:
  - KEYS lead metastore_id: TAG_GRAPH=[metastore_id,tag_key]; TAXONOMY=[metastore_id];
    IDENTITY=[metastore_id,canonical_id,member_ref]; DOMAIN=[metastore_id,domain_id];
    MEMBER=[metastore_id,domain_id,asset_fqn]. Run ledger stays keyed on run_id but carries
    metastore_id + provenance workspace_id.
  - DDL: add metastore_id STRING as the LEADING PK col on every genie_ont_* table; KEEP
    workspace_id STRING as PROVENANCE (nullable, comment 'NOT a key'). Drop NO column; CDF stays on.
  - MERGE: WHEN NOT MATCHED BY SOURCE DELETE predicate + merge(...) scope arg = metastore_id.
    A row from a DIFFERENT metastore must NOT be deleted by this run.
  - JOB run_ontology_materialize.py: resolve metastore_id (metastores.current(); else
    CURRENT_METASTORE(); else 'default' — degrade, MV-D43); add a metastore_id widget; keep
    workspace_id as PROVENANCE; pass metastore_id=... to run_materialize. Once per metastore;
    header notes the single-runner recommendation.
  - BACKEND mirror.py reads (latest_run, latest_succeeded_run, read_taxonomy_tree,
    read_tag_graph, + any domain/member reader) scope by metastore_id; resolve the app
    metastore ONCE + cache; thread through refresh/taxonomy/tags in place of workspace_id.
    NO response-shape change.
  - SYNCED TABLES: setup_synced_tables.py PKs lead metastore_id.
  - NO new API model/route/frontend/TS; NO new table; NO clustering/ER/miner logic change;
    NO SET/UNSET/CREATE GOVERNED TAG / manage_uc_tags / web_search; write set unchanged
    (snapshots + domains/members; pages/consents/suppressions still NEVER written here).
  - NO NEW DEPENDENCY (MV-D45): uv.lock untouched; uv lock --check green.
  - Do NOT build 17f (no Page miners); NO cross-metastore merge (grain = single metastore).
    NO DEPLOY (no deploy.sh/bundle deploy/uvicorn/npm dev/live job run).

ACCEPTANCE (all before done): ./scripts/test.sh green covering EVERY §11 case — contract-
frozen (routes + OntologyRefreshStatus/taxonomy/tag-lens byte-identical); keys lead
metastore_id; MERGE delete predicate metastore-scoped (assert generated SQL); CONVERGENCE
(two runs, same metastore_id + different provenance workspace_id -> ONE row set); metastore-
scoped idempotency (a different metastore's row NOT deleted); provenance retained
(workspace_id never in a key/predicate); resolver degrade; backend mirror scopes by
metastore_id; firewall unchanged. uv lock --check green; npm lint + tsc clean; §12 done.

WORKFLOW: ddl.py (metastore_id PK col + provenance workspace_id + delete predicate) ->
  materialize.py (*_KEYS + scope rename) -> transforms.py (rows carry metastore_id) ->
  run_ontology_materialize.py (resolve + per-metastore) -> backend mirror/refresh/taxonomy/
  tags (scope by metastore_id) -> setup_synced_tables.py -> test re-grain + convergence. Run
  ./scripts/test.sh per slice; stop if ambiguous or a guardrail breaks.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{ddl,materialize,transforms}.py,
                           #   packages/.../jobs/run_ontology_materialize.py, backend/ontology/**,
                           #   scripts/setup_synced_tables.py, backend/tests/**, packages/.../tests/unit/**
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..
uv lock --check            # UNCHANGED — no dependency touched (MV-D45)

git add packages/genie-space-optimizer backend/ontology backend/tests scripts
git commit -m "refactor(ontology): re-grain to metastore (MV-D49); workspace_id -> provenance"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds the wheel + redeploys the job (no dep change)
# In the live app: click "Refresh ontology" to run the materialize job, then query
#   SELECT metastore_id, count(*) FROM <gso_catalog>.<gso_schema>.genie_ont_domains GROUP BY 1
# Confirm ONE ontology per metastore (no per-workspace duplicates); workspace_id present as
# provenance only. Re-run once to confirm idempotency (identical rows).
```

The re-grain is offline and deterministic, but the metastore resolution, the Delta write,
and the synced mirror can only be validated in a deployed app — which is why this offline
slice stops here. 17f (Page miners) builds on this grain next.
