# Ontology — Phase 3e Goal-Mode driver

Copy-paste launcher for building **Phase 3e of the Ontology page** — the **Estate
Graph / "Ontology Map"** — with a long-running agent (Claude Code / Cursor Goal
Mode). Run it on the **`ontology`** branch, on top of the shipped Phase-1…3d spine.
The prompt is bounded to the **offline Step A** (persist + serve the graph + build
the three library-bakeoff mockups) and **STOPS before the frontend buildout** so a
human eyeballs the mockups and picks the library first.

- **Spec (source of truth):** `docs/design/ontology-phase3e-build.md` (Step A =
  §1/§3/§4/§6/§7/§8/§9-StepA/§11; the **bakeoff** = §5; Step B is deferred)
- **Baselines (already shipped):** `ontology-phase3a-build.md` (L2 graph),
  `ontology-phase3b-build.md` (L4 clustering → `domain_id`), 17f/17g
- **Design context:** `docs/design/ontology-engine-architecture.md` §5 (the fused
  signal graph this visualizes)
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (**Prompt 17k**;
  MV-D48 + MV-D35 / D36 / D39 / D41 / D45)
- **Visual contract:** three **static** bakeoff mockups `17.0h/i/j` (same
  `mockups.css` chrome as `17.0a–g`); **no** graph component and **no** npm dep in
  Step A
- **Project rules:** `AGENTS.md` (dependency policy — **no** Python dep is added;
  **no** npm dep in Step A; `uv.lock` untouched)

This phase is lettered **17k** (to avoid renumbering 17h–17j) but its **build order
is Phase 3e — after 17g**. Step-A acceptance is **offline**; the frontend library is
chosen by a **human bakeoff** and built in a separate follow-up run.

---

## Driver prompt (paste verbatim)

```text
GOAL: OFFLINE Step A of Phase 3e — Estate Graph / "Ontology Map". Persist the fused
signal graph + a precomputed igraph layout as a new snapshot, serve it read-only,
and stage a 3-library BAKEOFF (static mockups). Then STOP for a human to eyeball the
mockups + pick the frontend lib. Branch: ontology, atop shipped Phase-1..3d.

SPEC: ontology-phase3e-build.md — Step A = §1,§3,§4,§6,§7,§8,§11 + bakeoff §5; Step B
DEFERRED. BASELINE (no regress): phase3a/3b + 17f/17g + phase2/1. DESIGN: ontology-
engine-architecture.md §5. DECISIONS: mv-advisor-playbook.md 17k (MV-D48 + D35/D36/
D39/D41/D45). RULES: AGENTS.md.

REUSE, DON'T FORK:
  - graph.py build_signal_graph {nodes,edges} = layout INPUT; layout.py does ONLY
    layout + domain rollup (no rebuild, no re-signal).
  - materialize.py run_materialize builds the graph (~line164, discarded) + (17e)
    {node->domain_id}; feed both (+17g scores if any) to layout.py, then MERGE
    genie_ont_graph_snapshot as the LAST additive step (build_snapshot_merge_sql).
  - ddl.py — add genie_ont_graph_snapshot like genie_ont_taxonomy_snapshot (JSON blob
    keyed by workspace_id) + add to SNAPSHOT_TABLES (no proposal/consent semantics).
  - mirror.py — clone read_taxonomy_tree (synced->Delta->JSON) as read_graph_snapshot;
    graph route = taxonomy.py mirror-first serve, NO live fallback.

HARD GUARDRAILS:
  - igraph ALREADY a wheel dep (17e/MV-D39): add NO Python dep, uv.lock UNTOUCHED; add
    NO npm dep / NO EstateGraph component in Step A (Step B, after the pick).
  - ONE read model OntologyGraph{,Node,Edge,Level} + ONE read route GET /api/ontology/
    graph (mirror-only; cold->state="cold", empty, never raises) + TS mirror in
    types.ts. All Phase-1..3d models/routes BYTE-IDENTICAL.
  - WRITE only genie_ont_graph_snapshot (SP snapshot, key=workspace_id, NOT-MATCHED-BY-
    SOURCE DELETE scoped to workspace_id). NEVER write proposals/consents/suppressions
    or any tag. Route READ-ONLY. NO new signal, NO clustering re-run.
  - NO SET/UNSET TAG, NO CREATE GOVERNED TAG, NO manage_uc_tags, NO web_search;
    lakebase_* confined to similarity.py.
  - DETERMINISTIC layout: fixed igraph RNG seed + single thread. Coords NOT bit-
    identical across versions -> TESTS ASSERT STRUCTURE (node/edge set, domain_id
    colour, rollup counts), NOT exact (x,y). Two levels: domains (rollup, default) +
    assets (capped top-N by centrality; truncated=True + true node_count).
  - Degrade (MV-D43): layout LAST; failure records run 'failed' but must NOT corrupt
    earlier snapshots; empty graph -> empty snapshot, run 'succeeded'.
  - BAKEOFF (§5): 3 STATIC mockups of the SAME 17.0b estate, NO real libs / NO npm
    import: 17.0h (Sigma.js v3, flat/crisp), 17.0i (Reagraph, glow/depth/curved), 17.0j
    (Cytoscape.js, compound containers+collapse). Each: domain clusters; lineage-solid/
    co-query-dashed/scope-thin edges (incl 2 cross-domain co-query); legend; LOD+filter
    controls; hover evidence card; click->draft hint; "why this lib / trade-offs" strip.
    Do NOT pick a winner.
  - Do NOT pull forward §12: no EstateGraph.tsx/npm dep (Step B); no new signal/
    clustering (17d/17e); no external context (P4); no write/apply (P5).
  - NO DEPLOY: no deploy.sh/bundle deploy/uvicorn/npm dev/live run.

ACCEPTANCE (all before done): ./scripts/test.sh green (§11 = contract-frozen; layout-
  fixture; determinism x2 runs; top-N cap+honesty; route-shape incl cold;
  additive-safety; updated-firewall). uv.lock UNTOUCHED; npm ci --dry-run clean; lint +
  tsc clean (unused type OK). §12 "Step-A offline done" true. THEN STOP.

WORKFLOW: ddl.py (+graph_snapshot) -> layout.py (igraph layout+rollup, deterministic)
  -> materialize.py (feed graph+communities, MERGE last) -> models.py (OntologyGraph)
  -> routers/graph.py + __init__ + main.py -> mirror.read_graph_snapshot -> types.ts
  -> mockups 17.0h/i/j -> firewall/tests. Test per slice; STOP if ambiguous or a
  guardrail would be crossed.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{layout,materialize,ddl}.py,
                           #   backend/ontology/{models.py,routers/graph.py,routers/__init__.py,services/mirror.py},
                           #   backend/main.py, frontend/src/ontology/types.ts,
                           #   docs/design/mockups/17.0{h,i,j}-*-dark.html,
                           #   scripts/setup_synced_tables.py, backend/tests/test_ontology_firewall.py
                           #   NOT expected: uv.lock, frontend/package*.json (Step B only)
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..
uv lock --check            # UNCHANGED — no Python dep added in Phase 3e

git add packages/genie-space-optimizer backend frontend/src/ontology/types.ts docs/design/mockups scripts backend/tests
git commit -m "feat(ontology): Phase 3e Step A — estate-graph snapshot + /graph route + bakeoff mockups (MV-D48)"
git push -u origin ontology
```

Then **you** eyeball the bakeoff and pick the library:

```
# Open docs/design/mockups/17.0h/i/j-*-dark.html in a browser, run the §5.3 checklist,
# and record the winner in ontology-phase3e-build.md §5.4 + MV-D48.
# THEN run the deploy-and-verify gate, and only after that kick off Step B:
```

```bash
./scripts/deploy.sh --update   # writes genie_ont_graph_snapshot on the next job run
# In the live app: "Refresh ontology" -> query
#   SELECT node_count, edge_count, layout FROM <gso_catalog>.<gso_schema>.genie_ont_graph_snapshot
# and GET /api/ontology/graph. Re-run once to confirm the layout is structurally stable.
```

The layout runs offline and deterministically, but the SP read, the Delta write, and
the synced mirror can only be validated in a deployed app — which is why Phase 3e's
Step-A slice stops here, before the bakeoff pick and Step B.
