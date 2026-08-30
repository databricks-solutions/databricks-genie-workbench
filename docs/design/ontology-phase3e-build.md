# Ontology — Phase 3e: Estate Graph / "Ontology Map" (build spec)

**Status:** build-ready (offline slice + bakeoff STOP gate) · **Owner directive:**
MV-D48 (read-only force-graph of the materialized estate; `igraph`-precomputed
layout reused from MV-D39; frontend library chosen by a **bakeoff** before
buildout), inheriting MV-D35 / MV-D36 / MV-D39 / MV-D41 / MV-D45 (see
`mv-advisor-playbook.md`, **Prompt 17k**). **Design source of truth:**
`ontology-engine-architecture.md` §5 (the L2 fused signal graph + L4 clustering
this visualizes). This doc is a *buildable slice* — it persists and serves a graph
the engine **already builds in-memory and discards**; it invents no new signal.
**Builds on:** `ontology-phase3a-build.md` (the L2 signal graph + identity map) and
`ontology-phase3b-build.md` (L4 clustering → `domain_id` per node), both shipped on
the `ontology` branch, plus 17f/17g (Pages + rank/serve).

This prompt is lettered **17k to avoid renumbering the existing 17h–17j**, but its
**build order is Phase 3e — after 17g**: it needs the materialized graph (17d), the
domain colouring (17e), and the L6 scores (17g) to size/colour nodes, and it is
**independent of** the Phase-4 external tier (17h) and the Phase-5 apply (17i). It
does **not** depend on 17j (hardening).

> **The one-line contract:** the engine already assembles a fused signal graph
> (`build_signal_graph`) and clusters it — then throws the graph away. Phase 3e
> **precomputes an `igraph` layout for that graph, persists it as one snapshot
> blob, serves it read-only, and renders it as an interactive "Ontology Map"** so
> an admin can *see* the estate (domains as clusters, lineage/co-query as edges)
> and click a cluster straight into its Domain draft. It adds **no new signal, no
> new Python dependency, and no UC write** — it is a *view* of Phases 3a–3d.

---

## 1. Scope

Phase 3e ships in **two ordered steps with a human STOP between them** (this is the
"bakeoff before buildout" the owner asked for). The Goal-Mode agent builds **Step A
in full and STOPS**; **Step B is a follow-up run** after a human picks the library.

### Step A — persist + serve the graph, and stage the library bakeoff (the agent builds this, then STOPS)

- **Persist the fused graph + a precomputed layout (L7)** — a new
  `genie_ont_graph_snapshot` Delta table (one JSON blob per workspace), MERGE'd by
  an **additive final step** in `run_materialize` that reuses the in-memory graph
  the job *already* builds (and today discards at `materialize.py` ~line 164) plus
  17e's `domain_id` assignments and 17g's scores.
- **Layout compute (`ontology/layout.py`, wheel)** — build an `igraph.Graph` from
  the fused nodes/edges (`igraph` is **already a wheel dep from 17e** — MV-D39, so
  **zero new Python dependency**), run a **deterministic** force layout
  (`layout_drl` / `layout_fruchterman_reingold` with a fixed seed) to attach
  `(x, y)` to every node, size nodes by centrality/cost/score, colour by
  `domain_id`, and **roll the asset graph up to a domain/sub-domain graph** (the
  default level-of-detail). Pure/offline (no I/O; the SP write is the writer's job).
- **Serve it read-only (`GET /api/ontology/graph`)** — a new `OntologyGraph` model
  (+ TS mirror), served **mirror-only** (the layout is a batch artifact — there is
  no cheap live rebuild). Cold/absent mirror → an empty graph with `state="cold"`
  and a plain "Run the ontology refresh to build the map" message (zero-burden). No
  existing route contract changes.
- **The bakeoff artifact (Step A deliverable, the eyeball gate)** — **three static
  mockups** rendering the **same** sample estate, one per candidate library, so a
  human can *see* the look before any code is committed: `17.0h` (Sigma.js v3),
  `17.0i` (Reagraph), `17.0j` (Cytoscape.js), plus the decision matrix in §5. These
  are standalone HTML review scaffolds (same `mockups.css` chrome as `17.0a–g`);
  they **approximate** each library's characteristic rendering — they do **not**
  import the real libraries and add **no npm dependency**.
- **STOP** for the human to eyeball `17.0h/i/j`, run the §5 verification checklist,
  and record the winner in §5's decision box + MV-D48. **No frontend graph
  component and no graph npm dependency is added in Step A.**

### Step B — render the chosen library (a SEPARATE follow-up run, after the pick)

- Add the **one** chosen graph library to `frontend/package.json` (exact-pinned,
  `npm ci` clean, no `--legacy-peer-deps`) and build `frontend/src/ontology/`
  `EstateGraph.tsx` (the "Ontology Map" view): render the precomputed coordinates,
  colour by domain, size by centrality/cost/score, style edges by kind (lineage
  solid, co-query dashed, agent-scope thin), **hover → evidence card**, **click a
  cluster → the existing `17.0d` Domain draft card** (reuse, do not fork), and the
  LOD toggle (domains default → drill to assets) + edge-kind filters + a top-N-by-
  centrality cap. Light-theme + a11y pass on the winner only.

### Out (deferred — see §12)

Any new *signal* or clustering (that is 17d/17e — this only visualizes them);
external context on the map (Phase 4); any write/apply from the map (Phase 5 owns
the only mutation — the map's "click → draft" reuses 17g's read-only draft, and
"Apply for me" stays disabled until 17i); the Step-B component + npm dependency
(gated on the bakeoff pick); light variants of the three bakeoff mockups.

---

## 2. Decisions honored (and which sleep in Phase 3e)

| Decision | Phase-3e posture |
|---|---|
| **MV-D48 Estate Graph** | **Active — this is the phase.** Persist the fused graph + precomputed `igraph` layout as `genie_ont_graph_snapshot`; serve `GET /api/ontology/graph` (mirror-only); render via a **bakeoff-chosen** library (Sigma.js v3 / Reagraph / Cytoscape.js) after a human eyeballs `17.0h/i/j`. No new signal, no new Python dep, no UC write. |
| MV-D35 evidence-first trust | **Active** — node size/colour and the hover card are the same signals (usage, lineage centrality, score) the drafts carry; the map asserts nothing new. |
| MV-D36 standalone admin-gated estate page | **Active** — the Map is a view *inside* the admin-gated Ontology page, not a new surface. |
| MV-D37 governed-tag substrate | **Active (read-only)** — clusters are coloured by 17e's `domain_id`; clicking one opens the 17g Domain draft (reuse/create/reassign proposal). The map **never** writes a tag. |
| MV-D39 in-job `igraph` engine | **Reused, not extended** — the layout runs on the **same** `python-igraph` 17e added; **no new dependency**, no new job, no clustering re-run. |
| MV-D41 nightly batch + on-demand | **Active** — the snapshot is written **inside** the existing `ontology_materialize` job (reuse `GSO_ONT_JOB_ID`); refreshing the map = the existing "Refresh ontology". |
| MV-D42 catalog allowlist | **Active** — the graph is exactly the allowlisted graph the job already built. |
| MV-D43 degrade-not-hang | **Active** — a layout failure logs and records `failed` **without** corrupting the tag/taxonomy/identity/domain snapshots written earlier in the run; a cold mirror serves an empty map with a plain message, never a hang. |
| MV-D45 minimal install footprint | **Active** — the **only** net-new artifacts are one wheel module, one Delta table, one read route/model, and (in Step B) **one npm dependency**. Zero new Python dep, zero new managed service, zero new job. |
| MV-D38 / D44 / D46 / D47 external context | **Dormant** — the map reads estate signals only; no external enrichment touches it. |

**Load-bearing consequence:** the map is a **pure projection** of Phases 3a–3d. If
the mirror is cold it renders empty with a refresh hint; it can never invent a node,
edge, or domain that the proposal engine did not already produce.

---

## 3. Subsystem layout

Step A is batch-side (wheel) + a thin read route + the three static mockups. Step B
(gated) adds the one frontend component + npm dep.

```
packages/genie-space-optimizer/
  src/genie_space_optimizer/ontology/
    layout.py               # NEW (Step A) — igraph layout + domain rollup → graph JSON (pure)
    materialize.py          # MODIFIED (Step A) — additive final step: MERGE genie_ont_graph_snapshot
    ddl.py                  # MODIFIED (Step A) — + genie_ont_graph_snapshot (snapshot table)
    graph.py                # (unchanged — 17d already emits the fused heterograph layout.py consumes)
backend/ontology/
  models.py                 # MODIFIED (Step A) — + OntologyGraph{,Node,Edge,Level} (read model)
  routers/graph.py          # NEW (Step A) — GET /api/ontology/graph (mirror-only, read-only)
  routers/__init__.py       # MODIFIED (Step A) — register ontology_graph_router
  services/mirror.py        # MODIFIED (Step A) — + read_graph_snapshot(workspace_id)
backend/main.py             # MODIFIED (Step A) — include ontology_graph_router
frontend/src/ontology/
  types.ts                  # MODIFIED (Step A) — + OntologyGraph TS mirror (no component yet)
docs/design/mockups/
  17.0h-ontology-estate-graph-sigma-dark.html      # NEW (Step A) — bakeoff: Sigma.js v3
  17.0i-ontology-estate-graph-reagraph-dark.html   # NEW (Step A) — bakeoff: Reagraph
  17.0j-ontology-estate-graph-cytoscape-dark.html  # NEW (Step A) — bakeoff: Cytoscape.js
scripts/setup_synced_tables.py   # MODIFIED (Step A) — register genie_ont_graph_snapshot
backend/tests/test_ontology_firewall.py  # MODIFIED (Step A) — graph_snapshot is an allowed SP write; still read-only route, no SET TAG/web_search

# Step B (SEPARATE run, after the pick — NOT built in Step A):
frontend/package.json + package-lock.json   # + the ONE chosen graph lib (exact-pinned)
frontend/src/ontology/EstateGraph.tsx        # the Map view (chosen lib), click→17.0d draft
```

**Reuse, do not fork:**

- `genie_space_optimizer/ontology/graph.py` (`build_signal_graph`) — the fused
  nodes/edges are the layout **input**; do not rebuild or re-signal. `layout.py`
  consumes the exact `{"nodes":[...],"edges":[...]}` shape it returns.
- `genie_space_optimizer/ontology/materialize.py` (`run_materialize`) — it already
  builds the graph (~line 164, currently discarded) and (post-17e) the communities;
  feed both to `layout.py` and MERGE the result as the **last** additive step.
- `genie_space_optimizer/ontology/ddl.py` (`build_snapshot_merge_sql`,
  `SNAPSHOT_TABLES`, `ensure_ontology_tables`) — add `genie_ont_graph_snapshot`
  exactly like `genie_ont_taxonomy_snapshot` (one JSON blob keyed by
  `workspace_id`); reuse the idempotent MERGE.
- `backend/ontology/services/mirror.py` (`read_taxonomy_tree`) — the graph read is
  the **same** pattern (synced-pool-first → Delta-via-warehouse fallback → JSON
  parse); do not invent a new read path.
- `backend/ontology/routers/taxonomy.py` — mirror-first serve shape; the graph route
  is the read-only twin (minus the live fallback — there is none for a layout).
- `frontend/src/ontology/` `17.0d` Domain draft card + `useOntology` hooks — the
  click-through target and the data-fetch pattern (Step B).

---

## 4. Backend contract (`backend/ontology/models.py`)

**One new READ model; every Phase-1/2/3a/3b/3c/3d model is FROZEN.** The map is
served through its own route; no existing payload changes.

```python
# backend/ontology/models.py  (append; keep 1:1 with frontend/src/ontology/types.ts)
GraphNodeKind = Literal["domain", "subdomain", "table", "metric_view",
                        "dashboard", "genie_agent", "tag"]
GraphEdgeKind = Literal["lineage", "co_query", "agent_scope", "tag_assignment",
                        "semantic_sim", "rollup"]
GraphState = Literal["cold", "fresh", "stale"]  # cold = never materialized


class OntologyGraphNode(BaseModel):
    id: str
    label: str
    kind: GraphNodeKind
    domain_id: str | None = None   # 17e cluster id → colour key (None = ungrouped)
    x: float                        # precomputed layout coordinate
    y: float
    size: float                     # render radius (centrality / cost / score-derived)
    cost: float | None = None       # 30d spend, when known (hover evidence)


class OntologyGraphEdge(BaseModel):
    src: str
    dst: str
    kind: GraphEdgeKind
    weight: float | None = None


class OntologyGraphLevel(BaseModel):
    nodes: list[OntologyGraphNode] = Field(default_factory=list)
    edges: list[OntologyGraphEdge] = Field(default_factory=list)
    truncated: bool = False          # asset level capped at top-N by centrality


class OntologyGraph(BaseModel):
    domains: OntologyGraphLevel      # rollup — the DEFAULT level of detail
    assets: OntologyGraphLevel       # drill-in (capped; truncated=True when > cap)
    layout: str                      # igraph algo used (e.g. "drl")
    node_count: int                  # total asset nodes BEFORE capping (honesty)
    edge_count: int
    state: GraphState
    as_of: str
```

`GET /api/ontology/graph` → `OntologyGraph`. **Mirror-only** (no live fallback —
building the layout is the batch job's job): read `genie_ont_graph_snapshot`; parse
the blob; stamp `state` from `refresh.mirror_is_fresh` (`fresh`/`stale`), or
`state="cold"` with empty levels when absent. Never raises (degrade-not-hang).

---

## 5. The bakeoff (Step A deliverable — the eyeball gate before buildout)

The frontend library is chosen **after** a human looks at three mockups of the
**same** estate. The agent builds the mockups and the matrix; it does **not** pick.

### 5.1 The three candidates (from the visualization-library research)

| Candidate | Renderer / character | Why it's in the running | Watch-outs |
|---|---|---|---|
| **Sigma.js v3 + graphology** (`17.0h`) | WebGL; flat, crisp circular nodes, thin straight edges, precomputed coords | Purpose-built to render **precomputed** `(x,y)` at scale; light bundle; React-19-clean; **MIT** | Lower-level API; compound "container" domains need faking (hulls), not native |
| **Reagraph** (`17.0i`) | WebGL, React-first; glow/depth nodes, curved edges, modern look | Least glue code for a **modern** look; React components out of the box; good defaults | Heavier bundle; more opinionated; verify React-19 peer range |
| **Cytoscape.js** (`17.0j`) | Canvas; **compound nodes** (domains as visual containers) + collapse/expand | Native "domains as boxes" is the closest to the taxonomy metaphor; very mature | Canvas (not WebGL) → heavier at 10⁴⁺ nodes; larger API surface |

**Ruled out up front (do not mock):** `react-force-graph` (React-19 ref-handling
red flag) and Cosmograph (non-commercial license).

### 5.2 What each mockup must show (identical estate, three looks)

The **same** sample estate as `17.0b` — **Commercial** {Sales, Marketing,
Partnerships}, **Finance** {Revenue & Billing}, **Operations** {Fulfillment} — as a
node-link graph: domains as coloured clusters, sub-domains and member assets as
nodes (anchor sized largest), **lineage** solid / **co-query** dashed / **agent-
scope** thin edges (incl. two cross-domain co-query edges), a legend, the LOD +
edge-filter controls, a **hover evidence card** (e.g. `orders · anchor · 41% of
Genie volume · lineage spine finance.sales`), and a **click → Domain draft** hint.
Each renders in that library's characteristic style (Sigma = flat/crisp; Reagraph =
glow/depth/curved; Cytoscape = compound containers with a collapse chevron) and ends
with a "why this library / trade-offs" strip so the reviewer eyeballs look **and**
cost together.

### 5.3 Verification checklist (human, before recording the winner)

- Dependency pins clean (exact version, `npm ci` OK, no `--legacy-peer-deps`, React-19
  peer range satisfied) — the CLAUDE.md dependency policy.
- Bundle-size delta acceptable (Sigma < Reagraph < Cytoscape, roughly).
- Renders the estate's node/edge count smoothly at the asset LOD (target: the
  top-N cap, e.g. ≤ 2 000 nodes, interactive).
- License is permissive (MIT/Apache) — all three qualify; Cosmograph did not.

### 5.4 Decision box (a human fills this in at the STOP, then Step B proceeds)

> **Chosen library:** ______________  ·  **Recorded in MV-D48:** yes/no  ·
> **Date:** ______  ·  **Notes (bundle size, peer range, fallback):** ______

---

## 6. Layout / rollup engine rules (`ontology/layout.py` — the load-bearing rules)

- **Deterministic by construction.** Seed `igraph`'s RNG (fixed seed + single
  thread) so a re-run over the same graph yields a **stable** layout — the client
  render doesn't jump between refreshes and the snapshot MERGE is idempotent. Note
  honestly (architecture §5 "honest gap"): float coordinates are **not** guaranteed
  bit-identical across `igraph` versions, so **tests assert structural stability**
  (node set, edge set, `domain_id` colouring, rollup counts), not exact `(x,y)`.
- **Reuse, don't re-signal.** `layout.py` takes the `build_signal_graph` output +
  17e's `{node → domain_id}` map + 17g's `{node → score}` (optional) and does
  **only** layout + rollup. It computes **no** new edges and runs **no** clustering.
- **Two levels of detail.** Emit `domains` (rollup: one node per Domain/Sub-Domain,
  edges aggregated across member pairs — the default view) **and** `assets` (the
  full node-link graph). Cap `assets` at **top-N by centrality** (module constant,
  default 2 000) and set `truncated=True` + report the true `node_count` when capped
  — never silently drop without saying so (zero-burden honesty).
- **Size + colour = existing signals only (MV-D35).** Node `size` = a bounded
  function of lineage centrality / `cost` / L6 `score`; `domain_id` = the colour
  key. No new metric is minted for the picture.
- **Degrade, never block (MV-D43).** Layout is the **last** additive step; a failure
  logs, records the run `failed`, and leaves every earlier snapshot intact. An empty
  graph → an empty snapshot (the MERGE clears stale rows), run still `succeeded`.

---

## 7. Persistence / DDL

**One new snapshot table** (schema mirrors `genie_ont_taxonomy_snapshot` — a JSON
blob keyed by `workspace_id`), written by the SP inside the existing job.

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_graph_snapshot (
    workspace_id  STRING     COMMENT 'Derived PK — one serialized estate graph per workspace',
    graph         STRING     COMMENT 'JSON: {domains:{nodes,edges}, assets:{nodes,edges}, layout, meta} — precomputed (x,y)',
    node_count    INT        COMMENT 'Total asset-level nodes BEFORE the top-N cap',
    edge_count    INT        COMMENT 'Total asset-level edges',
    layout        STRING     COMMENT 'igraph layout algorithm used (e.g. drl)',
    run_id        STRING     COMMENT 'FK to genie_ont_runs.run_id',
    as_of         TIMESTAMP  COMMENT 'Materialization time'
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

- Register `TABLE_ONT_GRAPH_SNAPSHOT = "genie_ont_graph_snapshot"` and add it to
  **`SNAPSHOT_TABLES`** (SP-written; it is a materialized view of the estate, **not**
  a proposal table — no consent/suppression semantics).
- **Key (idempotent MERGE, §7.2 precedent):** `(workspace_id)`; update cols
  `(graph, node_count, edge_count, layout, run_id, as_of)`; `WHEN NOT MATCHED BY
  SOURCE ... DELETE` scoped to `workspace_id`. A re-run replaces the one blob.
- **Lakebase mirror** — register `genie_ont_graph_snapshot` in
  `scripts/setup_synced_tables.py`; it reads Delta-via-warehouse through the Phase-2
  `mirror.py` interface until the synced-table flip (no new read path).
- **Firewall guard** — `genie_ont_graph_snapshot` is an **allowed SP snapshot
  write** (add to the snapshot allow-set, like taxonomy); the `graph` route stays
  **read-only**; still **no** `SET/UNSET TAG`, `CREATE GOVERNED TAG`,
  `manage_uc_tags`, or `web_search` anywhere.

---

## 8. Batch job implementation notes

No new job task — the snapshot is written **inside** `run_materialize`, as the
**final additive step** after clustering (17e):

1. The job already builds the fused graph (`materialize.py` ~line 164) and (post-17e)
   the `{node → domain_id}` communities. Pass both (+ 17g scores if present) to
   `layout.build_graph_snapshot(...)`.
2. `layout.py`: build the `igraph.Graph`, run the deterministic force layout (fixed
   seed), attach `(x,y)`/`size`/`domain_id`, roll up to the domain level, cap the
   asset level at top-N, serialize to the §7 JSON blob.
3. `writer.merge("genie_ont_graph_snapshot", [row], ["workspace_id"], workspace_id)`.
4. Degrade (MV-D43): a layout exception records `failed` but does **not** corrupt the
   tag/taxonomy/identity/domain snapshots written earlier; an empty graph → empty
   snapshot, run still `succeeded`.

---

## 9. Frontend wiring

**Step A: none beyond the TS type + the three static mockups.** `types.ts` gains the
`OntologyGraph` mirror (so the contract is pinned), but **no** component consumes it
yet and **no** npm dependency is added — `tsc`/`lint` stay green on an unused type.

**Step B (gated on the bakeoff pick):** `EstateGraph.tsx` renders the precomputed
graph with the chosen library — domains-default LOD with drill-to-assets, edge-kind
filters, top-N cap, hover evidence card, and **click a cluster → the existing
`17.0d` Domain draft** (reuse the 17g read-only draft; "Apply for me" stays disabled
until 17i). Light theme + a11y pass here.

---

## 10. Grants / deploy (DABs)

- **No new system-table grant** — the map visualizes signals the job already reads.
- **No new job, no new env var** — the snapshot writes in the existing
  `ontology_materialize` task (`GSO_ONT_JOB_ID` reused).
- **No new Python dependency** — `python-igraph` was added in 17e (MV-D39);
  `layout.py` reuses it. `uv.lock` is **untouched** in Phase 3e.
- **One npm dependency — Step B only, after the pick:** add the chosen graph library
  exact-pinned (`cd frontend && npm install <lib>@<exact> --save-exact`), commit
  `package.json` + `package-lock.json`; `npm ci` must pass with no
  `--legacy-peer-deps`.
- **Synced tables** — add `genie_ont_graph_snapshot` to
  `scripts/setup_synced_tables.py`.

---

## 11. Tests (offline, `backend/tests/` + GSO `tests/unit/`, run via `./scripts/test.sh`)

All Step-A acceptance is **offline** — layout is deterministic and in-process; no
cluster, no Lakebase, no browser.

- **Contract-frozen guard** — every Phase-1…3d model byte-identical; the taxonomy /
  tags / drafts routes unchanged; the **only** additions are the `OntologyGraph`
  model + the `/graph` route.
- **Layout on a fixture** — the `17.0b` worked example (Commercial/Finance/Operations)
  produces a snapshot whose **domain** level has one node per Domain/Sub-Domain with
  aggregated edges, and whose **asset** level carries every fixture node with an
  `(x,y)`, a `domain_id` colour, and `orders` as the largest (anchor) node.
- **Structural determinism** — two runs over the same fixture yield the **same** node
  set, edge set, `domain_id` colouring, and rollup counts (assert structure, **not**
  exact floats — §6 honesty); the MERGE replaces the single blob (no dup rows).
- **Top-N cap + honesty** — a fixture above the cap sets `truncated=True` and reports
  the true `node_count`; below the cap, `truncated=False`.
- **Route shape** — `GET /api/ontology/graph` returns a valid `OntologyGraph`; a cold
  mirror returns `state="cold"` with empty levels and never raises; a fresh mirror
  returns the parsed blob with `state` from freshness.
- **Additive safety** — a `layout.py` exception records the run `failed` but leaves
  the tag/taxonomy/identity/domain snapshots (written earlier) intact.
- **Firewall (updated)** — `test_ontology_firewall.py`: `genie_ont_graph_snapshot` is
  an allowed SP snapshot write; the `/graph` route is read-only (no mutation); still
  **no** `SET/UNSET TAG` / `CREATE GOVERNED TAG` / `manage_uc_tags` / `web_search`
  anywhere; `lakebase_*` still confined to `similarity.py`.
- **No-lockfile-change guard** — `uv.lock` unchanged in Step A (no Python dep added);
  `npm ci --dry-run` still clean (no npm dep in Step A).

**Step B tests (that run):** the chosen lib pins clean (`npm ci`, exact version,
React-19 peer OK); `EstateGraph.tsx` renders the fixture graph and the click-through
opens the `17.0d` draft; `tsc`/`lint` green.

---

## 12. Definition of done & explicit deferrals

**Step-A offline done (the agent stops here) when:** `layout.py` + the materializer
wiring + the `genie_ont_graph_snapshot` DDL/MERGE + the `OntologyGraph` model + the
mirror read + `GET /api/ontology/graph` + the `types.ts` mirror land; the three
bakeoff mockups `17.0h/i/j` render the sample estate; the taxonomy/tags/drafts routes
and all prior contracts are provably unchanged; `uv.lock` is untouched and no npm dep
is added; and `./scripts/test.sh` + `cd frontend && npm run lint` + `tsc` are green
(layout-fixture, structural-determinism, top-N/honesty, route-shape, additive-safety,
updated-firewall, no-lockfile-change tests). **Then STOP for the human bakeoff.**

**Bakeoff STOP (human):** eyeball `17.0h/i/j`, run the §5.3 checklist, record the
winner in §5.4 + MV-D48. **Only then** does Step B run.

**Deploy-gated (human, after the offline run — the agent must NOT do these):** run
the `ontology_materialize` job once against the live workspace (via "Refresh
ontology"), confirm `genie_ont_graph_snapshot` populates in Delta + the synced
mirror, and that `GET /api/ontology/graph` returns a sensible domain/asset graph;
re-run once to confirm the layout is structurally stable.

**Explicitly deferred (do NOT pull forward):**

- **Step B** — the chosen-library `EstateGraph.tsx` + the one npm dependency + the
  click-through to `17.0d` + light/a11y. Gated on the §5.4 pick.
- **No new signal or clustering** — the map only visualizes 17d/17e/17g output.
- **No external context on the map** (Phase 4) and **no write/apply from the map**
  (Phase 5 owns the only mutation; the map's "Apply for me" stays disabled until 17i).
