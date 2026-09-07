# Ontology Map north-star — **Lane D (data contract)** Goal-Mode driver (MV-D82)

## ⚙️ Parallel-build lane header (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). This is the **data** lane of the north-star map (MV-D81). It is
**renderer-independent**: it enriches the estate-graph snapshot + contracts so the future tree
renderer (Lane R) can draw `org → domain → sub-domain → asset` with typed verb edges — but it
**degrades gracefully** (every new field optional/absent → today's map still renders). Additive
only; offline; STOP before deploy.

- **OWNS (create/edit freely):**
  - `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/layout.py` (the emit)
  - `packages/genie-space-optimizer/tests/unit/test_ontology_graph.py` (append tests)
  - `backend/ontology/models.py` (OntologyGraph* — **additive optional** fields only)
  - `frontend/src/ontology/types.ts` (mirror the **additive optional** fields — declarations ONLY,
    NO renderer logic)
  - `backend/ontology/routers/graph.py` + `backend/ontology/services/mirror.py` (ONLY to confirm the
    blob→model deserialization passes the new optional fields through — do not reshape the route)
- **OFF-LIMITS (do NOT touch):** the frontend **renderer** (`EstateGraph.tsx`, `estateGraphModel.ts`,
  `components/`, `harness/`), every other wheel module (`cluster.py`, `pages.py`, `rank.py`, `graph.py`,
  `materialize.py` beyond confirming inputs, `ddl.py`), and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** independent of Lane R (contract-first is cleaner but not required — renderer degrades).

### 🔒 Frozen blob + contract additions (all ADDITIVE, blob-only — NO DDL/column, MV-D49)

The `genie_ont_graph_snapshot.graph` JSON blob gains, additively (existing keys unchanged):
- top-level **`root: {id, label, kind:"org"}`** — one metastore-root node (id = `metastore_id`, label =
  `"Estate"`; the renderer may override with the company name). Absent on empty input.
- every **`assets.nodes[]`** gains **`parent_id`** (its ONE canonical tree parent — see rule) and
  **`attach_level: "asset" | "subdomain" | "domain"`**. `domain_id` stays unchanged (degrade path).
- every **`assets.edges[]`** (and `domains.edges`/`subdomains.edges`) gains **`verb`** (plain language)
  and **`rel_class: "shared" | "xdom"`** (shared = same domain ancestor, xdom = different).

Contracts mirror these: `OntologyGraphNode.attach_level: str|None`; `OntologyGraphEdge.verb: str|None`
+ `rel_class: str|None`; `OntologyGraph.root: OntologyGraphNode|None`. (`parent_id` already exists on
the node model.) Keep `models.py` and `types.ts` field-for-field in sync.

---

## Spec & decisions

- **Spec (source of truth):** `docs/design/ontology-map-DESIGN.md` §3.4 (derivation & honesty rules,
  edge-kind→verb table, canonical-parent rule, emit-now-vs-defer). **Decisions:** `mv-advisor-playbook.md`
  **MV-D82** (+ MV-D81/D83 context); honor **MV-D49** (blob grain, no DDL), **MV-D43** (degrade-not-hang),
  **MV-D45** (no new dep), **MV-D23** (plain-language verbs).
- **Read first:** `layout.build_graph_snapshot`, the `graph.py` `add_edge` kinds (`mv_membership`,
  `agent_scope`, `lineage_adjacency`, `join_key`, `co_query`, `semantic_sim`), and `AGENTS.md`.

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: Ontology Map north-star — DATA LANE only (MV-D82). Enrich the estate-graph snapshot + contracts so
a tree renderer can draw the org→domain→sub-domain→asset hierarchy + typed edges: (A) an org root node,
(B) ONE canonical tree parent_id + attach_level per asset, (C) a plain verb + shared|xdom class per edge.
Additive, deterministic, blob-only (NO DDL — MV-D49); every new field OPTIONAL so today's map still
renders (MV-D43). Offline code + green wheel tests; STOP before deploy. Branch: ontology. NO renderer
edits, no other wheel module, no playbook.

SPEC: docs/design/ontology-map-DESIGN.md §3.4. DECISIONS: MV-D82 (+MV-D81/D83); honor MV-D49/D43/D45/D23.
RULES: AGENTS.md. Read layout.build_graph_snapshot + graph.py add_edge kinds first.

CONTEXT: build_graph_snapshot(signal_graph, node_domain_id, …, metastore_id) already turns EVERY signal
node (asset:/mv:/agent:/tag:/schema:) into asset_nodes {id,label,kind,domain_id,…} and builds asset_edges
(present-node filter). Containment edges exist: mv_membership (mv:→table asset:), agent_scope
(agent:→asset:); other kinds in §3.4. domain_meta {domain_id→{name,parent_id,origin}}; a domain_id WITH a
parent_id is a sub-domain.

BUILD A — org root (layout.py): add top-level blob key root={id:metastore_id,label:"Estate",kind:"org"};
omit on empty input. Do NOT rewire domain rollups' parent_id (keep sub→top semantics; degrade path).

BUILD B — canonical parent_id + attach_level (layout.py): per asset, from edges where it is the TARGET
pick the strongest containment by precedence mv_membership > agent_scope (tie → higher weight → id asc):
parent_id = that mv:/agent: source, attach_level="asset". If none: parent_id = its domain_id with
attach_level="subdomain" when that domain_id has a parent_id, else "domain"; ungrouped/absent →
parent_id=null, "domain". EXACTLY ONE parent — a table read by >1 mv keeps only the strongest; surplus
memberships stay edges (Build C).

BUILD C — verb + rel_class per edge (layout.py, all edge sets): verb from kind per the §3.4 table
(mv_membership/lineage_adjacency→"reads"; agent_scope→"uses"; join_key→"shares"; co_query→"also queried
with"; semantic_sim→"similar to"; else the kind); rel_class="shared" if both endpoints resolve to the
SAME top domain (node_domain_id + domain_meta parent-walk) else "xdom".

CONTRACTS (models.py ↔ types.ts in lockstep): add OntologyGraphNode.attach_level:str|None=None;
OntologyGraphEdge.verb:str|None=None + rel_class:str|None=None; OntologyGraph.root:OntologyGraphNode|None
=None (parent_id already exists). Confirm graph.py/mirror.py pass the new keys through (no drop).

DEFER, DON'T INVENT (R13): agent⊃metric_view (agent_scope targets tables today) and measure→measure
composition are follow-ups only; measures already ride snippets/expand — do NOT touch them.

GUARDRAILS: blob-only + additive (NO DDL — MV-D49); deterministic (fixed seed, sorted iteration); NO new
dep (MV-D45); degrade-not-hang (empty/failed input → valid snapshot, new keys absent — MV-D43);
plain-language verbs (MV-D23); NEVER touch the renderer, other wheel modules, or the playbook.

ACCEPTANCE (offline, append to tests/unit/test_ontology_graph.py):
  - table with an mv_membership edge → parent_id=that mv:, attach_level="asset".
  - table read by TWO mvs → parent_id=stronger mv only; the other stays an edge verb="reads".
  - no-containment asset: sub-domain→"subdomain", direct-domain→"domain", ungrouped→parent_id null.
  - join_key across two top domains → rel_class="xdom" verb="shares"; within one → "shared".
  - non-empty→ blob has root{kind:"org"}; empty graph → no root key, valid empty blob, run ok.
  - ./scripts/test.sh green; uv.lock untouched; same input twice → byte-identical blob.

WORKFLOW: branch ontology. Do NOT deploy/run the job. When offline-green, STOP and report the worktree
branch, `git diff --stat`, and the test summary; a human runs the deploy-verify gate.
```

---

## After the run (human-gated)

Mergeable independently of the renderer (Lane R). Once merged, a human runs the deploy-verify gate
(deploy `--update` on fevm-serverless → materialize scoped to the airline catalog → eyeball the blob:
`root` present, assets carry `parent_id`/`attach_level`, edges carry `verb`/`rel_class`, and today's map
still renders unchanged). Then Lane R consumes the enriched contract to build the tree (MV-D81/§9-A).
