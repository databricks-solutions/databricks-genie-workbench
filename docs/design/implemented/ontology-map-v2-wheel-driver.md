# Ontology Map v2 — **wheel** Goal-Mode driver (Lane 1 of Wave 3 — MV-D73 blob shape)

## ⚙️ Parallel-build lane header — Lane 1 of Wave 3 (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Two sibling lanes edit the repo concurrently. This lane owns a
disjoint subtree (the **wheel** snapshot builder + its test), so merges are conflict-free —
keep it that way. This lane produces the **layered snapshot-blob shape** (MV-D73) that Lane 2
(backend) reads; build to the frozen blob contract below so Lane 2/3 integrate at merge.

- **OWNS (create/edit freely):**
  - `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/layout.py`
  - `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/materialize.py`
    (ONLY the graph-snapshot build site ~L560–590: extend `domain_meta`; add `snippets`/
    `mv_membership` inputs to the `build_graph_snapshot` call — do NOT reflow earlier stages)
  - `packages/genie-space-optimizer/tests/unit/test_ontology_graph.py` (append tests)
- **OFF-LIMITS (do NOT touch):** all of `backend/`, all of `frontend/`, every other wheel
  module (`cluster.py`, `pages.py`, `rank.py`, `graph.py`, `ddl.py`, …), and
  `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** FIRST (Lane 2 depends on the blob shape; Lane 3 depends on Lane 2).
- **Launch:** via the `ontology-lane-builder` subagent — see `ontology-wave3-launcher.md`.

### 🔒 Frozen blob contract (Lane 1 produces — Lane 2 reads; do not drift)

The `genie_ont_graph_snapshot.graph` JSON blob (still a blob — no DDL, MV-D49) gains, ADDITIVELY:
- every `domains.nodes[]` rollup gains **`origin: "applied" | "proposed"`** — `applied` iff the
  domain's `tag_decision ∈ {"reuse","reassign"}` (a governed tag), else `proposed`; `ungrouped` →
  `proposed`. Existing keys (`kind`,`domain_id`,`parent_id`,`parent_name`,…) unchanged.
- a new top-level key **`subdomains: { "edges": [ {src,dst,kind,weight} ] }`** — asset edges
  aggregated to the sub-domain grain (cross-sub only, deduped by `(src_sub,dst_sub,kind)`).
- a new top-level key **`snippets: { "<parent_id>": { "measures": [{ref,name,expression,fmt}],
  "pages": [{page_id,title,archetype,domain_id}] } }`** — measures keyed by the `mv:<fqn>` node id;
  pages keyed by the sub-domain rollup `domain_id`. Bounded per parent. This is the expand-on-demand
  source Lane 2 slices; it is baked in the deterministic batch (no request-path warehouse).

---

## Spec & decisions

- **Spec (source of truth):** `docs/design/ontology-map-v2-build.md` §2 (§2.1 origin, §2.2 sub-domain
  edges, §2.3 snippets). **Decisions:** `mv-advisor-playbook.md` **MV-D73**; honor **MV-D49**
  (metastore grain, JSON blob — NO DDL/column), **MV-D43** (degrade-not-hang), **MV-D45** (no new dep).
- **Project rules:** `AGENTS.md`. Read the spec section + `layout.build_graph_snapshot` first.

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: Ontology Map v2 — WHEEL ONLY. Enrich the estate-graph snapshot blob (MV-D73) so the map can
render domain→sub-domain→asset→measure/Page: (A) tag every domain rollup with origin applied|proposed,
(B) emit a sub-domain aggregated edge set, (C) bake a bounded per-node "snippets" index (MV measures +
attached Pages) for expand-on-demand. Additive, deterministic, blob-only (NO DDL/column — MV-D49);
offline code + green wheel tests; STOP before deploy. Branch: ontology. NO backend/frontend edits.

SPEC: docs/design/ontology-map-v2-build.md §2. DECISIONS: MV-D73; honor MV-D49/D43/D45. RULES: AGENTS.md.
Read layout.build_graph_snapshot + the materialize graph-snapshot call site first.

CONTEXT: layout.build_graph_snapshot already carries each asset's real kind (node.get("kind")) and
threads domain_meta {domain_id→{name,parent_id}} (MV-D71) built in materialize (~L579). domain_rows
carry tag_decision + tag_key (~L160). measure_signals (MeasureSignal: mv_fqn,name,expression,fmt,
source_fqns,domain_id) is in page_in["measures"]; Pages are the domain page candidates. mv hub nodes
are "mv:<fqn>". Blob today = {domains:{nodes,edges}, assets:{nodes,edges}, layout, node_count, edge_count}.

BUILD A — origin on rollups (layout.py + materialize.py):
  Extend domain_meta to {domain_id→{name,parent_id,origin}} where origin="applied" if tag_decision in
  {"reuse","reassign"} else "proposed" (built in materialize from domain_rows). In build_graph_snapshot
  set node["origin"] on each rollup from meta; ungrouped → "proposed". Keep all existing keys.

BUILD B — sub-domain edges (layout.py):
  After the asset-edge/domain-edge build, add subdomains.edges: map each asset to its sub-domain
  (the domain_id whose meta has a parent_id, i.e. kind=="subdomain"), aggregate asset_edges to
  (src_sub,dst_sub,kind), keep cross-sub only, dedupe. Emit blob key subdomains:{edges:[...]}.
  Never emit an edge whose endpoint sub-domain is absent (emitted-only guard).

BUILD C — snippets index (layout.py, fed by materialize):
  build_graph_snapshot gains an optional param snippets_in: measures grouped by mv_fqn + pages grouped
  by domain_id (built in materialize from page_in["measures"] + the domain page candidates). Emit blob
  key snippets:{ "mv:<fqn>":{measures:[{ref,name,expression,fmt}]}, "<sub domain_id>":{pages:[{page_id,
  title,archetype,domain_id}]} }. Bounded: cap measures per mv and pages per sub-domain (module const,
  e.g. 50). Absent snippets_in ⇒ omit the key (byte-stable with today). NO warehouse call here.

HARD GUARDRAILS: blob-only, additive — NO new table/column/DDL (origin/subdomains/snippets RIDE the
JSON blob, MV-D49); deterministic (same fixed seed, no new RNG); NO new dependency (MV-D45);
degrade-not-hang — empty/failed input still yields a valid snapshot, new keys empty (MV-D43); NEVER
touch backend/, frontend/, other wheel modules, or the playbook.

ACCEPTANCE (offline, tests/unit/test_ontology_graph.py, append):
  - a domain whose tag_decision="reuse" → rollup origin="applied"; a "create" domain → "proposed";
    ungrouped → "proposed".
  - two assets in different sub-domains with a cross edge → exactly one subdomains.edges entry, deduped;
    an intra-sub edge → none; an edge to a filtered/absent sub → none (no dangling endpoint).
  - snippets_in with one mv's measures + one sub-domain's pages → snippets["mv:<fqn>"].measures and
    snippets["<domain_id>"].pages present and capped; absent snippets_in → no snippets key.
  - empty signal graph → valid blob, new keys empty/absent, run still succeeds.
  - ./scripts/test.sh green; the wheel suite green; uv.lock untouched; determinism (same input twice →
    byte-identical blob).

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and report the
worktree branch, `git diff --stat`, and the test summary; a human runs the deploy-verify gate.
```

---

## After the run (human-gated)

Merged first. The blob shape it produces is exercised by Lane 2's `/graph?origin=` filter and
`/graph/expand`; the live deploy-verify runs once all three lanes merge (see the launcher).
