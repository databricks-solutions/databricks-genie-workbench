# Ontology Map v2 — Build Spec (MV-D73 / MV-D74 / MV-D75)

> Status: **BUILD-READY** (proposed). Supersedes the *rendering* of MV-D71/D72 (which
> stay as the compound-LOD baseline) by adding the **layered knowledge-graph model**,
> an **Applied-vs-Proposed** provenance toggle, and a **modern visual system** on the
> existing Cytoscape/Canvas stack. **No new npm/py dependency** (MV-D45). Grain stays
> metastore-scoped (MV-D49). Zero-jargon detail copy (MV-D23). Read-only surface.

## §0. Why (the three problems this closes)

From the live `fevm-serverless` eyeball of the airline estate:

1. **"1990s" look.** Stock Cytoscape defaults — flat discs, black text-outlines,
   translucent hard-bordered boxes that overlap into mush at the Assets LOD, straight
   edges, no icons, no inspector, no search.
2. **"Where did IFEC come from?"** The map's domains are the **engine's *proposed*
   taxonomy** (`cluster.cluster` → `build_domain_rows` → `asset_domain`), not applied
   governed tags. `Alaska Airlines Ifec` is a *named cluster*, not an official domain.
   Users cannot tell current-state from suggestion.
3. **"Where are the measures / linkages?"** The snapshot models Tables / Metric Views /
   Genie Agents (+ tag/mv/schema hubs) and lineage/co-query/FK/mv_membership/
   schema_affinity/semantic_sim edges — but **measures/KPIs and Pages are not nodes**,
   and cross-domain edges are both sparse and hidden-by-default. The desired chain
   `domain → sub-domain → asset → measure/Page` is only half-built in the data.

The renderer is 20% of the fix; the **typed hierarchical model** and **expand-on-demand**
are the other 80% (confirmed by the Databricks industry viewer — same Cytoscape engine,
clean because the input is a curated hierarchy).

## §1. Scope / non-goals

- **In:** enrich the snapshot (typed assets + `origin` provenance + sub-domain edges);
  a lazy **expand endpoint** for the measure/Page "business-snippet" layer; an
  Applied-vs-Proposed graph source + toggle (default Applied); a modern Cytoscape visual
  system + shell (icons, semantic color, curved edges, inspector rail, search,
  breadcrumb, minimap, expand-on-demand LOD, honest states).
- **Out:** no WebGL renderer swap (revisit only if we drop the 2,000-node cap); no new
  clustering; no write path (still read-only, apply stays MV-D37/D50); no DDL change —
  the snapshot is a JSON blob (MV-D49) and the snippet layer is served live, not baked.

## §2. Data model — layered, typed, provenanced snapshot (MV-D73)

### §2.1 Node kinds + `origin` provenance (wheel)
- `layout.build_graph_snapshot`: emit each asset with its **real type** as `kind`
  (`table` | `metric_view` | `dashboard` | `genie_agent` — already carried by
  `build_signal_graph.add_asset`; confirm it flows to the rollup, don't default to
  `table`).
- Add **`origin: "applied" | "proposed"`** to every domain/sub-domain rollup node:
  `applied` when the domain_id is backed by a governed-tag assignment (cross-ref the
  `tag`-kind nodes / `tag_assignment` edges already in the signal graph, or the
  `genie_ont_taxonomy_snapshot` applied set), else `proposed` (pure engine cluster).
  Thread it through `domain_meta` in `materialize.py` from `domain_rows`
  (add a `tag_key`/`applied` marker to the meta dict).

### §2.2 Sub-domain aggregated edges (wheel)
- Today `layout` builds domain-level and asset-level edges only. Add a **sub-domain
  rollup edge set** (aggregate asset edges to the sub-domain grain, cross-sub only,
  deduped by `(src_sub, dst_sub, kind)`) so the Sub-domains LOD is not edge-empty.
  Additive key in the snapshot blob: `subdomains: { edges: [...] }` (nodes are still
  derived frontend-side from `domain_meta`).

### §2.3 Expand-on-demand "business snippet" layer (backend route — NOT baked)
- New read-only route: `GET /api/ontology/graph/expand?node=<id>&origin=<applied|proposed>`
  → returns the children of one node, hydrated on click (Bloom's
  `addAndUpdateElementsInGraph` pattern), keeping the snapshot blob small (MV-D49):
  - asset (metric_view) → its **measures/KPIs** (from `measure_signals` / MV YAML),
    nodes `kind="measure"`, edges `kind="mv_measure"`.
  - sub-domain / asset → attached **Pages** (`genie_ont_pages`, surfaced only), nodes
    `kind="page"` (+ archetype), edges `kind="page_source"` to their `source_fqns`.
- Bounded + degrade (MV-D43): cap children per parent; any failure ⇒ empty children.
- Served by the SP/OBO reader already used by the graph route; no new grant.

### §2.4 Contracts
- `backend/ontology/models.py`: add `origin` to `OntologyGraphNode`; add
  `OntologyGraphExpand` (`nodes`, `edges`, `parent_id`, `as_of`). Mirror 1:1 in
  `frontend/src/ontology/types.ts` (the unused `"measure"` kind finally ships).

## §3. Applied vs Proposed (MV-D74)

- Two sources already exist: **applied** = `OntologyTaxonomy` (governed-tag current
  state); **proposed** = the clustering snapshot rendered today.
- Graph route takes `?origin=applied|proposed`, **default `applied`**. The applied graph
  is built from applied tag assignments (domains = tag values with members); proposed is
  the existing snapshot.
- Rendering: `applied` = solid/authoritative; `proposed` = **dashed border + a
  "Suggested" chip**. If applied is sparse, the near-empty map is the *honest* current
  state (MV-D43 empty-state copy) with a one-tap nudge to view Proposed. This resolves
  the IFEC ambiguity permanently.

## §4. Modern visual system + shell (MV-D75, Cytoscape/Canvas)

- **Nodes:** per-type **icon** (SVG `background-image`; lucide set) + caption, **size** by
  centrality/cost/usage, **color** by domain (semantic palette), **border** encodes
  `origin` (solid=applied, dashed=proposed). Measures/Pages render as small satellite
  "snippet" nodes off their MV/sub-domain.
- **Edges:** curved bezier; color/dash by kind (lineage solid, co-query dashed, FK thin,
  `mv_measure` hairline); width by weight; hidden at deep LOD, **reveal-on-select**
  (FK-view-on-demand — already implemented, keep).
- **Expand-on-demand + LOD fix:** at the Assets LOD, **require a focused domain** (do not
  render every domain's assets at once — the current overlapping-box mush); click a
  domain/sub to expand its children (calls §2.3); **breadcrumb** `Estate ▸ Domain ▸
  Sub-domain`.
- **Shell:** docked **right-rail inspector** (upgrade the popover; plain language,
  MV-D23) with a "View measures/Pages" expand action; **search + focus/pin**; **minimap**;
  legend; LOD toggle + expand/collapse; loading/empty/error/stale states (MV-D43).
- **Layout:** reuse the seeded-compound-`fcose` (deterministic Group-in-a-Box) from the
  prior fix; **no new dependency** (MV-D45).

## §5. Phasing (three disjoint lanes → parallel worktrees)

1. **Lane 1 — wheel/backend model** (`layout.py`, `materialize.py`, `graph.py` if edge
   plumbing needed, `models.py`): §2.1 typed+`origin`, §2.2 sub-domain edges, §2.4
   models; offline `pytest` (extend `test_ontology_graph.py`).
2. **Lane 2 — backend routers** (`routers/graph.py` + a new expand handler,
   `services/`): §2.3 expand endpoint + §3 `origin`/applied-source wiring; offline tests.
3. **Lane 3 — frontend** (`estateGraphModel.ts`, `EstateGraph.tsx`, new inspector/search/
   minimap components, `types.ts`, `lib/api.ts`): §3 toggle UI + §4 visual system +
   expand-on-demand; `tsc`/lint/`vitest`.

Lanes are disjoint subtrees (wheel / backend / frontend) — safe for parallel Claude Code
worktrees with `OWNS / OFF-LIMITS / MERGE-ORDER` headers (mirror the Wave-2 launcher).

## §6. Gates & guardrails

- Additive-only; no DDL (MV-D49 JSON blob + live-served snippets); no new dependency
  (MV-D45); read-only (no UC write; apply stays MV-D37/D50); zero-jargon copy (MV-D23);
  every view handles loading/empty/error/stale (MV-D43).
- Frozen-surface guard: new models appended to the allowed set following the existing
  append-only pattern.
- Offline suites green per lane before integration; frontend `tsc` + ESLint clean.

## §7. Deploy-verify (STOP checkpoint)

`SKIP_FRONTEND_BUILD` unset (frontend changed) → `./scripts/deploy.sh --update`
(`fevm-serverless`) → trigger the ontology materialize job scoped to
`["serverless_stable_6t92c3_catalog"]`. Assert:
- snapshot rollup nodes carry `origin` and typed asset `kind`s; sub-domain edges present;
- `GET /graph/expand` returns measures for a metric_view and Pages for a sub-domain;
- UI: Applied is the default and reads as current-state; Proposed shows dashed/"Suggested";
  drill Domain → Sub-domain → Assets → (expand) measures/Pages; no overlapping-box mush;
  reload is byte-stable (deterministic seed).

## §8. Open decisions (defaults chosen; flag to change)

- **Snippet layer = lazy expand (default)** vs pre-baked into the blob. Lazy keeps the
  snapshot small and matches Bloom/NVL; pre-baked is simpler offline but grows node
  counts and blob size.
- **`origin=applied` default.** If the applied taxonomy is too sparse to be useful for a
  given estate, we may default to Proposed with a prominent "Suggested" banner instead.
- Minimap/search are must-have in v2 vs fast-follow — currently in-scope for Lane 3.
