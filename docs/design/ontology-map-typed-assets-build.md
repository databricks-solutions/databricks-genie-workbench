# Ontology Map — Typed Estate Assets build plan (MV-D89 · MV-D90 · MV-D91 · MV-D92)

> **Who runs this:** Claude in Goal Mode, one **stage** per run, sequential on the
> **`ontology`** branch. **Read-only** to data/governed tags; **additive** wheel + reader
> edits (no new table/column — MV-D49; no governed-tag write — MV-D26).
> **Predecessor:** Ontology Map north-star (MV-D81/D84) + Lane P2/E (MV-D87/D88) are LANDED
> + deploy-verified. This is a **signal-model** change — it puts the estate's *typed* assets
> (Metric Views, Genie Agents, Dashboards, Measures) into the fused graph so the map renders
> them; it is **not** a renderer change (the renderer already types them — see §2).

---

## §0. Why (what the live map actually shows today)

Live diagnosis on `serverless_stable_6t92c3_catalog` (2026-09-08 snapshot
`genie_ont_graph_snapshot`, cross-checked against the catalog + workspace):

- The catalog **has** the assets the user expects: **52 Metric Views** (24 already carry a
  domain tag), **17 Genie spaces**, **30 Lakeview dashboards** — none of them render as their
  own type on the map.
- `assets.nodes` is dominated by **1906 `tag:<key>` hub nodes** plus `schema:`/`mv:` hubs.
  These are *structural* plumbing (they exist so the layout can resolve domains + draw
  membership edges), but `layout.build_graph_snapshot` emits **every** signal-graph node as a
  display asset. They then win the `TOP_N_BY_CENTRALITY = 2000` cap by sheer degree, crowding
  real assets down to ~94 surviving `table` nodes.
- The 24 tagged Metric Views that *do* survive are **mis-typed as `table`** — `transforms.
  assemble_tag_graph` hardcodes `asset_type="table"` for every tagged member — so they render
  as generic tables, and their **Measures** (which the renderer surfaces by expanding a
  `metric_view`) never appear.
- **Genie Agents** never appear: `graph.build_signal_graph` only emits `agent:<id>` nodes when
  `agent_scopes` is supplied, and `materialize` never passes it (the reader's `agents()` is
  taxonomy-count only).
- **Dashboards** never appear: there is no reader, no `dashboard_scopes` kwarg, and no
  `dashboard` node kind anywhere in the wheel.

**Root cause is the producer, not the renderer.** The frontend `typeForKind`
(`estateGraphModel.ts`) already maps `metric_view` / `agent` / `dashboard` to first-class
node types with the right glyphs + expand behavior, and `NodeType` already enumerates them.
The fused graph simply never feeds those kinds through — and buries the ones it does behind
tag plumbing.

---

## §1. The four defects (verified)

| # | Defect | Seam | Stage |
|---|--------|------|-------|
| A | Tag/schema/`mv:` **hub nodes emitted as display assets**; consume the 2000-node cap, crowd out real assets | `layout.build_graph_snapshot` (`nodes` loop → `asset_nodes`; cap at `TOP_N_BY_CENTRALITY`) | **1** |
| B | Tagged **Metric Views mis-typed as `table`** (Measures therefore invisible) | `transforms.assemble_tag_graph` (member `asset_type="table"` hardcode) | **1** |
| C | **Genie Agents** never nodes — `agent_scopes` never wired | `run_ontology_materialize` reader + `materialize` → `graph.build_signal_graph(agent_scopes=…)` | **2** |
| D | **Dashboards** absent — no reader/kwarg/kind | reader + `graph.build_signal_graph(dashboard_scopes=…)` + `layout` label | **3** |

---

## §2. Contract & invariants (all stages)

1. **Renderer is frozen.** `NodeType`, `typeForKind`, `TYPE_RANK`, glyphs, MV→measure expand,
   and the `assets.{nodes,edges}` shape are already correct. **No frontend edit in any stage.**
   A stage is "done" when the wheel emits the right typed nodes and the *existing* renderer
   draws them. (If a stage tempts a frontend change, stop — it is out of scope.)
2. **Display-kind vocabulary.** The map's *display assets* are exactly
   `{table, metric_view, agent, dashboard, measure}`. Everything else a signal node carries
   (`tag`, `schema`, and the `mv:` measure-hub) is **structural** — used for domain
   resolution + membership edges, **never** emitted into `assets.nodes`.
3. **Additive / blob-only (MV-D49).** No new table or column. New signal rides existing
   kwargs (`agent_scopes` exists; `dashboard_scopes` is a new *kwarg*, not a new table) and
   the existing `assets` blob in `genie_ont_graph_snapshot`.
4. **No governed-tag write (MV-D26).** Agents + dashboards get their domain by **derived
   lineage** (§6, MV-D92) because Genie spaces + Lakeview dashboards are not UC relations and
   cannot carry `information_schema` table tags (and `system.tags.governed_tags` is empty in
   this estate). Read-only.
5. **Degrade-not-hang (MV-D43).** A reader that returns no MVs / agents / dashboards, or a
   lineage probe that fails, degrades to the current (pre-stage) graph — byte-identical for
   the kinds it lacks. No stage may make the materialize job fail on missing signal.
6. **Deterministic (MV-D82).** Sorted inputs, stable ids (`agent:<space_id>`,
   `dashboard:<dashboard_id>`, `asset:<fqn>` for MVs), no wall-clock in node payloads.
7. **Metastore-grained (MV-D49).** Keys stay metastore-level; catalog allowlist scopes the
   read only.

---

## §3. Stage 1 — typed display-assets, not tag plumbing (MV-D89 + MV-D90)

**Goal.** The map shows the estate's *assets*, correctly typed, and stops rendering tag/schema
plumbing. Metric Views become first-class `metric_view` nodes (so Measures expand).

**Build A — display-kind projection in `layout.build_graph_snapshot`.** When building
`asset_nodes` from the signal graph `nodes`, **include only display kinds**
(`table`/`metric_view`/`agent`/`dashboard`/`measure`) — skip `tag:`/`schema:`/`mv:` hubs.
Keep those hubs in the *structural* structures the function already builds (domain resolution
via `node_domain_id`, `mv_membership` edges, `_top_of_asset`) so domain rollups + membership
edges are unchanged; only the *display* projection narrows. The `TOP_N_BY_CENTRALITY` cap now
ranks real assets, so surviving-asset count jumps from ~94 to the true head of the estate.

**Build B — per-member `asset_type` in `transforms.assemble_tag_graph`.** Replace the
`asset_type="table"` hardcode with a type resolved from an **asset-type map** the reader
supplies (`{fqn → table_type}` from `system.information_schema.tables`, where
`table_type='METRIC_VIEW'` ⇒ `metric_view`, else `table`). Absent map ⇒ `table` (degrade).
Thread the map from `run_ontology_materialize` (a small `information_schema` query, allowlist
-scoped) into `assemble_tag_graph`.

**Build C — dedupe MV double-emit.** A tagged MV can appear twice: as an `asset:<fqn>`
(now typed `metric_view`) *and* as the structural `mv:<fqn>` measure-hub. Fold the hub into
the typed asset node of the same FQN — the typed `metric_view` node **keeps the hub's
`mv_membership` edges** (so Measures still expand) while only one display node survives.

**Measures.** No separate reader. Measures are children the renderer materializes by expanding
a `metric_view` (mv_membership edges + the baked snippet index already carry them). Correct MV
typing in Builds A–C is sufficient for Measures to appear.

---

## §4. Stage 2 — Genie Agents as first-class nodes (MV-D91)

**Goal.** Each Genie space renders as an `agent` node, connected to the tables/MVs it reads,
placed in the domain it most draws from.

**Build A — reader `agent_scopes`.** Add a reader method that returns
`{agent_id → [table_fqn, …]}` — the tables/MVs each Genie space reads. Derive the scope from
the space's configured table list (the create/scan path already resolves a space's tables;
reuse that resolution), degrading to `{}` when a space exposes none. Deterministic, sorted.

**Build B — wire it.** In `materialize`, pass `agent_scopes=reader.agent_scopes(allowlist)`
into `graph.build_signal_graph(...)` (the kwarg already exists and already emits `agent:<id>`
nodes + `agent_scope` edges). `layout` already labels `agent` nodes; confirm the label reads
the space display name (add it to the node payload if missing — additive).

**Domain placement:** §6 (lineage home-concentration).

---

## §5. Stage 3 — Dashboards as first-class nodes (MV-D92)

**Goal.** Each Lakeview dashboard renders as a `dashboard` node, connected to the tables/MVs
its datasets query, placed in the domain it most draws from.

**Build A — reader `dashboard_scopes`.** Add a reader method returning
`{dashboard_id → [table_fqn, …]}` from dashboard dataset lineage
(`system.access.table_lineage` filtered to the dashboard's queries, or the dashboard
serialized datasets). Degrade to `{}` on failure.

**Build B — new signal kwarg + kind.** Add an **optional** `dashboard_scopes` kwarg to
`graph.build_signal_graph` mirroring `agent_scopes`: emit `dashboard:<id>` nodes + a
`dashboard_scope` edge to each read asset. Unset kwarg ⇒ byte-identical graph (MV-D43). Add a
`dashboard` label branch in `layout` (`_label_for_node`) + an edge verb for `dashboard_scope`
(`_verb_of` → "reads"). Frontend already renders the `dashboard` type.

**Domain placement:** §6.

---

## §6. Cross-cutting — domain by derived lineage (MV-D92)

Genie spaces + Lakeview dashboards are **not UC relations** and cannot carry
`information_schema` table tags; `system.tags.governed_tags` is empty in this estate, so there
is no tag surface to place them on. **Zero user burden:** an agent/dashboard inherits the
domain of the tables/MVs it reads by **home-concentration** — take its scope's assets, look up
each asset's `domain_id` (`node_domain_id`), and assign the **modal** top-domain (ties broken
deterministically by domain id). No new tag, no user action, read-only. An agent/dashboard
whose scope is empty or all-untagged stays **ungrouped** (renders under the Estate root),
never guessed. This is *placement only* — it never proposes or writes a tag.

---

## §7. Acceptance (per stage, offline)

- **Stage 1:** wheel unit tests — (i) `layout` emits **no** `tag`/`schema`/`mv` node in
  `assets.nodes`; a tagged MV emits exactly one `metric_view` node (deduped) that retains its
  `mv_membership` edges; (ii) `transforms.assemble_tag_graph` types a member as `metric_view`
  when the asset-type map says `METRIC_VIEW`, `table` otherwise, and is byte-identical when the
  map is absent; (iii) cap now ranks display assets. `./scripts/test.sh` + wheel suite green.
- **Stage 2:** `graph.build_signal_graph` with `agent_scopes` emits `agent:<id>` nodes +
  `agent_scope` edges; `layout` emits them as `agent` display nodes with a name label; an
  agent with an all-tagged scope lands in the modal domain, an empty-scope agent is ungrouped.
- **Stage 3:** `dashboard_scopes` kwarg emits `dashboard:<id>` nodes + `dashboard_scope`
  edges; unset kwarg ⇒ byte-identical graph; layout labels + places them per §6.
- **Every stage:** additive-only diff, no frontend change, `package-lock.json` untouched.

---

## §8. Deploy-verify (human-gated, after each stage lands offline)

`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (`fevm-serverless`), trigger the
materialize job scoped to `["serverless_stable_6t92c3_catalog"]`, then eyeball
`genie_ont_graph_snapshot.assets`:

- **S1:** zero `tag`/`schema`/`mv` node kinds; `metric_view` count ≈ the 24 tagged MVs;
  Measures expand under an MV in the UI.
- **S2:** `agent` nodes ≈ 17, each edged to its read tables, sitting in a domain.
- **S3:** `dashboard` nodes ≈ 30, each edged to its datasets' tables, sitting in a domain.

---

## §9. Decisions (register in `mv-advisor-playbook.md`)

- **MV-D89** — The map renders **typed display-assets, not tag plumbing.** `assets.nodes` is
  projected to `{table, metric_view, agent, dashboard, measure}`; `tag`/`schema`/`mv` hubs
  stay structural (domain resolution + membership edges) and are excluded from the display
  projection, so `TOP_N_BY_CENTRALITY` ranks real assets.
- **MV-D90** — **Heterogeneous asset typing.** Tagged members carry a real `asset_type`
  derived from `information_schema.table_type` (MV ⇒ `metric_view`); the `mv:` hub folds into
  the typed MV node (dedupe) and Measures ride MV expansion — no separate Measure reader.
- **MV-D91** — **Genie Agents are first-class** via a reader-derived table scope wired through
  the existing `agent_scopes` kwarg; the renderer already types them.
- **MV-D92** — **Non-UC assets (agents, dashboards) get their domain by derived lineage
  home-concentration** (modal top-domain of the assets they read), never a tag write — Genie
  spaces + dashboards can't carry UC table tags. Empty/untagged scope ⇒ ungrouped, never
  guessed. Dashboards ride a new optional `dashboard_scopes` kwarg + `dashboard` kind.

---

## §10. Drivers

- **Stage 1:** `docs/design/ontology-map-typed-assets-stage1-driver.md`
- **Stage 2:** `docs/design/ontology-map-typed-assets-stage2-driver.md`
- **Stage 3:** `docs/design/ontology-map-typed-assets-stage3-driver.md`

Run sequentially on `ontology`; each is offline + additive + **STOPs before deploy** (§8 is a
human-gated gate between stages).
