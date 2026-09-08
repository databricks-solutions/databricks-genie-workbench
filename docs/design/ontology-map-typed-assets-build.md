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
| C | **Genie Agents** never nodes, and their **applied governed-tag domain membership is unread** (it lives outside `information_schema`) | reader `entity_tag_assignments(geniespaces)` → tag graph (domain) + `graph.build_signal_graph(agent_scopes=…)` (read edges) | **2** |
| D | **Dashboards** absent — no reader/kwarg/kind, and their **applied governed-tag domain membership is unread** | reader `entity_tag_assignments(dashboards)` → tag graph (domain) + `graph.build_signal_graph(dashboard_scopes=…)` (read edges) + `layout` label | **3** |

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
4. **Domain membership is READ, never written (MV-D26).** Genie spaces + Lakeview dashboards
   **can** carry the domain governed tag — governed tags apply to workspace objects (apps,
   dashboards, Genie Agents, notebooks), not just UC relations — but those assignments do
   **NOT** appear in `information_schema.*_tags`; they live behind the
   `entity-tag-assignments` API (§2.5). So an agent/dashboard joins a Domain/sub-domain by an
   **applied governed-tag read** (`origin=applied`), mapped through the *same* tag-graph path
   as tables. Derived-lineage placement (§6) is a **labeled `origin=proposed` fallback** for
   *untagged* assets only — never a substitute for the applied read, and never a tag write.
5. **Degrade-not-hang (MV-D43).** A reader that returns no MVs / agents / dashboards, or an
   `entity-tag-assignments` / lineage call that fails, degrades to the current (pre-stage)
   graph — byte-identical for the kinds it lacks. No stage may make the materialize job fail
   on a missing/Beta API, a permission error, or empty signal.
6. **Deterministic (MV-D82).** Sorted inputs, stable ids (`agent:<space_id>`,
   `dashboard:<dashboard_id>`, `asset:<fqn>` for MVs), no wall-clock in node payloads.
7. **Metastore-grained (MV-D49).** Keys stay metastore-level; catalog allowlist scopes the
   read only.

---

## §2.5. Probe-first gate (run BEFORE building Stage 2/3 — confirm the tag actually lands)

The whole agent/dashboard-domain design rests on one unverified assumption: that this estate's
domain governed tag is actually **assigned to Genie spaces + dashboards** on the
`entity-tag-assignments` surface. Confirm it on a real space + dashboard **before** we commit
any thresholds or wire the reader — a five-minute read, no writes.

**Surface (Beta, API scope `tags`):**
- REST: `GET /api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags`
- SDK: `w.workspace_entity_tag_assignments.list_tag_assignments(entity_type, entity_id)` →
  `Iterator[TagAssignment{entity_id, entity_type, tag_key, tag_value?}]`
- `entity_type ∈ {geniespaces, dashboards, apps, notebooks}` — **not** `information_schema`.

**Probe (read-only, run as the job's `run_as` identity — MV-D50):**
1. **Enumerate** one of each: a Genie space id via `list_spaces(w)` (already used by
   `reader.agents()`); a dashboard id via `w.lakeview.list()`.
2. **List tags** on each: `list_tag_assignments("geniespaces", <space_id>)` and
   `list_tag_assignments("dashboards", <dashboard_id>)`. If the pinned `databricks-sdk`
   (`==0.117.0`) lacks the method, hit REST directly via
   `w.api_client.do("GET", f"/api/2.0/entity-tag-assignments/{et}/{eid}/tags")` — **no**
   dependency bump (keeps the exact-pin policy).
3. **Inspect** the returned `tag_key`/`tag_value` pairs: do they carry the **same** domain
   governed-tag key(s) tables use (top-level domain tag + `mvm_subdomain=<value>`)?

**Decision from the probe:**
- **Tag present** ⇒ proceed: the reader maps these assignments into the tag graph exactly like
  table members (§4/§5, `origin=applied`); lineage (§6) stays a fallback for untagged assets.
- **API/permission unavailable** (Beta not enabled, or `run_as` lacks the `tags` scope /
  entity read) ⇒ record it, and Stage 2/3 degrade to **lineage-only placement** (§6, all
  `origin=proposed`) until the grant lands. No stage blocks on the applied read.
- **Tag absent** (spaces/dashboards simply aren't tagged yet in this estate) ⇒ the applied
  path is inert-but-correct; lineage fallback carries placement until curators tag them.

Capture the probe output (a few `TagAssignment` rows + which decision it triggered) in the
Stage-2 driver's run notes so the thresholds/wiring are tuned to what actually exists.

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

**Goal.** Each Genie space renders as an `agent` node, placed in the Domain/sub-domain its
**applied governed tag** names, and connected by read edges to the tables/MVs it queries.
**Gated on §2.5** — build the applied path only if the probe confirms the tag lands (else this
stage ships the edge/lineage-fallback slice and the applied read activates once the tag/grant
exists).

**Build A — reader `entity_tag_assignments("geniespaces")` → domain (PRIMARY).** Add a reader
method that enumerates Genie spaces (`list_spaces(w)`) and, per space, calls
`list_tag_assignments("geniespaces", <space_id>)` (SDK, or REST `api_client.do` fallback —
§2.5), returning rows shaped like the `assignments()` union: `{tag_name, tag_value, member_id:
"agent:<space_id>"}`. Filter to the domain governed-tag key(s). Feed these into the **same**
tag-graph assembly tables use (`transforms.assemble_tag_graph` / `tag_key_of` / `tag_value_of`),
so a tagged space lands in its Domain/sub-domain with `origin=applied`. Bounded fan-out (~17
calls); degrade to `[]` on any API/permission failure (MV-D43). Deterministic, sorted.

**Build B — reader `agent_scopes` → read edges (SECONDARY, relational overlay only).** Return
`{agent_id → [table_fqn, …]}` (the space's configured tables; reuse the create/scan
resolution), pass `agent_scopes=` into `graph.build_signal_graph(...)` (the kwarg already emits
`agent:<id>` nodes + `agent_scope` edges). These edges are the "reads" overlay — **not** the
domain mechanism (that is Build A). `layout` already labels `agent` nodes; confirm the label
reads the space display name (thread it onto the node payload if missing — additive).

**Domain placement:** applied tag (Build A). Untagged spaces ⇒ §6 fallback (`origin=proposed`).

---

## §5. Stage 3 — Dashboards as first-class nodes (MV-D92)

**Goal.** Each Lakeview dashboard renders as a `dashboard` node, placed in the Domain/sub-domain
its **applied governed tag** names, and connected by read edges to the tables/MVs its datasets
query. **Gated on §2.5** (same posture as Stage 2).

**Build A — reader `entity_tag_assignments("dashboards")` → domain (PRIMARY).** Enumerate
dashboards (`w.lakeview.list()`) and, per dashboard, call
`list_tag_assignments("dashboards", <dashboard_id>)`, returning `{tag_name, tag_value,
member_id: "dashboard:<id>"}` rows filtered to the domain governed-tag key(s), fed into the
**same** tag-graph assembly as tables ⇒ `origin=applied`. Bounded fan-out (~30 calls); degrade
to `[]` on failure (MV-D43). Deterministic, sorted.

**Build B — `dashboard_scopes` kwarg + kind → read edges (SECONDARY).** Add an **optional**
`dashboard_scopes` kwarg to `graph.build_signal_graph` mirroring `agent_scopes`: emit
`dashboard:<id>` nodes + a `dashboard_scope` edge to each read asset (from dashboard dataset
lineage — `system.access.table_lineage` filtered to the dashboard's queries, or the serialized
datasets). Unset kwarg ⇒ byte-identical graph (MV-D43). Add a `dashboard` label branch in
`layout` (`_label_for_node`) + an edge verb for `dashboard_scope` (`_verb_of` → "reads").
Frontend already renders the `dashboard` type.

**Domain placement:** applied tag (Build A). Untagged dashboards ⇒ §6 fallback.

---

## §6. Fallback — derived-lineage placement for UNTAGGED agents/dashboards (labeled proposed)

This is the **fallback**, not the mechanism (§4/§5 Build A is the mechanism). When an
agent/dashboard carries **no** applied domain governed tag, it may borrow a domain from what it
reads: take its `agent_scopes`/`dashboard_scopes` assets, look up each asset's `domain_id`
(`node_domain_id`), and assign the **modal** top-domain (ties broken deterministically by
domain id). Because this is inferred, not asserted, it is emitted with **`origin=proposed`**
(renders as a dashed "Suggested" hull), so a lineage guess never masquerades as an applied
assignment. An agent/dashboard whose scope is empty or all-untagged stays **ungrouped** (under
the Estate root), never guessed. Zero user burden, read-only, never a tag write. This fallback
is **optional per stage** — if it complicates the applied slice, ship applied-only first and
add the fallback later; ungrouped is an honest current state (MV-D43).

---

## §7. Acceptance (per stage, offline)

- **Stage 1:** wheel unit tests — (i) `layout` emits **no** `tag`/`schema`/`mv` node in
  `assets.nodes`; a tagged MV emits exactly one `metric_view` node (deduped) that retains its
  `mv_membership` edges; (ii) `transforms.assemble_tag_graph` types a member as `metric_view`
  when the asset-type map says `METRIC_VIEW`, `table` otherwise, and is byte-identical when the
  map is absent; (iii) cap now ranks display assets. `./scripts/test.sh` + wheel suite green.
- **Stage 2:** the `geniespaces` tag reader maps a `TagAssignment{tag_key=<domain>, tag_value}`
  on a space into the tag graph so the `agent:<id>` node lands in that Domain/sub-domain with
  **`origin=applied`** (same assertion as a tagged table); a space with no domain tag is either
  ungrouped or (if the fallback ships) placed via §6 with **`origin=proposed`**. `build_signal_graph`
  with `agent_scopes` still emits `agent_scope` read edges; `layout` labels agents with the space
  name. Reader degrades to `[]` on API/permission failure ⇒ graph unchanged.
- **Stage 3:** the `dashboards` tag reader places a tagged `dashboard:<id>` node with
  **`origin=applied`**; `dashboard_scopes` kwarg emits `dashboard:<id>` nodes + `dashboard_scope`
  edges and is byte-identical when unset; layout labels + `_verb_of("dashboard_scope")="reads"`;
  untagged ⇒ ungrouped or §6 `origin=proposed`.
- **Every stage:** additive-only diff, no frontend change, `package-lock.json` untouched, no new
  dependency (REST `api_client.do` fallback keeps the `databricks-sdk==0.117.0` pin).

---

## §8. Deploy-verify (human-gated, after each stage lands offline)

`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (`fevm-serverless`), trigger the
materialize job scoped to `["serverless_stable_6t92c3_catalog"]`, then eyeball
`genie_ont_graph_snapshot.assets`:

- **S1:** zero `tag`/`schema`/`mv` node kinds; `metric_view` count ≈ the 24 tagged MVs;
  Measures expand under an MV in the UI.
- **S2:** `agent` nodes ≈ 17; each **tagged** space sits in its governed-tag Domain/sub-domain
  with `origin=applied` (solid), each carries `agent_scope` read edges; untagged ⇒ ungrouped
  or `origin=proposed` (dashed) if the §6 fallback shipped.
- **S3:** `dashboard` nodes ≈ 30; each **tagged** dashboard sits in its governed-tag domain
  (`origin=applied`) with `dashboard_scope` read edges; untagged ⇒ ungrouped / `proposed`.
- **Applied vs. probe:** if §2.5 found the tag absent/unavailable, S2/S3 acceptance is that the
  applied path is inert (no crash) and placement falls to the labeled `proposed` fallback —
  re-verify `origin=applied` once curators tag spaces/dashboards or the `tags` grant lands.

---

## §9. Decisions (register in `mv-advisor-playbook.md`)

- **MV-D89** — The map renders **typed display-assets, not tag plumbing.** `assets.nodes` is
  projected to `{table, metric_view, agent, dashboard, measure}`; `tag`/`schema`/`mv` hubs
  stay structural (domain resolution + membership edges) and are excluded from the display
  projection, so `TOP_N_BY_CENTRALITY` ranks real assets.
- **MV-D90** — **Heterogeneous asset typing.** Tagged members carry a real `asset_type`
  derived from `information_schema.table_type` (MV ⇒ `metric_view`); the `mv:` hub folds into
  the typed MV node (dedupe) and Measures ride MV expansion — no separate Measure reader.
- **MV-D91** — **Genie Agents are first-class**, placed by their **applied governed tag** read
  from the `entity-tag-assignments` API (`geniespaces`), mapped through the same tag graph as
  tables (`origin=applied`); their `agent_scope` read edges (via the existing `agent_scopes`
  kwarg) are a relational overlay, not the domain mechanism. Renderer already types them.
- **MV-D92** — **Agents + dashboards get their domain from an APPLIED governed-tag read, not a
  lineage guess.** Governed tags apply to workspace objects (Genie spaces, dashboards), but
  those assignments live behind `GET /api/2.0/entity-tag-assignments/{entity_type}/{entity_id}
  /tags` (SDK `workspace_entity_tag_assignments.list_tag_assignments`, entity types
  `geniespaces`/`dashboards`), **not** `information_schema.*_tags` — which is why the
  materializer was blind to them. The reader enumerates entities + fans out the per-entity
  list call (bounded ~47), maps the domain tag into the tag graph (`origin=applied`), and
  degrades to `[]` on any Beta/permission failure (MV-D43). **Derived-lineage home-concentration
  is a labeled `origin=proposed` fallback for untagged assets only**, never a tag write
  (MV-D26); empty scope ⇒ ungrouped. Dashboards also ride a new optional `dashboard_scopes`
  kwarg + `dashboard` kind for read edges. No dependency bump (REST fallback keeps the SDK pin).
  **Gated on the §2.5 probe** confirming the tag actually lands before wiring/thresholds.

---

## §10. Drivers

- **Stage 1:** `docs/design/ontology-map-typed-assets-stage1-driver.md`
- **Stage 2:** `docs/design/ontology-map-typed-assets-stage2-driver.md`
- **Stage 3:** `docs/design/ontology-map-typed-assets-stage3-driver.md`

Run sequentially on `ontology`; each is offline + additive + **STOPs before deploy** (§8 is a
human-gated gate between stages).
