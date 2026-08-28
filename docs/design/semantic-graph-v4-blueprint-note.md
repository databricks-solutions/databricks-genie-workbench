# Semantic model visualization v4 — the "Semantic Blueprint" reimagining (design note)

**Status: BUILT (P1–P3 frontend + P2 backend), behind the `blueprint` canvas
toggle in `SemanticModelTab`.** Production canvas lives in
`frontend/src/components/model/SemanticBlueprint.tsx` with pure modules under
`frontend/src/components/model/blueprint/` (`model` · `layout` · `routing` ·
`cardinality` · `annotate` · `advisor`); the Phase-2 `ON`-predicate column model
ships in `backend/routers/auto_optimize.py` + `backend/models.py`. Gates green:
frontend suite + `tsc -b` + eslint clean, `./scripts/test.sh` green. The one
deploy-gated remainder is the Join-Advisor **candidate source** (server-side
FK/name-type discovery + containment probe) and wiring `onSeed` to a real
Auto-Optimize run — until then the advisor renders its honest-empty state and the
validated-seed contract (§7) stands ready for that data. The original brainstorm
is settled; this note remains the implementation contract. Build against **two
sources of truth**:

- **North star (visual fidelity + feature parity):**
  `docs/design/mockups/10-blueprint-prototype.html` — the interactive prototype
  that already *computes* the layout, orthogonal routing, crow's-foot cardinality,
  semantic zoom, measure/MV lineage, the Insights inset, and the Join Advisor
  exactly as the shipped canvas should behave. **The production canvas must reach
  complete fidelity and feature parity with it** (adapted to live data). When the
  prose here and the prototype disagree on *look or behavior*, the prototype wins;
  when they disagree on *data contract or determinism*, this note wins.
- **This note (rationale + data/determinism contract):** why each choice was made
  and the invariants (§2, §8) that must not be relitigated.

Per-phase execution steps live in the handoff prompt
(`docs/design/semantic-graph-v4-build-prompt.md`); the parity checklist and the
real-code starting points are in **§11**.

This note follows v2 (`semantic-graph-v2-note.md`, grouping/fit/focus) and v3
(`semantic-graph-v3-note.md`, the MV-as-semantic-model boundary model, MV-D33).
v2 fixed legibility; v3 fixed the modeling of a metric view as a subgraph. v4
does **not** overturn either — it asks a different question: *the picture is
correct, but is it world-class?* The prompt was to research how interactive
modeling tools (Power BI, Erwin), graph/lineage explorers, molecule viewers
(Mol\*, PyMOL), CAD (Creo), and NYT-grade infographics achieve clarity that data
modeling tools rarely do, and to translate those principles into a buildable
reimagining of the canvas in the existing stack (React 19 + TS + Tailwind v4 +
hand-rolled SVG).

This note captures the agreed direction: **reimagine the canvas around an
adaptive layered "blueprint," keep the deterministic/pure contract, and phase
three tiers of improvement.** The direction is now approved and this note is the
contract the build executes against (§11).

---

## 1. Why data-modeling tools feel boring (research distilled)

The boring tools (Power BI relationship view, Erwin, SSMS diagrams) share a
small set of failures, and the great references share a small set of principles.

**The failures:**

- **One flat level of detail.** Every column of every table is always visible →
  a wall of text no reader parses. Detail is never *earned*.
- **Undifferentiated linework.** Right-angle ODBC spaghetti that crosses
  indiscriminately; a load-bearing PK–FK edge looks identical to a decorative
  one; cardinality is a text label, if shown at all.
- **No annotation layer.** The diagram never tells you what to look at. The
  reader hunts.

**The transferable principles (the references that get it right):**

1. **Semantic zoom / level-of-detail (Mol\*, PyMOL, Creo).** The *same* object
   renders at different fidelities by intent/zoom — cartoon vs ball-and-stick;
   assembly silhouette vs resolved features. Our model has the identical latent
   hierarchy: **schema → table → column** and **table → metric view → measure**.
   Detail should resolve as the reader approaches, never all at once.
2. **Linework is a first-class deliverable (CAD, circuit schematics, transit
   maps).** Orthogonal routing with consistent elbow radii, **real gaps at
   crossings** (line jumps/bridges), one line = one relationship, and endpoint
   glyphs that *encode* cardinality (crow's-foot) rather than a text plate.
3. **Column-accurate relationships (good ERD tools).** A join line physically
   touches the two columns it equates. This is the single biggest "world-class"
   upgrade and the thing every boring ERD fakes by connecting box-to-box.
4. **Everything earned by interaction (Neo4j Bloom, dbt docs DAG).** Rest state
   is calm; richness appears on hover/select. v2/v3 already do this
   (lineage/impact dimming) — it is our strongest existing move.
5. **The graphic annotates itself (NYT infographics).** Anomalies, gaps, and
   suggestions are called out *on the canvas* with a headline and a limited
   palette where color always means something. We already enforce the palette
   (governance traffic light); we do not yet annotate.
6. **Proposals are visually distinct from truth, carry evidence, and are
   reversible.** v3's ghost overlay already nails this; v4 extends it to joins.

---

## 2. Constraints carried forward (unchanged, non-negotiable)

v4 inherits v2/v3's invariants verbatim. They eliminate whole design branches, so
they are restated to prevent relitigation:

1. **Determinism / diff-overlay.** The whole render stays a **pure function of
   `(nodes, edges, selection, dragOffsets, measuredViewport)`** →
   `renderToStaticMarkup`-testable; two renders of one space produce one picture.
   **No runtime force/physics solver, no layout library** (v3 §8 grounds hold).
   "Reimagine" here means a new *deterministic* layout algorithm and a new visual
   grammar — not an auto-layout engine.
2. **Arrows require proof.** A base-canvas edge is drawn only where a
   relationship is *declared* (a metric view `joins.on`/`using`, or a declared
   config relationship). **No declared relationship → no arrow.** This is the
   load-bearing constraint for the Join Advisor (§6, Phase 3): advisor candidates
   are **proposals rendered as an overlay**, never base truth, and each must be
   grounded in FK metadata or a data probe.
3. **No duplication.** One node per table, fact or dimension; a shared dim is one
   node with multiple owners.
4. **Grounding, not invention.** Any element without a backing read is not drawn.

---

## 3. The reimagined canvas — paradigm A ("Semantic Blueprint")

Three paradigms were weighed:

- **A. Adaptive layered blueprint** — an ERD-accurate base plane (fact + dims
  with column-level, crow's-foot linework) with the governance layer (metric
  views, measures) rolling up out of the tables that source it. Semantic zoom
  collapses the ERD to card headers when zoomed out.
- **B. Fact-centric radial / orbital (molecule-style)** — fact as nucleus, dims
  orbiting, joins as bonds, an MV as a highlighted shell. Beautiful for a single
  star; awkward for snowflakes/multi-fact; harder to keep label-legible and
  deterministic.
- **C. Seeded-deterministic "frozen force"** — compute once with a fixed seed,
  then freeze. Most flexible topologically, but "reproducible force" is fragile
  across library versions and fights the no-dependency, byte-stable-render rule.

**Decision (brainstorm): build the canvas around A. Design B later as an optional
view-mode ("Layered" ⇄ "Constellation"), never as a dependency. Reject C as a
base** — it violates constraint (1)'s spirit. A is the most faithful to what a
semantic layer *is* (schemas → views → measures), reuses the deterministic
layered math, and delivers column-accuracy + clean linework + the semantic stack
without a solver. B becomes the delight flourish and the demo screenshot.

---

## 4. Phasing (each phase independently shippable + gated)

- **Phase 1 — New canvas skeleton + craft (frontend-only, no data change).** The
  adaptive blueprint layout, orthogonal routing with crossing bridges,
  crow's-foot cardinality, semantic zoom, a self-annotating callout layer, and a
  health headline. Tables stay single cards; ports attach at card edges. This is
  the first mockup. (§5)
- **Phase 2 — Column model (backend + frontend).** `SemanticGraphNode` gains
  participating-column sub-structure; a server-side `ON`-predicate parser emits
  `(table, column)` endpoints. Cards expand to column rows; join lines
  re-terminate at exact column ports; both columns light on select. Phase-1
  linework refines to column-accuracy with no routing rework. (§7)
- **Phase 3 — Join Advisor + Genie Agent advisor rail.** (a) Data-grounded
  candidate relationships → `MvProposal`-shaped rows with confidence + evidence;
  an inset below the canvas; checkbox → `proposed_join` overlay edge
  (column-accurate, dashed); accepting **persists a validated *seed* (not a
  locked declared `join_spec`)** that Auto-Optimize re-validates and adds itself,
  since the optimizer can add/refine but never remove a declared join (§7,
  persistence contract).
  (b) A compact **Insights inset below the canvas** surfacing only deal-breakers
  (unrelated/island tables, over the 30-table limit, name collisions) as
  click-to-focus rows — a filtered read of the existing IQ scan, with the full
  checklist deferred to the IQ Scan tab (§7.5). No new engine; frontend-only apart
  from a small finding→node-id deep-link map.

---

## 5. Phase 1 in detail — the "craft leap" (mock-ready)

**New component `SemanticBlueprint.tsx`**, built alongside the existing
`SemanticGraph.tsx` (kept until parity) so `SemanticModelView` can swap behind a
flag and the two can be mocked side-by-side. Pure logic split into testable
modules that mirror how `SemanticGraph` already exports `layoutCards` /
`distributeEdgePorts` / `computeFit`:

- `layout.ts` — ranking + within-layer ordering + placement
- `routing.ts` — orthogonal paths, elbows, crossing bridges
- `annotate.ts` — callout anchors + health headline
- `cardinality.ts` — crow's-foot marker selection

### 5.1 Adaptive layered layout (replaces the 4 fixed `COL_X` columns)

- **Rank (x): longest-path layering.** Fact/source tables at rank 0; each dim at
  `1 + max(rank of the tables it joins from)` (snowflake dims push right
  naturally); metric views one rank past their sourced tables; measures ride
  inside their MV card (v3). Pure topological pass; deterministic label tiebreak.
- **Order within a rank (y): a one-shot barycenter pass.** Order each node by the
  median y of its previous-rank neighbors, stable-sorted, **no iteration, no
  randomness**. This is the Sugiyama crossing-reduction step minus the physics —
  a deterministic placement rule, not a solver (consistent with constraint (1)
  and v3 §8). It replaces the current server-row-order sort and is what
  materially reduces the "hodge-podge."

### 5.2 Orthogonal routing + rounded elbows

Every edge: right-port of source → shared mid-gutter x → left-port of target,
corners drawn as fixed-radius quarter-arc fillets. Default style, not just the
bundled-fan case. Keep `collapsePairJoins` (one line = one relationship).

### 5.3 Crossing bridges (the linework trick)

After routing, intersect all H/V segments across *different* edges; on the
lower-index edge insert a small arc hop at each crossing. Deterministic (edge
index decides who hops). This is what makes 30-table linework read as engineered
rather than tangled. Pure geometry.

### 5.4 Crow's-foot cardinality endpoints

Replace the `N:1` text plate with SVG markers derived from `edge.relationship`
(crow's foot on the many-side, a bar on the one-side). No truncation,
ERD-literate; the full `ON` predicate still rides on hover/select (labels-on-
demand, v2 §5). `relationshipGlyph` stays for the inset/legend.

### 5.5 Semantic zoom (level-of-detail)

Detail resolves off `view.scale` bands (pure given scale):

- **far** — card header + governance rollup pill only
- **mid** — + measure chips (today's expanded state)
- **near** — + the participating join columns as ghost rows (a visual preview of
  Phase 2, drawn client-side from `edge.on` leaf names before the backend column
  model exists)

The derived `collapseThreshold` (v2 §9) becomes one band boundary.

### 5.6 Self-annotating callout layer

On-canvas annotations anchored the way `edgeBundleAnchors` already anchors bundle
counts (pure): the **ungoverned region** (v3, kept), the **worst cold spot** (min
`coverage` table promoted to a labeled callout), **name collisions**
(`node.overlaps`, currently panel-only), and **proposal conflicts**. Each callout
carries a verb that deep-links (`Govern these 4` → advisory; `Review collision` →
panel) through the existing `onReviewCreate` / `locateInGraph` plumbing.

### 5.7 Health headline

One derived sentence above the canvas + the governance ladder, e.g. *"31 governed
· 15 curated · 4 ungoverned — 4 tables in no metric view, 2 cold spots."* Every
number already exists (`countGovernance`, `unmodeledTableIds`, coverage).

### 5.8 Data needed for Phase 1

**None new.** All of the above is computable from the current
`SemanticGraphResponse`. Column ports at rest attach to card edges; near-zoom
column rows use `ON`-predicate leaf names client-side.

### 5.9 Mockup deliverable (same stack, gated)

A `SemanticBlueprintFidelityFrames.tsx` under
`frontend/src/components/auto-optimize/mockups/` (matching the `Mv*FidelityFrames`
pattern), dark + light, over 3-table / 10-table / 30-table fixtures (reuse/extend
`mvMockData.ts`), gated by `mockups.test.tsx` asserting the vocabulary renders
(crow's-foot markers, bridges, callouts, headline). This is the buildable proof
before production `SemanticBlueprint.tsx` lands.

### 5.10 Measure → source-table lineage (already built — no new data)

An earlier draft flagged "measure → originating table" tracing as net-new. That
was wrong: the backend already emits it. `_build_semantic_graph`
(`backend/routers/auto_optimize.py`) resolves a measure to its tables three ways,
all present today:

- **Governed measures** reach tables transitively: `measure —membership→ MV
  —uses→ table` (the MV's `uses` edges are the proven member set).
- **Loose / curated (Space-config) measures** get an explicit `derives` edge per
  table their **expression** references (Round-7: `_expr_table_refs(expr)` emits
  one `derives` edge each; an expr-proven table the space never modeled is added
  to the unmodeled region — the honest read).
- **Ungoverned proposals** carry `evidence.source_tables`, already rendered in
  the `NodeDetail` "Evidence" section.

So selecting any measure — including a Space-config measure — already lights its
source tables. The prototype's lineage lines just draw these existing
`derives` / `membership`+`uses` edges; **no backend dependency, nothing new to
build.** The only frontend delta is styling the (already-present) edges and
listing the resolved tables in the measure inset.

**Lineage rendering (on select).** Lineage obeys the **same orthogonal routing
discipline as joins** — no diagonal cutting across cards. A selected measure
draws a **dashed** accent path to each source table (`derives` / transitive
`uses`); a selected **metric view** draws a **dotted** accent path back to each
origin table it sources (the `uses` edges). Routing:

- **Verticals ride the clear inter-rank gutters** (computed from the laid-out
  boxes), so a lineage segment never runs through a card.
- **One horizontal leg rides a clean lane above the cards** to cross the
  intervening ranks; lanes are channelized (stacked) so parallel lineage lines
  don't overlap, and the drop point in the destination gutter is fanned per edge.
- **Adjacent ranks skip the lane** and route as a simple H-V-H, exactly like a
  join between neighbouring columns.
- **Crossings over join legs are bridged** with the same hop-arc idiom the join
  router uses (`vSegPath`), applied to a vertical instead of a horizontal.

Solid crow's-foot lines remain reserved for declared joins — dashed/dotted
distinguishes "lineage/derivation" from "relationship," and the shared
gutter/lane/bridge routing keeps the two visually consistent.

### 5.11 Robust to any schema shape (no fact/dim assumption)

The visualizer must render **any** agent, not just a clean star. The only things
it *asserts* are (a) relationships between tables (joins) and (b) measure ↔
metric-view ↔ Space-config lineage. Everything else — fact vs dim, "dimensions
(snowflake)" columns — is **added benefit, never a precondition**. This mirrors
the backend, which already assigns `role` only where a metric view definition
proves fact/dim and otherwise leaves it `None` ("prove it or stay neutral",
`_build_semantic_graph`). Concretely, the blueprint must degrade gracefully in
three ways the mock now exercises with a scenario switcher:

- **Roles unknown** → tables render with a neutral `TABLE` marker, never a guessed
  `FACT`/`DIM`. Column headers are **connectivity-derived, not semantic**:
  "Tables" (shallowest table rank), "Joined tables" (deeper), "Metric views ·
  config" — so an unproven role never forces a wrong label and never errors with
  "couldn't find facts and dims."
- **Single denormalized wide table** → one table, no joins, is a *valid* model.
  The layout adapts (rank x is computed from the ranks actually present + widest
  card, so a 2-column model isn't stranded in a 4-column frame), measures + MV +
  config still render with lineage, and the table is **not** flagged
  "unrelated" (the island check only fires when there's more than one table).
  A wide table earns at most a soft "wide" note, never a failure.
- **Arbitrary depth / partial connectivity** → longest-path style ranks place
  whatever join graph exists; islands, snowflakes, and MV-only members all lay
  out without special-casing.

Layout stays deterministic and pure (§8): ranking and x-placement are functions
of the present ranks and card widths, not of a fixed fact/dim column model. The
principle: **relationships and measure lineage are the load-bearing story; role
classification is a bonus that is shown when known and silently omitted when
not.**

### 5.12 Fact-center layering (read a metric view as a sentence)

**The problem with the shipped layering.** Today the backend assigns the fact
`col = 0` and a dimension `col = 1` (`_build_semantic_graph`), so the columns read
**fact → dims → metric view**, left to right. That places the two most tightly
coupled nodes — the fact and the metric view it is the `source` of — at *opposite
ends* of the canvas, which is exactly why measure/MV lineage lines have to
traverse the whole diagram (the reason §5.10's gutter/lane/bridge routing had to
exist at all). It also inverts the dependency: dimensions are *joined into* the
fact, so they are upstream of it, not downstream.

**What a metric view actually is.** A metric view = a **`source`** (usually the
fact/base table) + **joins** to dimension tables + **dimensions** (slicing
attributes) + **measures** (aggregations over the source/joins). The true
dependency flow is:

```
dimension ──join──▶ fact (source) ──feeds──▶ metric view ──contains──▶ measures
 (context)            (grain)                  (semantic layer)          (outputs)
```

So the correct reading order is **dimensions upstream, the fact in the middle,
the metric view + its measures downstream** — i.e. **dims left · fact center ·
metric view + measures right**. This matches the conventions users already carry:
lineage/dbt DAGs (sources → models → marts, left→right), and OLAP / semantic-layer
editors (SSAS cube designer, AtScale, Cube, Looker) that list **dimensions on one
side and measures on the other**. A true radial Kimball star (fact dead-center,
dims in a ring) is more iconic but is hard to keep deterministic, tangles past
~6–8 dims, and gives measures no clean home — so we keep the deterministic layered
engine and simply **re-rank**.

**Why it's better (concrete):** the fact sits *adjacent* to its metric view, so
fact→MV and measure lineage become **short, local** lines instead of
cross-canvas routes; the picture reads as a sentence ("slice by *these* → over
*this grain* → to get *these numbers*"); and it needs no new layout engine.

**Algorithm (mock: `deriveFactCenterRanks`).** Robust, and never asserts a role it
can't prove:

1. **Fact anchor**, by evidence strength: (a) tables a metric view *proves* are
   facts (`role === "FACT"`), else (b) the most **measure-referenced** table(s) —
   the MV source signal, which is what correctly picks `orders` in the unknown-
   roles fixture — else (c) the highest-degree join hub, else (d) every table is
   its own center (single-table model).
2. **BFS join-distance** from the anchor(s); `rank = maxDist − dist` so the fact
   (dist 0) lands center-right at `maxDist` and dimensions fan **left** (snowflake
   dims further left). Islands sit in the outermost dim band.
3. **Metric views** rank = `maxDist + 1` (one column right of the fact); the
   **Space-config** bucket gets its OWN column at `maxDist + 2` (one further
   right), headed "Space config", so the loose-measure/config surface reads as a
   distinct governance column rather than another metric view.

**Headers follow the bands** but stay honest: "Dimensions · Fact · source ·
Metric view · measures" **only when a role was proven**; unknown-role models keep
neutral wording ("Tables / Related tables") so we never guess fact vs dim (§5.11).

**Edge orientation.** Because the fact now sits to the *right* of its dimensions,
a join authored `from = fact (N) → to = dim (1)` is drawn right→left relative to
its authored direction. Rendering therefore orients every edge **left→right by
rank** and tracks which physical end is the **many** side, so the **crow's-foot
always lands on the fact** and the single "1" tick on the dimension, regardless of
which column each ended up in. Port-fanning and channelization key on the physical
left/right node, so routing (§5.10 hops/bridges) is unchanged.

The **many end is derived from the declared relationship**, not author order:
`many-to-one → from`, `one-to-many → to` (both render N:1 but the foot belongs on
opposite ends), `one-to-one → 1:1` (twin bars, no foot). The adapter records this
as `BlueprintJoin.manyEnd`; the crow's-foot glyph then points **toward the
connector's midpoint** (`sign(midX − manyX)`) rather than a fixed left/right
offset, which keeps the foot *on the line* in every orientation.

**Intra-rank joins** (two co-anchor facts land in the *same* column) are the case
that broke the naive left→right leg: `sx > dx` produced a backwards edge and
stranded the foot in empty space. Those are detected (`rank[from] === rank[to]`)
and routed as a **side bracket** — both ends attach to the same facing edge and
bow into the adjacent gutter (leftmost column bows right, others bow left;
multiple brackets in a column stagger). The midpoint-relative glyph direction then
lands the foot on the bracket. Same-rank edges are excluded from gutter
fanning/channelization/hops and carry their own geometry.

**Shipped status.** The prototype shipped a `Fact-center` / `Source-left` toggle to
compare the two layouts; **production keeps only Fact-center** — a fact-anchored
star/snowflake is the one honest reading of a semantic model, so the toggle was
removed rather than offer a worse arrangement to choose. (`BlueprintLayoutMode`
still carries `"source"` internally for the pure-layout tests; the UI just fixes it
to `"fact"`.) To make it real, the backend change is small and localized: replace the `fact = col 0 /
dim = col 1` assignment in `_build_semantic_graph` with the longest-path-to-MV
rank above (keep `role = None` → neutral). The frontend `layout()` already adapts
x to whatever ranks are present, so it mostly follows; `rankLabel` gains the
semantic-band wording. A one-pass **barycenter** ordering within each band (order
by mean neighbour y) is the natural follow-on to cut crossings (§8).

**Open decision.** Measures currently render as **chips inside the MV card**
(cleaner, one object). Breaking them into their own rightmost column would echo
the OLAP-editor look and make measures independently scannable, at the cost of
more nodes/edges — deferred unless the measures column needs to stand alone.

**Viewport pan/zoom.** The scene renders inside a single `<g transform="translate
(tx ty) scale(k)">`; the toolbar's Overview / Standard / Columns segmented control
is *semantic* zoom (level of detail), while **scroll-to-zoom** (pivoting on the
pointer) and **drag-the-background-to-pan** are the *viewport* transform, kept
distinct. The transform is owned by `SemanticBlueprint` (not the canvas) so
**Reset view** restores it in one action alongside the manual drag offsets and the
selection. The default is identity (`translate(0 0) scale(1)`), so a static render
stays byte-stable (§8). Node drag maps through the scene group's CTM (so a card
tracks the pointer 1:1 at any zoom); background pan/zoom maps through the SVG's own
CTM (viewBox space, where the translate/pivot are expressed). A click on empty
canvas (press+release without movement) still clears the selection — a pan does
not.

**Classic canvas removed.** The `Classic` / `Blueprint` toggle is gone; the
Blueprint is the only canvas. The classic-only surfaces it carried — the proposal
**overlay** (ghost proposed-MV cards + "would govern" links) and the advisory's
**"View in graph"** deep-link — retired with it: proposals now live only in the
advisory list, and the Blueprint stays grounded (arrows require proof, §2), so it
draws no speculative ghosts. `SemanticGraph`, `NodeDetail`, and `withOverlay` stay
*exported* (their unit tests and the checked-in v7 fidelity frame still build from
them), just no longer rendered by the live Model tab.

---

## 6. Phase 2 — the column model (data contract)

The only non-trivial data change. `SemanticGraphNode` (table kind) gains an
optional `columns` sub-structure (participating columns only — join keys and
dimension bindings, never the full column list, to avoid the wall-of-text trap).
A server-side `ON`-predicate parser turns `fact.user_id = dim_user.user_id` into
`(fact, user_id) ↔ (dim_user, user_id)` endpoints emitted alongside the existing
join edge (`on` text retained for the inset). Parsing is server-side so it stays
deterministic and unit-testable, consistent with "parse serialized_space
server-side." Additive: a response without `columns` renders exactly as Phase 1
(ports at card edges).

**Column-band geometry (one source of truth).** When a table card expands to list
its columns (the "Columns" LOD), the column rows MUST start *below* the card
header (role caption + name), separated by a divider — otherwise the first rows
overprint the title (the bug the prototype hit and fixed). The rendered row
offset, the card **height** (`nodeHeight`), and the per-column **edge attach y**
(`colY`, where a join meets a specific column) are the same three consumers of one
geometry and WILL drift if hand-tuned separately. The prototype fixes this by
factoring a single constant band — `COL_TOP` (first-row y-offset below the
header), `COL_H` (row height), `COL_PAD` (bottom pad) — that all three read, so
the header never collides with the rows and the join endpoints always land on the
correct row center. The real component (`SemanticGraph.tsx`) does not render
per-column rows yet (measures ride as chips; this is Phase 2), so it should adopt
the same single-constant discipline when the column model lands. Join-key columns
also get a subtle row highlight so they read as the anchors the edges attach to.

---

## 7. Phase 3 — Join Advisor (reconciled with "arrows require proof")

The prompt asked for a "join suggestions inset below, where a checkbox selects a
relationship" that seeds the optimizer. This is net-new intelligence (no join
advisor exists today; the MV advisor is measure-recurrence-based, MV-D25).

**Grounding (so suggestions are "not completely wrong"):** candidates come from
(a) declared UC foreign-key constraints, (b) name+type matching (`*_id` columns
across tables), and — strongest — (c) **data-driven warehouse probes**
(containment: is `fact.user_id ⊆ dim_user.user_id`? observed cardinality?
overlap %). Each candidate is an `MvProposal`-shaped row carrying confidence +
evidence (match kind, cardinality observed, sample overlap), mirroring the MV
advisor's shape so one card style renders both and the run-seed path is reused.

**Reconciliation with constraint (2):** advisor candidates are **never drawn as
base-canvas edges.** Checking a candidate ghosts a **`proposed_join` overlay
edge** (new edge kind beside `governs`/`derives`), dashed and column-accurate,
exactly as v3 ghosts a proposed MV. Framing: *"Selected relationships are handed
to Auto-Optimize, which validates and refines them"* — proposals as run input,
matching the existing `MvProposal` → run flow. Honest-empty discipline
(v3/advisor): "schema is fully connected — no candidate joins" vs "couldn't
probe — no warehouse."

**The asymmetry that makes this safe (why "seed," not "declared join").** A
load-bearing optimizer fact drives the persistence design: the Auto-Optimize loop
can **add** and **update** joins but **cannot remove** them. Its patch allowlist
(`unified_loop.py:_ALLOWED_PATCH_TYPES`) admits only `add_join_spec` and
`update_join_spec` — any LLM proposal outside the set is dropped
(`unified_loop.py`, "Dropping unsupported LLM patch type"). `remove_join_spec`
*does* exist in `config.py`/`applier.py`, but only as the **inverse/rollback of an
add**; the loop never emits it. So a join, once written into
`instructions.join_specs` as a **declared** relationship, is effectively locked —
the optimizer will refine its `ON`/type via `update_join_spec` but never delete
it. A wrong join is the worst class of error (plausible, silent, wrong numbers),
so a one-click checkbox that hardens a guess into a locked declared join the
optimizer can't undo is a foot-gun. This is why selections persist as a **seed**,
not a declaration.

**Persistence contract (the "changes survive to the optimizer" requirement).**
A checked candidate therefore does **not** write a declared `join_spec`. It
persists as a **proposed seed** — the same `MvProposal`-shaped row, carried
forward as run input — that Auto-Optimize **re-validates and adds itself** (via
`add_join_spec`) on the next step. The human's schema knowledge still survives to
the optimizer, but as a validated hypothesis rather than locked ground truth: if
a seed doesn't hold up under the optimizer's own probe, it simply isn't added, and
nothing has to be un-declared. Two-stage, explicit, and reversible:

1. **Stage** — checking a candidate marks it pending (ghosted `proposed_join`
   overlay edge on the canvas). Nothing is committed.
2. **Seed to Auto-Optimize** — commits the pending set as the run's proposed-join
   seed, so the optimizer starts from the curated proposal set (not a mutated
   config), validates each seed against data, and adds only the ones that hold.

**Guardrails (so a weak guess never rides forward unnoticed).** Each candidate
shows its **containment-probe verdict** inline — a match-rate bar reading
*validated · 97% row containment* (green), *partial* (amber), or *unverified ·
11%* (red). Turning on a weak-containment candidate (`< 50%`) triggers an explicit
**confirm** ("a join here can silently produce wrong results, and Auto-Optimize
can refine but not remove it"). A standing **ground-truth warning** under the seed
action restates the asymmetry. Selections stay reversible until seeded.

The prototype models this with a persistence bar under the Join advisor ("*N
relationships seeded · proposed to Auto-Optimize → re-validated & added there,
never written as a locked declared join*" + a **Seed to Auto-Optimize** action),
per-candidate probe-verdict bars, the weak-probe confirm gate, and the
ground-truth warning line. Backing plumbing is the existing run-seed path (the
`MvProposal` → run flow the MV advisor already uses); the advisor hands the run
its proposed joins rather than mutating `serialized_space` up front. Selections
are **suggestions the optimizer validates** — never silently applied as locked
truth.

---

## 7.5 Insights inset (top 1-2 callouts, below the canvas)

**Not a rail, and not a second IQ scan.** An early iteration put a full
best-practice advisor rail to the right of the canvas; it stole width from the
picture and duplicated the IQ Scan's job. Replaced with a **compact "Insights"
inset below the visual** that calls out only the **top 1-2 things most worth
fixing**, ranked by impact. The complete checklist stays where it belongs — the
**IQ Scan tab** (which this note does not change) — and the inset carries a
one-line pointer to it (*"Full best-practice checklist lives in the IQ Scan"*),
so the two surfaces are complementary, not competing.

**Candidate pool, ranked by impact** (the inset renders the top 1-2; everything
else is left to the IQ Scan). Each maps to an existing check / graph signal:

1. **Unrelated / island table** — a table with **no join edge** (check 5). Genie
   can't combine it at all; the canvas already draws it disconnected. *(fail)*
2. **Over the 30-table hard limit** — check 6 above 30; tables past the limit are
   dropped, so answers silently miss data. *(fail)*
3. **Too many data sources** — check 6 warning (≥9); best practice is ≤5 focused
   tables. *(warn)*
4. **Wide table** — a table with too many columns (check 10 signal); lowers
   column-selection accuracy. *(warn)*
5. **Cold spot** — joined but no curated SQL exercises it (coverage lens).
   *(warn)*
6. **Name collision** — a loose measure duplicates a governed name under a
   different definition (`node.overlaps`); one question, two numbers. *(warn)*

The **top-2 cap is the whole point** — Insights is a "what should I look at
first" glance, not the audit. Severity colors distinguish fail (red) from warn
(amber). If nothing ranks, the inset shows one clean line and defers to the IQ
Scan.

**Actionability = focus.** Clicking an insight focuses what it flags — a
node-scoped issue selects the node (opening its detail inset + neighbourhood
highlight) or drops the canvas to the Overview band; a collision selects the
measure. Reuses the existing `locateInGraph` plumbing (§5.6); the only new wiring
is a small **finding → node-id** map.

**Data needed.** None new — the ranked candidates are derived from the same
`/api/spaces/{id}/scan` result (checks 5/6/10, coverage, `node.overlaps`) the IQ
Scan already computes. The inset is a filtered, ranked, top-2 read of it — not a
new engine.

---

## 8. Determinism and the diff-overlay invariant (still holds)

Every v4 addition is a pure function of placed coordinates and the model:

- Longest-path ranking + one-shot barycenter ordering — pure, reproducible.
- Orthogonal routing, crossing bridges, crow's-foot markers — pure geometry of
  the resolved boxes/ports.
- Semantic-zoom bands — pure function of `view.scale`.
- Callouts + headline — pure functions of the model's counts.
- `proposed_join` overlay — drawn over the base like v3's `governs`, no base-edge
  mutation, no node duplication.

The boundary that would flip the no-layout-library position is unchanged from v3
§8: a genuinely free-form many-to-many graph with no meaningful ranks. v4's model
is still a ranked, membership-structured DAG.

---

## 9. Test plan (pure-function, `renderToStaticMarkup`)

- **Deterministic layout:** the same model yields byte-identical placement across
  renders; the barycenter pass is order-stable; drag offsets apply additively and
  clear on reset.
- **Routing/bridges:** given a fixture with a known crossing, exactly one edge
  hops and the over/under choice is index-stable; no edge passes through a card's
  own body.
- **Cardinality markers:** each `relationship` maps to the correct crow's-foot/bar
  pair; an unknown relationship draws no marker (never a wrong one).
- **Semantic zoom:** each scale band renders exactly its detail set; the far band
  never renders chips; the near band renders join-column rows.
- **Annotations:** the ungoverned region, worst cold spot, collisions, and
  conflicts each render iff the backing data is present; each callout's deep-link
  verb is present.
- **Arrows require proof (regression):** every base-canvas edge maps to a declared
  join/relationship; an unmodeled table renders zero base edges; a `proposed_join`
  renders only under the overlay.
- **Additivity:** a Phase-1 response (no `columns`) renders identically before and
  after the Phase-2 field exists.

---

## 10. Open questions (do not settle silently)

1. **Layout algorithm cost.** Prototype longest-path + one-shot barycenter against
   3/10/30-table fixtures before committing; confirm crossing reduction is worth
   the change vs the current server-row-order sort.
2. **Crossing-bridge scale.** Confirm bridges read well at 30 tables and do not
   themselves become noise; fall back to fewer/no bridges above a density
   threshold if so.
3. **Constellation (paradigm B).** Confirm it is a *later* optional view-mode, not
   a Phase-1 deliverable.
4. **Join Advisor probe budget.** Warehouse containment/cardinality probes cost
   query time; confirm the candidate set is bounded (top-N by name/type match)
   before probing, and that "no warehouse" degrades to name/FK-only with the
   reason named (never a silent empty).
5. **Column selection for Phase 2.** Confirm "participating columns only" (join
   keys + dimension bindings) is the default and "show all columns" is opt-in.

Checkpoint: these five are **validation tasks to confirm during the build**, not
blockers to starting it. The direction is approved; each phase carries its own
fidelity frame as the visual contract and settles the confirmations above.

---

## 11. Build handoff — north star, parity checklist, and starting points

### 11.1 Sources of truth
- **North star:** `docs/design/mockups/10-blueprint-prototype.html`. The shipped
  canvas must reach **complete visual fidelity and feature parity** with this
  prototype, adapted to real data. Disagreements on *look/behavior* → the
  prototype wins; on *data contract / determinism* → this note wins (§2, §8).
- **This note:** rationale + invariants.
- **Build prompt:** `docs/design/semantic-graph-v4-build-prompt.md` (per-phase
  execution, constraints, and "read-first" references).

### 11.2 Real-code starting points (today)
- **Canvas + pure fns:** `frontend/src/components/model/SemanticGraph.tsx` —
  already exports `buildCards`, `layoutCards`, `distributeEdgePorts`, `computeFit`,
  `collapsePairJoins`, `relationshipGlyph`, `edgeBundleAnchors`, `memberBoundary`,
  `unmodeledTableIds`, `focusSet`, `impactSet`, `countGovernance`, the coverage
  badge, etc. Build the new pure modules (`layout.ts` / `routing.ts` /
  `annotate.ts` / `cardinality.ts`) beside it and keep `SemanticGraph.tsx` until
  parity, per §5.
- **Host:** `frontend/src/components/model/SemanticModelTab.tsx` (swap the new
  canvas in behind a flag).
- **Fidelity frames + gate:** `frontend/src/components/auto-optimize/mockups/`
  (`Mv*FidelityFrames.tsx` + `mockups.test.tsx`); fixtures extend `mvMockData.ts`.
- **Backend graph builder:** `_build_semantic_graph` in
  `backend/routers/auto_optimize.py` (assigns `role` only when proven; emits
  `derives` / `uses` / membership edges + `coverage`). The Phase-2 column model and
  `ON`-predicate parser land here.
- **Types to keep in sync:** `SemanticGraphResponse` / `SemanticGraphNode` /
  `SemanticGraphEdge` in `frontend/src/types` mirroring `backend/models.py`.

### 11.3 Feature-parity checklist (the prototype's surface)
Every item is present in the north-star prototype and must ship (phase noted).

**Canvas & layout (P1)**
- [x] Adaptive layered layout; rank-x derived from the ranks actually present +
      widest card (no fixed `COL_X`); one-shot barycenter y-ordering.
- [x] Fact-center layering (§5.12). The prototype's Fact-center / Source-left
      toggle was dropped in production — Fact-center is the only layout.
- [x] Semantic-zoom bands: **Overview / Standard / Columns** (§5.5).
- [x] Rank/band headers: semantic ("Dimensions · Fact · source · Metric view ·
      measures") only when a role is proven; neutral otherwise (§5.11).
- [x] Node drag (per-card offsets, applied additively over the deterministic
      layout) + Reset view (clears drag offsets, viewport pan/zoom, and selection);
      light/dark parity.
- [x] Viewport pan/zoom: scroll-to-zoom (pointer pivot) + drag-background-to-pan,
      distinct from the semantic Overview/Standard/Columns bands; a scene
      `<g transform>` owned by `SemanticBlueprint`; identity default (byte-stable).
- [x] One relationship line per table pair — composite / redundant join_specs
      between the same two cards collapse into a single edge (`keyCount`), per
      ERD best practice, instead of stacking overlapping lines/labels.
- [x] Classic canvas retired — Blueprint is the only Model-tab canvas; the
      classic proposal overlay + "View in graph" deep-link retired with it
      (proposals live in the advisory list). `SemanticGraph`/`NodeDetail`/
      `withOverlay` stay exported for their unit tests + the v7 fidelity frame.

**Linework (P1 → column-accurate in P2)**
- [x] Orthogonal routing + rounded elbows; one line = one relationship.
- [x] Crossing bridges (index-stable hop arcs).
- [x] Crow's-foot cardinality, orientation-aware (foot on the many/fact end,
      derived from the declared relationship via `manyEnd`, pointing toward the
      connector midpoint); intra-rank (same-column) joins route as a side bracket
      so the foot lands on the line, not stranded; full `ON` predicate on
      select/hover.
- [x] Port fanning + channelization for parallel edges.
- [x] Columns LOD attaches to exact column ports on one `COL_TOP/COL_H/COL_PAD`
      band; join-key row highlight. (P1 uses `ON` leaf names client-side; P2 uses
      the real column model — adapter prefers server `from_column`/`to_column` /
      `columns`, falls back to the leaf parse.)

**Nodes (P1)**
- [x] Table cards: proven role caption or neutral `TABLE`; coverage badge;
      wide-columns pill; island "no join" tag; cold-spot dashed border.
- [x] Metric-view cards + Space-config card; measures as governance-colored chips
      (governed / curated / ungoverned) with an overlap "!" marker.

**Lineage on select (P1)**
- [x] Measure → source tables (dashed); metric-view → sources (dotted), routed
      with the same gutter/lane/bridge discipline as joins (§5.10).
- [x] Space-config measures **and** the Space-config card light their sources;
      Overview falls back to the card edge when measure chips aren't drawn.

**Annotations (P1)**
- [x] Health headline (governance ladder + counts).
- [x] Callouts: ungoverned region, worst cold spot, unmodeled/island — each with a
      deep-link verb; region + MV-membership boundary boxes; legend.

**Detail inset (P1, mirrors `NodeDetail`)**
- [x] Selecting any card/measure opens a detail inset below the canvas for
      table / metric view / measure / Space config, with the measure's **lineage →
      source tables** listed.

**Insights inset (P3, frontend-only)**
- [x] Top **1–2** ranked deal-breakers only (island, >30-table limit, ≥9 sources,
      wide table, cold spot, name collision); severity colors; click-to-focus; IQ
      Scan pointer; clean-state line (§7.5).

**Join Advisor (P3, validated-seed model, §7)**
- [x] Declared joins shown locked; each candidate carries evidence + a
      **containment-probe verdict bar** (validated / partial / unverified).
- [x] Checking ghosts a dashed `proposed_join` overlay edge (never a base edge).
- [x] **Confirm gate** on weak-probe (<50%) candidates; a standing **ground-truth
      warning**; reversible until seeded.
- [x] Persist as a **seed** ("Seed to Auto-Optimize"), never a locked declared
      `join_spec` — because the optimizer can add/update joins but never remove
      them (`unified_loop.py:_ALLOWED_PATCH_TYPES`). *(UI + `onSeed` contract shipped;
      candidate discovery + probe + run-seed wiring are the deploy-gated remainder —
      until then the advisor shows its honest-empty state.)*

### 11.4 Acceptance gates
- Each phase ships its **fidelity frame first** (the visual contract, gated by
  `mockups.test.tsx`), then the production component.
- Pure-function tests per §9 (`renderToStaticMarkup`, byte-stable placement).
- `cd frontend && npm run lint && npm run build` clean; `./scripts/test.sh` green
  when Phase-2/3 backend lands; types mirrored (§11.2).
- Integration/E2E only by deploying (`./scripts/deploy.sh --update`) — there is
  **no local dev server** (repo rule).
