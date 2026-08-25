# Semantic model visualization v3 — the metric view *is* a semantic model (design note)

**Status: CHECKPOINT — awaiting reviewer sign-off. Docs-only; no code lands
until this note is approved.** It follows the v2 note
(`semantic-graph-v2-note.md`, Prompt 12c) and the 12d hygiene pass. v2 fixed the
*legibility* defect (measures exploded a column, tall graphs shrank to
illegibility) by grouping measures into cards, adding real fit, focus/search,
and collapse. v3 fixes a *modeling* defect v2 did not touch: the graph drew a
metric view as a single opaque card, but a Databricks metric view is **itself a
semantic model** — a `source` fact, a `joins` tree of dimensions, and its own
`dimensions`/`measures` — and the current picture hides all of that.

The direction here was worked out interactively (brainstorm mockups v4→v7); v7
is the reference frame this note codifies. It decides **MV-D33** and drives
**Prompt 12e**.

---

## 1. What a metric view actually is (the fact that forces the redesign)

Per the Databricks metric-view spec (`WITH METRICS LANGUAGE YAML`, DBR 17.2+ /
v1.1 — see `.claude/skills/databricks-metric-views/yaml-reference.md` and
<https://docs.databricks.com/en/metric-views/data-modeling/syntax>), a metric
view has four structural parts:

- **`source`** — the grain/fact table (or a pre-joined subquery source).
- **`joins`** — a **star or snowflake** tree of dimension tables, each with
  `name`, `source`, and an `on`/`using` predicate (`source.fk = customer.id`).
  Joins **nest** for snowflake (`orders → customer → nation → region`). Our own
  `packages/genie-space-optimizer/.../optimization/mv_yaml.py` emits exactly
  this — a first-level ladder, nested joins, or a pre-joined subquery source,
  chosen by cardinality proofs.
- **`dimensions`** — grouping attributes whose `expr` binds to `source.col`, a
  joined table (`customer.name`), or is a pure expression (`DATE_TRUNC`,`CASE`).
- **`measures`** — aggregates over source columns, ratios, filtered and
  window/derived measures.

**The load-bearing consequence:** an MV's join tables are usually the *same*
`dim_*` tables that stand alone on the canvas. An MV therefore does not "consume
a fact" as a leaf — it **claims a subgraph**: one source fact + a specific set
of joined dims (some snowflaked) + a set of measure/dimension expressions. The
v1/v2 "Revenue MV · 6 measures" box collapses that entire star into one node.

There is a second structural truth the picture must carry: a space's Genie
config can define **measures that live in no metric view** (the loose
"config measures"), which are ungoverned/curated and free to drift.

---

## 2. Two hard constraints from review (these are not negotiable)

Recorded verbatim because they eliminate whole design branches:

1. **A relational model is duplicate-free.** No table — fact *or* dimension —
   may appear more than once on the canvas. This kills the "each MV is a
   container holding its own copies of its tables" shape (brainstorm v5): a dim
   shared by two MVs must be **one node**, not two.
2. **Arrows require proof.** Draw an edge only where a relationship is
   *declared* — a `joins.on`/`using` in the metric view YAML, or a declared
   relationship in the Genie space config. **No declared relationship → no
   arrow.** We never infer a join from name similarity or co-membership.

A corollary the reviewer named directly: tables can be present in three states
that must all render correctly on one deduplicated canvas — (a) a table used
*inside* an MV, (b) a table *shared* across MVs, (c) a table in the space/catalog
that is *in no MV* (unmodeled).

---

## 3. The model v3 draws

**One deduplicated relational canvas of tables; metric views are boundaries
over it; measures are boxed by the MV that owns them.** Concretely:

- **Tables (facts + dims) are the deduplicated node set.** Each appears once.
- **Measures are grouped into boxes by owner** on the right: one box per metric
  view carrying that MV's measures, plus a **Space-config box** for the loose
  measures. Each measure is tagged by provenance — **MV measure** (governed) vs
  **space-config measure** (curated/ungoverned) — and unnamed measures collapse
  to a **count** (`+8 unnamed`), never rendered as internals (12d / MV-D29
  hygiene carried forward, extended to this surface).
- **The metric view is a boundary, not a node.** At rest the boundary is the
  **measure box** (the MV "lives" where its measures are), and a subtle arrow
  runs **from the MV box to each table it uses** (declared joins only). On
  **selection**, a second boundary is drawn that **wraps the tables in the MV's
  definition** ("tables used by Revenue MV"), so the whole definition —
  measures *and* their fact+dims — is visible at once.
- **Reuse is membership, not duplication.** A dim shared by two MVs is one
  node; selecting it lights up the *multiple* MV boxes that use it and their
  arrows — one node, two owners, no copy. The inset states "used by N metric
  views" and the ripple warning.
- **Unmodeled tables get a neutral region** ("Unmodeled · no MV") with **no
  arrows** — the governance gap made visible per constraint (2).
- **Direct manipulation.** Any table or MV box is **draggable** to spread the
  canvas; pan is on empty background; "Reset layout" restores the deterministic
  home layout. Drag is a session-only user transform (see §7).

---

## 4. Interaction model

- **At rest:** measure boxes are bordered; each MV box arrows to its declared
  tables; tables carry **no boundary**; unmodeled tables have no arrows.
- **Select an MV box** → wrap its tables in a labeled boundary, highlight its
  declared join arrows (`N:1` glyph at rest, full `on` on focus/hover — the
  labels-on-demand rule from v2 §5 / 12d item 3), light its measures, dim the
  rest. This is the "encompass the upstream table definitions" behaviour the
  reviewer asked for.
- **Select a table** → light the MV box(es) that use it (a shared table lights
  *several*), its declared edges, and those MVs' measures. Lineage vs Impact:
  **Lineage** shows both what feeds and what depends; **Impact** shows only the
  downstream blast radius (which MVs/measures break if this table changes).
- **Select a measure** → its owning MV box + the tables in that MV's
  definition; a config measure shows its `from` table(s) and the
  consolidation/overlap hint (e.g. `avg_daily_rate ≈ ADR`).
- **Inset (curator panel)** carries the structured internals the graph can't:
  the MV's **join tree from YAML** (indented for snowflake), **dimensions
  grouped by binding** (from source / from join / pure expression),
  **measure governance** roll-up, **filter** and **materialization**, and the
  **reuse/conflict** signals (shared dims; two MVs defining overlapping
  measures; a config measure overlapping a governed one).

---

## 5. Why boundaries are on-demand, not persistent (the v6 finding)

Drawing every MV boundary at once over a shared node set is the classic
overlapping-set problem, and it is visibly cluttered even at two MVs (brainstorm
v6, "all boundaries on" state). v3 therefore keeps the **base canvas a clean
deduplicated relational diagram** and **lights one MV's boundary on
selection**; at-rest membership is conveyed by the **measure boxes + their
arrows** (and, optionally, small MV-colored membership dots on shared tables).
This is the same "navigate, don't read-all" posture as v2 §4, applied to
boundaries rather than to detail.

---

## 6. Layout is the real engineering cost

For the select-time table boundary to be a tidy rectangle that encloses **only**
the MV's members, the deterministic *home* layout must keep each MV's member
tables **spatially contiguous**, with shared tables placed on the **seam**
between the MVs that share them (an Euler-aware placement: cluster by MV
membership, seam the overlaps). This is a hand-rolled deterministic pass, not a
layout library (see §8). For dense estates, reuse the v2 collapse/focus/fit
machinery and the **derived collapse threshold** (v2 §9) unchanged — v3 changes
*what* is grouped (measures into MV boxes; tables deduplicated), not the
legibility levers v2 established.

---

## 7. Determinism and the diff-overlay invariant (still holds)

The whole render remains a **pure function of
`(tables, edges, mvBoxes, selection, dragOffsets, measuredViewport)`** —
`renderToStaticMarkup`-testable, as v2 §7 requires. Two notes:

- **Home layout is deterministic** (a function of the model), so the diff
  overlay — a proposal drawn over the current model — produces one picture per
  space, not two. The proposed MV renders as a **dashed boundary over the
  existing tables** it would source (no ghost/table copies), consistent with
  constraint (1).
- **Drag is an explicit user transform layered on top**, session-scoped, and
  **not** part of the persisted model. "Reset layout" clears it. So drag never
  perturbs the diff or the deterministic seed; it is honest interaction state,
  not model state. (Reviewer question flagged in §10.)

---

## 8. The layout-library position — reaffirmed

**Still NO layout library; the hand-rolled deterministic layered/Euler layout
stays.** All four grounds recorded at Prompt 12 and reaffirmed in v2 §6 hold
unchanged: determinism is a hard requirement (diff overlay); the spatial
semantics (which tables belong to which MV, source vs joined) are *product*
meaning an auto-layout would discard; the dependency policy stands; and the real
problem is membership/cardinality, not layout quality. v3 adds **boundary
placement and free-form drag**, both pure functions of placed coordinates plus a
user offset — no external engine. **The boundary that would flip this** is
unchanged from v2 §6 (a genuinely free-form many-to-many graph with no
meaningful ranks); v3's model is still a ranked, membership-structured DAG, and
the Euler seam is a deterministic placement rule, not a solver.

---

## 9. Data the reader must now fetch (grounding, not invention)

v3 needs data v1/v2 did not surface, and it must come from real readers:

- **MV internal structure** (`source`, `joins.on`/`using`, `dimensions`,
  `measures`) from the metric view's YAML — read via the existing describe path
  / the space's `data_sources.metric_views`, **not** re-derived. This is the
  source of both the join arrows (their *proof*) and the inset join tree.
- **Config measures** (loose, not in any MV) from the Genie space config —
  already assembled by the semantic-graph reader; v3 boxes them by "not in any
  metric view" and tags them ungoverned/curated.
- **Declared config relationships** (if any) as the *only* other admissible
  edge proof. Absent a declared relationship, a table shows **no arrow**.

Any element without a backing read is **not drawn** — the arrows-need-proof
constraint is a data contract, not a styling choice.

---

## 10. Test plan and open checkpoint

**Structural tests (pure-function, `renderToStaticMarkup`):**

- **No duplication:** a dimension shared by two MVs yields exactly **one** table
  node; selecting it marks membership in **both** MV boxes with no second node.
- **Proof-only edges:** every rendered arrow maps to a declared join
  (`on`/`using`) or a declared config relationship; an unmodeled table renders
  with **zero** edges.
- **Measures boxed by owner:** each measure sits in its owning MV box or the
  Space-config box; provenance tag correct; unnamed collapsed to a **count**,
  never as internals (no `?n`/`?s`/`sug_` in any label — the 12d validator
  extended to this surface).
- **Select-to-wrap:** selecting an MV box draws a boundary enclosing **exactly**
  its member tables (no foreign table inside the rect — the Euler-contiguity
  guarantee) and highlights **only** its declared arrows.
- **Governance roll-up survives grouping** (v2 §8 spirit): the per-box and
  panel-level governed/curated/ungoverned counts render even when detail is
  collapsed.
- **Determinism:** home layout is a pure function of the model; drag offsets are
  applied additively and cleared by reset; the diff-overlay picture is stable.

**Open questions for the checkpoint (do not settle silently):**

1. **Shared-dim at-rest cue** — locked here to on-demand boundaries + measure
   boxes/arrows (and optional membership dots), *not* persistent hulls (§5).
   Confirm.
2. **Layout algorithm** — the Euler-aware contiguity pass (§6) is the real cost;
   prototype it against 3/10/30-table fixtures before committing the boundary
   rectangle (fall back to a member-hull outline if contiguity cannot be
   guaranteed for pathological membership overlaps).
3. **Drag persistence** — proposed session-only, reset restores home (§7).
   Confirm we do **not** persist user layout (keeps the model/diff clean).

Checkpoint: this note ends at the reviewer's sign-off; Prompt 12e implements it
as its own commit, carrying the three confirmations above and the interactive
v7 frame as the visual contract.
