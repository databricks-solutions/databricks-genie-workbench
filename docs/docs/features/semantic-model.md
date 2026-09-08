---
sidebar_position: 4
description: "The Model tab's Semantic Blueprint — an interactive, grounded view of the Agent's tables, joins, metric views, and measures."
---

# Semantic Model (Blueprint)

The **Model** tab on an Agent's detail page renders a **Semantic Blueprint** — an
interactive diagram of the Agent's `serialized_space`: its tables, declared joins,
metric views, and measures. It is the picture of the model that the
[IQ Scanner](/docs/features/iq-scanner) scores and that [Auto-Optimize](/docs/features/auto-optimize)
improves, so you can *see* the structure instead of reading JSON.

The Blueprint is **read-only and grounded**: it draws only relationships the
config actually declares — an invariant we call *arrows require proof*. It never
invents a join or guesses a table's role, and it never edits the Agent (ad-hoc
`serialized_space` edits belong in the Genie product UI). The one place it can
propose something — the [Join Advisor](/docs/features/join-advisor) — is drawn as
a distinct dashed overlay and only ever produces *advice* for the optimizer.

:::note
The Blueprint replaced the earlier "classic" node-graph canvas. There is no longer
a Classic/Blueprint toggle — the Blueprint is the only Model-tab canvas.
:::

## Where the data comes from

The canvas is built from a single call:

```
GET /api/auto-optimize/spaces/{space_id}/semantic-graph
```

The backend (`_build_semantic_graph` in `backend/routers/auto_optimize.py`) reads
the live `serialized_space`, parses join `ON` predicates into column endpoints,
attaches metric views and their measures, and assigns a `role` (fact / dimension)
**only when it can prove one**. The frontend adapter (`blueprint/model.ts`) turns
that response into the pure layout model the canvas renders.

## Layout — fact-centric

The Blueprint uses a deterministic, fact-centered layout so a star or snowflake
schema reads left-to-right like a sentence:

| Column | Contents |
|--------|----------|
| **Left** | Dimensions (snowflake dimensions fan further left) |
| **Center** | The fact / source table(s) — the join anchor |
| **Right** | Metric views (one column right of the fact) |
| **Far right** | **Space config** — loose measures and config-only artifacts, in its **own column** |

Ranking is derived from join distance to the anchor, not authored column order.
Anchors are chosen by proven `FACT` roles first, then the most measure-referenced
table, then the highest-degree join hub, and finally (single-table models) every
table is its own center. **Headers stay honest**: they read "Dimensions · Fact ·
Metric view · Space config" only when a role was proven; otherwise they fall back
to neutral "Tables / Related tables".

:::note
The **Space config** bucket (loose measures not attached to any metric view, plus
config-only artifacts) sits in its own rightmost column, headed "Space config", so
it reads as a distinct governance surface rather than another metric view.
:::

## Semantic zoom — levels of detail

A segmented control switches the **level of detail** (this is distinct from
viewport zoom, below):

| Band | Shows |
|------|-------|
| **Overview** | Cards only — no measure chips, no role captions. The shape of the model at a glance. |
| **Standard** | Cards with measure chips, metric-view membership, and governance. The default. |
| **Columns** | Expands each table to its participating columns, with join-key rows highlighted and edges attached to the exact column ports. |

## Linework

- **One line per table pair.** A composite key or a redundant second `join_spec`
  between the same two cards collapses into a **single** relationship line
  (`keyCount` records how many predicates it represents), following ERD best
  practice instead of stacking overlapping lines.
- **Crow's-foot cardinality.** The "many" foot and the "one" tick are derived from
  the *declared* relationship (`many-to-one`, `one-to-many`, `one-to-one`), so the
  foot always lands on the proven many end regardless of author order. Same-column
  (intra-rank) joins route as a side bracket so the glyph sits on the line.
- **Crossing bridges.** Where one edge's trunk crosses another's leg, an
  index-stable hop arc keeps the two relationships separable.
- **Column-accurate ports** at the Columns band attach each edge to its exact join
  key row.

## Interaction

| Gesture | Result |
|---------|--------|
| **Click a card / measure** | Traces its neighborhood (focus + context dimming) and opens a **detail inset** below — mirroring the node detail for a table, metric view, measure, or the Space config bucket, including measure lineage back to source tables. |
| **Scroll / wheel** | Viewport zoom, pivoting on the pointer. |
| **Drag the background** | Pan the viewport. |
| **Drag a card** | Nudge it to declutter (an additive offset over the deterministic layout). |
| **Reset view** | Restores the framing — clears pan/zoom, drag offsets, and the selection in one action. |

## Insights inset

Below the canvas, a compact **Insights** inset surfaces the top one or two
"deal-breaker" issues derived directly from the model:

- **Unrelated table** — an island with no joins to the rest of the model.
- **Too many tables** — a data-source count that hurts retrieval.
- **Cold spot** — a table no curated SQL touches.
- **Name collision** — a loose measure whose name conflicts with a metric view.

Insights is intentionally shallow — it points at the [IQ Scanner](/docs/features/iq-scanner)
for the full best-practice checklist rather than duplicating it.

## Annotations for messy models

The Blueprint is built to stay useful (and never error) on real, imperfect
schemas:

- **Islands** — unjoined tables are flagged, not hidden.
- **Unmodeled region** — tables in no metric view are outlined as an "unmodeled"
  zone.
- **Cold spots** — zero-coverage tables are called out when SQL-coverage data is
  present.
- **Unknown roles** — when no fact/dimension can be proven (e.g. a single
  denormalized wide table, or ambiguous roles), the layout falls back to neutral
  wording and still renders relationships and measures without guessing.

## Join Advisor

Selecting the Model tab lazily loads the [Join Advisor](/docs/features/join-advisor),
which proposes **data-grounded** candidate joins the model doesn't declare yet.
Candidates render as a dashed `proposed_join` overlay (never a base edge), and
"seeding" a candidate persists it as **advice for the next Auto-Optimize run** — it
is never written as a declared join. See the dedicated page for the full model.

## Source files

- `frontend/src/components/model/SemanticModelTab.tsx` — the Model tab container
- `frontend/src/components/model/SemanticBlueprint.tsx` — the canvas (viewport
  pan/zoom, drag, detail inset, insets)
- `frontend/src/components/model/blueprint/` — pure modules: `model.ts` (adapter),
  `layout.ts` (fact-center ranking + boxes), `routing.ts` (edges, gutters, hops),
  `cardinality.ts` (crow's-foot glyphs), `annotate.ts` (insights), `advisor.ts`
  (Join Advisor logic)
- `backend/routers/auto_optimize.py` — `_build_semantic_graph` and the
  `GET /spaces/{space_id}/semantic-graph` endpoint

## Related documentation

- [Join Advisor](/docs/features/join-advisor) — data-grounded join advice for the optimizer
- [IQ Scanner](/docs/features/iq-scanner) — the full best-practice checklist behind the Insights inset
- [Auto-Optimize](/docs/features/auto-optimize) — the pipeline that improves the model you see here
