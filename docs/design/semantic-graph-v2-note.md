# Semantic model visualization v2 — design note (Prompt 12c Part 2)

> **Superseded in part by `semantic-graph-v3-note.md` (MV-D33 / Prompt 12e).**
> v3 keeps every legibility lever below (grouping, fit, focus/search, the
> derived collapse threshold §9, edge ports §5, governance-roll-up §8) but
> replaces the "MV drawn as a single leaf card" model with a deduplicated
> relational canvas where the metric view is an on-demand boundary and measures
> are boxed by owner. Read v3 for the current model; this note stands for the
> legibility reasoning it still owns.

**Status: CHECKPOINT — awaiting reviewer sign-off. Docs-only; no code lands
until this note is approved.** Part 1 (the pan/zoom null-race fix + error
boundary, smoke finding 5) already shipped (`9c2737f7`); this note covers the
Part-2 redesign for smoke finding 4 (the layout is unusable on real spaces).

Scope of the note, per the Prompt 12c body: evaluate **grouping**,
**fit-to-content**, and **focus/search** against 3-, 10-, and 30-table fixtures,
and take an **explicit position on whether a layout library is needed**. It does
not implement; it ends at the reviewer's checkpoint.

---

## 1. The v1 defect, quantified

`SemanticGraph.tsx` lays out four fixed columns — `source / fact` · `dimensions`
· `metric views` · `measure concepts` — at `COL_X = [40, 220, 400, 560]`, and
gives **every node its own row** at `ROW_TOP(44) + row · ROW_GAP(58)`. The
viewBox is content-sized (`0 0 width height`), but the rendered SVG is capped at
`maxHeight: 480` with `preserveAspectRatio` meet, so once the tallest column
outgrows ~8 rows the whole picture scales down uniformly to fit 480px — nodes
and 10px labels shrink together until nothing is legible. Two structural facts
make this bite:

- **Every measure is its own node in column 3.** A real space has dozens of
  measures; they stack into one tall column that drives the height.
- **The table columns also stack one-per-row.** A star schema puts one fact in
  column 0 and many dimensions in column 1; either column can become the
  row-driver on its own.

Concrete scaling (container ≈ 734px wide, so horizontal scale ≈ 1.0; vertical
scale = `min(1, 480 / height)`):

| Fixture | Tallest column (rows) | `height` px | fit scale | Node height @ fit | Verdict |
|---|---|---|---|---|---|
| **3 tables** | ~6 (measures) | ~392 | 1.0 | 34px | Legible today. |
| **10 tables** | ~15 (measures) | ~914 | 0.53 | ~18px, 5px labels | Borderline; requires manual zoom. |
| **30 tables** | ~40 (measures) | ~2364 | 0.20 | ~7px, 2px labels | Unusable — finding 4, plus 30→15→3→40 edge spaghetti. |

The v1 layout is not *badly tuned*; it is **structurally wrong for cardinality
> ~8 in any column**. Retuning constants moves the cliff, it does not remove it.

---

## 2. Direction 1 — grouping over sprawl (the primary lever)

**Measures render INSIDE their owning node, not as their own column.** A metric
view becomes a card that carries its measure chips (expandable); a table becomes
a card with a **column count**, not one node per column. This is the single
biggest lever because it removes the measure explosion that dominates every row
count above:

- Column 3 (`measure concepts`) stops being a row-driver. Its cardinality — the
  thing that was 40 rows at 30 tables — collapses into the MV/source cards that
  own those measures.
- The governance ladder (the headline, §8) moves onto the **chips inside the
  card** and a **card-level roll-up** ("3 governed · 1 curated · 2 ungoverned"),
  so the traffic-light semantics survive grouping rather than being lost with
  the per-measure nodes.

**Residual, stated honestly:** grouping fixes the *measure* explosion but **not
the table explosion**. Re-run the math with measures grouped: at 30 tables the
tallest column is now the ~30-row table column, `height ≈ 1784`, fit scale
≈ 0.27 — better than 0.20, still not legible. So grouping is **necessary but not
sufficient at 30 tables**; it must be paired with fit + focus (§3, §4). This is
the finding the reviewer asked for: no single direction carries 30 tables alone.

---

## 3. Direction 2 — fit-to-content (and what "fit" must actually mean)

Today the "Fit to view" button calls `reset()` → `{scale: 1, tx: 0, ty: 0}`,
which is **100%, not fit** — on a tall graph it zooms *in*, worsening the
problem. Two distinct needs hide under one word:

1. **Initial framing.** The viewBox is already content-sized, so meet-scaling
   *does* frame the whole graph on mount — the defect is that "the whole graph"
   is 2364px tall and 480px is not enough room. The fix is upstream: grouping
   (§2) and collapse-above-threshold (§4) shrink the content so the initial
   frame is legible, rather than fighting the frame.
2. **A real fit control.** Replace `reset()` with a fit that measures the
   *rendered* SVG box (a ref + `getBoundingClientRect`) and sets `scale/tx/ty`
   to frame the current content in the current viewport — a genuine "fit
   everything" that also serves as the escape hatch after the user pans away.

Both stay pure/deterministic (a function of layout extents + measured viewport),
so the diff-overlay invariant (§8) is untouched.

---

## 4. Direction 3 — focus/search + collapse-above-threshold

At 30 tables the right interaction is **navigate, don't read-all**:

- **Focus:** selecting a node highlights its edges and its 1-hop neighborhood
  and dims the rest (the edge-active styling already exists; this extends it to
  nodes and makes selection center the neighborhood). Pure function of the
  selected id — deterministic.
- **Search/jump box:** type a table/measure name, jump-and-focus. This is what
  makes a large estate usable — you find `orders` and see its neighborhood, not
  the whole wall.
- **Collapse-by-default above a threshold:** when node count exceeds a limit
  (proposed: collapse the dimension and measure detail, show counts, expand on
  click), so a 30-table space opens *summarized* and the user drills in. This is
  what keeps the *initial* frame (§3.1) legible without a layout library.

---

## 5. Direction 4 — edge routing

The v1 carry-forward: straight lines drawn through node stacks are spaghetti at
scale. Move to **orthogonal elbows or column-aware quadratic curves**, keeping
the existing labels-on-demand (join `ON` predicate on hover, `replaces`/
`membership` styling). Routing is a pure function of the two endpoints' placed
coordinates — no new dependency, still deterministic.

**Edge ports on grouped cards (reviewer note).** With grouping (§2), attach
edges to the card's **edge-midpoints** — a left port and a right port — not to
its geometric center. A center-attached edge on a grouped card runs *through*
the card's own chip rows, re-importing a miniature of the exact spaghetti
grouping just removed; anchoring at the left/right ports keeps every edge in the
inter-column gutter where it belongs. Ports are a pure function of the placed
card box (`{x, y, w, h}`), so determinism is unaffected.

---

## 6. The layout-library question — explicit position

**Position: NO layout library. The hand-rolled deterministic layered layout
stays.** The four grounds recorded when force-graph was overturned at Prompt 12
(playbook lines 1434-1448) all still hold, and none of the three directions
above needs an external layout engine:

1. **Determinism is a hard requirement, not a preference.** The view is a diff
   overlay (a proposal drawn over the current model); a force layout produces
   two pictures for one space, which is poison under a diff. `dagre`/`elk` are
   deterministic, but see (2).
2. **The columns are product semantics, not an auto-layout artifact.** The
   left-to-right axis *is* the governance/lineage story (source → dim → metric
   view → measure), matching Catalog Explorer's ERD language users already know.
   A rank-based auto-layout would repack nodes by graph topology and **discard
   the column meaning** — we would be fighting the library to reimpose the axis
   it exists to compute.
3. **The dependency policy.** The repo pins exact versions with lockfile hashes
   and the standing "no new graph/layout dependency" rule; adding `dagre`/`elk`/
   `react-flow` for a 4-rank layered DAG we lay out in ~15 lines is a large,
   audited dependency for negative product value (it removes the axis in (2)).
4. **The real problem is cardinality within a column, not layout quality.** The
   fix is to *reduce* what a column holds (grouping §2, collapse §4) and let the
   user *navigate* (focus/search §4) — none of which a layout engine provides;
   they are product decisions above the layout primitive.

**The boundary that would flip this** (stated so a future reversal reads as a
decision, not drift): if the model ever became a genuinely free-form graph with
no meaningful ranks — many-to-many measure lineage across MVs where "which
column" is undefined — then a deterministic layered engine (`elk`, not force)
would be justified, and the "no new graph dependency" rule would be amended
**knowingly, in the register**, not by a quiet `npm install`. That is not
today's shape: the model is a 4-rank layered DAG, and grouping keeps it one.

---

## 7. What stays unchanged

- **Governance ladder is still the headline** — moved onto card chips + a
  card-level roll-up (§2), never leaning on hue alone (non-color labels stay).
- **Coverage badges (Prompt 12b)** stay on the grouped cards.
- **Determinism** — every direction here is a pure function of `(nodes, edges,
  selection, measured viewport)`; `renderToStaticMarkup` testability holds.
- **The error boundary + null-safe pan (Part 1)** are untouched.

---

## 8. Test plan (restating 12c's, made concrete against this note)

- **3/10/30-table fixtures render legibly** — structural assertions: column 3
  carries no per-measure nodes (grouping held); measure chips live inside their
  owning card; above the collapse threshold the 30-table fixture opens collapsed
  (counts present, detail absent) and expands on demand.
- **The collapsed view still tells the governance story at a glance** (reviewer
  note — the Suggest-Surface-Contract spirit applied to the graph). Assert
  explicitly that the collapsed 30-table fixture renders the **governance
  roll-up counts** — the panel-level ladder totals and the per-card
  governed/curated/ungoverned counts — *even when the per-measure detail is
  absent*. Collapsing detail must never collapse the governance signal; a
  30-table space that opens summarized still says how much of it is governed.
- **Fit-to-content** — the fit control frames content, never zooms a tall graph
  to 100% (assert the computed transform, not `reset()`'s identity).
- **Focus/search** — selecting a node dims non-neighbors; the search box
  jump-focuses by name (pure-function assertions).
- **Threshold derivation** — assert the collapse threshold is *computed* from
  the measured viewport (below), not a literal, and that the static fallback is
  used when no viewport measurement is available (SSR / initial paint).
- **Regression** — the pan/zoom null-race test and the error-boundary test
  (Part 1) stay green.

---

## 9. The collapse threshold is a derivation, not a magic number

The one parameter that looked like a taste call — the node count above which the
30-table view opens collapsed (§4) — is actually derivable from the same §1
arithmetic run backwards, and recording it as a derivation makes it
self-maintaining. Grouped table cards stack at `ROW_TOP(44) + N · ROW_GAP(58)`;
the legibility floor is a fit-scale of ≈ **0.7** in the viewport (below that,
labels stop being readable — the §1 cliff). Solving for the largest `N` whose
grouped stack still fits at ≥ 0.7 in a viewport of height `H`:

```
N_max = floor( (H / floorScale - ROW_TOP) / ROW_GAP )
      = floor( (480 / 0.7 - 44) / 58 )  ≈  11
```

So the proposed "~12" is not taste — it is what the arithmetic yields for the
default 480px viewport. **Implement it as a runtime derivation:** compute the
threshold from the *measured* viewport height and the legibility-floor scale,
with **12 as the static / SSR fallback** for the first paint before a
measurement exists. §7 already admits measured viewport as a layout input, so
determinism holds (the render is still a pure function of its inputs). The
payoff: a taller container automatically opens denser, a shorter one collapses
sooner, and no one ever retunes a literal. Record the derivation
(`viewport ÷ floor-scale ÷ row-height`, less the header offset) in the code
comment so the next reader sees a formula, not a guess.

Checkpoint closed — the note is approved; 12c Part 2 implementation proceeds as
its own commit when the queue reaches it, carrying the two reviewer notes (edge
ports §5, collapsed-roll-up test §8) and this derived threshold.
