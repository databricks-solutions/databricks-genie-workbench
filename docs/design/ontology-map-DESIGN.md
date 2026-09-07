# Ontology Map — DESIGN.md (Director artifact)

The single source of visual + interaction truth for the estate graph ("Ontology Map"). The
**Director** owns this file and the rubric at the end; the **Developer** builds to it; the
**Reviewer** scores against it (see the loop in `ontology-map-v3-fable-build.md` §11, MV-D80).

**NorthStar (the bar):** `docs/design/mockups/genie-ontology-knowledge-graph-v2.html` — an
interactive, hand-built reference. This DESIGN file specifies *the product* that meets that bar.
Read the mockup source once before building; it is the ground truth for feel, the spec below is the
ground truth for scope + reconciliation with the rest of the system.

Related decisions: MV-D81 (knowledge-graph tree model), MV-D82 (deep containment + typed-verb data
contract), MV-D79 (theme tokens), MV-D80 (the loop), MV-D74 (applied vs proposed), MV-D73 (snapshot +
expand), MV-D23 (plain language). Where the northstar and a prior MV-D disagree, §9 records the call.

---

## 0. What the northstar demonstrates (the bar)

The mockup is **one continuous hierarchical knowledge graph**, not a mode-switched box diagram. Study
it and you get, in a single legible surface:

1. **A real tree, root to leaf.** `Organization → Domain → Sub-domain → Asset → {Measure, Table}` —
   six levels, one canvas. The whole shape is an org-chart you can read at a glance.
2. **Expand/collapse in place.** Click any node to open or close its children; collapsed nodes wear a
   `+N` badge (descendant count). No LOD toggle, no re-mode — you *drill by clicking*, and the tree
   grows/heals with a 420 ms eased transition (children animate out of / back into their parent).
3. **Assets can hang directly off a domain or sub-domain.** `ref_calendar` sits under the *domain*;
   `mv_labor` sits under the *sub-domain*; `mv_fx_rates` under the *domain* — shared reference assets
   are not forced under a fake parent. Containment is honest.
4. **Agents contain their semantics.** `Genie Agent → Metric View → {Measure, Table}`. You can see the
   measures a metric view defines (`net_sales = SUM(sales_amt - returns_amt)`) and the tables it reads.
   This is exactly the "business snippets / measures inside a metric view" the estate view has been
   missing.
5. **Typed, verb-labeled relationships.** A second edge layer overlays the tree: dashed arcs carrying a
   verb — *also queries*, *shares dim_item*, *reads POS fact*, *reconciles with*, *converts via*,
   *composes*. Each is auto-classed **shared-within-domain** (slate) or **cross-domain** (maroon), by
   comparing each endpoint's domain ancestor. These are the "sensible lines based on validated data,"
   not a hairball.
6. **A right-rail inspector that is navigable.** Select a node → type pill, name, description,
   metadata, and a **Relationships** list (`part of`, `contains`, `queries`, `defines`, `reads`, plus
   the cross-link verbs with an `X-DOM` badge). Every relationship is a link — click it to jump and
   auto-expand the path to that node.
7. **Breadcrumb path** from root to the selected node (`NorthStar ▸ Finance ▸ Revenue Accounting ▸ …`).
8. **Search-to-reveal.** Type a name → matching nodes get an accent ring and the path to each is
   auto-expanded so hits are never hidden inside a collapsed branch.
9. **Legend = a type filter.** Click a type in the legend to dim everything else (focus by type).
10. **Direct manipulation.** Drag any node to reposition it; its edges follow live, and the offset
    survives re-layout (your tailoring sticks). Scroll to zoom, drag canvas to pan, **Fit** on demand.
11. **Deterministic layout.** `d3.tree` is pure geometry — the same estate looks identical every render
    and reload. No physics jitter, no settling.

Reference captures (my session): initial collapsed state and full `Expand all` — see the two
screenshots in the turn that created this revision. The feel to hit: **calm, typographic, deterministic,
drill-anywhere.**

---

## 1. What this surface is

- **Mode (impeccable): Operate.** The visitor navigates the estate to *understand and act*, not to be
  marketed to. Success = go Domain → Sub-domain → Asset → Measure/Table, read the relationships, get a
  quick fact on hover — fast, legible, in either theme.
- **It is the current estate, not a pitch.** Everything rendered is real snapshot/expand data (MV-D73).
  Proposals are visibly *suggested* (MV-D74). Never invent structure to look fuller (*reveal, don't
  invent*, R13).
- **The tree is the *organized* estate; the mess lives outside it (§3.5).** Real estates are messy —
  most assets carry no governed group. The solid tree shows only organized (Applied) structure;
  ungrouped assets sit in a separate **Ungrouped tray**, and engine proposals render as dashed
  **"Suggested" boxes** over that tray. *Reveal, don't invent* means we show the mess honestly, not tidy
  it away.
- **It is a panel inside the workbench**, so it obeys the app's light/dark theme (MV-D79). The northstar
  mockup is drawn light-only; **the product is dual-theme** (§4.2, §9-D).

## 2. Non-negotiables (the spirit the detectors can't see)

1. **Dual-theme parity.** Every state first-class in **light and dark**. No forced theme. WCAG AA on both
   grounds is a *gate*, not a nicety.
2. **Honest states.** Loading / empty / error / stale each get a real, composed treatment (MV-D43). An
   empty Proposed reads "nothing to suggest," never a blank canvas.
3. **Legible at a glance.** No nameless dots, no hairball, no label warping. If you can't read it at the
   default zoom of the current expansion, it's broken.
4. **Mental-map stability.** Deterministic tree geometry; the same estate is identical across renders,
   reloads, and refreshes. Manual drags persist as offsets on top of that geometry.
5. **Plain language first (MV-D23).** No SQL / ids / jargon in the primary read. The northstar mockup
   surfaces `Expression: SUM(...)` and `Path: retail.gold.fact_sales_line`; in the product these live
   behind an opt-in **"Technical details"** disclosure, never in the at-a-glance line (§9-C).
6. **Reveal, don't invent.** Nothing on the canvas that isn't in the snapshot/expand contract.
7. **Provenance honesty (MV-D74).** Applied (governed) vs Proposed (engine-suggested) are visually
   distinct and labeled.

---

## 3. The model it renders (data)

### 3.1 Node taxonomy (8 types)
| Type | Role | Encoding intent |
|---|---|---|
| `org` | Metastore root (one) | Largest disc, neutral ink |
| `domain` | Business area | Large disc, ink; carries domain hue as a tint (§4.2) |
| `subdomain` | Sub-area | Medium disc |
| `agent` | Genie Agent | **Lava accent — agents only** (brand rule) |
| `dashboard` | AI/BI Dashboard | Blue |
| `metric_view` | Metric View | Green (+ the type glyph) |
| `measure` | Measure inside an MV | Amber, small chip |
| `table` | Table | Slate, small disc |

### 3.2 Containment rules (the tree)
- Canonical spine: `org → domain → subdomain → asset → {measure, table}`.
- **Assets may attach directly to a `domain` or `subdomain`** (shared reference assets) — the parent is
  wherever the governance actually puts it, not a synthesized bucket.
- **Agent → Metric View** (an agent is grounded in one or more MVs).
- **Metric View → Measure** (defines) and **Metric View → Table** (reads).
- **Dashboard → Metric View / Table** (queries / reads).
- Every non-root node has exactly one hierarchy parent; a node is a *cross-link* target for anything
  else (§3.3), which is how one conformed table (`dim_item`) is "shared by 4 MVs across 2 domains"
  without being duplicated in the tree.

### 3.3 Typed relational edges (the overlay)
- A cross-link carries a **verb** (annotation) and is **auto-classed** from its endpoints' domain
  ancestors: same domain → `shared` (slate dashed); different domains → `xdom` (maroon dashed, bowed
  more, bold verb).
- Verb vocabulary observed in the northstar (extend as signals allow): `also queries`, `shares <dim>`,
  `reads <fact>`, `joins calendar`, `converts via`, `composes`, `reconciles with`.
- Rendered **only when both endpoints are visible** (i.e., their branches are expanded) — this is what
  keeps the overlay from becoming a hairball at scale.

### 3.4 Data-contract gap (what the backend must add — MV-D82)
Today's `OntologyGraph` (types.ts / `layout.py`) provides: `domains` rollup nodes (2-level, `parent_id`),
a **flat** `assets` level keyed by `domain_id` (kind/size/cost), `assets.edges` + `subdomains.edges`
(FK/lineage/co-query), and a `snippets` blob (measures per `mv:` , pages per sub-domain) with an
expand-on-demand endpoint. The taxonomy endpoint separately gives applied `domain → subdomain → member`.

The northstar needs a **containment tree down to measures/tables** plus **typed verb edges**, neither of
which is fully in the contract:

| Northstar need | Today | Gap to close (MV-D82) |
|---|---|---|
| `org` root | none | Emit a single metastore root node |
| asset **parent within the estate** (agent→mv, mv→table, dash→mv) | assets flat, only `domain_id` | Emit `parent_id`/`contains` for asset→asset containment (agent⊃mv, mv⊃table) — the `agent:`/`mv:`/`asset:` edges already exist in the fused 17d graph |
| measures as tree children | in `snippets` only | Promote MV measures into the containment tree (they already exist in the snapshot) |
| asset attaches to domain **or** subdomain | `domain_id` only | Keep, but distinguish direct-to-domain vs direct-to-subdomain (parent is a domain or subdomain id) |
| edge **verb** + within/cross class | `kind` (lineage/coquery/other), no verb, no class | Derive a plain-language verb per edge kind; classify `shared` vs `xdom` from endpoints' domain ancestors |
| measure→measure composition/reconciliation | absent | Surface when the signal exists (e.g. a measure expression references another MV's measure); otherwise omit (reveal-don't-invent) |

This is a genuine backend enrichment, scoped as its own lane (§11). The renderer must **degrade
gracefully** to today's shallower contract (assets-as-leaves, no verbs) until the data lands, so the two
lanes ship independently.

**Derivation & honesty rules (Lane D).** The fused 17d graph already carries the raw edges — map each
`kind` to a plain verb and classify shared/xdom from the endpoints' domain ancestors:

| Signal edge `kind` | Plain verb | Notes |
|---|---|---|
| `mv_membership` (mv→table) | *reads* | Primary containment candidate for a table under an MV |
| `agent_scope` (agent→asset) | *uses* | Agent's grounded assets |
| `lineage_adjacency` | *feeds* / *reads* | Directed lineage |
| `join_key` (fk/shared-dim) | *shares <col>* / *joins* | The conformed-dimension links |
| `co_query` | *also queried with* | Weighted co-usage |
| `semantic_sim` | *similar to* | Embedding; keep low-weight |
| `tag_assignment` | (grouping) | Already consumed for domains, not a map edge |

- **One tree parent per node (canonical-parent rule).** A node has exactly ONE hierarchy parent; pick
  the strongest containment (`mv_membership` > `agent_scope` > direct domain/sub-domain attach). **Every
  surplus relationship becomes a typed verb cross-link** — this is exactly how a conformed table read by
  4 MVs appears *once* in the tree and as 3 `shares dim_item` cross-links (as the mockup does).
- **Emit now (from existing signals):** `org` root; `mv ⊃ table` (mv_membership); `agent ⊃ asset`
  (agent_scope); measures promoted into the tree (already in `snippets.measures`); asset `attach_level`
  (domain vs sub-domain); per-edge `verb` + `class`.
- **Defer, don't invent (R13):** `agent ⊃ metric_view` (today `agent_scope` targets tables, not the
  `mv:` node — needs the Genie space's referenced-MV list, a new signal) and `measure → measure`
  composition/reconciliation (no expression-cross-reference signal today). Flag both as follow-ups; the
  renderer simply won't draw what isn't emitted.

The **Ungrouped tray + proposal boxes** (§3.5) reuse data that already exists — `UngroupedBucket`
(taxonomy), `OntologyDrafts.domains[].members` (proposed groupings + their member assets), and the
reassignment decisions (MV-D37/D39). No new snapshot fields; the renderer just draws them **off-tree**.

### 3.5 Real estates are messy — the Ungrouped tray + Proposal boxes (MV-D83)
The idealized northstar nests everything cleanly. A real estate won't: most tables carry no governed tag
and no confident cluster. Three rules keep the map honest **without hiding the mess**:

- **The tree is the *organized* estate (Applied only).** `org→domain→sub-domain→asset` shows only
  governed/certified structure. It stays clean because we never force a loose asset under a fake parent
  (reveal-don't-invent). **`Ungrouped` is NOT a tree node** (this replaces the old "ungrouped domain
  node" — §9-F).
- **The mess lives *outside* the tree, in an Ungrouped tray.** A spatially separate zone on the same
  zoomable canvas (a demoted gutter/lane, divided + labeled `Ungrouped · N`) holds every asset with no
  applied group — neutral, dotted, packed, collapsed to a count, expandable + searchable. It is the
  honest backlog of un-organized assets, and its size is a KPI (curation progress = shrink the tray).
- **Proposals render as dashed "Suggested" boxes over the tray.** Each engine-proposed domain/sub-domain
  (`OntologyDrafts.domains`) draws a **dashed rounded hull** around the tray assets it would group, with a
  `Suggested: <name>` chip + a confidence **band** (MV-D35 — never a %). **Reassignment** proposals
  (MV-D37/D39) draw a dashed halo on an already-grouped *tree* asset + a dashed arrow to the suggested
  target ("Suggested move").
- **Approve = promote.** Selecting a dashed box opens the proposal in the inspector (why / evidence /
  confidence / members) with **Approve / Dismiss** (Phase-5 consented `SET TAG` apply, MV-D37/D26/D50).
  On approve, its members animate out of the tray and into the tree as a new **solid** group — the map
  visualizes curation as "shrink the tray."

This makes provenance (MV-D74) **spatial + structural**, not just a line style: Applied = solid *in* the
tree; Proposed = dashed hull *over* the tray; Ungrouped = neutral *in* the tray.

---

## 4. Visual language

### 4.1 Layout — deterministic tree (not force)
- Vertical tidy tree: `d3.tree().nodeSize([DX, DY]).separation(...)`, `linkVertical` hierarchy edges.
  Depth × row-height for y; sibling gap for x. **Pure geometry, single pass, no physics.**
- Cross-links are quadratic-bezier arcs bowed off the straight line (xdom bows ~1.4×), with the verb at
  the arc midpoint on a bg-colored plate.
- **Manual drag** writes a per-node `{dx,dy}` offset applied *after* layout, so tailoring survives
  expand/collapse and refresh; edges redraw live during drag (no transition).
- **Fit** computes the bbox of visible nodes and eases the camera to frame it; runs on first render,
  `Expand all`, and `Reset`.

### 4.2 Colour — type-primary, domain-tint, theme-token
- **The northstar colours by node *type*** (agent=Lava, dashboard=blue, MV=green, measure=amber,
  table=slate; org/domain/subdomain = ink shades). Adopt that as primary — it is what makes the graph
  instantly parseable ("green things are metric views"). This **supersedes the domain-hue-as-fill**
  choice from the shipped renderer (§9-B).
- **Domain identity** is carried by (a) tree position + breadcrumb and (b) a subtle **domain-hue tint**
  on the domain/sub-domain node ring and on that domain's hierarchy edges — never on asset fills.
- All colour comes from `graphTokens(resolvedTheme)` (MV-D79). Two literal sets (light/dark) for ground,
  label plate, edges, type hues, and the domain-tint palette. The renderer hardcodes no hex. Every type
  hue and domain tint clears **AA** (≥3:1 fill-vs-ground, ≥4.5:1 text-on-plate) in *both* themes.
- One accent = provenance, not decoration: Applied = solid, Proposed = dashed + "Suggested", Ungrouped =
  neutral dotted.
- **Lava is reserved for Genie Agents** (and nowhere else), matching the mockup and the brand rule.

### 4.3 Node rendering
- `circle.core` (radius per type) with a theme-appropriate stroke; **selected** = ink ring, **search
  hit** = accent ring.
- **Collapse badge**: a small ink disc + white `+N` on any collapsed node that has children (`N` =
  descendant count).
- **Name label** below the node on a **label plate** (§4.4) — never a bare halo. Font size steps by
  type/depth (org 14 → asset 10). Container/domain titles use display type; labels use body type.

### 4.4 Label plate
A translucent rounded chip behind every label — paper chip + dark text on light, `#0D1321` + light text
on dark. It flips with the theme and is the single element that "washed out" when the theme flipped in
the earlier build. Non-negotiable for legibility over edges/fills at any zoom.

### 4.5 Edge rendering
- **Hierarchy**: solid, low-contrast bezier (`linkVertical`). Faded to ~0.1 opacity under type-focus.
- **Shared (within-domain)**: slate dashed arc + slate verb label.
- **Cross-domain**: maroon dashed arc (bowed more) + bold maroon verb label + `X-DOM` treatment.
- Edges animate opacity on enter/exit; during drag they redraw without transition (follow the node).

### 4.6 Space & density
Generous row/sibling spacing so the tree breathes; collapsed-by-default beyond sub-domain keeps the
first paint calm (§7). Dot-grid ground gives drafting-table depth (fainter on light).

### 4.7 Tray + proposal-box styling (§3.5)
- **Tray zone:** visually demoted — a subtle divider + label (`Ungrouped · N`), fainter ground, neutral
  dotted node treatment, a packed grid/pack layout, capped with a `+N more` chip. It reads clearly as
  "not organized yet," never as a peer of the domains.
- **Proposal box:** dashed rounded hull (`stroke-dasharray`), domain-tint stroke at low weight, a
  `Suggested: <name>` plate + a confidence **band** (High/Medium/Low, never a %). Overlapping proposals
  resolve by rendering the focused/hovered hull emphasized and the rest hairline (or one-at-a-time on
  hover) so the tray never becomes a tangle of boxes.
- **Reassignment:** a dashed halo on the affected *tree* asset + a dashed arrow to the suggested target
  box; labeled "Suggested move" in the inspector.
- **Promote motion:** on approve, member nodes ease from their tray positions into the newly-solid tree
  group (honor `prefers-reduced-motion` → snap).

---

## 5. Interaction model

- **Drill in place.** Click a node → toggle expand/collapse of its children; selection + breadcrumb +
  inspector update together. Initial state: `org + domains + sub-domains` open, asset level visible,
  measure/table level collapsed (badge shows the count).
- **Hover = instant insight.** Debounced tooltip with 2–3 plain-language facts (type · what it is ·
  a number that matters). Direct-DOM/motion-value driven, never per-frame React state. Distinct from the
  click inspector.
- **Select = inspector.** Right rail: type pill, name, plain-language description, metadata KVs
  (plain-language; technical `Expression`/`Path` behind a "Technical details" toggle, §9-C), and a
  **Relationships** list that is fully navigable (click → expand path + select target).
- **Breadcrumb** shows the full ancestor path of the selection with `▸` separators.
- **Drag** to reposition (offset persists); **scroll** to zoom (`scaleExtent [.3, 2.5]`); **drag canvas**
  to pan; background click clears selection.
- **Legend type-focus.** Click a type → dim all other types (nodes + their edges). Click again to clear.
- **Search** filters by name: hits get an accent ring and their ancestor paths auto-expand so no hit
  hides in a collapsed branch.
- **Expand all / Reset view** buttons; **Fit** frames the visible set.
- **Motion.** Eased (cubic-out ~420 ms) enter/exit from the parent's position on expand/collapse; eased
  camera on Fit/drill; honor `prefers-reduced-motion` (snap, no tween).
- **Provenance toggle — Applied | Proposed | Both** (extends MV-D74). *Applied* = solid tree only;
  *Proposed* = the dashed "Suggested" hulls over the tray; *Both* = solid tree + dashed hulls together.
  Selecting a proposal box opens the inspector with **Approve / Dismiss** (Phase-5 apply); approving
  promotes its members from the tray into the tree (§3.5).
- **Honest states.** loading / empty / error / stale each composed; empty-Proposed reads "nothing to
  suggest"; an empty tray reads "every asset in scope is organized" (a *good* end-state, not a blank).

---

## 6. Scale strategy (the mockup is ~60 nodes; a real estate is thousands)

The northstar's feel must survive an estate with thousands of tables. Rules:
- **Collapsed by default** below sub-domain; the user reveals branches deliberately. First paint is
  always the calm `org→domain→subdomain` skeleton.
- **Per-parent cap** with a `+N more` child chip (as today's `perContainerCap`); the chip drills into a
  focused/scrolled list rather than dumping.
- **Search-to-expand** is the primary "jump deep" path; typing a name expands only the needed paths.
- **Top-N by centrality** at the asset tier (today's `TOP_N_BY_CENTRALITY = 2000`) with a truncation
  marker; the tree never tries to lay out every leaf at once.
- **`Expand all` is guarded** on large estates (confirm / cap) — it is a small-estate convenience, not a
  default.
- Cross-links render only between visible (expanded) endpoints, so the overlay density scales with what's
  open, not with the whole estate.

---

## 7. Reference exemplars & anti-references
- **Toward:** the northstar mockup (`genie-ontology-knowledge-graph-v2.html`) first; Neo4j Bloom/NVL
  (icons, captions, expand-on-demand, navigable relationships); NYTimes network infographics
  (typographic hierarchy, purposeful colour, verb annotation); Databricks LIDM (clean domain layout).
- **Away from:** a force-directed "hairball"; stock discs with black text-outlines; a single-theme
  microsite; nameless grey pills; random-looking lines; a mode-switch that hides the hierarchy.

---

## 8. RUBRIC / SCORECARD (the Reviewer fills this per phase)

Score each row **Pass / Fail** with a one-line reason + severity (**P0** blocks the phase / **P1** fix
this batch / **P2** follow-up). Copy this table into `docs/design/reviews/map-<phase>.md`.

### 8.A Mechanical gates (Gatekeeper — must be green before the Reviewer looks)
| # | Gate | How |
|---|---|---|
| G1 | `tsc -b` clean | build |
| G2 | eslint clean | `npm run lint` |
| G3 | `vitest` green (incl. model tests) | `npm run test` |
| G4 | Runtime lockfile graph byte-identical (unless a new dep is an explicit MV-D45) | `git diff package-lock.json` |
| G5 | No `backend/` / `packages/` edits in a frontend phase | `git diff --stat` |
| G6 | Layout-stability hash stable across 3 reloads | `__ontologyHarness` position hash |
| G7 | Visual-diff vs baseline only where intended | `diff.mjs` heatmaps |

### 8.B Craft rubric (Reviewer — scores the contact-sheet in BOTH themes)
| # | Criterion | Pass condition |
|---|---|---|
| R1 | **Dual-theme parity** | Every state legible + on-brand in light AND dark; no washed-out fills/labels |
| R2 | **Contrast** | Node fills ≥3:1 vs ground; label text ≥4.5:1 on plate; both themes |
| R3 | **No nameless dots** | Every node readable at the current expansion's default zoom |
| R4 | **No hairball** | Relations read as structure; cross-links gated to visible endpoints |
| R5 | **Hierarchy legible** | `org→domain→subdomain→asset→measure/table` obvious; collapse badges + counts |
| R6 | **Hover insight** | Hover shows 2–3 plain facts, smooth, distinct from click |
| R7 | **Drill smoothness** | Expand/collapse eases from parent; no jump-cut, no flicker |
| R8 | **Provenance honest** | Applied solid / Proposed dashed+"Suggested" / Ungrouped neutral |
| R9 | **Honest states** | loading/empty/error/stale each composed; empty-Proposed not blank |
| R10 | **Plain language** | No SQL/ids/jargon in any at-a-glance string; technical detail is opt-in only |
| R11 | **Type encoding** | Type readable by colour + shape/glyph, not colour alone |
| R12 | **Northstar fidelity** | Reads at the quality bar of `genie-ontology-knowledge-graph-v2.html` |
| R13 | **Reveal-don't-invent** | Nothing rendered that isn't in the snapshot/expand data |
| R14 | **Typed verb edges** | Cross-links carry a plain verb + within/cross-domain class; correct + legible |
| R15 | **Navigable relationships** | Inspector relationships link + expand-path to the target |
| R16 | **Breadcrumb + search-reveal** | Breadcrumb path correct; search expands paths to every hit |
| R17 | **Drag persistence** | Manual drag offset survives expand/collapse + refresh |
| R18 | **Deterministic geometry** | Same estate → identical layout across reloads (G6) |
| R19 | **Mess is honest, off-tree** | Ungrouped assets in a demoted tray with a count; never forced into the tree; empty tray reads as a good end-state |
| R20 | **Proposals read as suggestions** | Dashed "Suggested" hulls over the tray (+ reassignment marks); Approve/Dismiss present; approve promotes tray→tree |

A phase is **done** when G1–G7 are green and every applicable R-row is Pass (P2s may defer with a filed
follow-up). Bounded passes: one Reviewer round → Developer fixes P0/P1 in one batch → one confirm round →
stop (impeccable).

---

## 9. Reconciliations / decisions the northstar changes

- **9-A — Renderer model — RESOLVED (2026-09-07): d3-hierarchy layout math + a thin React-controlled
  SVG renderer** (MV-D84). The northstar is a **deterministic hierarchy tree + typed overlay**, authored
  in the winning idiom (the mockup uses `d3.hierarchy`/`d3.tree`/`linkVertical`/`d3.zoom`/`d3.drag` — zero
  Cytoscape/force), so it is a **porting spec**, not a reimplementation target. `d3.tree` (Reingold-Tilford
  tidy tree) is analytic/O(n)/physics-free → **byte-stable screenshots by construction** (the harness
  requires this; it disqualifies fcose + react-force-graph-2d). Worst-case visible scale (~2k nodes) is
  inside plain-SVG's comfort zone, and real DOM text/CSS-token theming/a11y are exactly what R1–R20 score
  (Canvas/WebGL trade these away for scale we don't occupy). **Runner-up: Cytoscape + `cytoscape-dagre`**
  (only path reusing the shell, but a layered-DAG layout mismatches a tidy-tree spine, and the four
  signature behaviors — drag-offset persistence, enter-from-parent, bowed verb-labeled arcs, dashed
  hulls + off-tree tray — fight the grain). **Dep footprint is small:** 4 of 5 runtime modules
  (`d3-shape`/`d3-zoom`/`d3-drag`/`d3-selection`) are already resolved in the lockfile transitively via
  `react-force-graph-2d` (which Lane R retires), so the genuinely net-new module is **`d3-hierarchy`**;
  animations stay CSS transitions (no `d3-transition`, no umbrella `d3`). **The two-renderer spike is
  skipped** — the gating criteria (fidelity, determinism, theme/a11y) are decided; a spike could only move
  the perf score, so it is replaced by an **in-path perf gate** (synthetic 2k-node/1.5k-edge worst case,
  measure pan/zoom in the Playwright loop; levers if needed: zoom-threshold label culling →
  `content-visibility` subtree culling → Canvas edge layer under SVG nodes, all API-preserving). The LOD
  segmented control is replaced by expand/collapse-in-place.
- **9-B — Colour by type, not by domain (supersedes the MV-D79 *fill* palette).** Type is the primary
  hue; domain identity moves to tree position + a subtle domain-hue **tint** on domain/sub-domain rings
  and edges. MV-D79's theme-token machinery + AA gates stay; only the *fill semantics* change.
- **9-C — Technical detail is opt-in (upholds MV-D23).** The mockup shows raw expressions/FQNs inline;
  the product keeps the at-a-glance read plain-language and puts `Expression`/`Path`/`Source` behind a
  "Technical details" disclosure in the inspector + tooltip.
- **9-D — Dual-theme (the mockup is light-only).** Ship both themes via graphTokens; the northstar's
  light palette is the seed for the light theme, with a dark counterpart at parity.
- **9-E — Deep levels need data (MV-D82).** Measures/tables-under-MV and agent→mv containment require the
  contract enrichment in §3.4; the renderer degrades to today's shallower contract until it lands.
- **9-F — Provenance is spatial (extends MV-D74; MV-D83).** Applied lives *in* the solid tree; Proposed
  renders as dashed "Suggested" hulls *over* an off-tree **Ungrouped tray**; Ungrouped is neutral *in*
  the tray. Approving a proposal (Phase-5 apply) promotes it into the tree. This **replaces** the old
  "ungrouped as a tree node" — the mess is shown honestly, outside the hierarchy, not tidied into it.

## 10. Reviewer prompt (paste-ready, run as a SEPARATE agent/context)

```text
You are the Reviewer for the Ontology Map. You did NOT write this code. Read, in order:
docs/design/ontology-map-DESIGN.md (esp. §2 non-negotiables + §8 rubric), the northstar mockup
docs/design/mockups/genie-ontology-knowledge-graph-v2.html, and the contact-sheet + visual-diff
heatmaps at frontend/src/ontology/harness/shots/<phase>/ (states × {light,dark}).

Score EVERY applicable row of the §8 rubric Pass/Fail with a one-line reason and a severity (P0 blocks /
P1 fix-now / P2 follow-up). Judge the contact-sheet in BOTH themes. Be specific and visual ("the amber
measure chip fill is invisible on the light ground — R2 Fail, P0"), cite the state+theme. Do NOT fix
anything and do NOT praise; output only the filled scorecard table + a short ranked defect list.
Write it to docs/design/reviews/map-<phase>.md. One round.
```

## 11. Build phasing (9-A resolved → d3/SVG; Lane R BUILD-READY)

Three lanes, parallelizable now that the renderer is chosen (wave pattern, disjoint subtrees):
- **Lane D (data, wheel/backend, MV-D82):** emit the `org` root + asset→asset containment + measures in
  the tree + per-edge verb + within/cross class. Additive to the snapshot; renderer degrades without it.
- **Lane R (renderer, frontend, MV-D81 + 9-A/B/C):** the deterministic tree, expand/collapse-in-place,
  typed verb overlay, drag-offset, breadcrumb, navigable inspector, legend focus, search-reveal — dual
  theme via graphTokens, behind the harness + DDRG loop (MV-D80), scored against §8.
- **Lane P (polish):** motion, reduced-motion, scale guards (§6), honest states, accessibility.

## 12. Open decisions (Director)
- ~~**9-A renderer choice** (d3 vs Cytoscape-dagre)~~ — **RESOLVED 2026-09-07: d3-hierarchy + SVG**
  (MV-D84, §9-A). Two-renderer spike skipped; replaced by the in-path perf gate.
- ~~**Layout dep**~~ — **RESOLVED:** adopt `d3-hierarchy` (net-new) + promote the already-transitive
  `d3-shape`/`d3-zoom`/`d3-drag`/`d3-selection` to direct deps; retire `react-force-graph-2d`, then the
  Cytoscape trio after the flag flips (MV-D84, carve to MV-D45).
- **Measure→measure edges** ship only when a real signal exists (expression cross-reference); until then
  omit (R13).
- **Deep drill breadth** (asset→table children, dashboard→table) tracks the Lane-D data as it lands.
