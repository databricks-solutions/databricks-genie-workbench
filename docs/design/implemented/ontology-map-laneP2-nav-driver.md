# Ontology Map — Lane P2 (navigation & relationship legibility) — MV-D87 — BUILD-READY

Live-feedback pass (2026-09-08) on the deployed map. Four interaction gaps: (1) the minimap
is inert (no viewport box, no click/drag pan); (2) relational lines show only a bare verb on
the focused node's arcs — no hover detail, no direction, no legend; (3) no collapse-all, no
fullscreen, canvas stuck at 520px so the tree strands in a thin band; (4) no way to show/hide
individual domains. **P0 = fullscreen + edges** (the two the user flagged first).

## 🚀 How to launch (Claude Code, isolated worktree)
> Worktree pinned to `ontology` HEAD (after the edge pre-seed carve lands), branch
> `laneP2-nav`. Run the paste-prompt in Goal Mode; frontend-only; harness inner loop; **STOP
> before deploy**. Do NOT spawn the `ontology-lane-builder` agent (its baked `isolation:
> worktree` re-cuts from the stale merge-base) — build in-session in the pre-made worktree.

## OWNS (frontend only)
- `frontend/src/ontology/components/EstateGraph.tsx` (fullscreen, collapse-all, minimap wiring,
  edge hover + arrowheads + rel-legend, domain-visibility filter application)
- `frontend/src/ontology/components/GraphMinimap.tsx` (make interactive: viewport box + click/drag pan)
- `frontend/src/ontology/components/GraphInspector.tsx` (edge selection detail, if useful)
- `frontend/src/ontology/components/DomainVisibilityPanel.tsx` (NEW — right-rail show/hide)
- `frontend/src/ontology/components/GraphTooltip.tsx` (add an edge-tooltip variant)
- `frontend/src/ontology/estateGraphModel.ts`, `frontend/src/ontology/ontologyTreeLayout.ts`,
  `frontend/src/ontology/graphTokens.ts` (arrowhead/edge-legend/panel tokens)
- the matching `*.test.ts(x)` + harness scenes; `docs/design/ontology-map-DESIGN.md` §5/§8 (add R24–R27)

## OFF-LIMITS
`backend/**`, `packages/**`, the **shape** of `types.ts` `OntologyGraphEdge` (the optional
`detail?` comes from the edge pre-seed carve — CONSUME only), `src/watch/**`, the mockup, the
playbook, harness `tools/**`, and `baselines/**` (shoot into a fresh phase, never reset).

## MERGE-ORDER
0. **Edge pre-seed carve (prerequisite — lands on `ontology` first):** `OntologyGraphEdge` gains
   optional `detail?: Record<string,string>|null` (`models.py` + `types.ts`, `graph.py` flows it
   through `_level`). Branch from that HEAD.
1. Lane P2 and **Lane E (MV-D88)** are PARALLEL & independent. Lane P2 MUST degrade when `detail`
   is null (edge tooltip then shows verb + endpoints + class only) — never require Lane E to ship.
2. Merge after its own fresh-context §8 re-review PASS.

## Spec / decisions
`docs/design/ontology-map-DESIGN.md` (§5 interaction, §6 scale, §8 rubric); MV-D87 (this lane) +
MV-D79 tokens + MV-D80 DDRG loop/rubric. KG-idiom reference (Neo4j Bloom/Explore): edge caption =
relationship type; hover reveals type + selected properties; legend styles/labels rel-types with
counts; direction via arrowheads; parallel edges group with a number.

---
## PASTE-PROMPT (Goal Mode) — 4 interaction fixes, frontend-only, additive, dual-theme

You are the Developer for Lane P2 (Ontology Map navigation & relationship legibility), branch
`laneP2-nav`, worktree pinned to `ontology` HEAD. Build ONLY within OWNS; degrade gracefully;
keep determinism byte-stable; STOP before deploy. Build in P0→P1 order.

**P0-a — Fullscreen + bigger canvas + collapse-all.** Add a **Maximize** toolbar button that
pops the map into a full-viewport overlay (Esc / a close button restores; the graph re-fits on
enter/exit). Raise the default canvas height (e.g. `70vh`, min 520). Add a **Collapse all**
button beside `Expand all` that collapses every container to the domain tier and re-fits.

**P0-b — Relationship lines carry real info.** For every VISIBLE cross-link (not just the focused
node's), make the arc hoverable and show an **edge tooltip** (reuse `GraphTooltip`): `From → To`
(names), the verb, within-domain ("shared") vs cross-domain, and — when the edge carries the
pre-seed `detail` map — its evidence lines (e.g. "Shares: customer_id, flight_id", "Co-queried:
42 sessions"); degrade to verb+endpoints+class when `detail` is null. Add **direction arrowheads**
(SVG `marker-end`, themed). Add **relationship-type rows to the legend** — one per verb/kind
present, with a count, click-to-highlight that type (Bloom-style); keep the existing node-type
focus rows.

**P1-a — Minimap navigation.** Feed the live viewport rect into `GraphMinimap` and draw the
"you-are-here" box; make click and drag on the minimap recenter the main view (map minimap coords
→ `d3.zoom` transform). Keep it presentational-pure otherwise.

**P1-b — Domain show/hide.** New right-rail `DomainVisibilityPanel`: list domains (expandable to
sub-domains) with checkboxes; unchecking hides that subtree (+ its cross-links) from the tree,
minimap, and legend counts. "Show all / Hide all" affordance. Visibility state is view-only
(never mutates the snapshot).

Gates: `npm ci`; `npx tsc -b` clean; eslint clean; `npx vitest run` green + NEW tests (collapse-all
tier, fullscreen enter/exit re-fit, minimap click→transform math, edge-tooltip content assembly +
degrade, domain-hide filters edges+counts, rel-type legend counts). Harness inner loop into phase
`p6`: `map:shots --phase p6` → `map:contact` → `map:diff`; READ the contact sheet and self-critique
vs DESIGN §8 incl. new **R24** (fullscreen + framed tree, no stranded band) / **R25** (edge hover
detail + arrowheads legible) / **R26** (minimap navigable) / **R27** (domain show/hide). Determinism:
re-shoot `p6` twice → 0%. Commit on `laneP2-nav`. Do NOT merge/deploy, no Databricks/GSO job or API,
no governed tags, no dependency (`package-lock.json` byte-identical), no uvicorn — only the local
Vite fixture harness may run. The §8 Reviewer is a SEPARATE fresh-context run — do not fold it in.
