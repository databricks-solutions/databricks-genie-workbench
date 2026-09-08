# Ontology Map — Lane P (renderer richness / north-star fidelity polish) — MV-D85 — BUILD-READY

Closes the ~40% **renderer half** of the north-star gap (region analysis 2026-09-07): the
live map has the right skeleton (tidy tree, breadcrumb, inspector, legend, toggle, search,
minimap) but reads flat/gray/inert — no hover snippets, measures never render, generic
descriptions, container nodes look like UI chrome, hierarchy edges sweep off-canvas.

## 🚀 How to launch (Claude Code, isolated worktree)
> Create a worktree pinned to `ontology` HEAD (after the pre-seed carve lands), branch
> `laneP-polish`, then run the paste-prompt below in Goal Mode. Do NOT let tool-level
> `isolation: worktree` cut from the merge-base — pin the worktree to the exact HEAD SHA
> (the recorded gotcha). Build offline, run the harness inner loop, **STOP before deploy**.

## OWNS (frontend only)
- `frontend/src/ontology/components/EstateGraph.tsx`
- `frontend/src/ontology/components/GraphTooltip.tsx` (NEW)
- `frontend/src/ontology/components/GraphInspector.tsx` (surface real description + meta KV)
- `frontend/src/ontology/estateGraphModel.ts` (thread `description`/`meta`; merge expand results)
- `frontend/src/ontology/graphTokens.ts` (only if a new token is genuinely needed)
- the matching `*.test.ts(x)` + harness fixtures/scenes
- `docs/design/ontology-map-DESIGN.md` §8 rubric (add R21–R23)

## OFF-LIMITS
`backend/**`, `packages/**`, the **shape** of `types.ts` `OntologyGraphNode` (the optional
`description?`/`meta?` come from the pre-seed carve — CONSUME only, do not re-declare),
`src/watch/**`, the mockup, the playbook, harness `tools/**`, and `baselines/**` (shoot into a
fresh phase, never reset baselines).

## MERGE-ORDER
0. **Pre-seed carve (prerequisite — lands on `ontology` first):** `OntologyGraphNode` gains
   optional `description?: string|null` + `meta?: Record<string,string>|null` in `models.py`
   + `types.ts`, with `graph.py` passing them through from the blob. Branch from that HEAD.
1. Lane P and **Lane D2 (MV-D86)** are PARALLEL & independent. Lane P MUST degrade when
   `description`/`meta` are null (today's estate) — never require Lane D2 to ship.
2. Merge Lane P only after its own fresh-context §8 re-review PASS.

## Spec / decisions
`docs/design/ontology-map-DESIGN.md` (§0/§4/§5/§8); MV-D85 (this lane) + MV-D73 (expand-on-
demand, already shipped) + MV-D79 theme tokens + MV-D80 DDRG loop/rubric in the playbook.

---
## PASTE-PROMPT (Goal Mode) — fix these 6, frontend-only, additive, dual-theme

You are the Developer for Lane P (Ontology Map renderer polish), branch `laneP-polish`,
worktree pinned to `ontology` HEAD. Fix ONLY these 6 within OWNS; degrade gracefully; keep
determinism byte-stable; STOP before deploy.

1. **Hover-snippet tooltip** (new `GraphTooltip.tsx`): on node hover show — type label · full
   name · description · up to ~6 `meta` key/values · measure `Expression` in mono. Follows the
   cursor, clamps to the viewport, `pointer-events:none`, dual-theme via `graphTokens`, hides on
   drag/blur/leave. Keep the native `<title>` for a11y. This is the north-star's core "everything
   tells you something" layer.
2. **Re-enable measure/Page hydration (MV-D73):** on expanding a `metric_view`, call the existing
   `expandNode(id, origin)` and merge returned `kind="measure"` nodes (amber) as children (+
   `mv_measure` edges); on `subdomain` expand, merge `kind="page"` as attached Pages. Cache per
   node id; bounded; degrade-to-empty on error. Measures promote into the tree — the missing
   middle tier that makes the estate colourful and deep.
3. **Real descriptions/meta:** inspector + tooltip prefer `node.description` / `node.meta` over the
   generic `describe()`; fall back to the generic strings only when null.
4. **De-chrome containers:** render NO glyph on `org`/`domain`/`subdomain` (keep glyphs for
   `agent`/`dashboard`/`metric_view`/`measure`/`table`); container identity = fill + ring + size.
   Kills the "hamburger menu" look on sub-areas.
5. **Edges:** render spine links AND cross-links only when BOTH endpoints are on-screen (no arcs
   to off-canvas nodes); use a neutral hierarchy stroke (`tokens.spine`, not the domain-green
   tint); make `fit` reserve top padding so the top domain tier is never cropped.
6. **Legend:** add edge-type rows (hierarchy solid / shared dashed-slate / cross-domain
   dashed-maroon) + a one-line "click to drill · drag to move · hover for details" hint.

Gates: `npm ci`; `npx tsc -b` clean; eslint clean; `npx vitest run` green + NEW tests (tooltip
content assembly, measure-hydration merge + cache, container-has-no-glyph, spine/cross clip to
visible). Harness inner loop into phase `p5`: `npm run map:shots -- --phase p5` → `map:contact`
→ `map:diff -- --current shots/p5 --baseline baselines`; READ `shots/p5/contact.png` yourself and
self-critique vs DESIGN §8 incl. new **R21** (hover snippet present + legible) / **R22** (measures
render under MVs) / **R23** (containers read as areas, not chrome). Determinism: re-shoot `p5`
twice → 0% diff. Commit on `laneP-polish`. Do NOT merge/deploy, no Databricks/GSO job or API, no
governed tags, no uvicorn — only the local Vite fixture harness may run.
