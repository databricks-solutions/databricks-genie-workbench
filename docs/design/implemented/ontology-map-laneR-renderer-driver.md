# Ontology Map north-star — **Lane R (tree renderer)** Goal-Mode driver (MV-D81 / D83 / D84)

## 🚀 How to launch (Claude Code, on `ontology`)

This driver is the **build spec the subagent obeys** — it does **not** itself create the worktree.
Worktree isolation comes from the launch: `.claude/settings.local.json` (`{"worktree":{"baseRef":
"head"}}`) + the `ontology-lane-builder` agent (`isolation: worktree`) are already in place. On a
clean `ontology` working tree, paste this **launch prompt** into Claude Code (NOT the Driver-prompt
block below — that one is for the subagent):

```text
Launch ONE ontology-lane-builder subagent (isolation: worktree, branched from the current ontology
HEAD). Give it exactly this driver: docs/design/ontology-map-laneR-renderer-driver.md — tell it to
read the driver in full and obey its OWNS / OFF-LIMITS / MERGE-ORDER header literally.
```

The subagent gets its own temporary worktree off your local `ontology` HEAD, runs the Driver prompt
(below) end-to-end offline, and STOPs before deploy. **The §8 Reviewer is a SEPARATE run** (fresh
context) — do not fold it into this subagent (MV-D80). If you instead paste the Driver-prompt block
into a plain session, it edits your working tree directly on `ontology` (no isolation).

---

## ⚙️ Parallel-build lane header (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). This is the **renderer** lane of the north-star map. §9-A is
**RESOLVED → d3-hierarchy layout math + a thin React-controlled SVG renderer** (MV-D84). You
replace the Cytoscape-fcose-LOD render core with a deterministic `d3.tree` tidy tree + typed
verb overlay + off-tree Ungrouped tray + dashed proposal hulls, dual-theme via `graphTokens`,
behind the MV-D80 harness/DDRG loop and scored against the §8 rubric (R1–R20). Frontend-only;
offline; **STOP before deploy**. Consumes the Lane-D contract (MV-D82, already landed +
deploy-verified: `root`, `parent_id`/`attach_level`, edge `verb`/`rel_class`) and **degrades**
when those fields are absent.

- **OWNS (create/edit freely):**
  - `frontend/src/ontology/estateGraphModel.ts` (reshape to feed the tree) + a **new**
    `frontend/src/ontology/ontologyTreeLayout.ts` (pure layout fn — no DOM, no d3-selection)
  - `frontend/src/ontology/components/EstateGraph.tsx` (render core → SVG) + its
    `EstateGraph.test.tsx`, `estateGraphModel.test.ts`, + a new `ontologyTreeLayout.test.ts`
  - `frontend/src/ontology/components/GraphInspector.tsx`, `GraphSearch.tsx`, `GraphMinimap.tsx`
    (rewire against the layout output — behavior preserved)
  - `frontend/src/ontology/graphTokens.ts` (§9-B: colour-by-**type**; keep dual-theme + AA gates)
  - `frontend/src/ontology/harness/` (add tray/proposal scenes to `tools/matrix.mjs` if useful;
    keep existing scene ids stable), `frontend/src/ontology/cytoscape-shims.d.ts` (delete once
    Cytoscape is gone)
  - `frontend/package.json` + `frontend/package-lock.json` (deps below — exact pins only)
- **OFF-LIMITS (do NOT touch):** ALL backend + wheel (`backend/**`, `packages/**`),
  `frontend/src/ontology/types.ts` (frozen by Lane D — read only), `api.ts`, `OntologyPage.tsx`
  beyond the single `<EstateGraph>` mount, the non-graph components (`DraftsView`, `TaxonomyView`,
  `TagsLens`, `ApplyPreview`, `*DraftCard`, `SettingsForm`, `PermissionBanner`, `FreshnessControls`),
  and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** independent of any other open lane; Lane D already merged. Lane P (motion,
  reduced-motion depth, scale-guard tuning, deep a11y) is a **follow-up** lane, not this one.

### 🔒 Dependency change (carve to MV-D45, per MV-D84)

Add (runtime, exact pins — confirm latest patch on npm at install time): `d3-hierarchy 3.1.2`,
`d3-shape 3.2.0`, `d3-zoom 3.0.0`, `d3-drag 3.0.0`, `d3-selection 3.0.0` (the last four are
already resolved transitively via `react-force-graph-2d` — promote to direct). Dev: matching
`@types/d3-*`. **Remove** `react-force-graph-2d` now; remove `cytoscape`/`cytoscape-fcose`/
`react-cytoscapejs` once the SVG renderer is green. NO `d3-transition`, NO umbrella `d3` —
animations are CSS transitions (`prefers-reduced-motion`-aware). Must install without
`--legacy-peer-deps`; lockfile must validate.

---

## Spec & decisions

- **Spec (source of truth):** `docs/design/ontology-map-DESIGN.md` — §0 (the bar) → §3 (node
  taxonomy / containment / typed edges / §3.4 contract / §3.5 tray+proposals) → §4 (layout /
  colour-by-type / node+edge rendering / §4.7 tray+proposal styling) → §5 (interaction) → §6
  (scale) → §8 (rubric R1–R20) → §9 (reconciliations 9-A..9-F) → §11 (phasing).
- **Decisions:** `mv-advisor-playbook.md` **MV-D81** (tree model), **MV-D83** (messy reality:
  off-tree tray + dashed proposals + promote-on-approve), **MV-D84** (renderer = d3/SVG, dep
  carve). Honor **MV-D79** (theme tokens + AA), **MV-D80** (DDRG loop + rubric), **MV-D74**
  (Applied|Proposed|Both), **MV-D23** (plain-language; technical detail opt-in), **MV-D35** (never
  a raw %; confidence band), **MV-D43** (degrade-not-hang), **MV-D45** (dep discipline).
- **Read first:** the northstar mockup `docs/design/mockups/genie-ontology-knowledge-graph-v2.html`
  (it is the porting spec — `d3.hierarchy`/`tree`/`linkVertical`/`zoom`/`drag`), the current
  `EstateGraph.tsx` + `estateGraphModel.ts` (what survives = chrome, model parsing), `graphTokens.ts`,
  `harness/README.md` + `harness/tools/*` (shoot/contact/diff), and `AGENTS.md`.

---

## 🔁 Live inner loop (localhost harness — frontend-only, vision-driven)

The subagent is authorized (and expected) to **iterate against a live localhost render** — this is
the "Developer" inner loop of the MV-D80 DDRG pattern, made vision-driven:

1. `npm run dev` in `frontend/` starts **Vite serving the harness** at `http://localhost:5173`
   (entry `harness/main.tsx`), driven entirely by `harness/mockApi.ts` **fixtures**. This is
   **NOT** `uvicorn` / the full app — it needs **no Databricks, OBO, Lakebase, or serving
   endpoint** and does not violate the repo's no-local-server rule (`AGENTS.md`). Scenes/themes
   are selected via the harness URL params + `localStorage` (see `harness/README.md`).
2. Edit renderer/layout/tokens → the harness **hot-reloads** on `:5173`.
3. `npm run map:shots` (→ `map:contact`) captures the theme × scene matrix and builds
   `contact.png`. **The subagent then READS the contact sheet PNG itself** (image input),
   critiques it against the §8 rubric (R1–R20) + the northstar mockup, and edits again. Loop
   until the render matches the mockup's bar.
4. Determinism holds throughout: `shoot` waits on `window.__ontologyHarness.ready` (synchronous
   seeded layout), so screenshots are stable and `map:diff` is meaningful.

**Role separation (do NOT collapse):** this self-critique loop is the **Developer** improving its
own work. The **final §8 scoring stays a SEPARATE Reviewer** agent/context (fresh eyes, did not
write the code) writing `docs/design/reviews/map-laneR.md` — per MV-D80. The inner loop makes the
Developer's output good; the Reviewer decides if it passes. Still **STOP before deploy** — the
localhost harness is the only "live" surface the subagent touches; the real app deploy-verify is
human-gated.

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: Ontology Map RENDERER LANE (MV-D81/D83/D84). Replace the Cytoscape-fcose-LOD core with a
DETERMINISTIC d3.tree tidy tree + typed verb overlay + off-tree Ungrouped tray + dashed proposal hulls,
dual-theme (graphTokens), behind the MV-D80 harness. Frontend-only; branch ontology. Consumes the landed
Lane-D contract; DEGRADES when absent (MV-D43).

SPEC (truth): docs/design/ontology-map-DESIGN.md §0/§3/§4/§5/§8/§9. DECISIONS: MV-D81/D83/D84; honor
MV-D79/D80/D74/D23/D35/D43/D45. RULES: AGENTS.md. READ FIRST: the mockup
genie-ontology-knowledge-graph-v2.html (porting spec), EstateGraph.tsx + estateGraphModel.ts,
graphTokens.ts, harness/README.md.

DEPS (exact pins; MV-D45 carve): add d3-hierarchy + promote d3-shape/-zoom/-drag/-selection to direct
(+@types/d3-*, dev); REMOVE react-force-graph-2d now, the cytoscape trio + shims once SVG is green. NO
d3-transition/umbrella d3 — CSS only. npm ci WITHOUT --legacy-peer-deps.

P-A ontologyTreeLayout.ts (NEW, PURE — no DOM/d3-selection; unit-testable): (blob, expandedSet,
dragOffsets, cfg) -> {nodes, spineLinks, crossLinks, trayItems, proposalHulls, bounds}. Visible hierarchy
from root+parent_id+expandedSet; children sorted by STABLE key (attach_level,name,id) => byte-stable.
d3.hierarchy->tree().nodeSize; dragOffsets as post-layout deltas; cross-link bezier ctrl pts (bow by
rel_class, xdom wider) + label mids; Ungrouped tray = fixed off-tree column right of bounds. Degrade: no
root/parent_id -> today's shape.

P-B EstateGraph.tsx -> SVG: layered <g>s (spine, cross-links, labels, nodes, tray+proposals)
React-rendered from P-A. d3 via TWO refs: d3.zoom on <svg> (transform) + d3.drag on nodes
(offsets ref + persisted store; commit on end; defaultPrevented => drag vs drill-click). Expand/collapse-
in-place (+N badge); NO LOD control; CSS enter-from-parent.

P-C graphTokens.ts §9-B colour-by-TYPE (agent/mv/measure/table/dashboard hues; domain identity = tree
position + ring/edge tint), dual light+dark, AA kept. Port chrome over P-A: GraphInspector
(relationships = links that expand-path+select; technical detail behind a disclosure, MV-D23), breadcrumb,
GraphSearch (search-to-reveal), legend type-focus, GraphMinimap, Fit/Expand-all/Reset.
Nodes: tabindex/role/aria-expanded + focus ring. Applied|Proposed|Both toggle drives tray/proposals (MV-D74).

P-D tray+proposals (MV-D83 §3.5/§4.7): solid tree = Applied only; Ungrouped is NOT a tree node — it lives
in the tray (divider + "Ungrouped · N", neutral dotted nodes, +N-more cap). Proposals = dashed "Suggested:
<name>" hulls over the tray + confidence BAND (never %, MV-D35); reassignment = dashed halo + arrow.
Approve opens the inspector + stub-animates members tray->tree as a solid group (Phase-5 apply out of
scope). Empty tray => all-organized.

P-E PERF GATE (replaces the spike): harness stress scene 2000 nodes/1552 edges all expanded; measure
pan/zoom. If < ~50fps: zoom-threshold label culling, then content-visibility subtree culling.

INNER LOOP (localhost, frontend-only — NOT uvicorn, no Databricks): npm run dev (Vite harness @:5173,
fixtures) -> edit -> map:shots -> map:contact -> READ the contact PNG yourself (vision) +
self-critique vs §8/the mockup, repeat until it matches. Developer role; FINAL §8 scoring is a SEPARATE
Reviewer (MV-D80).

GUARDRAILS: deterministic (stable sort => byte-identical layout); degrade-not-hang; NO dep beyond d3;
NEVER touch backend/wheel/types.ts/playbook.

ACCEPTANCE (offline): vitest — ontologyTreeLayout unit+snapshot (stable sort, drag deltas, bow by
rel_class, tray, degrade) + EstateGraph render/interaction (expand/collapse, drag!=click, search-reveal);
tsc+eslint clean; lockfile validates; no cytoscape import left; the SEPARATE Reviewer scores §8 R1–R20
to docs/design/reviews/map-laneR.md (no P0/P1); P-E passes.

WORKFLOW: Do NOT deploy. When offline-green + Reviewer PASS, STOP and report branch, git diff --stat,
gate summary, Reviewer verdict; a human runs deploy-verify.
```

---

## After the run (human-gated)

This is the largest single lane — a render-core rewrite behind a frozen data contract. When the
subagent stops offline-green with a Reviewer PASS (no P0/P1), a human runs the deploy-verify gate
(`SKIP_FRONTEND_BUILD` **unset** — the frontend bundle changes here — `./scripts/deploy.sh --update`
on `fevm-serverless`, then eyeball the live Ontology Map: domains → drill to sub-domains → assets →
measures/tables, typed verb cross-links, Ungrouped tray, dashed proposals, light+dark). **Lane P**
(richer motion, deep a11y traversal, scale-guard tuning, honest-state polish) follows as its own
driver against the same spec §8 rubric.
