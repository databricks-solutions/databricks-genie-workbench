# Ontology Map v3 — Fable build plan (MV-D76 visual bar · MV-D77 local visual feedback loop)

> **Who runs this:** Claude **Fable 5.1** in Claude Code (`/model fable` or
> `ANTHROPIC_MODEL=claude-fable-5-1`), long-horizon autonomous mode, vision-in-the-loop.
> **Branch:** `ontology`. **Read-only** to data/governed tags; **additive** frontend edits.
> **Predecessor:** Ontology Map **v2** (MV-D73/74/75) is LANDED + deploy-verified — v3 is a
> *visual + interaction* rebuild on the same data/API contract, **not** a data-model change.

---

## §0. Why (what "still doesn't make sense")

Map v2 is functionally correct (LOD toggle, Applied/Proposed, drill-down, expand-on-demand,
inspector, search, minimap all work; the React-#185 tap-crash is fixed via the per-instance
`cy`-callback idempotency guard in `EstateGraph.tsx` — **keep that guard**). But it *looks and
feels* archaic. Concrete, verified starting defects (from live screenshots + code):

1. **Garbled/warped labels.** Heavy `text-outline-width` + the wrong/absent web font makes
   container titles look distorted. (`STYLESHEET` in `EstateGraph.tsx`; fonts in `public/fonts/`.)
2. **Dashed = misleading.** `node[ntype="subcontainer"]` is *always* dashed regardless of
   `origin`, so **Applied** sub-areas read as "Suggested". Provenance styling must be driven by
   `origin`, not node role.
3. **Sparse asset boxes.** Assets look nearly empty because of the per-group cap
   (`cap = opts.perContainerCap ?? 60` in `estateGraphModel.ts`, and `EstateGraph` doesn't even
   pass one) **and** edges are hidden until you tap (edge-on-demand). "89 Assets" but each box
   shows 1–3. This is a *presentation* choice, not missing data.
4. **Thin inspector.** `nodeFacts` emits only domain + cost for an asset — no measures, lineage,
   usage, or "what is this / why here".
5. **No sense of a designed system.** No typographic hierarchy, flat/undifferentiated color,
   no motion, no annotation, no focus+context camera — it reads like a 1990s applet, not an
   information graphic.

**Root cause is framing, not model IQ.** The bottleneck has been (a) no crisp visual target,
(b) no fast loop for the agent to *see* its own output, (c) render-vs-data problems muddled
together. This plan fixes all three so Fable's vision-driven autonomy actually converges.

---

## §1. The target bar — "NYTimes-infographic quality" (MV-D76)

Fable must treat this as a **designed information graphic of the current estate**, not a
node-dump. The bar, made concrete:

**A. Visual system (modern).**
- **Typographic hierarchy:** use the shipped fonts (`public/fonts/` — Cabinet Grotesk display,
  General Sans text, JetBrains Mono for identifiers). Distinct scale/weight for
  Domain ≫ Sub-domain ≫ Asset ≫ measure/Page. **No heavy text-outline halos** — use a subtle
  label plate/shadow instead so text is crisp at every zoom.
- **Purposeful color:** one hue per top-domain (stable hash — keep `colorForTop`), assets tinted
  to their domain; reserved accents for metric-view (cyan) / agent (violet) / measure / Page.
  Color must *encode* (domain membership, type), never decorate. Respect dark theme tokens
  (`bg-surface`, `bg-elevated`, `text-primary/secondary/muted`, `border-default`, `accent`).
- **Encoding:** size = importance (member_count / centrality / cost), not random; edge weight =
  line weight/opacity; type = icon + shape. State each encoding in the legend.
- **Iconography:** per-type inline-SVG icons (already started) refined to a coherent set; crisp at
  small sizes; legible on the fills.
- **Whitespace & density:** generous padding, clear cluster separation (Group-in-a-Box), no
  overlap/collision of labels or boxes at the default zoom.

**B. Slick (feel).**
- **Motion with intent:** animated, eased camera transitions on drill-down / focus / clear
  (not instant jump-cuts); fade-in of expanded satellites; hover/selected micro-states.
- **Focus + context:** selecting a node dims the rest (keep the fade classes), smoothly frames
  the neighborhood; background tap restores. A selected node's edges animate in.
- **Progressive disclosure:** aggregate first → drill → expand-on-demand. Never show the whole
  hairball; reveal on intent.

**C. Fast.**
- Deterministic seeded layout stays (`randomize:false`) — instant, stable, no jitter run-to-run.
- Must stay smooth at **prod density (~2,892 nodes / 1,557 edges)**: honor `hideEdgesOnViewport` /
  `textureOnViewport`, debounce viewport→state, keep the per-group cap + "+N more" but make the
  *default* view legible (tune caps/spacing, not raw dump).
- **No new runtime dependency (MV-D45).** cytoscape canvas is the renderer; if a scale ceiling is
  hit, document it as a follow-up — do **not** add a WebGL lib in this pass.

**D. Drill-downable & explanatory (the "infographic" part).**
- Clean hierarchy: **Domain → Sub-domain → Assets/Pages → business snippets (measures/KPIs)**,
  each level self-explaining via breadcrumb + inspector.
- **Annotation layer:** short, plain-language captions/callouts that make the graphic *readable at
  a glance* — e.g. a domain's one-line "what this business area is / N assets / top measures",
  a selected asset's "in <domain>, ~$X/mo, feeds <measures>". Zero jargon, no SQL, no IDs
  (MV-D23).
- **Cross-concept linkages visible on intent:** measures/Pages appear as satellites on
  expand; lineage/co-query edges reveal on select. The graphic should make relationships
  *discoverable*, not hidden forever.

**E. Honest & robust.**
- **Applied vs Proposed** styling must be correct: Applied = solid/current-state; Proposed =
  dashed + "Suggested" chip — driven by `origin`, never by node role.
- Every state handled: loading / empty / error / stale (MV-D43). On this airline estate,
  `origin=proposed` is essentially just Ungrouped — the empty-proposed state must read honestly.

**Reference set Fable must study before coding:**
- Local mockups: `docs/design/mockups/17.0h-ontology-estate-graph-sigma-dark.html` (and any
  `17.0i` / `17.0j` siblings) — the intended look (clusters, hollow agents, evidence card).
- External exemplars (research, don't copy): **Neo4j Bloom / NVL** (captions, per-type icons,
  hierarchical layout, expand-on-demand), **Databricks LIDM** industry-model viewer (preset
  layout, domain regions, snappy drill), and **NYTimes/□ FiveThirtyEight graph infographics**
  (annotation, typographic restraint, purposeful color, guided reading).

---

## §2. The feedback loop — local visual dev + vision verification (MV-D77) — **BUILD THIS FIRST**

Nothing else converges without this. The graph is **prop-driven + pure** (`EstateGraph` takes
`graph: OntologyGraph`; `estateGraphModel.ts` is side-effect-free; the only runtime calls are
`getGraph(origin)` and `expandNode(node, origin)` in `api.ts`). So it renders fully on
`localhost` with **no backend** (the "no local server" rule is a *backend* rule — Vite dev is
fine).

**§2.1 Harness entry (dev-only, not in the prod bundle).**
- New dev-only entry, e.g. `frontend/graph-harness.html` + `frontend/src/ontology/harness/main.tsx`
  that mounts `<EstateGraph graph={fixture} />` inside the real app shell (same Tailwind, same
  fonts, dark theme) so what Fable sees == prod.
- Do **not** add it to `vite build` inputs / do **not** import it from `App.tsx`, so
  `npm run build` ignores it and the prod bundle is byte-unaffected. `npm run dev` serves it at
  `/graph-harness.html`.
- The harness exposes controls to force each state (origin, LOD, focused domain, a pre-selected
  node, loading/error) via URL query so screenshots are scriptable and deterministic.

**§2.2 Mock seam (no new runtime dep).** Preferred: an **injectable API seam** — thread
`getGraph`/`expandNode` (and the bulk/expand fns) through a small provider/prop so the harness
injects fixture-backed implementations; prod keeps the real `api.ts`. (Acceptable alt: a tiny
Vite dev-middleware plugin serving canned JSON for `/api/ontology/graph` + `/graph/expand` — also
zero runtime dep.) Do **not** reach for MSW (adds a devDependency).

**§2.3 Realistic fixtures (make-or-break).** Capture **real** payloads once, save as JSON under
`frontend/src/ontology/harness/fixtures/`:
- `graph.applied.json`, `graph.proposed.json` — from the live API
  (`GET /api/ontology/graph?origin=applied|proposed`) against the deployed app
  (`fevm-serverless`, catalog `serverless_stable_6t92c3_catalog`) with an OAuth+OBO token — this
  guarantees the fixture matches the contract and **prod density** (~2,892/1,557), not a toy.
- `expand.mv.json` (a metric-view → measures), `expand.subdomain.json` (a sub-domain → Pages),
  plus a **slow** and a **failing** variant so the loading/degraded states (MV-D43) get tuned.
- `graph.focused.json` — a single-domain subset for the typical drill-down.
- The tiny graphs in `estateGraphModel.test.ts` remain for unit logic; they are **too sparse**
  for visual tuning — never tune the look against them.
- Capturing is **read-only** (a `curl` of the deployed endpoint, or a read-only SELECT of the
  `genie_ont_graph_snapshot` blob). No writes, no job runs.

**§2.4 Vision verification step (the actual loop).**
- Drive `localhost:5173/graph-harness.html?...` with a **headless browser**, wait for the fcose
  layout to settle (listen for `layoutstop`, or a fixed idle), then screenshot each state:
  Domains, Sub-domains, Assets (focused), node-selected (inspector open), search-hit, expanded
  (measures/Pages), empty-proposed, loading, error. Write PNGs to
  `frontend/src/ontology/harness/shots/` (git-ignored).
- **Browser tooling — zero-runtime-dep first:** use the `user-playwright` MCP or the
  `web-devloop-tester` / agent-browser subagent to screenshot (no package.json change). If a
  scripted JS Playwright runner is genuinely preferred, add it **as an exact-pinned
  devDependency only**, update `package-lock.json`, and note it — never a runtime dep, never
  `--legacy-peer-deps` (Dependency Security Policy).
- Fable reads the PNGs with **native vision**, compares against the §1 target + the mockups, and
  iterates. Because layout is deterministic, shots are stable → these double as **visual
  regression** baselines.
- **Fonts:** the harness must load `public/fonts/*` so the label-rendering fix is judged against
  real type (defect #1 is likely font + outline).

**§2.5 Loop economics.** HMR edit → sub-second; screenshot cycle → seconds. vs. deploy-and-eyeball
≈ 5–6 min. That ~50–100× speedup is the whole point — it lets a long-horizon vision model
*converge* instead of thrash.

---

## §3. Render-vs-data split (so Fable fixes the right layer)

**Pure frontend (in scope for this plan — the majority):** all of §1 A–E; label/font fix;
origin-driven dashed styling; caps/spacing/legibility; motion & camera; inspector depth from
data already present; annotation layer; expand-on-demand polish; empty/loading/error/stale.

**Data already present, just un-surfaced (frontend to reveal):** cross-domain edges
(`domains.edges` / `assets.edges`; `subdomains.edges`=37), measures & Pages (baked `snippets`;
served by `/graph/expand`), `origin`, `parent_id`/`parent_name`, cost, member_count. Reveal these
— don't claim they're missing.

**Genuinely backend/wheel (OUT of this plan — flag, don't attempt):** any *new* fact not in the
snapshot/expand contract (e.g. new KPI types, usage-per-node, richer lineage). If §1 needs a
field the API doesn't emit, Fable **stops and documents a backend follow-up** (wheel
`layout.py`/`materialize.py` + `/graph/expand`, MV-D49 blob-only) — it must **not** edit
`backend/**` or `packages/**` in this pass.

---

## §4. Phasing (one long Fable run, screenshot-gated between phases)

- **Phase 0 — Loop (MV-D77):** harness entry + injectable mock seam + captured fixtures +
  screenshot step. **Exit:** Fable can render all states on localhost and produce PNGs offline.
- **Phase 1 — Visual system:** fonts/labels fix, typographic hierarchy, semantic color, icons,
  spacing, dark tokens, **origin-driven** provenance styling. **Exit:** Domains + Sub-domains read
  as a designed graphic (screenshot vs mockup).
- **Phase 2 — Assets legibility + inspector depth:** tune caps/spacing/nesting so Assets is
  legible at prod density; enrich `nodeFacts` + the right-rail inspector (what/why/measures/cost);
  polish expand-on-demand satellites. **Exit:** drilled Assets view is clear, inspector is rich.
- **Phase 3 — Interaction & motion:** eased camera transitions (drill/focus/clear), focus+context,
  search-to-focus, minimap fidelity, edge-reveal-on-select; verify smoothness at ~2,892 nodes.
  **Exit:** feels slick + fast; no jank; deterministic on reload.
- **Phase 4 — Infographic polish & a11y:** annotation/caption layer, legend, guided reading,
  keyboard/focus/ARIA, final visual pass against the NYT bar. **Exit:** the whole rubric (§5) is
  green.

Each phase: keep `estateGraphModel.ts` pure + unit-tested; `tsc`/`lint`/`vitest` green; commit on
`ontology`; produce a screenshot set; **STOP before deploy**.

---

## §5. Definition of Done — rubric + gates

**Visual rubric (Fable self-scores from its own screenshots, target = all "yes"):**
- [ ] Labels crisp at every zoom; no warped/outlined halos; correct fonts.
- [ ] Applied = solid/current; Proposed = dashed + "Suggested" — driven by `origin` only.
- [ ] Clear Domain ≫ Sub-domain ≫ Asset ≫ measure/Page hierarchy (type/size/color/icon encode meaning).
- [ ] Assets view legible at prod density — no empty-looking boxes, no overlap, "+N more" where capped.
- [ ] Inspector answers what/why/measures/cost in plain language (MV-D23) for every node type.
- [ ] Drill-down + focus use eased camera transitions; expand fades satellites in; hover/selected states.
- [ ] Cross-concept links discoverable on intent (expand + edge-on-select).
- [ ] Annotation/legend make the graphic readable at a glance.
- [ ] Empty (proposed=Ungrouped) / loading / error / stale all read honestly (MV-D43).
- [ ] Smooth pan/zoom at ~2,892 nodes; deterministic + byte-stable on reload.
- [ ] Side-by-side vs `17.0h` mockup + NYT bar: reads as a designed information graphic.

**Hard gates:**
- `cd frontend && npm ci && npm run lint && npx tsc -b && npm run test` — all green; **lockfile
  byte-identical** (or, if a dev-only Playwright runner was added, only `devDependencies` changed,
  exact-pinned, documented).
- `estateGraphModel.ts` stays pure + covered; determinism preserved.
- **No** edits to `backend/**`, `packages/**`, non-ontology frontend, or `mv-advisor-playbook.md`.
- Read-only to data + governed tags; STOP before deploy — a human runs the deploy-verify gate.

---

## §6. Guardrails (non-negotiable)

- **OWNS:** `frontend/src/ontology/**` (renderer, model, inspector/search/minimap components,
  types/api extensions, tests) + the **dev-only** harness (`frontend/graph-harness.html`,
  `frontend/src/ontology/harness/**`, fixtures, shots).
- **OFF-LIMITS:** `backend/**`, `packages/**`, any non-ontology frontend, the playbook, `app.yaml`,
  deploy scripts, `databricks.yml`.
- **Keep** the `react-cytoscapejs@2.0.0` gotcha fix: the `cy` callback runs on **every** update —
  the per-instance idempotency guard (`initedCyRef`) must remain or the tap-loop (#185) returns.
- **MV-D45:** no new **runtime** dependency; `package.json` runtime deps + `package-lock.json`
  runtime graph unchanged. Icons via inline SVG / existing `lucide-react`.
- **MV-D23** zero jargon/SQL/IDs in user copy; **MV-D43** every state handled; **MV-D49** the data
  contract is blob-only — don't invent fields (see §3).
- Determinism: seeded layout, stable on reload.

---

## §7. Launch prompt for Fable (paste into Claude Code, `/model fable`)

```text
You are Claude Fable 5.1 in Claude Code on branch `ontology`. Build "Ontology Map v3": a modern,
slick, fast, drill-downable estate graph at NYTimes-infographic quality. FULL SPEC (read in full
first): docs/design/ontology-map-v3-fable-build.md. Also read, in order:
frontend/src/ontology/{EstateGraph.tsx,estateGraphModel.ts,api.ts,types.ts,components/*},
OntologyPage.tsx, docs/design/mockups/17.0h-ontology-estate-graph-sigma-dark.html, and AGENTS.md.

MANDATE:
- BUILD §2 (the local visual feedback loop) FIRST: a dev-only Vite harness that mounts EstateGraph.
  The REAL fixtures are ALREADY captured in frontend/src/ontology/harness/fixtures/ (see its
  README: graph.applied/proposed + mv/subdomain expands) — LOAD these; do NOT hit the live API.
  Derive the focused view in-memory (focusTop) from graph.applied; synthesize slow/failing in the
  mock seam. Add an injectable mock seam for getGraph/expandNode (NO new runtime dep), loading the real
  public/fonts, and a headless-browser screenshot step (use the user-playwright MCP / agent-browser
  — zero-dep — or an exact-pinned DEV-only Playwright). Confirm you can screenshot every state
  offline before touching visuals.
- Then iterate §4 Phases 1→4 using your OWN screenshots vs the §1 target + the mockup, self-scoring
  the §5 rubric each phase. Fix the known defects: warped/outlined labels (fonts), dashed styling
  that ignores origin, sparse/empty Assets boxes (caps/edges), thin inspector.
- Keep it a graph of the CURRENT ESTATE (data already in the snapshot/expand contract — reveal it,
  don't invent). If §1 needs a field the API lacks, STOP and write a backend follow-up note; do NOT
  edit backend/ or packages/.

HARD RULES: additive within frontend/src/ontology/** + the dev-only harness ONLY. Preserve the
`cy`-callback idempotency guard (initedCyRef) — react-cytoscapejs calls it every update; losing it
re-introduces the #185 tap-loop. NO new runtime dependency (MV-D45); zero jargon/SQL/IDs (MV-D23);
handle loading/empty/error/stale (MV-D43); keep estateGraphModel.ts pure + tested; determinism
preserved. Do NOT deploy, run the job, write governed tags, or edit the playbook.

GATES (offline, each phase + at end): cd frontend && npm ci && npm run lint && npx tsc -b &&
npm run test — all green; lockfile runtime-graph byte-identical (dev-only Playwright, if added, is
exact-pinned + documented). Commit CODE on `ontology` per phase (capture a screenshot set each
phase but do NOT commit it — `harness/shots/` is git-ignored). When the §5 rubric
is all-green, STOP and report: files changed, git diff --stat, test summary, and the final
screenshot set (SHOW/attach it — `harness/shots/` is git-ignored, so commit CODE only, not the
PNGs). A human runs deploy-verify on fevm-serverless.
```

---

## §8. Open decisions (defaults chosen; flag to change)

- **Screenshot tool:** default = zero-dep (MCP/agent-browser). Flip to an exact-pinned dev-only
  Playwright runner if you want a committed, repeatable `npm` script for regression.
- **Motion library:** default = none (CSS/cytoscape animate). No `framer-motion` (would be a new dep).
- **Scale ceiling:** default = cytoscape canvas at current density. If it can't stay smooth,
  document a WebGL follow-up (new dep → separate decision), don't add it here.
- **Playbook:** proposed **MV-D76** (visual bar) + **MV-D77** (feedback loop) — add the BUILD-READY
  pointer + register entries on greenlight (mirrors prior stages).

---

## §9. v3.1 — light-mode gap remediation (MV-D78) — LANDED, then SUPERSEDED by §10

> ⚠️ **Superseded.** The forced-`.dark` scope below correctly diagnosed the root cause but fixed it
> with a shortcut (it drops light mode). Keep the diagnosis; the *remedy* is replaced by the
> theme-token system in **§10 (MV-D79)**. This section is retained as the paper trail.

**What the deploy-verify eyeball found.** v3 landed green, but on the live app the map read
"1990s / washed-out / empty grey pills". Root cause (found by re-running the §2 loop): the harness
forces `document.documentElement.classList.add("dark")` (`main.tsx`), so every fill (0.06 opacity),
white icon, glow and dark label plate in the stylesheet was tuned for the dark canvas `#0D1321` — but
production `OntologyPage` inherits the **app theme**, and the user's workbench is in **light** mode,
where `--bg-sunken` is `#f1f5f9`. Same code, opposite ground → the map washed out. It was never a
layout/data defect; it was a theme-scope defect the dark-only harness could not surface.

**Decision (MV-D78): the map is a fixed dark "observatory" panel, theme-independent.** All three
bakeoff mockups (`17.0h/i/j`) are dark by design; a dark instrument panel embedded in a light app is
the intended, deliberate look (cf. Neo4j Bloom). `EstateGraph`'s root (and its loading/empty/error
cards) now carry a local `.dark` class, which re-scopes the CSS variables **and** Tailwind `dark:`
variants for that subtree only — the surrounding workbench chrome keeps the user's chosen theme.

**Also in v3.1 (verified via the loop, both themes):** bigger, higher-contrast asset nodes
(`px 14+…`), asset labels `font-size 10 / weight 600 / #CBD5E1` with `min-zoomed-font-size 7` (no more
nameless dots), and a touch more Assets-LOD separation/repulsion so the FK fans read as hub-spoke
structure, not "random lines". Edges are already validated-only (FK/lineage/co-query from the
snapshot; cross-box hidden until tap — MV-D43), so "sensible lines" was a contrast problem, now fixed.

**Loop note (MV-D77 hardened):** the harness gained a `?theme=light|dark` param (default dark) so the
loop can reproduce the production light-mode rendering — the one state the dark-only harness hid.
Screenshots driven headless via the `user-playwright` MCP against `npm run dev`; zero runtime-dep
change (lockfiles byte-identical).

---

## §10. v3.2 — theme-token visual system (MV-D79) — BUILD-READY

**Goal.** Make the map's colour a first-class **dual-mode** system that follows the app theme, and
delete the §9 forced-`.dark` shortcut. The map is an **Operate** surface — it must obey `useTheme`
like every other panel. Verified in BOTH themes by the §11 loop; contrast is a mechanical gate.

**Why a token function (not CSS).** Cytoscape renders to `<canvas>`; canvas paint cannot read the
app's CSS custom properties the way DOM elements do. So the map's colours must be an explicit JS
palette, selected by the resolved theme, and fed into the stylesheet the renderer builds.

### 10.1 The token contract

Add a **pure** module `frontend/src/ontology/graphTokens.ts`:

```ts
export type ResolvedTheme = "light" | "dark"

export interface GraphTokens {
  ground: string            // canvas background (matches --bg-sunken per theme)
  dotGrid: string           // faint radial dot-grid overlay
  labelPlateBg: string      // chip under labels: paper (light) / #0D1321 (dark)
  labelPlateOpacity: number
  textPrimary: string       // container/domain titles
  textSecondary: string     // sub-domain / asset labels
  iconStroke: string        // glyph stroke: dark on light, light on dark
  containerFillOpacity: number   // higher on light (0.06 dark fills vanish on paper)
  edge: string              // lineage/default hue
  edgeCoquery: string       // co-query dashed hue
  edgeSnippet: string       // measure/Page satellite hairline
  hoverRim: string
  focusRing: string
  ungrouped: string
  palette: string[]         // 12 domain hues, LIGHTNESS-TUNED FOR THIS THEME
}

export function graphTokens(theme: ResolvedTheme): GraphTokens { /* light & dark literals */ }
```

Rules for the two literal sets:
- **Grounds** mirror the app tokens: light `#F1F5F9` / dark `#0D1321` (so the panel sits in the app,
  not against it). The dot-grid opacity drops on light.
- **Label plate flips**: light = near-white chip (`#FFFFFF`, ~0.85) + `#0F172A` text; dark = `#0D1321`
  (~0.82) + `#F8FAFC` text. This is the single most important flip — it's what "washed out" in §9.
- **Icon stroke flips**: `#0F172A` on light, `#F8FAFC` on dark (the current hardcoded `%23F8FAFC`
  becomes a token; the SVG data-URI helper takes the stroke colour as an argument).
- **Container fill opacity** is higher on light (~0.10–0.14) than dark (~0.06) — translucent hues
  disappear on paper otherwise.
- **`palette`**: the 12 domain hues get a **per-theme variant**. Each hue must clear **WCAG AA (≥3:1
  for the node fill vs ground, ≥4.5:1 for text on its plate)** in BOTH themes. Dark-tuned hues
  (`#818CF8`, `#6EE7B7`, `#FCD34D`…) are darkened/desaturated for the light ground; keep the SAME
  hue *family* per `colorForTop` hash so a domain's identity is stable across themes and LODs.

### 10.2 Wiring `EstateGraph`

1. `const { resolvedTheme } = useTheme()` at the top of the component.
2. `const tokens = useMemo(() => graphTokens(resolvedTheme), [resolvedTheme])`.
3. `const STYLESHEET = useMemo(() => buildStylesheet(tokens), [tokens])` — refactor the current
   module-level `STYLESHEET` array into a `buildStylesheet(tokens)` factory; every hardcoded colour
   (`#0D1321`, `#F8FAFC`, `#94A3B8`, `0.06`, edge hues…) reads from `tokens`.
4. The domain/asset colours come from `graphTokens(theme).palette` via `colorForTop`/`assetColor` —
   thread `tokens.palette` into `estateGraphModel.ts` (or pass a `palette` arg) so the model emits the
   theme-correct hue on each node's `data.color`.
5. Add `resolvedTheme` to the canvas remount `key` (`EstateGraph.tsx` ~L934) so a theme toggle
   rebuilds the styled canvas. (Cytoscape can restyle in place, but a remount is simplest and the key
   already changes on view changes; determinism is preserved because layout is seeded.)
6. **Delete** the four `dark ` class prefixes added in §9 (root + loading/empty/error cards). The
   surrounding chrome tokens (`bg-surface`, `border-default`, `text-*`) now resolve to the app theme
   again, which is correct.

### 10.3 Guardrails
No new runtime dep (MV-D45). `estateGraphModel.ts` stays pure + tested (extend the model tests for the
palette-arg path). Keep the `initedCyRef` guard and the seeded layout. Additive, read-only, **STOP
before deploy** — a human runs the deploy-verify after the §11 loop is green in both themes.

---

## §11. The Director / Developer / Reviewer / Gatekeeper loop + theme-matrix harness (MV-D80) — BUILD-READY

**This section is the loop-infra spec.** It generalises the §2/MV-D77 harness into a role-separated,
instant-feedback loop so long-horizon UI work converges without one agent grading its own paper — and
without an open-ended self-QA money-burn (`impeccable`: *bounded passes, not loops*).

### 11.1 Roles & artifacts

| Role | Who | Owns | Cadence |
|---|---|---|---|
| **Director** | human + orchestrator | `docs/design/ontology-map-DESIGN.md` + the **rubric/scorecard** in it; reference mockups `17.0h/i/j`; the phase list | set once per phase |
| **Developer** | build agent on a worktree branch | the code for ONE phase; runs the instant inner loop below | continuous within a phase |
| **Reviewer** | a SEPARATE agent (or `impeccable critique`/`audit` with vision) | scores the contact-sheet against the rubric; emits a defect list w/ severities to `docs/design/reviews/<phase>.md` | ONCE per phase boundary (1 round + 1 confirm) |
| **Gatekeeper** | deterministic tooling | `tsc -b` + lint + `vitest` + visual-regression diff + deploy-verify | every commit |

The Developer and Reviewer are **different contexts** (the author is blind to their own defects) — this
maps onto the existing worktree/lane + goal-mode pattern (the Reviewer is a distinct subagent).

### 11.2 The instant inner loop (Developer)

`npm run dev` (Vite HMR) + a save-triggered screenshot pass. The Developer looks at ONE image per
iteration, not 20.

**State matrix** (the harness is already URL-addressable, `?theme` added):
```
scenes  = domains, subdomains, assets, assets+select, mv-expand, proposed, stale, empty, error, stress
themes  = light, dark
```
≈ 20 shots. Two dev-only tools to add under `frontend/src/ontology/harness/tools/` (git-ignored
outputs, NOT in `vite build` inputs, prod bundle byte-unaffected):

1. **`shoot.mjs`** — drives the browser (zero-dep `user-playwright` MCP, or an exact-pinned DEV-only
   Playwright) across the matrix against `http://localhost:5173/graph-harness.html?…`. Waits for
   `window.__ontologyHarness.ready === true` (deterministic seeded layout ⇒ no flaky sleeps), shoots
   each state in each theme, writes PNGs to `harness/shots/<phase>/<state>.<theme>.png`.
2. **`contact.mjs`** — stitches the matrix into ONE **contact-sheet** montage (grid: rows = scenes,
   cols = light|dark) with captions, so the Developer/Reviewer reason over the whole surface at once.
   No new dep: compose via a `<canvas>`/sharp-free approach (draw the PNGs onto an offscreen canvas in
   a tiny headless page, or emit an HTML index that tiles them and screenshot THAT).

**On-save watch** (optional convenience): a `watch.mjs` (chokidar already transitively present, or
`fs.watch`) that re-runs `shoot` + `contact` on save and, when a baseline exists, a **visual-diff**.

### 11.3 Visual-regression diff (Gatekeeper + Developer)

Because layout is seeded/deterministic, a committed baseline set makes pixel-diffs meaningful.
`diff.mjs` compares `harness/shots/<phase>/*` against `harness/baselines/*` (a per-pixel delta; a
threshold %; write a heatmap PNG for any state over threshold). Baselines are updated deliberately
(`--update`) when a change is intended, never silently. This is what turns "did I regress the other
LOD/theme?" from an eyeball into a gate.

### 11.4 The Reviewer pass (batched, at the phase gate)

The Reviewer (separate context) reads `ontology-map-DESIGN.md` + the rubric, the contact-sheet, the
mockups, and the visual-diff heatmaps, then fills the **scorecard** (§ rubric in DESIGN.md) — pass/fail
per criterion with a one-line reason and a severity (P0 blocks / P1 fix-now / P2 follow-up). Output to
`docs/design/reviews/map-<phase>.md`. **One round**, then the Developer fixes the P0/P1s in **one
batch**, then **one confirm** round. Stop. (This is `impeccable`'s bounded-pass rule; the Reviewer is
not an every-keystroke oracle.) A ready-to-paste Reviewer prompt lives in the DESIGN.md.

### 11.5 Phasing (each phase: Developer builds → contact-sheet → Reviewer once → fix → confirm → Gatekeeper)

- **P0 — loop infra**: `shoot.mjs` + `contact.mjs` + `diff.mjs`, baselines captured, DESIGN.md + rubric
  written. (Enabling investment; no visual change.)
- **P1 — theme tokens (§10 / MV-D79)**: dual-mode colour, delete forced-`.dark`. Gate: contrast passes
  in both themes; both-theme contact-sheet clean.
- **P2 — hover insights**: debounced hover tooltip (motion-value driven, no per-frame React state) with
  2–3 quick facts; distinct from the click inspector.
- **P3 — relations & hierarchy**: typed/curved edges, no-hairball, a deterministic layered (no-dep)
  layout trial for the focused lineage view (elk/dagre only as an explicit dep decision, not silent).
- **P4 — navigation smoothness**: semantic zoom, double-click drill / background ascend, cross-refresh
  position persistence (mental-map stability).

The deeper drill levels (asset-type breadth, asset→table children) are a **data/backend** gap — the
snapshot's asset level is all tables today; *reveal, don't invent*. File a backend follow-up; the
frontend lights those levels up when the data exists.

### 11.6 Guardrails
All harness/loop tooling is **dev-only**: git-ignored outputs, never imported by the app, not in the
`vite build` input set, no runtime dep (an exact-pinned DEV Playwright, if used, is `devDependencies`
+ documented — the runtime lockfile graph stays byte-identical). Read-only to data + governed tags.
