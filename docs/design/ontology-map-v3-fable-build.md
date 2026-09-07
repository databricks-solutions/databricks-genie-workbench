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
