# Ontology Map — DESIGN.md (Director artifact)

The single source of visual + interaction truth for the estate graph ("Ontology Map"). The
**Director** owns this file and the rubric at the end; the **Developer** builds to it; the
**Reviewer** scores against it (see the loop in `ontology-map-v3-fable-build.md` §11, MV-D80).

Related: MV-D79 (theme tokens), MV-D80 (the loop), MV-D73/74/75 (the data/API + v2 shell it renders).
Reference mockups: `docs/design/mockups/17.0h` (Sigma), `17.0i` (Reagraph), `17.0j` (Cytoscape — the
chosen compound-container metaphor).

---

## 1. What this surface is

- **Mode (impeccable): Operate.** The visitor navigates the estate to *understand and act*, not to be
  marketed to. Success = they can go Domain → Sub-domain → Asset → detail, read relationships, and get
  a quick fact on hover — fast, legible, in either theme. Scanability, hierarchy, honest states, and
  native graph affordances outrank decoration. Brand lives in precise detail, not ornament.
- **It is the current estate, not a pitch.** Everything rendered is real snapshot/expand data
  (MV-D73). Proposals are visibly *suggested* (MV-D74). Never invent structure to look fuller
  (*reveal, don't invent*).
- **It is a panel inside the workbench**, so it obeys the app's light/dark theme (MV-D79). It is NOT a
  standalone dark microsite.

## 2. Non-negotiables (the spirit the detectors can't see)

1. **Dual-theme parity.** Every state is first-class in **light and dark**. No forced theme. WCAG AA
   on both grounds is a *gate*, not a nicety.
2. **Honest states.** Loading / empty / error / stale each have a real, composed treatment (MV-D43).
   An empty Proposed reads "nothing to suggest," never a blank canvas.
3. **Legible at a glance.** No nameless dots, no hairball, no label warping. If you can't read it at
   the default zoom of its LOD, it's broken.
4. **Mental-map stability.** Deterministic seeded layout; the same estate looks the same across
   renders, reloads, and (P4) refreshes.
5. **Plain language.** Zero SQL / ids / jargon in anything the user reads (MV-D23).

## 3. Visual language

### 3.1 Type
- Display (container/domain titles): **Cabinet Grotesk** (shipped `public/fonts/*`). Body/labels:
  **General Sans**. No text-outline halos — labels sit on a **plate** (§3.3).
- Sizes: container title 13/700; sub-container 10.5/600; domain hub caption two-line 11.5/600; asset
  10/600. `min-zoomed-font-size` guards only the far zoom-out, never the default drilled zoom.

### 3.2 Colour — theme tokens (MV-D79)
- Colour is produced by `graphTokens(resolvedTheme)` (build spec §10.1). The renderer never hardcodes
  a hex. Two literal sets (light/dark) for ground, label plate, icon stroke, edges, and the domain
  palette.
- **Domain palette**: 12 hues, one per top-domain via a stable hash (`colorForTop`), with a
  **lightness-tuned variant per theme**. Same hue *family* across themes so a domain's identity is
  constant. Each hue clears **AA** (≥3:1 fill-vs-ground; ≥4.5:1 text-on-plate) in both themes.
- Colour encodes **business area**; it is never the *only* signal for type (type = icon + shape too,
  so colour-blind users aren't stranded).
- One accent for provenance, not decoration: Applied = solid; Proposed = dashed + "Suggested";
  Ungrouped = neutral dotted (it is neither applied nor suggested).

### 3.3 Label plate (the §9 lesson)
A translucent rounded chip behind every label — paper chip + dark text on light, `#0D1321` + light
text on dark. This is what keeps labels readable over fills/edges at any zoom, and it is the single
element that "washed out" when the theme flipped. It flips with the theme.

### 3.4 Iconography & shape (type encoding)
Per-type SVG glyph (lucide-family, inline data-URI, stroke = `tokens.iconStroke`): table, metric_view
(+ cyan ring), dashboard, genie_agent (hollow + violet rim), measure, page. Shape reinforces:
containers = rounded rects; hubs/assets = discs; measures/pages = small rounded chips.

### 3.5 Space & density
Group-in-a-Box whitespace: domains breathe; sub-domain boxes are clearly separated; assets are large
enough to read with a "+N more" cap rather than a dump. Dot-grid ground gives drafting-table depth
(fainter on light).

## 4. Interaction model

- **Hierarchy / navigation.** LOD segmented control **Domains | Sub-domains | Assets**; Assets requires
  a focused domain (drill in, not all-at-once). Breadcrumb `Estate ▸ Domain ▸ Sub-domain`. P4:
  double-click to drill, background to ascend, semantic zoom (LOD follows zoom).
- **Hover = instant insight (P2).** A debounced tooltip with 2–3 quick facts (kind · business area ·
  degree/works-with · ~cost). Motion-value / direct-DOM driven, never per-frame React state. Distinct
  from the click inspector.
- **Select = focus+context.** Tap fades the rest, reveals the node's real connections, frames it with
  an eased camera; the right-rail inspector gives plain-language what / where / works-with / cost.
- **Expand-on-demand.** Metric view → measures; sub-domain → Pages (satellite chips), bounded +
  degrade-to-empty.
- **Relations.** Only validated edges (FK/lineage/co-query from the snapshot). Typed styling (lineage
  solid, co-query dashed, satellite hairline), consistent curvature, cross-box hidden until focus. P3:
  a deterministic layered layout for the focused lineage view so relations read as flow, not noise.
- **Motion.** Eased camera on drill/select/clear; hover rim; fade for focus+context. Honor
  `prefers-reduced-motion`. No motion without a reason (hierarchy / feedback / state).

## 5. Reference exemplars & anti-references
- **Toward:** Neo4j Bloom/NVL (icons, captions, expand-on-demand), Databricks LIDM (clean
  domain-based compound layout, preset positions), NYTimes network infographics (typographic
  hierarchy, purposeful colour, annotation).
- **Away from:** stock Cytoscape discs + black text-outlines; a force-directed "hairball"; a
  single-theme microsite; nameless grey pills; random-looking lines.

---

## 6. RUBRIC / SCORECARD (the Reviewer fills this per phase)

Score each row **Pass / Fail** with a one-line reason + severity (**P0** blocks the phase / **P1** fix
this batch / **P2** follow-up). Copy this table into `docs/design/reviews/map-<phase>.md`.

### 6.A Mechanical gates (Gatekeeper — must be green before the Reviewer looks)
| # | Gate | How |
|---|---|---|
| G1 | `tsc -b` clean | build |
| G2 | eslint clean | `npm run lint` |
| G3 | `vitest` green (incl. model tests) | `npm run test` |
| G4 | Runtime lockfile graph byte-identical | `git diff package-lock.json` |
| G5 | No `backend/` / `packages/` edits in a frontend phase | `git diff --stat` |
| G6 | Layout-stability hash stable across 3 reloads | `__ontologyHarness` position hash |
| G7 | Visual-diff vs baseline only where intended | `diff.mjs` heatmaps |

### 6.B Craft rubric (Reviewer — scores the contact-sheet in BOTH themes)
| # | Criterion | Pass condition |
|---|---|---|
| R1 | **Dual-theme parity** | Every state legible + on-brand in light AND dark; no washed-out fills/labels |
| R2 | **Contrast** | Node fills ≥3:1 vs ground; label text ≥4.5:1 on plate; both themes |
| R3 | **No nameless dots** | Every node readable at its LOD's default zoom |
| R4 | **No hairball** | Relations read as structure; cross-box edges gated to focus |
| R5 | **Hierarchy legible** | Domain→Sub-domain→Asset nesting obvious; containers titled + counted |
| R6 | **Hover insight (P2+)** | Hover shows 2–3 quick facts, smooth, distinct from click |
| R7 | **Drill smoothness** | LOD change / drill / select eased, no jump-cut, no flicker |
| R8 | **Provenance honest** | Applied solid / Proposed dashed+"Suggested" / Ungrouped neutral |
| R9 | **Honest states** | loading/empty/error/stale each composed; empty-Proposed not blank |
| R10 | **Plain language** | No SQL/ids/jargon in any visible string |
| R11 | **Type encoding** | Type readable by icon+shape, not colour alone |
| R12 | **Mockup fidelity** | Reads at the quality bar of `17.0j` (and the §5 exemplars), not "1990s" |
| R13 | **Reveal-don't-invent** | Nothing rendered that isn't in the snapshot/expand data |

A phase is **done** when G1–G7 are green and every R-row is Pass (P2s may defer with a filed
follow-up). Bounded passes: one Reviewer round → Developer fixes P0/P1 in one batch → one confirm
round → stop (impeccable).

---

## 7. Reviewer prompt (paste-ready, run as a SEPARATE agent/context)

```text
You are the Reviewer for the Ontology Map. You did NOT write this code. Read, in order:
docs/design/ontology-map-DESIGN.md (esp. §2 non-negotiables + §6 rubric), the reference mockups
docs/design/mockups/17.0h|17.0i|17.0j, and the contact-sheet + visual-diff heatmaps at
frontend/src/ontology/harness/shots/<phase>/ (states × {light,dark}).

Score EVERY row of the §6 rubric Pass/Fail with a one-line reason and a severity (P0 blocks / P1
fix-now / P2 follow-up). Judge the contact-sheet in BOTH themes. Be specific and visual ("the amber
domain hub fill is invisible on the light ground — R2 Fail, P0"), cite the state+theme. Do NOT fix
anything and do NOT praise; output only the filled scorecard table + a short ranked defect list.
Write it to docs/design/reviews/map-<phase>.md. One round.
```

## 8. Open decisions (Director)
- **Layered layout dep (P3):** try a no-dep deterministic layered layout first; `elkjs`/`dagre` is an
  explicit MV-D45 dependency decision, not a silent add.
- **Screenshot tool:** default zero-dep `user-playwright` MCP; a DEV-only exact-pinned Playwright is
  acceptable (devDependencies + documented) if a committed, repeatable script is wanted.
- **Deep drill levels:** asset-type breadth + asset→table children are a backend/data follow-up (the
  snapshot's asset level is all tables today) — file it; the map lights them up when the data lands.
