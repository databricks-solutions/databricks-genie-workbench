# Ontology Map — Reviewer Scorecard · Lane P2 (map navigation & relationship legibility, MV-D87, phase p6)

**VERDICT: FAIL** — **2 P1 fails, 0 P0.** Both fails land on the new Lane P2 rows scored with
most scrutiny: **R25** (the direction *arrowheads* the phase promises are not legibly rendered on the
one visible cross-link, in either theme) and **R27** (the domain show/hide *panel* is never observable
in any still — only the "Domains" toolbar toggle exists). Everything the prior Lane P p5 pass covered
(R1–R23) still holds against the p6 pixels — no regressions that flip a row — and the other two new
rows land: **R24** (Fullscreen + Collapse-all controls present, canvas is now a tall ~70vh framed panel,
the northstar/proposed trees are framed not stranded) and **R26** (a live cyan "you-are-here" viewport
box is drawn in the minimap across every graph scene). This is a fresh, skeptical review judged on the
p6 pixels, not on the changelog: I cropped both cross-link endpoints at 6× and inspected every scene's
right rail before scoring R25 and R27. Four rows remain N/A-static (hover / drill-motion /
relationship-click / drag). A new wide-estate weakness appears in the 53-node `degrade`/`stale`
fixtures — fit strands the tree in a thin horizontal band with ~70% vertical whitespace and shrinks
leaf labels — held at P2 (structure + domain tier stay legible).

## Mechanical gates (visual-review scope only)
| # | Gate | Result | Note |
|---|---|---|---|
| G1 | `tsc -b` clean | Not checked (out of visual-review scope) | |
| G2 | eslint clean | Not checked (out of visual-review scope) | |
| G3 | `vitest` green | Not checked (out of visual-review scope) | |
| G4 | lockfile byte-identical | Not checked (out of visual-review scope) | |
| G5 | no backend/packages edits | Not checked (out of visual-review scope) | |
| G6 | layout-stability hash stable | Not checked (out of visual-review scope) | d3-tree geometry is analytic/physics-free → deterministic by construction; no jitter artifact visible. |
| G7 | visual diff only where intended | Not checked (out of visual-review scope) | |

## Craft rubric (both themes) — R1–R27
| # | Criterion | Pass/Fail | Severity | One-line reason (scene · theme) |
|---|---|---|---|---|
| R1 | Dual-theme parity | PASS | — | All nine scenes first-class in both grounds (contact sheet): plates, dot-grid, inspector, hull, tray, minimap and toolbar all re-theme; no forced theme, no washed-out fill. |
| R2 | Contrast (fill ≥3:1; text ≥4.5:1 on plate; both themes) | PASS | P2 | Amber measures + green MV read on the pale ground (`northstar+select.light` bottom row); type discs read on both. Nit (carried): hierarchy spine edges are very pale on light (`northstar.light`, `degrade.light`, `stale.light`). |
| R3 | No nameless dots | PASS | P2 | Labels present with plates (`northstar`, `proposed`, `northstar+select`). Nit (worse at 53 nodes): `degrade.{light,dark}` / `stale.{light,dark}` shrink leaf discs+labels to near-illegible at the fit zoom — read by drilling/minimap. |
| R4 | No hairball | PASS | — | Overlay gated to selection: `stale`/`degrade` (nothing selected) draw the tidy tree + a `+65 links — select a node to trace its relationships` hint, not an arc cloud; `northstar+select` draws exactly one on-canvas `reads` arc. |
| R5 | Hierarchy legible | PASS | P2 | `northstar.{light,dark}`: `org(Acme)→domain(Finance/Operations)→asset` with a `Finance Agent +3` collapse badge. Nit: `degrade`/`stale` compress the wide asset row into a thin band at fit (see defect 3). |
| R6 | Hover insight | N/A-static | — | Debounced cursor-following tooltip is runtime; not observable in stills. Click inspector present and does the plain-language job. |
| R7 | Drill smoothness | N/A-static | — | Enter-from-parent easing / expand-collapse motion cannot be judged from stills; no frozen jump-cut visible. |
| R8 | Provenance honest | PASS | — | `northstar` = solid Applied tree; `proposed.dark` = dashed `Suggested: Loyalty` hull over a neutral dotted `Ungrouped · 5` tray; footers match (`Applied…` / `Proposed (engine suggestions — nothing applied yet)`). |
| R9 | Honest states | PASS | — | `loading` = spinner + build copy; `empty` = "No data…" + subtext; `error` = ⚠ + snapshot-read copy; `stale` = amber "Snapshot may be out of date" pill over the live map. Composed in both themes (contact sheet). |
| R10 | Plain language | PASS | — | `northstar+select` inspector reads "Net sales after returns and discounts, by day and channel.", relationships as `part of / contains / reads`; no SQL/ids/FQN at a glance, either theme. |
| R11 | Type encoding (shape/glyph, not colour alone) | PASS | P2 | Genie Agent is correctly the only Lava node; MV carries a bar-chart glyph (`net sales`, both themes). Nit (carried): agent/dashboard/table leaf discs still separate mostly by hue at default zoom — glyph coverage is uneven. |
| R12 | Northstar fidelity | PASS | P2 | `northstar.{light,dark}`: centered Fit, ellipsis labels (`Revenue Account…`), neutral spine, minimap w/ viewport box, breadcrumb, top `Acme` tier uncropped. Nits (both carried + new): legend overlaps the low `Finance Agent` node (defect 2); wide-estate fit strands the band (defect 3). |
| R13 | Reveal-don't-invent | PASS | — | Counts honest (`Ungrouped · 5`; `+26 more`; `+65 links`; `shares key with · 250` in `stale`); no measure→measure edges invented (correctly deferred per §3.4/R13). |
| R14 | Typed verb edges | PASS | — | `northstar+select.{light,dark}` draws a plain verb on the arc (`reads`) with the correct cross-domain class (maroon dashed + `X-DOM` in the inspector). |
| R15 | Navigable relationships | N/A-static | — | Inspector rows render with per-row chevrons (`part of`, `contains …`, long `contains` list in `northstar+select`); the link affordance is visible, but click→expand-path→select is interaction, not observable in stills. |
| R16 | Breadcrumb + search-reveal | PASS | — | Breadcrumb correct root→leaf: `Acme · Acme Finance · Revenue Accounting · Finance Agent · net sales`. Search-reveal is interaction-only, not exercised here. |
| R17 | Drag persistence | N/A-static | — | Manual drag-offset survival across expand/collapse + refresh cannot be judged from stills. |
| R18 | Deterministic geometry | PASS | — | d3-tree is analytic/physics-free → identical layout by construction; no jitter visible. (No `det/` heatmap in this shot set to re-confirm the 0% hash, but architecture supports it.) |
| R19 | Mess is honest, off-tree | PASS | — | `proposed.{light,dark}` puts ungrouped assets in a spatially separate, labeled `Ungrouped · 5` tray with neutral dotted nodes, never in the solid tree; the Applied view (`northstar`) hides the tray. |
| R20 | Proposals read as suggestions | PASS | P2 | `proposed.dark` draws a dashed `Suggested: Loyalty` hull over the tray — clearly a suggestion. Nit (carried): still no confidence **band** (High/Med/Low) on the hull per §4.7/MV-D35; Approve/Dismiss + promote are interaction-gated. |
| R21 | Hover snippet present + legible (MV-D85) | PASS | — | Docked inspector shows type pill (`Metric View`) · full name (`net sales`) · description · plain meta KVs (`Grain / Measures / Freshness`), and the top `Acme` tier is not cropped on fit. The cursor-following tooltip itself is N/A-static (live hover). |
| R22 | Measures render under MVs | PASS | — | Expanding `net sales` (green MV) hydrates an amber measure leaf tier (`Azure-only spend`, `Commit amount`, `Contractual di…`) capped `+26 more` (`northstar+select.{light,dark}`) — a deep, colourful middle tier, not a flat row. |
| R23 | Containers read as areas, not chrome | PASS | — | `Acme` / `Acme Finance` / `Acme Operations` carry no glyph (identity = fill + ring + size) across `northstar`, `degrade`, `proposed`; only leaf assets get a type mark. Sub-areas never read as a hamburger menu. |
| R24 | Fullscreen + framed tree (MV-D87 P0-a) | PASS | P2 | Toolbar carries `Fullscreen` + `Collapse all` in every graph scene; the default canvas is a tall ~70vh framed panel and the `northstar`/`proposed` trees are framed, not a thin band. Overlay-on-Maximize + collapse-re-fit are runtime. Nit: on the wide 53-node `degrade`/`stale` estates the *tree* still fits to a thin band inside the tall canvas (defect 3). |
| R25 | Edge hover detail + arrowheads legible (MV-D87 P0-b) | **FAIL** | **P1** | The maroon `reads` cross-link terminates at `ops_events` in BOTH themes (`northstar+select.{light,dark}`, verified at 6× on both endpoints) with **no visible marker-end arrowhead** — direction is only asserted by the legend hint `→ shows direction`, not rendered legibly on the edge. The relationship-type legend itself (verb + count: `also queried with · 1`, `joins calendar · 1`, `reads · 1`) IS present and good; the row's headline "arrowheads legible" is unmet. Edge hover tooltip is runtime (N/A-static). |
| R26 | Minimap navigable (MV-D87 P1-a) | PASS | — | A live cyan "you-are-here" viewport box is drawn in the minimap across `northstar`, `northstar+select` (`+26 more` overlay), `proposed`, `degrade`, `stale` — both themes. Click/drag recenter is runtime. |
| R27 | Domain show/hide (MV-D87 P1-b) | **FAIL** | **P1** | Only the `Domains` **toolbar toggle** is observable; **no scene shows the right-rail domain panel open** — every right rail across the 18 cells shows the *inspector* (`Nothing selected` / the `net sales` inspector), never a domain list with checkboxes + `Show all / Hide all`. The row's observable deliverable (the panel) is unverifiable from these pixels. Subtree-hide is runtime. |

**Tally (R1–R27):** PASS 21 · FAIL 2 · N/A-static 4. Severity of fails: **P0 = 0 · P1 = 2** (R25, R27).
N/A-static rows: R6 (hover), R7 (drill motion), R15 (relationship click), R17 (drag). R21's live tooltip
and R24/R26's runtime sub-behaviors are N/A-static within otherwise-PASS rows.

## Defects (ranked) — P1 first
1. **(P1) Cross-link arrowheads not legibly rendered — R25/MV-D87 P0-b** — `northstar+select.{light,dark}`:
   the single visible cross-link (`reads`, net sales ↔ ops_events) shows no marker-end arrowhead at
   either terminus at 6× zoom; the dashes just stop at the node edge (likely absent, or occluded under the
   target disc). Direction is only stated in the legend (`→ shows direction`), not on the edge. This is the
   headline deliverable of the "relationship legibility" phase. *Fix:* render (and un-occlude) a themed
   `marker-end` triangle sized to read at the default fit zoom; offset it off the node radius so it isn't
   hidden behind the target disc.
2. **(P1) Domain show/hide panel never observable — R27/MV-D87 P1-b** — all 18 cells: only the `Domains`
   toolbar toggle exists; the right rail always shows the inspector, so the promised panel (domain list +
   checkboxes + `Show all / Hide all`) cannot be verified. *Fix:* add a harness scene that captures the
   Domains panel open (and, if it isn't built yet, build the observable panel); at minimum the shot set must
   demonstrate the P1-b deliverable.
3. **(P2) Fit strands wide estates in a thin band** — `degrade.{light,dark}` / `stale.{light,dark}` (53
   nodes): the tree fits to its wide bbox and occupies ~15vh of the ~70vh canvas, leaving ~70% vertical
   whitespace above/below and shrinking leaf discs+labels to near-illegible. Structure + domain tier stay
   readable, so not blocking, but it dents R3/R5/R12 on the most realistic fixtures. *Fix:* let Fit use
   vertical space (min-scale floor / aspect balancing) or default-collapse below sub-domain on wide estates.
4. **(P2) Legend overlaps the `Finance Agent` node** — `northstar.{light,dark}`: the multi-row legend
   sits over the low Lava agent node in the 8-node fixture — chrome over content in the flagship scene.
   Node stays labeled + `+3` badged, so comprehension survives. *Fix:* dock the legend as a yielding
   overlay or add bottom Fit padding. (Carried from p5.)
5. **(P2) Proposal hull lacks a confidence band** — `proposed.{light,dark}`: the `Suggested: Loyalty`
   hull shows no High/Med/Low band (§4.7/MV-D35). *Fix:* add the band chip beside the `Suggested:` plate.
   (Carried from p5.)
6. **(P2) Faint light-theme hierarchy edges** — `northstar.light` / `degrade.light` / `stale.light`:
   untinted spine links are nearly invisible on the pale ground. *Fix:* raise the light edge token's
   opacity/weight (R2). (Carried from p5.)
7. **(P2) Leaf type encoding is colour-dominant** — `northstar` / `degrade` / `stress`: agent/dashboard/
   table leaf discs separate mainly by hue at default zoom (MV now carries a glyph; others don't). *Fix:*
   complete per-type leaf glyph coverage so type survives colour-blindness (R11). (Carried from p5.)

## Light + dark parity note
Parity holds across all nine scenes (contact sheet, both columns): label plates flip
paper↔`#0D1321`, dot-grid/inspector/tray/hull/minimap all re-theme, type hues stay on-brand, and the
honest states (`loading`/`empty`/`error`/`stale`) compose identically in both grounds. Critically, BOTH
P1 fails reproduce in BOTH themes — the missing arrowhead terminates identically in `light` and `dark`
(verified at 6×), and the domain panel is absent from every cell regardless of theme — so neither is a
parity break; they are feature/observability gaps common to both themes. The one theme-asymmetric
weakness remains the faint light-theme hierarchy edges (defect 6), a light-only legibility nit.

## Lane P2 claim verification (verified against pixels)
- **Claim — Fullscreen + framed tree (R24, P0-a): CONFIRMED.** `Fullscreen` and `Collapse all` controls
  are present in every graph scene's toolbar; the default canvas is a tall ~70vh framed panel and the
  `northstar`/`proposed` trees are framed rather than stranded. The Maximize overlay + collapse-re-fit are
  runtime (N/A-static). Caveat: wide 53-node estates (`degrade`/`stale`) still fit the *tree* into a thin
  band inside the tall canvas → logged as P2 defect 3, not an R24 flip.
- **Claim — arrowheads + relationship legend (R25, P0-b): PARTIALLY REFUTED → FAIL.** The
  relationship-type legend with per-type verb + count (`also queried with · 1`, `joins calendar · 1`,
  `reads · 1`) is present and legible. But the themed direction **arrowheads are not observable** on the
  one visible cross-link in either theme (6× crops of both `ops_events` and `net sales` endpoints show the
  maroon dashes terminating with no marker-end triangle). Since the row's headline is "arrowheads legible,"
  this fails (P1). Edge hover tooltip is N/A-static.
- **Claim — minimap "you-are-here" box (R26, P1-a): CONFIRMED.** A live cyan viewport box is drawn in the
  minimap across `northstar`, `northstar+select`, `proposed`, `degrade`, `stale`, both themes. Click/drag
  recenter is runtime.
- **Claim — domain show/hide panel (R27, P1-b): REFUTED → FAIL.** No still shows the panel; only the
  `Domains` toolbar toggle is observable. Every right rail across all 18 cells shows the inspector, never a
  domain list with checkboxes + `Show all / Hide all`. The observable deliverable is unverifiable from these
  pixels (P1) — needs a Domains-open harness capture (and/or the panel built).
