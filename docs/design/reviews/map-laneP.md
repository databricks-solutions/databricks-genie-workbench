# Ontology Map — Reviewer Scorecard · Lane P (renderer polish, MV-D85, phase p5)

**VERDICT: PASS** — every applicable R-row passes; **zero P0, zero P1**. Four rows are
N/A-static (hover/drill/drag/relationship-click are runtime interactions not observable in stills).
This is a fresh, independent, skeptical review of the Lane P polish pass; I verified each Lane P
claim against the pixels rather than the changelog. The three new rows all pass: **R21** — the docked
inspector carries a real description (`Net sales after returns and discounts, by day and channel.`)
plus plain-language meta (`Grain: day × channel · Measures: 6 · Freshness: 2h`), and the top domain
tier (`Acme`) is no longer cropped on fit (fit reserves top padding); the cursor-following tooltip
itself is a live hover and is marked **N/A-static**, not failed. **R22** — expanding the `net sales`
metric view hydrates a colourful **amber measure tier** (`Avg cost per wo…`, `Commit amount`,
`Cost (all appl…`, …) capped with a deterministic `+26 more` chip — a real deep middle tier, not a flat
row. **R23** — `Acme` / `Acme Finance` / `Acme Operations` render as plain glyph-free discs (identity =
fill + ring + size), so sub-areas never read as "hamburger" UI chrome; only leaf assets carry a
type mark. The one blocking-adjacent item is Lane P's self-flagged legend/agent-node overlap in the
tiny 8-node `northstar` fixture, which I confirm and hold at **P2** (the node stays labeled + badged;
comprehension is not lost).

## Mechanical gates (visual-review scope only)
| # | Gate | Result | Note |
|---|---|---|---|
| G1 | `tsc -b` clean | Not checked (out of visual-review scope) | |
| G2 | eslint clean | Not checked | |
| G3 | `vitest` green | Not checked | |
| G4 | lockfile byte-identical | Not checked | |
| G5 | no backend/packages edits | Not checked | |
| G6 | layout-stability hash stable | Not checked (no `det/` heatmaps in this shot set) | d3-tree geometry is analytic/physics-free → deterministic by construction; no jitter artifact visible. |
| G7 | visual diff only where intended | Not checked (no `det/` heatmaps in this shot set) | |

## Craft rubric (both themes) — R1–R23
| # | Criterion | Pass/Fail | Severity | One-line reason (scene · theme) |
|---|---|---|---|---|
| R1 | Dual-theme parity | PASS | — | Every scene first-class in both grounds; plates/dot-grid/inspector/hull/tray all re-theme (`northstar`, `northstar+select`, `proposed`, `degrade`, `stale`, `empty`, `loading`, `error`, `stress` all read in `.light` and `.dark`). |
| R2 | Contrast (fill ≥3:1; text ≥4.5:1 on plate; both themes) | PASS | P2 | Type-hue discs read on both grounds; amber measures + green MV legible in `northstar+select.dark`. Nit (carried): hierarchy edges are very pale on the light ground (`northstar.light`, `stress.light`, `degrade.light`). |
| R3 | No nameless dots | PASS | P2 | `stress.{light,dark}`: per-parent cap → ~11 **labeled** children (`ref_calendar…`, `Ops Inventory…`) + a `+N more` sentinel — a cap, not a dump. Nit: leaf labels are small at the fit zoom, but present. |
| R4 | No hairball | PASS | — | Overlay gated to selection: `stress`/`northstar` (nothing selected) draw the tidy tree only; `northstar+select.dark` draws exactly one on-canvas maroon `reads … X-DOM` arc. No arc cloud. |
| R5 | Hierarchy legible | PASS | P2 | `northstar.{light,dark}` shows `org(Acme)→domain(Finance/Operations)→asset` with a `Finance Agent +2` collapse badge; `northstar+select` adds the deep measure tier. Nit: `stress` compresses the wide asset row at fit. |
| R6 | Hover insight | N/A-static | — | Debounced cursor-following tooltip is runtime-only; not observable in stills. Click inspector is present and does the plain-language job. |
| R7 | Drill smoothness | N/A-static | — | Enter-from-parent easing / expand-collapse motion cannot be judged from stills; no frozen jump-cut visible. |
| R8 | Provenance honest | PASS | — | `northstar` = solid Applied tree; `proposed.dark` = dashed "Suggested: Loyalty" hull over a neutral dotted tray; footer labels match ("Applied…" / "Proposed (engine suggestions — nothing applied yet)"). |
| R9 | Honest states | PASS | — | `loading` = spinner + build message; `empty` = "No data in the estate graph yet" + subtext; `error` = ⚠ + snapshot-read message; `stale` = amber "out of date" pill over the live map. All centered/composed in both themes (contact sheet). |
| R10 | Plain language | PASS | — | `northstar+select` inspector reads "Net sales after returns and discounts…", relationships as `part of / contains / reads`; no SQL/ids/FQN at a glance in either theme. |
| R11 | Type encoding (shape/glyph, not colour alone) | PASS | P2 | Org (largest disc) + domains (medium, ring) carry size/shape; Genie Agent is correctly the only Lava node. Nit (carried): agent/dashboard/MV/table **leaf** discs differ mainly by hue at default zoom — add per-type leaf glyph so type survives colour-blindness. |
| R12 | Northstar fidelity | PASS | P2 | `northstar.{light,dark}`: centered content-bounds Fit, ellipsis labels (`Revenue Account…`), neutral spine, minimap, top tier not cropped. **Nit (P2, self-flagged + confirmed):** the taller 3-row legend overlaps the low `Finance Agent` node in the 8-node fixture — chrome over content, though the node stays labeled + `+2` badged. |
| R13 | Reveal-don't-invent | PASS | — | Counts honest (`Ungrouped · 5`; `+26 more`; `+N more` reflect real remainders); no measure→measure edges invented (correctly deferred per §3.4/R13). |
| R14 | Typed verb edges | PASS | — | `northstar+select.dark` draws a plain verb on the arc (`reads`) with the correct cross-domain class (maroon dashed + `X-DOM` in the inspector). |
| R15 | Navigable relationships | N/A-static | — | Inspector rows render with per-row chevrons (`part of`, `contains`, `reads` in `northstar+select`), so the link affordance is visible, but click→expand-path→select is interaction, not observable in stills. |
| R16 | Breadcrumb + search-reveal | PASS | — | Breadcrumb correct root→leaf: `Acme · Acme Finance · Revenue Accounting · Finance Agent · net sales`. Search-reveal is interaction-only, not exercised here. |
| R17 | Drag persistence | N/A-static | — | Manual drag-offset survival across expand/collapse + refresh cannot be judged from stills. |
| R18 | Deterministic geometry | PASS | — | d3-tree is analytic/physics-free → identical layout by construction; no jitter visible. (No `det/` heatmap in this shot set to re-confirm the 0% hash, but architecture supports it.) |
| R19 | Mess is honest, off-tree | PASS | — | `proposed.dark` puts ungrouped assets in a spatially separate, labeled `Ungrouped · 5` tray with neutral dotted nodes, never forced into the solid tree; the Applied view (`northstar`) correctly hides the tray. |
| R20 | Proposals read as suggestions | PASS | P2 | `proposed.dark` draws a dashed "Suggested: Loyalty" hull over the tray — clearly a suggestion. Nit (carried): no confidence **band** (High/Med/Low) on the hull per §4.7/MV-D35; Approve/Dismiss + promote are interaction-gated. |
| R21 | Hover snippet present + legible (MV-D85) | PASS | — | Docked inspector shows real **type pill** (`Metric View`) · **full name** (`net sales`) · **description** · plain **meta KVs** (`Grain / Measures / Freshness`), and the top domain tier (`Acme`) is not cropped on fit. The cursor-following **tooltip itself is N/A-static** (live hover) — judged on what is visible, per the review note; not failed. |
| R22 | Measures render under MVs | PASS | — | Expanding `net sales` (green MV) hydrates an **amber measure leaf tier** capped with `+26 more` (`northstar+select.dark`) — the middle tier is present, colourful, and deep, not a flat table row. |
| R23 | Containers read as areas, not chrome | PASS | — | `Acme` / `Acme Finance` / `Acme Operations` carry **no glyph** (identity = fill + ring + size) across `northstar`, `stress`, `proposed`; only leaf assets get a type mark. Sub-areas never read as a hamburger menu. |

**Tally (R1–R23):** PASS 19 · FAIL 0 · N/A-static 4. Severity of fails: **P0 = 0 · P1 = 0.**
N/A-static rows: R6 (hover), R7 (drill motion), R15 (relationship click), R17 (drag). R21's live
tooltip is N/A-static within an otherwise-PASS row.

## Defects (ranked) — all P2, none blocking
_No P0 or P1 defects._

1. **(P2) Legend overlaps the `Finance Agent` node** — `northstar.{light,dark}`: the 3-row legend
   (type row + edge-type rows + interaction hint) sits over the low Lava agent node in the 8-node
   fixture — chrome drawn on top of content in the flagship scene. Node stays labeled + `+2` badged, so
   comprehension survives, but it dents R12 fidelity. *Fix:* dock the legend as an overlay that yields to
   node bounds, or add bottom Fit padding / auto-nudge the legend when the fit bbox is short. (Self-flagged
   by Lane P; confirmed.)
2. **(P2) Proposal hull lacks a confidence band** — `proposed.{light,dark}`: the "Suggested: Loyalty"
   hull shows no High/Med/Low band (§4.7/MV-D35). *Fix:* add the band chip beside the `Suggested:` plate. (Carried from p4.)
3. **(P2) Faint light-theme hierarchy edges** — `northstar.light` / `stress.light` / `degrade.light`:
   untinted spine links are nearly invisible on the pale ground. *Fix:* raise the light edge token's
   opacity/weight (R2). (Carried from p4.)
4. **(P2) Leaf type encoding is colour-dominant** — `northstar` / `stress`: agent/dashboard/MV/table
   leaf discs differ mainly by hue at default zoom. *Fix:* add a per-type leaf glyph so type survives
   colour-blindness (R11). (Carried from p4; note R23 correctly removes glyphs from *containers* only.)
5. **(P2) Small leaf labels at large-fit zoom** — `stress.{light,dark}`: capped-row leaf labels are
   present but small at the fit zoom. *Fix:* zoom-threshold label cull or deeper default-collapse (R3/R5). (Carried from p4.)

## Light + dark parity note
Full parity held across all nine scenes (contact sheet, both columns): label plates flip
paper↔`#0D1321`, the dot-grid/inspector/tray/hull all re-theme, type hues stay on-brand, and the
honest states (`loading`/`empty`/`error`/`stale`) compose identically in both grounds. No forced-theme
or washed-out fills. The only theme-asymmetric weakness is the **faint light-theme hierarchy edges**
(defect 3) — a light-only legibility nit, not a parity break. The self-flagged legend/agent overlap
(defect 1) reproduces in **both** themes, so it is not a parity issue.

## Lane P claim verification (verified against pixels)
- **Claim 1 — hover-snippet tooltip:** runtime-only; **N/A-static**. The *docked inspector* portion of
  the same data (real description + type pill + plain meta KVs) is present and legible in
  `northstar+select.dark` → R21 PASS on what is visible.
- **Claim 2 — measures as amber MV children + `+N more`:** CONFIRMED. `net sales` expands to an amber
  measure tier with a `+26 more` cap (R22 PASS).
- **Claim 3 — inspector shows real description/meta:** CONFIRMED. `Net sales after returns and
  discounts…` + `Grain / Measures / Freshness` (R10/R21 PASS).
- **Claim 4 — containers glyph-free, leaves keep marks:** CONFIRMED for containers (`Acme`/domains are
  plain discs, R23 PASS). Leaf marks read mostly by hue at default zoom → the remaining weakness lands
  on R11 (P2), not R23.
- **Claim 5 — neutral spine, edges clipped to on-screen endpoints, fit reserves top padding:**
  CONFIRMED. Spine is neutral grey, the top `Acme` tier clears the frame top, and the selected-node arc
  stays on-canvas (R4/R12/R21 support).
- **Claim 6 — legend edge-type rows + interaction hint:** CONFIRMED. Legend carries Hierarchy (solid) /
  Shared key (dashed) / Cross-domain (maroon dashed) rows + `click to drill · drag to move · hover for
  details` in every scene.
- **Self-flagged P2 — legend overlaps `Finance Agent` in the 8-node fixture:** CONFIRMED; held at P2
  (defect 1), does not flip the verdict.
