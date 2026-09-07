# Ontology Map — Reviewer Scorecard · Lane R (renderer, iteration p3)

**Overall verdict: FAIL (3 applicable R-rows fail — all P1, no P0).**
The renderer proves out the hard, structural half of the northstar: a deterministic d3-tree
(det diff = 0% on all 18 cells), a full dual-theme surface (ground, dot-grid, label plates, and
inspector all flip cleanly light↔dark), typed + classed verb cross-links (`reads`, `joins calendar`,
`also queried with`, and a maroon `reads ops_events · X-DOM`), a correct root→leaf breadcrumb, a
navigable-looking inspector with a Technical-details disclosure, an Applied/Proposed/Both provenance
toggle, an off-tree `Ungrouped · N` tray with a dashed "Suggested" hull, and centered
loading/empty/error/stale compositions. But the flagship `northstar` scene reads *below* the mockup
bar — poorly fit (tree crammed up-left over a large dead canvas), colliding/truncated labels
(`Revenue Accountin_`, `Ops Ove`), unlabeled ghost discs floating top-right, and the selected node's
verb arcs spilling off the bottom of the canvas. At realistic/expanded scale the typed overlay
collapses into a sweeping arc **hairball** with an unreadable red verb-label cloud (`degrade`/`stale`,
106 nodes), and the 1,702-node `stress` case renders a single solid teal band of nameless nodes with
no cap/truncation. Phase is not done until R3, R4, R12 pass.

## Mechanical gates (visual-review scope only)
| # | Gate | Result | Note |
|---|---|---|---|
| G1 | `tsc -b` clean | Not checked (out of visual-review scope) | |
| G2 | eslint clean | Not checked | |
| G3 | `vitest` green | Not checked | |
| G4 | lockfile byte-identical | Not checked | |
| G5 | no backend/packages edits | Not checked | |
| G6 | layout-stability hash stable | Pass (verified via det) | `det/diff/diff.json`: all 18 cells `pct: 0`, none `over` — geometry is byte-stable across reloads. |
| G7 | visual diff only where intended | Pass (verified via det) | Determinism re-shoot is 0% everywhere; no unintended drift. |

## Craft rubric (both themes)
| # | Criterion | Pass/Fail | Severity | One-line reason (scene · theme) |
|---|---|---|---|---|
| R1 | Dual-theme parity | PASS | — | Every scene is first-class in both grounds; plates flip paper↔`#0D1321`, dot-grid + inspector + hulls all re-theme (`northstar`/`proposed`/`stale`/`empty` all read in `.light` and `.dark`). Defects below hit both themes equally, so *parity* itself holds. |
| R2 | Contrast (fill ≥3:1 vs ground; text ≥4.5:1 on plate; both themes) | PASS | P2 | Type-hue discs read on both grounds and plated labels clear text-contrast (`northstar.light`/`.dark`); the `stale` amber "Snapshot may be out of date" pill is legible in both. Nit: hierarchy edges are very pale grey on the light ground (`degrade.light`/`stale.light`). Amber measure-chip-on-light could not be verified — the selected measure is pushed off-canvas (see R12). |
| R3 | No nameless dots | FAIL | P1 | `stress.{light,dark}` collapses 1,702 nodes into one solid teal band + a dotted row, zero labels, no `+N more` cap — a dump, not a cap; `northstar.{light,dark}` floats faint unlabeled ghost discs top-right; `degrade`/`stale` render long rows of sub-pixel-labeled discs. |
| R4 | No hairball | FAIL | P1 | `degrade.{light,dark}` + `stale.{light,dark}` (106 nodes, all expanded) draw dozens of long bowed cross-link arcs sweeping the full width with an overlapping red verb-label cloud in the lower half — reads as noise, not structure. Overlay is not culled/gated when many endpoints are simultaneously visible. |
| R5 | Hierarchy legible | PASS | P2 | `northstar.{light,dark}` shows `org(Acme) → domain(Acme Finance/Operations) → subdomain/asset` with collapse badges (`Finance Agent +2`). Nit: at the `degrade` full-expand the tidy-tree breadth compresses levels into a near-flat horizontal band, so depth is hard to read. |
| R6 | Hover insight | N/A (static) | — | Debounced hover tooltip is not observable in stills; the click inspector (below) is present and does the plain-language job. |
| R7 | Drill smoothness | N/A (static) | — | Enter-from-parent easing / expand-collapse motion cannot be judged from screenshots; no frozen jump-cut artifact visible. |
| R8 | Provenance honest | PASS | — | `northstar` = solid Applied tree; `proposed.{light,dark}` = dashed "Suggested" hull + neutral dotted tray nodes; Applied/Proposed/Both toggle present and set correctly per scene. |
| R9 | Honest states | PASS | — | `loading.*` = spinner + "Building the estate graph…"; `empty.*` = centered "No data in the estate graph yet" + subtext; `error.*` = ⚠ + "The estate snapshot could not be read" + "Try refreshing…"; `stale.*` = amber "Snapshot may be out of date" pill over the live map. All centered/composed in both themes (improvement over P1's banner-only). |
| R10 | Plain language | PASS | — | `northstar+select` inspector: "A curated set of business measures", relationships "part of Finance Agent / contains fact_sales_line / reads ops_events", with a `▸ Technical details` disclosure hiding FQN/expression. No SQL/ids/jargon in any at-a-glance string, either theme. |
| R11 | Type encoding (shape/glyph, not colour alone) | PASS | P2 | Org (hex glyph, largest) and domain (lines glyph) carry shape; Genie Agent is correctly the only Lava node (`northstar` red agent). Nit: agent/dashboard/metric-view/table leaf discs differ mostly by *colour* at the default zoom — little glyph/shape load, so colour does most of the work. |
| R12 | Northstar fidelity | FAIL | P1 | `northstar.{light,dark}` reads well below the calm, centered, generously-spaced mockup: tree is crammed into the upper-left third over a large empty canvas (poor Fit), labels collide and truncate (`Revenue Accountin_`, `Ops Ove`, `ref`), unlabeled ghost discs float disconnected top-right, and in `northstar+select` the selected node's `reads`/`also queried with` arcs bow off the bottom edge of the canvas. |
| R13 | Reveal-don't-invent | PASS | — | Counts are honest (`Ungrouped · 5`); `proposed` shows only the real ungrouped remainder; no measure→measure edges are invented (correctly deferred per §3.4/R13); nothing structural added to look fuller. |
| R14 | Typed verb edges | PASS | P2 | `northstar+select.{light,dark}` shows verbs on the arcs (`reads`, `joins calendar`, `also queried with`) and correct within/cross class — maroon `reads ops_events` carries an `X-DOM` badge in the inspector. Nit: at 106-node expand the verb labels pile into an unreadable red cloud and arcs clip the frame (folds into R4). |
| R15 | Navigable relationships | N/A (static) | — | Inspector relationships render with per-row chevrons + `X-DOM` badge (`northstar+select`), so the link affordance + correctness are visible, but click→expand-path→select is interaction and not observable in stills. |
| R16 | Breadcrumb + search-reveal | PASS | — | Breadcrumb is present and correct root→leaf: `Acme · Acme Finance · Revenue Accounting · Finance Agent · net sales` (`northstar+select.*`). Search-reveal (accent ring + auto-expand of hit paths) is interaction-only and not exercised in these shots. |
| R17 | Drag persistence | N/A (static) | — | Manual drag-offset survival across expand/collapse + refresh cannot be judged from screenshots. |
| R18 | Deterministic geometry | PASS | — | `det/diff/diff.json` = `pct: 0` on all 18 cells; `meta.json` shows pure single-pass layout times (27–85 ms). Same estate → identical layout. |
| R19 | Mess is honest, off-tree | PASS | — | `proposed.{light,dark}` puts ungrouped assets in a spatially separate, labeled `Ungrouped · 5` tray with neutral dotted nodes, never forced into the solid tree. |
| R20 | Proposals read as suggestions | PASS | P2 | `proposed.{light,dark}` draws a dashed rounded "Suggested: Loyalty" hull over the tray — clearly a suggestion, not applied. Nits: no confidence **band** (High/Med/Low) on the hull per §4.7; reassignment marks and Approve/Dismiss + promote-to-tree are interaction-gated and unverifiable here. |

**Tally:** PASS 13 · FAIL 3 · N/A (static) 4 · (of 20). Severity of fails: **P0 = 0 · P1 = 3.** P2 follow-ups noted inline (R2 faint light edges, R5 flat-band depth, R11 colour-alone leaves, R14 verb-label crowding, R20 missing confidence band).

## Top defects to fix (ranked, P0 → P1 → P2)
1. **(P1) Hairball at realistic scale** — `degrade.{light,dark}`, `stale.{light,dark}`: 106 fully-expanded nodes draw a full-width sweep of bowed cross-link arcs with an overlapping red verb-label cloud (R4). Cull/gate the overlay to hovered/selected neighborhoods (or drop verb labels past a density/zoom threshold); §6 collapse-by-default mitigates first paint but the expanded overlay still needs a cap.
2. **(P1) Northstar scene reads below the mockup bar** — `northstar.{light,dark}` + `northstar+select.*`: poor Fit (tree crammed up-left over dead canvas), colliding/truncated labels (`Revenue Accountin_`, `Ops Ove`), unlabeled ghost discs top-right, and selected-node arcs spilling off the bottom edge (R12). Re-fit to the visible bbox, widen sibling separation / plate labels so names don't collide, and resolve/label or remove the floating ghost discs.
3. **(P1) Stress renders a nameless solid band** — `stress.{light,dark}`: 1,702 nodes draw as one dense teal bar + a dotted row, no labels, no `+N more` cap, no truncation marker (R3), brushing the "no nameless dots / legible at a glance" non-negotiable. Apply the §6 scale strategy (collapsed-by-default below sub-domain, per-parent `+N more` cap, top-N-by-centrality truncation). (Scale guards are nominally Lane P per §11, but the raw dump surfaces here.)
4. **(P2) Faint light-theme hierarchy edges** — `degrade.light`/`stale.light`: pale-grey links nearly vanish on the pale ground; darken/raise-opacity the light edge token.
5. **(P2) Proposal hull lacks a confidence band** — `proposed.{light,dark}`: the "Suggested: Loyalty" hull shows no High/Med/Low band (§4.7/MV-D35); add the band chip.
6. **(P2) Leaf type encoding is colour-dominant** — `northstar`: agent/dashboard/MV/table discs differ mainly by hue; add per-type glyph/shape so type survives colour-blindness at the default zoom.
