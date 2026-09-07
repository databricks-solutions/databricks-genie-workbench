# Ontology Map — Reviewer Scorecard · Lane R (renderer, iteration p4)

**Overall verdict: PASS (all applicable R-rows pass — zero P0, zero P1).**
This is a fresh, independent re-review of the fix pass that followed the p3 FAIL. All three
previously-failing rows are now fixed and verified from the p4 shots, and none of the 13 rows that
passed in p3 regressed. **R3** is fixed: `stress.{light,dark}` no longer dumps a nameless solid band —
it renders the calm `org→domain` skeleton with a per-parent child cap of ~11 **labeled** nodes per
parent, each row terminated by a deterministic `+N more` sentinel (`+266 more` / `+228 more`), so 25
nodes stand in for the full estate honestly. **R4** is fixed: the cross-link overlay is now gated to
the selected node — `degrade.{light,dark}` and `stale.{light,dark}` (53-node full expand) draw the tidy
hierarchy only, with a top-right `+65 links — select a node to trace its relationships` affordance
where the p3 hairball used to be; `northstar+select.{light,dark}` shows exactly one on-canvas
maroon `reads … X-DOM` arc. **R12** is fixed: the `northstar` scene is now centered with a proper
content-bounds Fit, labels use clean ellipsis (`Revenue Account…`, no hard `Revenue Accountin_`
truncation), the top-right ghost discs are gone (Ungrouped tray gated out of the Applied view), and the
selected node's verb arc stays inside the frame instead of spilling off the bottom. Remaining items are
all P2 follow-ups carried over from p3, none blocking.

## Mechanical gates (visual-review scope only)
| # | Gate | Result | Note |
|---|---|---|---|
| G1 | `tsc -b` clean | Not checked (out of visual-review scope) | |
| G2 | eslint clean | Not checked | |
| G3 | `vitest` green | Not checked | |
| G4 | lockfile byte-identical | Not checked | |
| G5 | no backend/packages edits | Not checked | |
| G6 | layout-stability hash stable | Not checked (no `det/` heatmaps in this shot set) | `meta.json` shows single-pass layout times (26–42 ms) and d3-tree geometry is deterministic by construction; no jitter artifact visible. |
| G7 | visual diff only where intended | Not checked (no `det/` heatmaps in this shot set) | |

## Craft rubric (both themes)
| # | Criterion | Pass/Fail | Severity | One-line reason (scene · theme) |
|---|---|---|---|---|
| R1 | Dual-theme parity | PASS | — | Every scene first-class in both grounds — plates flip paper↔`#0D1321`, dot-grid/inspector/hull/tray all re-theme (`northstar`, `proposed`, `stale`, `stress`, `empty`/`loading`/`error` all read in `.light` and `.dark`). |
| R2 | Contrast (fill ≥3:1; text ≥4.5:1 on plate; both themes) | PASS | P2 | Type-hue discs read on both grounds; the amber measure `net_sales` and green `net sales` MV are now on-canvas and legible (`northstar+select.{light,dark}`); the `stale` amber pill clears text. Nit (carried): hierarchy edges are very pale on the light ground (`degrade.light`/`stale.light`). |
| R3 | No nameless dots | **PASS (was FAIL/P1)** | P2 | `stress.{light,dark}`: per-parent cap → ~11 **labeled** nodes/parent (`ref_calendar…`, `Ops Inventory…`) + a deterministic `+266 more`/`+228 more` sentinel — a cap, not a dump. Nit: leaf labels in the 53-node `degrade`/`stale` full-expand are small at the fit zoom, but present. |
| R4 | No hairball | **PASS (was FAIL/P1)** | — | Overlay gated to selection: `degrade.{light,dark}`/`stale.{light,dark}` draw the tidy tree only + a `+65 links — select a node…` chip; `northstar+select.{light,dark}` draws exactly one on-canvas `reads` arc. No arc cloud. |
| R5 | Hierarchy legible | PASS | P2 | `northstar.{light,dark}` shows `org(Acme)→domain(Acme Finance/Operations)→subdomain/asset` with a `Finance Agent +2` collapse badge. Nit (carried): the 53-node full-expand compresses depth into a near-flat horizontal band. |
| R6 | Hover insight | N/A (static) | — | Debounced hover tooltip not observable in stills; the click inspector is present and does the plain-language job. |
| R7 | Drill smoothness | N/A (static) | — | Enter-from-parent easing / expand-collapse motion cannot be judged from stills; no frozen jump-cut artifact visible. |
| R8 | Provenance honest | PASS | — | `northstar` = solid Applied tree; `proposed.{light,dark}` = dashed "Suggested: Loyalty" hull + neutral dotted tray nodes; Applied/Proposed/Both toggle set correctly per scene, footer labels match ("Applied…" / "Proposed (engine suggestions — nothing applied yet)"). |
| R9 | Honest states | PASS | — | `loading.*` = spinner + "Building the estate graph…"; `empty.*` = "No data in the estate graph yet" + subtext; `error.*` = ⚠ + "The estate snapshot could not be read" + "Try refreshing in a moment."; `stale.*` = amber "Snapshot may be out of date" pill over the live map. All centered/composed, both themes. |
| R10 | Plain language | PASS | — | `northstar+select` inspector: "A curated set of business measures", relationships "part of Finance Agent / contains fact_sales_line / contains net_sales / reads ops_events", FQN/expression hidden behind `▸ Technical details`. No SQL/ids/jargon at a glance, either theme. |
| R11 | Type encoding (shape/glyph, not colour alone) | PASS | P2 | Org (hex glyph, largest) + domain (lines glyph) carry shape; Genie Agent is correctly the only Lava node (`northstar` red agent, both themes). Nit (carried): agent/dashboard/MV/table leaf discs differ mostly by hue at default zoom. |
| R12 | Northstar fidelity | **PASS (was FAIL/P1)** | P2 | `northstar.{light,dark}`: centered tree, content-bounds Fit, ellipsis labels (`Revenue Account…`), ghost discs gone, minimap present; `northstar+select.{light,dark}`: correct arc stays on-canvas. Nit: the bottom leaf labels (`fact_sales_line`/`net_sales`) sit close to the legend row, and small trees leave generous side margins. |
| R13 | Reveal-don't-invent | PASS | — | Counts honest (`Ungrouped · 5`; `+266/+228 more` reflect the real remainder, not padding); no measure→measure edges invented (correctly deferred per §3.4/R13). |
| R14 | Typed verb edges | PASS | — | `northstar+select.{light,dark}` shows a verb on the arc (`reads`) and correct cross-domain class — maroon `reads ops_events` carries an `X-DOM` badge in the inspector. Prior verb-crowding folds away now the overlay is gated (R4). |
| R15 | Navigable relationships | N/A (static) | — | Inspector rows render with per-row chevrons + `X-DOM` badge (`northstar+select`), so the link affordance + correctness are visible, but click→expand-path→select is interaction, not observable in stills. |
| R16 | Breadcrumb + search-reveal | PASS | — | Breadcrumb correct root→leaf: `Acme · Acme Finance · Revenue Accounting · Finance Agent · net sales` (`northstar+select.*`). Search-reveal is interaction-only, not exercised in these shots. |
| R17 | Drag persistence | N/A (static) | — | Manual drag-offset survival across expand/collapse + refresh cannot be judged from stills. |
| R18 | Deterministic geometry | PASS | — | `meta.json` shows pure single-pass layout times (26–42 ms) and d3-tree is analytic/physics-free → identical layout by construction; no jitter visible. (No `det/` heatmap in this shot set to re-confirm the 0% hash, but architecture + times support it.) |
| R19 | Mess is honest, off-tree | PASS | — | `proposed.{light,dark}` puts ungrouped assets in a spatially separate, labeled `Ungrouped · 5` tray with neutral dotted nodes, never forced into the solid tree; the Applied view (`northstar`) correctly hides the tray. |
| R20 | Proposals read as suggestions | PASS | P2 | `proposed.{light,dark}` draws a dashed "Suggested: Loyalty" hull over the tray — clearly a suggestion, not applied. Nit (carried): no confidence **band** (High/Med/Low) on the hull per §4.7; Approve/Dismiss + promote-to-tree are interaction-gated and unverifiable here. |

**Tally:** PASS 16 · FAIL 0 · N/A (static) 4 · (of 20). Severity of fails: **P0 = 0 · P1 = 0.**
P2 follow-ups (all non-blocking, most carried from p3): R2 faint light-theme hierarchy edges, R3/R5 small
leaf labels + flat-band depth at 53-node full expand, R11 colour-dominant leaf encoding, R12 bottom-leaf
label/legend proximity, R20 missing confidence band on the proposal hull.

## Top defects (ranked, P0 → P1 → P2)
_No P0 or P1 defects._ Remaining P2 follow-ups:

1. **(P2) Faint light-theme hierarchy edges** — `degrade.light`/`stale.light`: untinted links are nearly invisible on the pale ground; raise the light edge token's opacity/weight (R2).
2. **(P2) Depth compresses at large full-expand** — `degrade.{light,dark}`/`stale.{light,dark}`: 53 nodes fully expanded flatten into a near-horizontal band with small leaf labels; consider a zoom-threshold label cull or deeper default-collapse so depth reads (R3/R5).
3. **(P2) Proposal hull lacks a confidence band** — `proposed.{light,dark}`: the "Suggested: Loyalty" hull shows no High/Med/Low band (§4.7/MV-D35); add the band chip (R20).
4. **(P2) Leaf type encoding is colour-dominant** — `northstar`: agent/dashboard/MV/table discs differ mainly by hue; add per-type glyph/shape so type survives colour-blindness at default zoom (R11).
5. **(P2) Bottom-leaf labels crowd the legend** — `northstar+select.*`: `fact_sales_line`/`net_sales` labels sit close to the legend strip; nudge the Fit padding or legend offset (R12).

## R3 / R4 / R12 fix verification
- **R3 — FIXED.** In p3, `stress` collapsed ~1,702 nodes into a single nameless teal band with no cap. In p4, `stress.{light,dark}` renders the calm `Acme → Acme Finance / Acme Operations` skeleton with a **per-parent cap** of ~11 **named** children per domain (`ref_calendar…`, `Ops Inventory…`) and a **deterministic `+N more` sentinel** closing each row (`+266 more`, `+228 more`); `meta.json` confirms the visible count dropped to 25. This is a legible cap, not a dump — the §6 scale strategy is now visibly applied. No nameless dots remain (the p3 top-right ghost discs are also gone).
- **R4 — FIXED.** In p3, `degrade`/`stale` (106 nodes, all expanded) drew a full-width sweep of bowed cross-link arcs with an overlapping red verb-label cloud. In p4, the overlay is **gated to the selected node**: with nothing selected, `degrade.{light,dark}`/`stale.{light,dark}` draw only the tidy hierarchy plus a top-right `+65 links — select a node to trace its relationships` affordance; with a node selected, `northstar+select.{light,dark}` draws exactly one maroon `reads … X-DOM` arc that stays on-canvas. The hairball is gone and the overlay density now scales with selection, per §3.3/§6.
- **R12 — FIXED.** In p3, `northstar` was crammed up-left over dead canvas, with colliding/hard-truncated labels (`Revenue Accountin_`, `Ops Ove`), floating unlabeled ghost discs top-right, and selected-node arcs spilling off the bottom edge. In p4, `northstar.{light,dark}` is **centered with a proper content-bounds Fit**, labels use **clean ellipsis** (`Revenue Account…`) instead of hard truncation, the **ghost discs are gone** (Ungrouped tray gated out of the Applied view), and in `northstar+select.{light,dark}` the verb arc **stays inside the frame**. It now reads at the mockup's calm, typographic bar.
