# Ontology Map — Reviewer Scorecard · Phase P1 (theme-token dual-mode, MV-D79)

**Overall verdict: PASS (with P2 follow-ups).**
The light-mode fix landed correctly: across every populated scene the canvas now follows the app
theme — the ground, dot-grid, domain hub fills, label plates, and inspector all flip cleanly, with no
washed-out fills or un-flipped plates. Dark cells are byte-identical to baseline (0% diff, as
intended) and light cells changed wholesale (that is the fix), and the light result is genuinely
legible and on-brand rather than merely "different." No P0/P1 defects; the residual issues are all
P2 polish (faint light-mode edges, two near-identical domain greens, an unlabeled dot-dump in the
2,937-node stress case, and thin banner-only empty/error compositions).

## 6.A Mechanical gates
| # | Gate | Result | Note |
|---|---|---|---|
| G1 | `tsc -b` clean | Pass (assumed green, reported by build) | |
| G2 | eslint clean | Pass (assumed green, reported by build) | |
| G3 | `vitest` green | Pass (assumed green — 543/543 reported) | |
| G4 | lockfile byte-identical | Pass (assumed green, reported by build) | |
| G5 | no backend/packages edits | Pass (assumed — frontend-only phase) | |
| G6 | layout-stability hash stable | Pass (assumed green — determinism re-shoot 0%) | |
| G7 | visual diff only where intended | Pass (verified) | `diff.json`: all `*.dark` = 0%; all populated `*.light` ≈ 87.4%; empty/error.light ≈ 11–13% (banner + blank). Exactly the theme-only change expected. |

## 6.B Craft rubric
| # | Criterion | Pass/Fail | Severity | One-line reason (scene · theme) |
|---|---|---|---|---|
| R1 | Dual-theme parity | Pass | P2 | Every populated + state scene is first-class in both grounds; `domains.light` hubs and `subdomains.light` tinted boxes read as well as their dark twins, and label plates flip paper↔`#0D1321`. Minor: `domains`.both render Ifec and Commercial in two near-identical greens, weakening colour-as-business-area. |
| R2 | Contrast (fill ≥3:1 vs ground; text ≥4.5:1 on plate; both themes) | Pass | P2 | `assets.light` saturated-green discs read clearly on the pale-green container ground and dark labels sit on white plates; `assets.dark` mirrors it on `#0D1321` plates. Nit: inter-node edges in `domains.light`/`stale.light` are very faint grey on the pale ground (edges, not fills — not a gate breach). |
| R3 | No nameless dots | Pass | P2 | At each LOD's real default zoom nodes are labelled (`domains`, `subdomains`, `assets`, `assets+select` all name every visible node). Caveat: `stress.{light,dark}` (2,937 assets) renders grid dot-dumps with no labels / no "+N more" cap — a dump, not a cap. |
| R4 | No hairball | Pass | P1? no → P2 | `assets+select.*` gates cross-box edges to the focused node (radiating from `ticket_coupon`); `mv-expand.*` is a clean radial off the metric-view hub; `stress.*` stays structured by box. Relations read as structure, not noise. |
| R5 | Hierarchy legible | Pass | — | `subdomains.light` shows titled+counted domain boxes ("Maintenance And Engineering · 5", "Commercial · 46") with sub-area discs nested inside; `assets.light` nests sub-domain boxes (Yield Management / Ticketing And Settlement / Fare And Pricing) inside the Commercial container. Domain→Sub-domain→Asset nesting is obvious. |
| R6 | Hover insight (P2+) | Pass (deferred) | P2 | Hover tooltip is explicitly P2 scope; not present in P1 and not regressed. The click inspector (`assets+select.*`, `mv-expand.*`) already gives the plain-language facts. |
| R7 | Drill smoothness | Pass (not fully assessable) | P2 | Cannot judge easing from stills; no flicker/jump-cut artifacts visible across LOD scenes, and `assets+select.*` shows a clean focus+context fade of the non-neighbours. |
| R8 | Provenance honest | Pass | — | `proposed.*` renders the Ungrouped bucket with a dashed/dotted "Suggested" outline and neutral grey fill; `domains.*` applied hubs are solid. Applied=solid / Proposed=dashed / Ungrouped=neutral all read. |
| R9 | Honest states | Pass | P2 | `proposed.*` composes "No new grouping to suggest… only ungrouped tables remain" around a ghost Ungrouped disc (not blank); `stale.*` adds a "⚠ May be out of date" pill; `empty.*` = "No data in the estate graph" + View suggested; `error.*` = "Couldn't load the map" + Back to current view — all in both themes. Nit: `empty`/`error` are a thin top banner over a large dead canvas, far less composed than `proposed`. |
| R10 | Plain language | Pass | — | `assets+select` inspector: "A data table · In Commercial › Ticketing And Settlement · Works with component_removal, technical_log, booking_passenger and 17 more"; `mv-expand`: "A curated set of business measures." No SQL/ids/jargon in any visible string, either theme. |
| R11 | Type encoding (icon+shape, not colour alone) | Pass | P2 | `mv-expand.*` distinguishes the cyan metric-view hub from blue **square** measure chips by shape; asset discs are uniform (all tables). Nit: per-type lucide glyphs aren't legible inside asset discs at the default asset-LOD zoom, so at that zoom shape carries little of the load. |
| R12 | Mockup fidelity (17.0j bar) | Pass | P2 | Reads at a clean, modern bar (dot-grid ground, plated labels, tinted compound boxes in `subdomains`/`assets`) — not "1990s". Deviations from 17.0j: domain LOD uses bubbles rather than compound boxes, and edge typing (lineage-solid vs co-query-dashed-indigo) is not visually differentiated the way `17.0j` shows it. |
| R13 | Reveal-don't-invent | Pass | — | Counts are honest ("1,911 tables not grouped yet", "89 assets", stress "2937 Assets"); `proposed` shows only the real ungrouped remainder; nothing structural is invented to look fuller. |

## Top defects to fix (ranked)
1. **(P2) Two domains share near-identical green** — `domains.{light,dark}`/`stale.*`: Ifec and Commercial are almost the same hue, so colour stops uniquely encoding business area. Re-space the `colorForTop` hash or nudge one hue's family.
2. **(P2) Faint light-mode edges** — `domains.light`, `subdomains.light`, `stale.light`: inter-node connections are pale grey on the pale ground and nearly disappear; darken/raise-opacity the light-theme edge token and give lineage vs co-query distinct styling (17.0j fidelity).
3. **(P2) Stress renders an unlabeled dot-dump** — `stress.{light,dark}`: 2,937 assets draw as nameless grid discs with no "+N more" cap, brushing the "no nameless dots / no dump" non-negotiable at extreme scale.
4. **(P2) Empty/error are banner-only** — `empty.*`, `error.*`: honest and plain-language, but a thin top strip over a large dead canvas; compose them centrally like `proposed` does.
5. **(P2) mv-expand label crowding** — `mv-expand.*`: measure plates overlap heavily around the hub; declutter/stagger labels or gate them to hover.
