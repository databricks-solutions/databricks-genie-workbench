# Ontology Map — §8 Reviewer Scorecard · Lane P2 **CONFIRM ROUND** (MV-D80)

**VERDICT: PASS-with-P2** — the two prior **P1 FAILs are both now PASS** (R25 arrowheads, R27
domain panel), verified at 5–9× on the p2 pixels in both themes. **0 P0, 0 P1** remain. The
regression sweep across the other cells (northstar, proposed, degrade/stale, empty/loading/error,
stress · both themes) shows **no new P0/P1 breakage** — every prior PASS row still holds. The five
carried P2 nits from the prior round are unchanged (they were out of scope for this fix round), and
one **new, cosmetic P2** appears: on `northstar+select` the target arrowhead tip slightly clips the
top edge of the `ops_events` label plate. It stays fully legible, so it does not gate.

This is a skeptical, pixel-first confirm: I cropped both cross-link endpoints (5×/9×) and the domain
panel (4×) before flipping the two rows, and I grounded each flip against the shipped code.

---

## Focus rows — explicit PASS/FAIL with pixel evidence

### R25 — Cross-link direction arrowheads legible (MV-D87 P0-b) → **PASS** (was P1 FAIL)
- **Pixel evidence (`northstar+select.light.png` + `.dark.png`, 9× crop of the `ops_events` terminus):**
  the maroon dashed `reads` cross-link now terminates in a **solid filled triangular arrowhead** that
  sits **outside the target disc**, pointing up-right into `ops_events`. Measured from the 9× crop the
  arrowhead tip is ~16px from the disc center vs a ~7px disc radius → ~9px **outside** the perimeter,
  matching the `endTrim = targetRadius + 7` fix. The arrowhead is **no longer occluded** under the disc
  (the prior failure).
- **Both themes:** the triangle reads in light (maroon on paper) and dark (maroon on `#0D1321`).
- **Source end (`net sales`, 5× crop):** the dashes are trimmed to just outside the green MV disc with
  **no** arrowhead — correct single-direction encoding (`startTrim = targetRadius + 2`).
- **Code grounding:** `frontend/src/ontology/ontologyTreeLayout.ts` L447–450 trims both endpoints
  (`endTrim = radius + 7`); `frontend/src/ontology/components/EstateGraph.tsx` L913–921 bumps the
  marker to `7.5 × 7.5` with `orient="auto-start-reverse"` and a filled `M0,0 L10,5 L0,10 z` triangle.
- **Nit (new, P2, cosmetic):** the arrowhead tip overlaps the top of the `ops_events` label plate /
  the "p" glyph. Still legible as a distinct filled triangle; does not gate.
- Edge **hover** tooltip remains N/A-static.

### R27 — Domain show/hide panel (MV-D87 P1-b) → **PASS** (was P1 FAIL)
- **Pixel evidence (`northstar+panel.light.png` + `.dark.png`, 4× crop of the right rail):** the
  **"Show / hide areas"** panel is present and first-class in both themes, with:
  - header (eye icon + title) and a **✕ close** control;
  - **`Show all`** and **`Hide all`** pill buttons;
  - a checkbox list of the **real top-level domains** — **`Acme Finance`** (with an expand chevron `›`)
    and **`Acme Operations`** — both checked.
- **Parent-closure fix confirmed:** the panel lists the two genuine business areas, **not** the org
  root `Acme` and nothing re-rooted — consistent with the backend fix that keeps a top domain whose
  assets all live in sub-domains present in both the tree and the panel. The `Domains` toolbar toggle
  shows its active state, and the tree behind the panel renders intact (org→domain→asset unchanged).
- **Harness grounding:** matrix scene `northstar+panel` (`panel=domains`) → `harness/main.tsx` L104
  `initialDomainPanelOpen` → `EstateGraph.tsx` L206 `showDomainPanel` state. Additive prop; no impact
  on other scenes.
- Subtree-hide interaction itself remains runtime (N/A-static), but the **observable deliverable** (the
  panel) is now captured — the exact gap the prior round flagged.

---

## Regression sweep (vs prior scorecard PASS items) — no new P0/P1

| Scene (both themes) | Result | Note |
|---|---|---|
| `northstar` | PASS (unchanged) | org→domain→asset tree, `+3` collapse badge, minimap viewport box; legend still overlaps `Finance Agent` (carried P2 #4). |
| `northstar+select` | PASS | arc + **arrowhead** + measure tier (`+26 more`) + inspector all intact; the R25 fix added no stray marks. |
| `northstar+panel` | PASS (new row) | panel open, tree intact behind it (see R27). |
| `proposed` | PASS (unchanged) | dashed `Suggested: Loyalty` hull over `Ungrouped · 5` dotted tray; footer honest; still no confidence band (carried P2 #5). No arrowhead artifacts. |
| `degrade` / `stale` (53 nodes) | PASS-with-P2 (unchanged) | `+65 links — select a node…` hint, **no arcs** (overlay correctly gated → the R25 trim change does not leak arrowheads into unselected scenes); wide-estate thin-band fit persists (carried P2 #3); faint light edges (carried P2 #6). |
| `empty` / `loading` / `error` | PASS (unchanged) | honest cards compose identically in both grounds. |
| `stress` (25 nodes) | PASS (unchanged) | tidy tree; leaf type still colour-dominant (carried P2 #7). |

**Key regression check:** the R25 trim/marker change is scoped to cross-links, which only draw on
selection — verified that `degrade`/`stale`/`proposed`/`northstar` (nothing selected) render **no**
arrowheads or stray triangles. The R27 prop is additive and default-off. No layout/geometry drift
visible across the sweep.

---

## New findings this round
- **(P2, new, cosmetic) Arrowhead clips the `ops_events` label plate** — `northstar+select.{light,dark}`.
  The target arrowhead tip overlaps the top edge of the label plate/glyph. Fully legible; non-blocking.
  *Optional fix:* nudge the label plate down a few px or shorten `endTrim` by ~2px on labeled leaves.

## Carried P2s (unchanged, out of scope for this fix round)
1. Wide-estate Fit strands the tree in a thin band (`degrade`/`stale`, 53 nodes).
2. Legend overlaps the `Finance Agent` node (`northstar`).
3. Proposal hull lacks a High/Med/Low confidence band (`proposed`).
4. Faint light-theme hierarchy spine edges (`*.light`).
5. Leaf type encoding is colour-dominant (glyph coverage uneven).

---

## Overall verdict
**PASS-with-P2.** R25 and R27 both flip from P1 FAIL → PASS with clear pixel evidence in both themes,
and the fixes introduce no P0/P1 regressions. Remaining issues are all P2 (five carried + one new
cosmetic arrowhead/label overlap). No P1+ remains — nothing to name as blocking.
