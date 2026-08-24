/**
 * MV-advisor mockups — REVIEW SCAFFOLD test (see mvMockData.ts). Pins the copy
 * that the Prompt 10 review depends on, and the two structural negatives that
 * would otherwise force a rebuild:
 *   - frame 7 carries NEITHER the "Lift not measured" label NOR [Re-run] (3a).
 *   - frame 2 names no run as its source (space-scoped, 3b / MV-D23).
 * Node env + renderToStaticMarkup — the repo's frontend test pattern.
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { MOCKUP_FRAMES } from "./frames"
import {
  DenialConfigFrame,
  FirstRunConfigFrame,
  RerunConfigFrame,
} from "./MvRunConfigMockups"
import {
  ModelNodeDetailFrame,
  ModelTabEmptyFrame,
  ModelTabPopulatedFrame,
  ModelTabProposalOverlayFrame,
} from "./MvSemanticModelFrame"
import {
  IqScanAdvisoryEmptyFrame,
  IqScanAdvisoryFoundFrame,
  IqScanAdvisoryNotEntitledFrame,
} from "./MvIqScanAdvisoryMockups"
import {
  ByoEntryPointsFrame,
  ByoRefusedFrame,
  ByoVerifiedFrame,
} from "./MvByoRegistrationMockups"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

describe("MV mockups — smoke", () => {
  it("every registered frame renders to static markup", () => {
    for (const frame of MOCKUP_FRAMES) {
      expect(render(frame.element).length).toBeGreaterThan(0)
    }
  })
})

describe("frame 1 — first run", () => {
  const html = render(<FirstRunConfigFrame />)
  it("offers the toggle and disables Create and attach with the MV-D1 rationale", () => {
    expect(html).toContain("Suggest metric views")
    expect(html).toContain("Available after this run produces proposals you approve")
    expect(html).toContain("disabled")
  })
})

describe("frame 2 — re-run (space-scoped, not run-scoped)", () => {
  const html = render(<RerunConfigFrame />)
  it("labels the source by Agent and enables Create and attach", () => {
    expect(html).toContain("Approved for this Agent")
    expect(html).toContain("Create and attach, then optimize")
    expect(html).toContain("You can create metric views in")
    expect(html).toContain("Also materialize")
  })
  it("never names a run as the data source (3b / MV-D23)", () => {
    expect(html).not.toContain("from run")
    expect(html).not.toContain("run_5c1e")
  })
})

describe("frame 3 — denial", () => {
  it("shows the three denial actions", () => {
    const html = render(<DenialConfigFrame />)
    expect(html).toContain("permission to create metric views")
    expect(html).toContain("Copy grant request")
    expect(html).toContain("Choose a different schema")
    expect(html).toContain("Continue in suggest-only mode")
  })
})

// Frames 4–5 (run output panels) graduated to production at Prompt 13; their
// copy is now pinned by the production panel tests (MvOutputPanels.test.tsx).

describe("frame 9 — Model tab (Prompt 12.0)", () => {
  it("9a populated: tab strip, all three governance rungs, labeled SCD2 join", () => {
    const html = render(<ModelTabPopulatedFrame />)
    // Tab strip so placement (Score | Model | Optimize | History) is reviewed.
    for (const tab of ["Score", "Model", "Optimize", "History"]) expect(html).toContain(tab)
    // Traffic-light ladder: governed=success, curated=warning, ungoverned=danger,
    // each carrying a non-color label discriminator.
    expect(html).toContain("Governed")
    expect(html).toContain("Curated")
    expect(html).toContain("Ungoverned")
    expect(html).toContain("--color-success")
    expect(html).toContain("--color-warning")
    expect(html).toContain("--color-danger")
    // Join edge labeled with the ON predicate + relationship + SCD2 flag.
    expect(html).toContain("ON orders.customer_id = customer.id")
    expect(html).toContain("SCD2")
    expect(html).toContain("discounted_revenue")
  })

  it("9b empty: config-scoped copy names both populators, ladder alarms neither green NOR red", () => {
    const html = render(<ModelTabEmptyFrame />)
    // Config-scoped fact, not "run first" — a run is one of two populators, and
    // suggestions need no run (13.5 / POV Delta 9). Fragments avoid the escaped
    // apostrophes (renderToStaticMarkup emits &#x27; for Agent's / don't).
    expect(html).toContain("configuration defines no joins, SQL snippets, or metric views yet")
    expect(html).toContain("let an optimization run discover and apply")
    expect(html).toContain("Metric view suggestions")
    expect(html).toContain("require a run")
    expect(html).toContain("No measure concepts yet")
    expect(html).toContain("none have been suggested")
    // No overlay to overlay on an empty space — the toggle is hidden here (9a keeps it).
    expect(html).not.toContain("Show proposal overlay")
    // Nothing green — no governed measure exists.
    expect(html).not.toContain("--color-success")
    expect(html).not.toContain("Governed")
    // Nothing red — an empty space has found nothing ungoverned either, so the
    // ladder must not draw a danger chip for a measure it never saw.
    expect(html).not.toContain("--color-danger")
    expect(html).not.toContain("Ungoverned")
  })

  it("9c overlay ON: ghosted proposed MV, dashed replaces edge, default-off toggle visible", () => {
    const html = render(<ModelTabProposalOverlayFrame />)
    expect(html).toContain("proposed metric view")
    expect(html).toContain("replaces")
    expect(html).toContain("Show proposal overlay")
    expect(html).toContain("default off")
  })

  it("9d node detail: measure expr/synonyms/format/evidence + join cardinality, reachable strategy only", () => {
    const html = render(<ModelNodeDetailFrame />)
    expect(html).toContain("SUM(items.quantity * items.unit_price")
    expect(html).toContain("net sales")
    expect(html).toContain("occurrences")
    expect(html).toContain("many-to-one")
    expect(html).toContain("Subquery source")
    // "nested" is unreachable on every compute today (MV-D14/D15).
    expect(html).not.toContain("Nested join")
  })
})

describe("frame 7 — IQ Scan advisory (MV-D23)", () => {
  it("7a found: shows the card and a consent CTA, but NEITHER lift label NOR re-run", () => {
    const html = render(<IqScanAdvisoryFoundFrame />)
    expect(html).toContain("Metric view suggestions")
    expect(html).toContain("Review and create metric view")
    // Structural negatives (correction 3a):
    expect(html).not.toContain("Lift not measured")
    expect(html).not.toContain("Re-run with this metric view")
  })

  it("7b empty: reads as a clean result, names what was read, never as error/unavailable", () => {
    const html = render(<IqScanAdvisoryEmptyFrame />)
    expect(html).toContain("No recurring measures to propose yet")
    expect(html).toContain("clean result")
    expect(html).toContain("example question SQL")
    expect(html).toContain("re-run the scan")
    // Must not frame it as failure or as the feature being off:
    expect(html.toLowerCase()).not.toContain("unavailable")
    expect(html.toLowerCase()).not.toContain("failed")
    expect(html.toLowerCase()).not.toContain("error")
  })

  it("7c not-entitled: reuses the frame-3 denial banner unchanged", () => {
    const html = render(<IqScanAdvisoryNotEntitledFrame />)
    expect(html).toContain("permission to create metric views")
    expect(html).toContain("Copy grant request")
  })
})

describe("frame 8 — BYO registration (MV-D24)", () => {
  it("8a entry points: tertiary self-create action + free-standing register input", () => {
    const html = render(<ByoEntryPointsFrame />)
    expect(html).toContain("I created this myself")
    expect(html).toContain("Register an existing metric view")
    expect(html).toContain("catalog.schema.metric_view")
  })

  it("8b verified: USER_CREATED, Type confirmed, validation passed, verbatim registered copy", () => {
    const html = render(<ByoVerifiedFrame />)
    expect(html).toContain("USER_CREATED")
    expect(html).toContain("METRIC_VIEW")
    expect(html).toContain("confirmed")
    expect(html).toContain("Validation passed")
    expect(html).toContain("attached and measured on the next optimization run")
    expect(html).toContain("dropping this one stays in your hands")
  })

  it("8b verified: NEVER renders a Drop view action (MV-D24 invariant 1)", () => {
    const html = render(<ByoVerifiedFrame />)
    expect(html).not.toContain("Drop view")
  })

  it("8b verified: offers a way to start the run it promises (no dead end)", () => {
    const html = render(<ByoVerifiedFrame />)
    expect(html).toContain("Start an optimization run")
  })

  it("8c refused: reuses the denial banner for both variants, both never-recorded (invariant 2)", () => {
    const html = render(<ByoRefusedFrame />)
    expect(html).toContain("That object")
    expect(html).toContain("Registration accepts only objects whose")
    expect(html).toContain("visible under your identity")
    expect(html).toContain("Enter a different identifier")
    // Invariant 2 applies to BOTH refusals, so the sentence appears twice.
    expect(html.match(/Nothing was recorded/g)?.length).toBe(2)
    // NOT_FOUND resolves to DENIED (mv_entitlement) — never offer "it may not exist".
    expect(html).not.toContain("it may not exist")
  })
})
