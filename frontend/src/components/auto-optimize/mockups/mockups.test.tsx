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
import { CreateAndAttachOutputFrame, SuggestOnlyOutputFrame } from "./MvOutputMockups"
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

describe("frame 4 — suggest-only output", () => {
  const html = render(<SuggestOnlyOutputFrame />)
  it("carries the verbatim Lift-not-measured label and both actions", () => {
    expect(html).toContain("Lift not measured")
    expect(html).toContain("was not created or attached during this run")
    expect(html).toContain("Approve for re-run")
    expect(html).toContain("Re-run with this metric view")
  })
  it("renders the DDL and GRANT panels", () => {
    expect(html).toContain("WITH METRICS")
    expect(html).toContain("GRANT (run before others query this Agent)")
  })
  it("depicts only reachable join strategies: card 1 has no ladder, card 2 keeps Subquery source", () => {
    // "nested" is unreachable today (MV-D14/D15); a single direct join needs no badge.
    expect(html).not.toContain("Nested join")
    expect(html).toContain("Subquery source")
  })
})

describe("frame 5 — create and attach (regression)", () => {
  const html = render(<CreateAndAttachOutputFrame />)
  it("shows DETACHED, drop, both eval links, tables freed and needs-review", () => {
    expect(html).toContain("DETACHED")
    expect(html).toContain("Drop view")
    expect(html).toContain("Baseline accuracy")
    expect(html).toContain("Post-attach accuracy")
    expect(html).toContain("Tables freed")
    expect(html).toContain("Needs review")
    expect(html).toContain("eval_a1")
    expect(html).toContain("eval_b2")
  })
  it("names provenance OBO_CREATED, drops the unreachable nested badge, and de-dups the eval prefix", () => {
    expect(html).toContain("OBO_CREATED")
    expect(html).not.toContain("Nested join")
    expect(html).not.toContain("evaleval")
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
