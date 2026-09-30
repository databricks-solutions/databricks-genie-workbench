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
import { AttachedProposalCardFrame } from "./MvAttachAtApprovalFidelityFrames"
import { IqScanCuratedLowFrame } from "./Mv158FidelityFrames"
import {
  BlueprintScale30Frame,
  BlueprintStarColumnsFrame,
  BlueprintStarMeasureLineageFrame,
  BlueprintStarMvSelectedFrame,
  BlueprintStarOverviewFrame,
  BlueprintStarStandardFrame,
  BlueprintUnknownRolesFrame,
  BlueprintWideTableFrame,
} from "./SemanticBlueprintFidelityFrames"
import {
  CheckingFrame,
  ModelViewerFrame,
  NoAccessPageFrame,
  OptimizeViewerEmptyFrame,
  OptimizeViewerFrame,
  ScoreEditorFrame,
  ScoreUnknownFrame,
  ScoreViewerFrame,
  ScoreViewerUnscannedFrame,
} from "./SpaceAccessFidelityFrames"
import {
  AttachedNotCreatedFrame,
  SelectionNoneFrame,
  SelectionSubsetFrame,
  SelectionTwoSchemasFrame,
  SelectionTwoSchemasSuggestOnlyFrame,
} from "./MvSelectionFidelityFrames"
import { IqScanUnservableFrame } from "./MvRenderFidelityFrames"
import {
  IqScanApprovedStaleFrame,
  IqScanLowStaleFrame,
  IqScanStaleFrame,
  RunOutputCurrentCalloutFrame,
  RunOutputStaleFrame,
} from "./MvStaleBodyFidelityFrames"
import {
  BaselineRetainedFrame,
  KeptAttachRunningFrame,
  KeptAttachTerminalFrame,
  SingularGainCardFrame,
} from "./MvM6cFidelityFrames"
import { STALE_PROPOSAL_NOTICE } from "../mvFormat"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)
// renderToStaticMarkup escapes the notice's apostrophe.
const NOTICE_MARKUP = STALE_PROPOSAL_NOTICE.replace(/'/g, "&#x27;")

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

describe("frame 15.10 — attach-at-approval (MV-D34)", () => {
  const html = render(<AttachedProposalCardFrame />)
  it("badges the card Attached and opens the accept flow on the attached terminal", () => {
    // Header badge: the scannable signal that replaces "still N to create".
    expect(html).toContain("Attached")
    // The shared accept flow's attached terminal, not the create action.
    expect(html).toContain("Attached to your Agent")
    expect(html).not.toContain("Create this metric view")
  })
  it("surfaces the SP grant an optimization run needs to read the attached view", () => {
    // The grant renders through SqlCodeBlock (syntax-highlighted spans), so assert
    // on the plain-text framing that introduces it rather than the tokenized SQL.
    expect(html).toContain("grant the optimizer service principal")
  })
})

describe("frame 15.11 — curated fact-passing LOW surfaced by default (MV-D100)", () => {
  const html = render(<IqScanCuratedLowFrame />)
  it("wears a factual 'Curated' chip and the evidence-limited caption — no percent, no 'confidence' (MV-D35)", () => {
    // The promotion marker is a provenance fact, not a revived strength badge.
    expect(html).toContain("Curated")
    // The evidence-limited honesty lives in the caption, unchanged.
    expect(html).toContain("Based on curated SQL only")
    // MV-D35 stays clean on the promoted card.
    expect(html).not.toMatch(/\d+%/)
    expect(html.toLowerCase()).not.toContain("confidence")
    // Retired strength badge never returns.
    expect(html).not.toContain("Strong (evidence-limited)")
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

// ── Frame 11 — Semantic Blueprint (v4) P1 fidelity gate ──────────────────────
// Pins the P1 vocabulary against the north-star prototype
// (semantic-graph-v4-blueprint-note.md §5.9 / §11.3 / §9): crow's-foot markers,
// crossing hops, callouts, the health headline, semantic-zoom bands, lineage on
// select, neutral-role degradation, and the arrows-require-proof invariant.
describe("frame 11 — Semantic Blueprint P1 fidelity", () => {
  it("11a star: deterministic — two renders are byte-identical (§9)", () => {
    expect(render(<BlueprintStarStandardFrame />)).toBe(render(<BlueprintStarStandardFrame />))
  })

  it("11a star: crow's-foot + one-tick cardinality glyphs, orientation-aware (§5.4)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    expect(html).toContain('data-glyph="crowfoot"')
    expect(html).toContain('data-glyph="one-tick"')
  })

  it("11a star: at least one crossing hop is computed (§5.3 bridges)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    expect(html).toMatch(/data-hops="[1-9]/)
  })

  it("11a star: self-annotations — unmodeled region, island tag, cold-spot callout (§5.6)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    expect(html).toContain("UNMODELED · in no metric view")
    expect(html).toContain('data-tag="island"')
    expect(html).toContain("Cold spot · dim_host")
    expect(html).toContain("no curated SQL touches it")
  })

  it("11a star: health headline carries the governance ladder counts (§5.7)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    expect(html).toContain("data-headline")
    expect(html).toContain("governed")
    expect(html).toContain("curated")
    expect(html).toContain("ungoverned")
    expect(html).toContain("cold spot")
  })

  it("11a star: toolbar exposes zoom bands + the Fact-center/Source-left toggle + Reset (§5.5/§5.12)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    for (const label of ["Overview", "Standard", "Columns", "Fact-center", "Source-left", "Reset view"]) {
      expect(html).toContain(label)
    }
  })

  it("11a star: semantic band headers only where a role is proven (§5.12)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    expect(html).toContain("FACT · SOURCE")
    expect(html).toContain("DIMENSIONS")
    expect(html).toContain("METRIC VIEW · MEASURES")
  })

  it("11a star: wide-columns pill on the 41-column fact", () => {
    expect(render(<BlueprintStarStandardFrame />)).toContain("41 cols")
  })

  it("11a star: arrows require proof — the island table draws zero base edges, nothing proposed (§2)", () => {
    const html = render(<BlueprintStarStandardFrame />)
    expect(html).not.toContain('data-edge-from="dim_campaign"')
    expect(html).not.toContain('data-edge-to="dim_campaign"')
    expect(html).not.toContain("proposed_join")
  })

  it("11b columns LOD: join-key rows highlighted, ON leaf columns rendered (§5.5/§6)", () => {
    const html = render(<BlueprintStarColumnsFrame />)
    expect(html).toContain('data-joinkey="user_id"')
    expect(html).toContain('data-joinkey="property_id"')
    expect(html).toContain("booking_date_id")
  })

  it("11c measure select: dashed lineage to each source table + inset lineage section (§5.10)", () => {
    const html = render(<BlueprintStarMeasureLineageFrame />)
    expect(html).toContain('data-lineage="measure"')
    expect(html).toContain('data-lineage-src="fact_booking_detail"')
    expect(html).toContain('data-lineage-src="dim_user"')
    expect(html).toContain("Lineage → source tables")
    expect(html).toContain("bookings_per_customer")
    expect(html).toContain("exposed by")
  })

  it("11d MV select: member boundary + dotted uses-lineage + join-tree inset (§5.10)", () => {
    const html = render(<BlueprintStarMvSelectedFrame />)
    expect(html).toContain('data-boundary="mv-member"')
    expect(html).toContain('data-lineage="mv"')
    expect(html).toContain("Join tree")
    expect(html).toContain("2 materializations · EVERY 1 DAY")
  })

  it("11e unknown roles: neutral TABLE captions and connectivity headers — never a guessed FACT/DIM (§5.11)", () => {
    const html = render(<BlueprintUnknownRolesFrame />)
    expect(html).toContain(">TABLE<")
    expect(html).toContain("RELATED TABLES")
    expect(html).not.toContain("FACT")
    expect(html).not.toContain(">DIM<")
    expect(html).not.toContain("DIMENSIONS")
  })

  it("11f wide table: a single joinless table is a valid model — no island flag, no unmodeled region (§5.11)", () => {
    const html = render(<BlueprintWideTableFrame />)
    expect(html).toContain("62 cols")
    expect(html).toContain("engagement_metrics")
    expect(html).not.toContain('data-tag="island"')
    expect(html).not.toContain("UNMODELED")
    expect(html).not.toContain('data-edge="join"')
  })

  it("11g 30 tables: renders at density with at least one bridge (§5.3 at scale)", () => {
    const html = render(<BlueprintScale30Frame />)
    expect(html).toContain('data-node-id="sub_14"')
    expect(html).toMatch(/data-hops="[1-9]/)
  })

  it("11h overview band: far zoom renders no measure chips and no role captions (§5.5)", () => {
    const html = render(<BlueprintStarOverviewFrame />)
    expect(html).not.toContain('data-chip="measure"')
    expect(html).not.toContain('data-caption="role"')
    expect(html).not.toContain("customer_count")
  })
})

describe("M1c-2 — viewer frames carry no write affordance", () => {
  const viewerFrames = [ScoreViewerFrame, ScoreViewerUnscannedFrame, ScoreUnknownFrame, NoAccessPageFrame, ModelViewerFrame, CheckingFrame, OptimizeViewerFrame, OptimizeViewerEmptyFrame]
  it.each(viewerFrames.map(f => [f.name, f] as const))("%s", (_name, Frame) => {
    const html = render(<Frame />)
    for (const label of ["Re-scan", "Run IQ Scan", "Run Optimization", "View Details", "Revert Options", "Remove From History", "View Active Run", "Start Optimization"]) {
      expect(html).not.toContain(label)
    }
    expect(html).not.toContain("confidence")
    expect(html).not.toContain("Run a new IQ Scan")
    // MV-D35 percent ban targets metric-view suggestion cards. Narrowed away from
    // frames that embed IQScoreTab (MaturityCurve SVG stop offsets 0%/33%/…) or
    // AutoOptimizeViewerView (championAccuracyText "N%") — those are legitimate
    // score/accuracy percents, not MV confidence display.
    if (
      Frame !== ScoreViewerFrame &&
      Frame !== ScoreUnknownFrame &&
      Frame !== OptimizeViewerFrame
    ) {
      expect(html).not.toMatch(/\d+%/)
    }
  })
  it("the editor reference keeps its controls", () => {
    const html = render(<ScoreEditorFrame />)
    expect(html).toContain("Re-scan")
    expect(html).toContain("Run Optimization")
    expect(html).toContain("Run a new IQ Scan")
  })
})

describe("M3 — IQ scan unservable empty (MV-D113 d4)", () => {
  it("m3-a renders the unservable empty state through the real component", () => {
    const html = render(<IqScanUnservableFrame />)
    expect(html).toContain("none can be proposed yet")
  })
})

describe("M6b — a stale proposal ranks last with a re-scan notice (MV-D117)", () => {
  const FRESH = "finance.sales.order_revenue"
  const STALE = "finance.sales.gross_margin"
  const iq = render(<IqScanStaleFrame />)
  const run = render(<RunOutputStaleFrame />)
  const approved = render(<IqScanApprovedStaleFrame />)

  it("every frame carries the re-scan notice", () => {
    for (const html of [iq, run, approved]) expect(html).toContain(NOTICE_MARKUP)
  })

  it.each([["m6b-a", iq], ["m6b-b", run]])("%s: one Recommended, the fresh card first, the stale card inert", (_id, html) => {
    expect(html.match(/Recommended/g)?.length).toBe(1)
    expect(html.indexOf(FRESH)).toBeGreaterThan(-1)
    expect(html.indexOf(STALE)).toBeGreaterThan(html.indexOf(FRESH))
    const staleSegment = html.slice(html.indexOf(STALE))
    expect(html.indexOf("Recommended")).toBeLessThan(html.indexOf(STALE))
    expect(staleSegment).not.toMatch(/validated/i)
    expect(staleSegment).not.toMatch(/executable/i)
    expect(staleSegment).not.toContain("Create this metric view")
    expect(staleSegment).not.toContain("I created this myself")
    expect(staleSegment).toContain(NOTICE_MARKUP)
  })

  it("m6b-b: the header counts are unchanged", () => {
    expect(run).toContain("2 proposed · none created")
  })

  it("m6b-c: approved, but not for the next run — the notice replaces the buttons", () => {
    expect(approved).toContain("Approved")
    expect(approved).not.toContain("Approved for the next run")
    expect(approved).not.toContain("Create it now")
  })

  it("m6b-a/b: fresh + stale is not an independence callout on either surface", () => {
    for (const html of [iq, run]) expect(html).not.toContain("are independent")
  })

  it("m6b-d: the callout names the current proposals only; the stale card ranks last, none Recommended", () => {
    const html = render(<RunOutputCurrentCalloutFrame />)
    expect(html).toContain("The 2 current proposals are independent — any or all can be created.")
    expect(html).not.toContain("All 3 are independent")
    expect(html).not.toContain("Recommended")
    expect(html).toContain("3 proposed · none created")
    const staleAt = html.indexOf("finance.sales.avg_order_value")
    expect(staleAt).toBeGreaterThan(html.indexOf(FRESH))
    expect(staleAt).toBeGreaterThan(html.indexOf(STALE))
    expect(html.slice(staleAt)).toContain(NOTICE_MARKUP)
  })

  it("no frame shows a percent or the word confidence (MV-D35)", () => {
    for (const html of [iq, run, approved, render(<RunOutputCurrentCalloutFrame />)]) {
      // Raw markup, so aria-labels and titles are covered too.
      expect(html.toLowerCase()).not.toContain("confidence")
      // Visible text only: the DDL block's syntax highlighter emits hsl(…%) styles.
      expect(html.replace(/<[^>]*>/g, " ")).not.toMatch(/\d+%/)
    }
  })
})

describe("M6b-e — the IQ LOW disclosure with a stale proposal (MV-D117)", () => {
  const CURRENT = "finance.sales.refund_rate"
  const STALE = "finance.sales.discount_depth"
  const html = render(<IqScanLowStaleFrame />)

  it("the header claims validated and executable for the current proposal only", () => {
    expect(html).toContain(
      "All 2 suggestions are ranked lower by demand evidence — the current one is still validated and executable; 1 found by an earlier version of the advisor needs a re-scan.",
    )
    expect(html).not.toContain("each is still validated and executable")
  })

  it("the current card leads with its facts; the stale card ranks last, inert, with the notice", () => {
    const currentAt = html.indexOf(CURRENT)
    const staleAt = html.indexOf(STALE)
    expect(currentAt).toBeGreaterThan(-1)
    expect(staleAt).toBeGreaterThan(currentAt)
    const currentSegment = html.slice(currentAt, staleAt)
    expect(currentSegment).toMatch(/validated/)
    expect(currentSegment).toMatch(/executable/)
    const staleSegment = html.slice(staleAt)
    expect(staleSegment).not.toMatch(/validated/i)
    expect(staleSegment).not.toMatch(/executable/i)
    expect(staleSegment).not.toContain("Create this metric view")
    expect(staleSegment).not.toContain("I created this myself")
    expect(staleSegment).toContain(NOTICE_MARKUP)
  })

  it("no Recommended, no percent, no confidence (MV-D35)", () => {
    expect(html).not.toContain("Recommended")
    expect(html.toLowerCase()).not.toContain("confidence")
    expect(html.replace(/<[^>]*>/g, " ")).not.toMatch(/\d+%/)
  })

  it("is registered after the other M6b frames", () => {
    const ids = MOCKUP_FRAMES.map((f) => f.id)
    expect(ids.indexOf("m6b-e-iqscan-low-stale")).toBe(ids.indexOf("m6b-d-run-output-current-callout") + 1)
  })
})

describe("M6c — the run headline counts a kept metric-view attach (MV-D118)", () => {
  it("m6c-a: the post-attach score is the improvement, not a retained baseline", () => {
    const html = render(<KeptAttachTerminalFrame />)
    expect(html).toContain("90.0%")
    expect(html).not.toContain("Baseline retained")
  })

  it("m6c-b: mid-run shows the post-attach score, not the in-progress dash", () => {
    const html = render(<KeptAttachRunningFrame />)
    expect(html).toContain("90.0%")
    expect(html).not.toContain("—")
  })

  it("m6c-c: a full-scope iteration 0 still reads Baseline retained", () => {
    expect(render(<BaselineRetainedFrame />)).toContain("Baseline retained")
  })

  it("is registered after the M6b frames, in order", () => {
    const ids = MOCKUP_FRAMES.map((f) => f.id)
    const at = ids.indexOf("m6b-e-iqscan-low-stale")
    expect(ids.slice(at + 1, at + 5)).toEqual([
      "m6c-a-kept-attach-terminal",
      "m6c-b-kept-attach-running",
      "m6c-c-baseline-retained",
      "m6c-d-singular-gain-card",
    ])
  })
})

describe("M6c-d — a one-measure proposal reads in the singular (MV-D118)", () => {
  const html = render(<SingularGainCardFrame />)

  it("states the gain in the singular, never 'These 1'", () => {
    expect(html).toContain("This measure recurs across 1 curated query")
    expect(html).not.toContain("These 1")
  })

  it("renders the expanded card's SQL block like the approved 15.8a frame", () => {
    expect(html).toContain("<pre")
    const text = html.replace(/<[^>]*>/g, "")
    expect(text).toContain("WITH METRICS")
    expect(text).toContain("GRANT SELECT ON VIEW")
  })

  it("is a current card: no re-scan notice, no percent, no confidence (MV-D35, MV-D117)", () => {
    expect(html).not.toContain(NOTICE_MARKUP)
    expect(html.toLowerCase()).not.toContain("confidence")
    expect(html.replace(/<[^>]*>/g, " ")).not.toMatch(/\d+%/)
  })
})

describe("M2 — the selection decides the create target", () => {
  it("names the selection's schema, or says why there is none", () => {
    const subset = render(<SelectionSubsetFrame />)
    expect(subset).toContain("finance.sales")
    expect(subset).toContain("You can create metric views in")
    expect(render(<SelectionNoneFrame />)).toContain("Select at least one metric view to create")
    const two = render(<SelectionTwoSchemasFrame />)
    expect(two).toContain("2 schemas (finance.sales, finance.marketing)")
    expect(two).not.toContain("Target:")
    expect(two).toContain("Available once the selected metric views are in one schema.")
    const suggestOnly = render(<SelectionTwoSchemasSuggestOnlyFrame />)
    expect(suggestOnly).toContain("2 schemas (finance.sales, finance.marketing)")
    expect(suggestOnly).not.toContain("Available once you have permission")
    for (const html of [subset, two, suggestOnly, render(<SelectionNoneFrame />)]) {
      expect(html).not.toContain("confidence")
      expect(html).not.toMatch(/\d+%/)
    }
  })
  it("the attached-not-created panel is USER_CREATED with no drop", () => {
    const html = render(<AttachedNotCreatedFrame />)
    expect(html).toContain("it already existed when it was approved")
    expect(html).toContain("dropping this one stays with its owner")
    expect(html).not.toContain("you registered this view")
    expect(html).not.toContain("Drop view")
  })
})
