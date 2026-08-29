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
  OntologyDomainDraftFrame,
  OntologyEmptyFrame,
  OntologyEnrichmentFailedFrame,
  OntologyGrantGateFrame,
  OntologyPageDraftFrame,
  OntologyTagsLensFrame,
  OntologyTaxonomyFrame,
} from "./OntologyPageMockups"

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

// ── Frame 17.0 — Ontology page (standalone, estate-wide, MV-D36) ─────────────
// Pins the MV-D36 contract: a STANDALONE admin-gated page (NOT a SpaceDetail tab),
// estate-wide inputs (workspace Agents + account metric views via
// information_schema), a copy-ready Domain→Sub-Domain→Page taxonomy, a grant
// preflight for the SP system-table signals, and NO API writes anywhere.
describe("frame 17.0 — Ontology page (standalone, estate-wide, MV-D36)", () => {
  it("17.0: NOT a SpaceDetail tab — no Score/Model/Optimize/History tab strip", () => {
    const html = render(<OntologyTaxonomyFrame />)
    // The page has its own chrome; the 5-tab SpaceDetail strip must be gone.
    expect(html).not.toContain('aria-label="Agent detail tabs"')
    expect(html).toContain("Ontology")
    expect(html).toContain("Northwind Trading Co.") // company context in the header
  })

  it("17.0a permission banner: five capability tiers incl. external enrichment, degrade-not-block (MV-D37 + MV-D38)", () => {
    const html = render(<OntologyGrantGateFrame />)
    expect(html).toContain("Tier 2 of 5")
    // All five capability tiers are named.
    expect(html).toContain("Metric-view + tag inventory")
    expect(html).toContain("Usage / lineage / cost ranking")
    expect(html).toContain("Governed-tag graph")
    expect(html).toContain("Membership write")
    expect(html).toContain("Context sources (external enrichment)")
    // The governed-tag read, the write, and the enrichment permissions are spelled out.
    expect(html).toContain("system.tags.governed_tags")
    expect(html).toContain("MANAGE DISCOVERY")
    // Enrichment is EXECUTE on AI Gateway MCP context sources (MV-D46/D47), not raw egress.
    expect(html).toContain("EXECUTE on the enabled Unity AI Gateway MCP services")
    // The Context Sources panel lists the lead registry MCPs classified by class + tier (MV-D47).
    expect(html).toContain("Context sources — Unity AI Gateway MCP")
    expect(html).toContain("system.ai.web_search")
    expect(html).toContain("You.com")
    expect(html).toContain("Genie One · Databricks SQL")
    expect(html).toContain("Copy GRANT EXECUTE")
    // Toggle disables (never blocks) when no source is available.
    expect(html).toContain("disabled when no source is available")
    // External enrichment is naming-only, never structural (MV-D38).
    expect(html).toContain("never structural")
    expect(html).toContain("never structure (MV-D38)")
    // Inventory tier needs no grant; read tiers degrade (write never required to view).
    expect(html).toContain("no explicit grant")
    expect(html).toContain("never required to view")
  })

  it("17.0b taxonomy: proposes Domain → Sub-Domain → Page, names the estate read via information_schema", () => {
    const html = render(<OntologyTaxonomyFrame />)
    expect(html).toContain("Proposed taxonomy")
    expect(html).toContain("Domain")
    expect(html).toContain("Sub-Domain")
    expect(html).toContain("Pages")
    expect(html).toContain("system.information_schema")
    expect(html).toContain("Genie Agents (workspace)")
    // archetype-prefixed Page chips still sit under the sub-domains
    expect(html).toContain("[Routing]")
    expect(html).toContain("[Taxonomy]")
  })

  it("17.0c tags-lens: reads governed tags, flags collisions (reuse-not-duplicate) and orphans (MV-D37)", () => {
    const html = render(<OntologyTagsLensFrame />)
    expect(html).toContain("Governed tags")
    expect(html).toContain("system.tags.governed_tags")
    // reuse-vs-create collisions are surfaced
    expect(html).toContain("Collisions")
    expect(html).toContain("Commercial/Sales")
    expect(html).toContain("REUSE")
    // cleanup of orphans / near-empty
    expect(html).toContain("Cleanup")
    expect(html).toContain("near-empty")
  })

  it("17.0d domain-draft: recommendation + plain reason + apply-for-me/do-it-yourself, NO backend jargon (zero-burden)", () => {
    const html = render(<OntologyDomainDraftFrame />)
    expect(html).toContain("Copy Domain for Discover")
    expect(html).toContain("Proposed Sub-Domains")
    expect(html).toContain("Member assets")
    expect(html).toContain("finance.sales.order_revenue")
    // Recommendation + reason in plain language.
    expect(html).toContain("New domain")
    expect(html).toContain("Why we\u2019re suggesting this")
    // A simple choice: we do it (with a preview) or you do it — no DDL, no grants.
    expect(html).toContain("Apply for me")
    expect(html).toContain("Preview first")
    expect(html).toContain("Prefer to do it yourself?")
    // Zero-burden: the governed-tag/DDL/permission machinery is NOT surfaced to the curator.
    expect(html).not.toContain("CREATE GOVERNED TAG")
    expect(html).not.toContain("SET TAG")
    expect(html).not.toContain("MANAGE DISCOVERY")
    expect(html).not.toContain("governed tag")
  })

  it("17.0e page-draft: 'why' reason + Related/Sources + synonyms + certify + copy; no API-plumbing language", () => {
    const html = render(<OntologyPageDraftFrame />)
    // The reason for the recommendation leads.
    expect(html).toContain("Why we\u2019re suggesting this")
    expect(html).toContain("Related assets")
    expect(html).toContain("Sources")
    // The concept→Agent link is a Related ASSET (identifier), not body prose.
    expect(html).toContain("Genie Agent")
    expect(html).toContain("01ef9a2b3c4d5e6f")
    expect(html).toContain("finance.sales.order_revenue")
    expect(html).toContain("Synonyms")
    expect(html).toContain("Certify: Yes")
    expect(html).toContain("Copy Page for Discover")
    expect(html).toContain("Recent context (informational, as of 2026-08-28)")
    expect(html).toContain("Not certified operational data")
    // Zero-burden: no API-plumbing language surfaced to the curator.
    expect(html).not.toContain("there is no API")
    expect(html).not.toContain("information_schema")
  })

  it("17.0e page-draft: NO API writes anywhere — no instructions, no domain/page write actions (MV-D36)", () => {
    const html = render(<OntologyPageDraftFrame />)
    expect(html).not.toContain("Apply to instructions")
    expect(html).not.toContain("text_instructions")
    expect(html.toLowerCase()).not.toContain("instruction diff")
  })

  it("17.0f enrichment-failed: draft complete, Recent-context absent, shown as left-out not failed", () => {
    const html = render(<OntologyEnrichmentFailedFrame />)
    expect(html).toContain("couldn\u2019t add public context")
    expect(html).toContain("left out, not failed")
    expect(html).toContain("Related assets")
    expect(html).toContain("finance.sales.order_revenue")
    expect(html).not.toContain("Recent context (informational")
    expect(html).not.toContain("Not certified operational data")
  })

  it("17.0g empty: clean result in plain language, no system-table names, never an error", () => {
    const html = render(<OntologyEmptyFrame />)
    expect(html).toContain("Nothing to suggest yet")
    expect(html).toContain("clean result")
    expect(html).toContain("metric views")
    // Zero-burden: the empty state names no system tables.
    expect(html).not.toContain("system.information_schema")
    expect(html).not.toContain("system.tags.governed_tags")
    expect(html.toLowerCase()).not.toContain("failed to")
    expect(html.toLowerCase()).not.toContain("error")
  })
})
