import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { DomainDraft, PageDraft } from "@/ontology/types"
import { DomainDraftCard } from "./DomainDraftCard"
import { PageDraftCard } from "./PageDraftCard"

// The zero-burden contract (Phase 3d §10): the rendered card must never leak the
// machinery. If any of these tokens appears in the markup, the copy has regressed.
const FORBIDDEN = [
  "set tag",
  "merge",
  "metastore_id",
  "workspace_id",
  "genie_ont_",
  "system.tags",
  "sql warehouse",
  "provenance tier",
  "lakebase",
  "mirror",
  "l6",
]

function assertZeroBurden(html: string) {
  const lower = html.toLowerCase()
  for (const token of FORBIDDEN) {
    expect(lower, `rendered card leaked forbidden jargon: "${token}"`).not.toContain(token)
  }
}

function domain(overrides: Partial<DomainDraft> = {}): DomainDraft {
  return {
    proposal_id: "sug_d",
    kind: "domain",
    name: "Commercial",
    description: "Sales and revenue assets that answer commercial questions.",
    tag_decision: "create",
    conflict_tag: null,
    subdomains: ["Sales", "Pipeline"],
    members: [
      { fqn: "finance.core.ledger", asset_type: "table" },
      { fqn: "finance.core.rev_mv", asset_type: "metric_view" },
    ],
    why: "These assets are asked about together and aren't grouped under a shared domain yet.",
    evidence: [
      { label: "Central to how the data connects", kind: "centrality" },
      { label: "Built on governed data", kind: "governance" },
    ],
    tier: "high",
    ...overrides,
  }
}

function page(overrides: Partial<PageDraft> = {}): PageDraft {
  return {
    proposal_id: "pg_1",
    archetype: "Routing",
    title: "[Routing] total_revenue",
    reason: "People ask about total revenue — point them at the governed answer.",
    body: "Description: total revenue is the net booked sales.",
    synonyms: ["TR", "net sales", "revenue booked"],
    related_fqns: ["Sales · 01ef"],
    source_fqns: ["finance.core.rev_mv"],
    certify: true,
    evidence: [{ label: "Backed by 2 sources", kind: "corroboration" }],
    tier: "medium",
    ...overrides,
  }
}

const noop = () => {}

describe("DomainDraftCard — zero-burden render (17.0d)", () => {
  it("renders the recommendation, why, and evidence for a CREATE proposal", () => {
    const html = renderToStaticMarkup(<DomainDraftCard draft={domain()} onDecide={noop} />)
    expect(html).toContain("Commercial")
    expect(html).toContain("New domain")
    expect(html).toContain("Why we&#x27;re suggesting this")
    expect(html).toContain("Central to how the data connects")
    expect(html).toContain("Copy for Discover")
    expect(html).toContain("Approve")
    expect(html).toContain("Dismiss")
    assertZeroBurden(html)
  })

  it("renders a REUSE proposal with its own recommendation line", () => {
    const html = renderToStaticMarkup(
      <DomainDraftCard draft={domain({ tag_decision: "reuse" })} onDecide={noop} />,
    )
    expect(html).toContain("Group under an existing domain")
    assertZeroBurden(html)
  })

  it("renders a REASSIGN proposal with the conflict tag + evidence and the reassign actions", () => {
    const html = renderToStaticMarkup(
      <DomainDraftCard
        draft={domain({
          kind: "reassign",
          tag_decision: "reassign",
          conflict_tag: "finance",
          evidence: [{ label: "Overlaps the “finance” tag", kind: "conflict" }],
        })}
        onDecide={noop}
      />,
    )
    expect(html).toContain("Resolve a tag overlap")
    expect(html).toContain("finance") // the conflict tag is named
    expect(html).toContain("Accept reassignment")
    expect(html).toContain("Keep current")
    assertZeroBurden(html)
  })

  it("keeps Apply-for-me disabled (17i)", () => {
    const html = renderToStaticMarkup(<DomainDraftCard draft={domain()} onDecide={noop} />)
    expect(html).toContain("Apply for me")
    // The only disabled action by default is Apply-for-me.
    expect(html).toContain("disabled")
  })

  it("renders the honest confidence band + signals + gap, never a percent (MV-D56/D35)", () => {
    const html = renderToStaticMarkup(
      <DomainDraftCard
        draft={domain({
          confidence: {
            band: "Medium",
            signals_present: ["central to how the data connects", "built on governed data"],
            gap: "connect query history to rank by usage",
          },
        })}
        onDecide={noop}
      />,
    )
    expect(html).toContain("Medium")
    expect(html).toContain("central to how the data connects")
    expect(html).toContain("connect query history to rank by usage")
    expect(html).not.toContain("%") // never a rendered percent
    assertZeroBurden(html)
  })
})

describe("PageDraftCard — zero-burden render (17.0e)", () => {
  it("leads with the reason and shows synonyms, sources, and certify", () => {
    const html = renderToStaticMarkup(<PageDraftCard draft={page()} onDecide={noop} />)
    expect(html).toContain("People ask about total revenue")
    expect(html).toContain("Also called")
    expect(html).toContain("net sales")
    expect(html).toContain("Sources")
    expect(html).toContain("Recommended to certify")
    expect(html).toContain("Copy for Discover")
    assertZeroBurden(html)
  })

  it("keeps Apply-for-me disabled (17i)", () => {
    const html = renderToStaticMarkup(<PageDraftCard draft={page()} onDecide={noop} />)
    expect(html).toContain("Apply for me")
    expect(html).toContain("disabled")
  })
})
