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
    asset_why: {},
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

  it("shows 'Draft pages with AI' on a sub-domain card, but not on a domain card (Step 4 MV-D66)", () => {
    const sub = renderToStaticMarkup(
      <DomainDraftCard draft={domain({ kind: "subdomain" })} onDecide={noop} onBulkDraft={noop} />,
    )
    expect(sub).toContain("Draft pages with AI")
    assertZeroBurden(sub)

    // A top-level domain (not a sub-domain) has no bulk-draft action.
    const dom = renderToStaticMarkup(
      <DomainDraftCard draft={domain({ kind: "domain" })} onDecide={noop} onBulkDraft={noop} />,
    )
    expect(dom).not.toContain("Draft pages with AI")
  })

  it("renders bulk-draft progress and the completion summary (Step 4 MV-D66)", () => {
    const running = renderToStaticMarkup(
      <DomainDraftCard
        draft={domain({ kind: "subdomain" })}
        onDecide={noop}
        onBulkDraft={noop}
        bulk={{ running: true, done: 1, total: 3, error: null, summary: null }}
      />,
    )
    expect(running).toContain("Drafting…")
    expect(running).toContain("1/3")

    const done = renderToStaticMarkup(
      <DomainDraftCard
        draft={domain({ kind: "subdomain" })}
        onDecide={noop}
        onBulkDraft={noop}
        bulk={{ running: false, done: 3, total: 3, error: null, summary: "Drafted 3 pages." }}
      />,
    )
    expect(done).toContain("Drafted 3 pages.")
    assertZeroBurden(done)
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

  it("renders the Draft with AI button (Step 3 MV-D66)", () => {
    const html = renderToStaticMarkup(<PageDraftCard draft={page()} onDecide={noop} />)
    expect(html).toContain("Draft with AI")
  })

  it("renders the one-line why under Sources/Related (MV-D55)", () => {
    const html = renderToStaticMarkup(
      <PageDraftCard
        draft={page({
          asset_why: {
            "finance.core.rev_mv": "Backs this metric — the governed answer for the concept.",
            "Sales · 01ef": "Serving Genie Agent that answers questions about this concept.",
          },
        })}
        onDecide={noop}
      />,
    )
    expect(html).toContain("Backs this metric")
    expect(html).toContain("Serving Genie Agent that answers questions")
    assertZeroBurden(html)
  })

  it("renders the page body (Step 3 MV-D66)", () => {
    const html = renderToStaticMarkup(
      <PageDraftCard
        draft={page({
          body: "Description: total revenue is the net booked sales.",
        })}
        onDecide={noop}
      />,
    )
    expect(html).toContain("Description: total revenue is the net booked sales.")
    expect(html).toContain("Description")
    expect(html).toContain("Draft with AI")
  })
})
