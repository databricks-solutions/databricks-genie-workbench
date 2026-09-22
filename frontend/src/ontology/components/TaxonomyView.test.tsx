import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { DomainDraft, DomainNode, OntologyTaxonomy } from "@/ontology/types"
import { TaxonomyView } from "./TaxonomyView"

function domain(overrides: Partial<DomainNode> = {}): DomainNode {
  return {
    tag_key: "finance",
    name: "Finance",
    member_count: 0,
    subdomains: [],
    members: [],
    ...overrides,
  }
}

function taxonomy(domains: DomainNode[]): OntologyTaxonomy {
  return {
    domains,
    ungrouped: { metric_views: [], genie_agents: [] },
    as_of: "2026-09-01T00:00:00+00:00",
  }
}

function draft(overrides: Partial<DomainDraft> = {}): DomainDraft {
  return {
    proposal_id: "p1",
    kind: "domain",
    name: "Finance",
    description: "",
    tag_decision: "create",
    conflict_tag: null,
    subdomains: [],
    members: [],
    why: "",
    evidence: [],
    tier: "high",
    ...overrides,
  }
}

describe("TaxonomyView first-use vs returning (MV-D107 P3 variant)", () => {
  it("first-use: nothing tagged yet + drafts present ⇒ leads with the review banner", () => {
    const html = renderToStaticMarkup(
      <TaxonomyView
        taxonomy={taxonomy([domain({ member_count: 0 })])}
        inventory={null}
        drafts={[draft(), draft({ proposal_id: "p2" })]}
      />,
    )
    expect(html).toContain("Nothing tagged yet")
    expect(html).toContain("Review 2 proposals")
  })

  it("returning: something is tagged ⇒ no first-use banner", () => {
    const html = renderToStaticMarkup(
      <TaxonomyView
        taxonomy={taxonomy([domain({ member_count: 4 })])}
        inventory={null}
        drafts={[draft()]}
      />,
    )
    expect(html).not.toContain("Nothing tagged yet")
  })

  it("decorative icons are hidden from assistive tech", () => {
    const html = renderToStaticMarkup(
      <TaxonomyView taxonomy={taxonomy([domain({ member_count: 4 })])} inventory={null} drafts={[]} />,
    )
    expect(html).toContain('aria-hidden="true"')
  })
})
