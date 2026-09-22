import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyTaxonomy } from "@/ontology/types"
import { EstateView } from "./EstateView"

function taxonomy(): OntologyTaxonomy {
  return {
    domains: [{ tag_key: "finance", name: "Finance", member_count: 3, subdomains: [], members: [] }],
    ungrouped: { metric_views: [], genie_agents: [] },
    as_of: "2026-09-01T00:00:00+00:00",
  }
}

describe("EstateView a11y (MV-D107 P3)", () => {
  it("exposes a labelled tablist with two tabs, Taxonomy selected by default", () => {
    const html = renderToStaticMarkup(
      <EstateView taxonomy={taxonomy()} tags={null} inventory={null} drafts={[]} />,
    )
    expect(html).toContain('role="tablist"')
    expect(html).toContain('aria-label="Estate view"')
    expect((html.match(/role="tab"/g) ?? []).length).toBe(2)
    // Default segment "taxonomy" is the selected tab.
    expect(html).toContain('aria-selected="true"')
  })
})
