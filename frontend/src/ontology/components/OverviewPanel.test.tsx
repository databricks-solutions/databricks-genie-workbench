import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type {
  OntologyDrafts,
  OntologyInventory,
  OntologyPreflight,
  OntologyTaxonomy,
} from "@/ontology/types"
import { OverviewPanel } from "./OverviewPanel"

function preflight(overrides: Partial<OntologyPreflight> = {}): OntologyPreflight {
  return {
    tiers: [],
    can_render_taxonomy: true,
    catalog_allowlist: ["finance"],
    company_name: "Acme",
    as_of: "2026-09-01T00:00:00+00:00",
    ...overrides,
  }
}

function inventory(overrides: Partial<OntologyInventory> = {}): OntologyInventory {
  return {
    catalogs_scanned: ["finance"],
    metric_view_count: 12,
    genie_agent_count: 4,
    governed_tag_count: 7,
    as_of: "2026-09-01T12:34:56+00:00",
    ...overrides,
  }
}

function taxonomy(overrides: Partial<OntologyTaxonomy> = {}): OntologyTaxonomy {
  return {
    domains: [],
    ungrouped: { metric_views: [], genie_agents: [] },
    as_of: "2026-09-01T00:00:00+00:00",
    ...overrides,
  }
}

function drafts(overrides: Partial<OntologyDrafts> = {}): OntologyDrafts {
  return {
    domains: [],
    pages: [],
    source: "live",
    as_of: "2026-09-01T00:00:00+00:00",
    ...overrides,
  }
}

const noop = () => {}

// Count rendered <button>s so we can assert exactly ONE primary CTA per state.
function buttonCount(html: string): number {
  return (html.match(/<button/g) ?? []).length
}

describe("OverviewPanel — one adaptive primary CTA per state (MV-D107)", () => {
  it("empty scope ⇒ 'Choose catalogs to scan', single button", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight({ catalog_allowlist: [] })}
        inventory={inventory()}
        taxonomy={null}
        drafts={null}
        onPrimary={noop}
      />,
    )
    expect(html).toContain("Choose catalogs to scan")
    expect(buttonCount(html)).toBe(1)
  })

  it("cold ⇒ 'Scan the estate', single button", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight()}
        inventory={inventory()}
        taxonomy={null}
        drafts={drafts({ source: "cold" })}
        onPrimary={noop}
      />,
    )
    expect(html).toContain("Scan the estate")
    expect(buttonCount(html)).toBe(1)
  })

  it("pending ⇒ 'Review N suggestions', single button", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight()}
        inventory={inventory()}
        taxonomy={taxonomy({ domains: [{ subdomains: [] } as never] })}
        drafts={drafts({ domains: [{} as never], pages: [{} as never] })}
        onPrimary={noop}
      />,
    )
    expect(html).toContain("Review 2 suggestions")
    expect(buttonCount(html)).toBe(1)
  })

  it("caught up ⇒ 'You're all caught up', single button", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight()}
        inventory={inventory()}
        taxonomy={taxonomy({ domains: [{ subdomains: [] } as never] })}
        drafts={drafts({ source: "live" })}
        onPrimary={noop}
      />,
    )
    expect(html).toContain("You&#x27;re all caught up")
    expect(buttonCount(html)).toBe(1)
  })
})

describe("OverviewPanel — hero + neutral status (MV-D107)", () => {
  it("names concepts, domains, and the company in the hero when the taxonomy has content", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight()}
        inventory={inventory()}
        taxonomy={taxonomy({ domains: [{ subdomains: [{}, {}] } as never] })}
        drafts={drafts()}
        onPrimary={noop}
      />,
    )
    // 1 domain + 2 sub-domains = 3 concepts across 1 domain in Acme.
    expect(html).toContain("Genie knows 3 concepts across 1 domain in Acme")
  })

  it("shows the neutral read-status line, not the amber access matrix", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight()}
        inventory={inventory()}
        taxonomy={taxonomy()}
        drafts={drafts()}
        onPrimary={noop}
      />,
    )
    expect(html).toContain("Reading as you (admin)")
    expect(html).toContain("12 metric views")
    expect(html).toContain("7 tags")
    expect(html).not.toContain("read tiers ready") // the demoted banner header
  })

  it("busy scan disables the CTA and shows a scanning label", () => {
    const html = renderToStaticMarkup(
      <OverviewPanel
        preflight={preflight()}
        inventory={inventory()}
        taxonomy={null}
        drafts={drafts({ source: "cold" })}
        onPrimary={noop}
        busy
      />,
    )
    expect(html).toContain("Scanning…")
    expect(html).toContain("disabled")
  })
})
