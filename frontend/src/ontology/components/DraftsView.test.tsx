import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyDrafts } from "@/ontology/types"
import { DraftsView } from "./DraftsView"

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

describe("DraftsView cold/caught-up states (MV-D107 Review)", () => {
  it("cold Review exposes the 'Scan the estate' button in-place", () => {
    const html = renderToStaticMarkup(
      <DraftsView
        drafts={drafts({ source: "cold" })}
        onScan={noop}
        onBrowseEstate={noop}
      />,
    )
    expect(html).toContain("Scan the estate")
    expect(html).toContain("Scan your estate for suggestions") // outcome heading
    expect(html).toContain("Browse the estate") // escape hatch
  })

  it("cold Review shows a scanning label while a scan is in flight", () => {
    const html = renderToStaticMarkup(
      <DraftsView drafts={drafts({ source: "cold" })} onScan={noop} scanning />,
    )
    expect(html).toContain("Scanning…")
  })

  it("caught-up (scanned, none pending) reads as all caught up, no scan button", () => {
    const html = renderToStaticMarkup(<DraftsView drafts={drafts({ source: "live" })} />)
    expect(html).toContain("all caught up")
    expect(html).not.toContain("Scan the estate")
  })
})
