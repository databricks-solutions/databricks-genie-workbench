import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import { DomainVisibilityPanel, type VisibilityGroup } from "./DomainVisibilityPanel"

const groups: VisibilityGroup[] = [
  { id: "d_fin", label: "Finance", subdomains: [{ id: "s_rev", label: "Revenue" }] },
  { id: "d_ops", label: "Operations", subdomains: [] },
]

const noop = () => {}

describe("DomainVisibilityPanel — domain show/hide (MV-D87 / R27)", () => {
  it("lists every business area with a checkbox + Show/Hide all", () => {
    const html = renderToStaticMarkup(
      <DomainVisibilityPanel groups={groups} hidden={new Set()} onToggle={noop} onShowAll={noop} onHideAll={noop} />,
    )
    expect(html).toContain("Finance")
    expect(html).toContain("Operations")
    expect(html).toContain("Show all")
    expect(html).toContain("Hide all")
    expect(html).toContain('type="checkbox"')
  })

  it("reflects hidden state: each hidden area drops one checked box", () => {
    const count = (h: string) => (h.match(/checked=""/g) ?? []).length
    // Sub-domains are collapsed by default, so only the two top rows render a box.
    const all = renderToStaticMarkup(
      <DomainVisibilityPanel groups={groups} hidden={new Set()} onToggle={noop} onShowAll={noop} onHideAll={noop} />,
    )
    const oneHidden = renderToStaticMarkup(
      <DomainVisibilityPanel groups={groups} hidden={new Set(["d_ops"])} onToggle={noop} onShowAll={noop} onHideAll={noop} />,
    )
    expect(count(all)).toBe(2)
    expect(count(oneHidden)).toBe(1)
  })

  it("renders an empty-state line when there are no areas", () => {
    const html = renderToStaticMarkup(
      <DomainVisibilityPanel groups={[]} hidden={new Set()} onToggle={noop} onShowAll={noop} onHideAll={noop} />,
    )
    expect(html).toContain("No business areas to filter")
  })
})
