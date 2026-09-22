import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import { Lock } from "lucide-react"
import { OnboardingState } from "./OnboardingState"

const noop = () => {}

function buttonCount(html: string): number {
  return (html.match(/<button/g) ?? []).length
}

describe("OnboardingState (MV-D107 empty/gate playbook)", () => {
  it("renders an outcome heading, ONE primary action, and an escape hatch", () => {
    const html = renderToStaticMarkup(
      <OnboardingState
        icon={<Lock className="h-6 w-6" />}
        heading="Unlock the reads"
        body="Some plain body."
        primary={{ label: "Open Access", onClick: noop }}
        escape={{ label: "Back to overview", onClick: noop }}
      />,
    )
    // Semantic heading (not just bold text).
    expect(html).toContain("<h3")
    expect(html).toContain("Unlock the reads")
    expect(html).toContain("Open Access")
    expect(html).toContain("Back to overview")
    // One primary <button> + one escape <button> = 2, never more than one primary CTA.
    expect(buttonCount(html)).toBe(2)
  })

  it("omits the escape hatch when none is given (one action only)", () => {
    const html = renderToStaticMarkup(
      <OnboardingState
        icon={<Lock className="h-6 w-6" />}
        heading="Scoped?"
        body="Body."
        primary={{ label: "Choose catalogs", onClick: noop }}
      />,
    )
    expect(buttonCount(html)).toBe(1)
  })

  it("renders a decorative ghost preview hidden from assistive tech", () => {
    const html = renderToStaticMarkup(
      <OnboardingState
        icon={<Lock className="h-6 w-6" />}
        heading="H"
        body="B"
        primary={{ label: "Go", onClick: noop }}
      />,
    )
    expect(html).toContain('aria-hidden="true"')
  })
})
