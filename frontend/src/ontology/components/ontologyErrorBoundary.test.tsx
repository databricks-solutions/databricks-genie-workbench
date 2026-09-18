/**
 * Phase 5 / 17j — the apply/undo write surface is wrapped in a render-error boundary
 * (playbook: mandatory on these surfaces). Mirrors SemanticGraph.crash.test.tsx: pins
 * the deterministic failure-state transition. The rendered-fallback path is a
 * client-only behavior (error boundaries don't catch under renderToStaticMarkup), so
 * here we pin the transition + the pass-through render, matching the repo's SSR test env.
 */
import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import { OntologyErrorBoundary } from "./OntologyErrorBoundary"

describe("OntologyErrorBoundary — a render throw never takes the surface down", () => {
  it("transitions to the failed state on a caught error", () => {
    expect(OntologyErrorBoundary.getDerivedStateFromError()).toEqual({ failed: true })
  })

  it("renders its children unchanged when nothing throws", () => {
    const html = renderToStaticMarkup(
      <OntologyErrorBoundary>
        <p>all good</p>
      </OntologyErrorBoundary>,
    )
    expect(html).toContain("all good")
    expect(html).not.toContain("Something went wrong")
  })
})
