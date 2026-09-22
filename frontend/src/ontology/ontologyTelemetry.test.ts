import { afterEach, describe, expect, it, vi } from "vitest"
import {
  emitOntologyEvent,
  setOntologyTelemetrySink,
  type OntologyFunnelEvent,
} from "./ontologyTelemetry"

afterEach(() => setOntologyTelemetrySink(null))

describe("ontologyTelemetry seam (MV-D107 P3)", () => {
  it("no-op by default — emitting without a sink does not throw", () => {
    expect(() => emitOntologyEvent({ name: "overview_view" })).not.toThrow()
  })

  it("delivers each funnel event to the installed sink", () => {
    const seen: OntologyFunnelEvent[] = []
    setOntologyTelemetrySink((e) => seen.push(e))
    emitOntologyEvent({ name: "overview_view" })
    emitOntologyEvent({ name: "overview_cta", action: "scan" })
    emitOntologyEvent({ name: "scan_start" })
    emitOntologyEvent({ name: "review_decision", kind: "domain", action: "approve" })
    expect(seen).toEqual([
      { name: "overview_view" },
      { name: "overview_cta", action: "scan" },
      { name: "scan_start" },
      { name: "review_decision", kind: "domain", action: "approve" },
    ])
  })

  it("swallows sink errors — instrumentation never breaks the UI", () => {
    setOntologyTelemetrySink(() => {
      throw new Error("sink boom")
    })
    expect(() => emitOntologyEvent({ name: "scan_start" })).not.toThrow()
  })

  it("stops delivering once the sink is cleared", () => {
    const sink = vi.fn()
    setOntologyTelemetrySink(sink)
    emitOntologyEvent({ name: "overview_view" })
    setOntologyTelemetrySink(null)
    emitOntologyEvent({ name: "overview_view" })
    expect(sink).toHaveBeenCalledTimes(1)
  })
})
