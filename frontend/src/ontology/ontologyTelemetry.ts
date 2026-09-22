/**
 * Ontology funnel instrumentation seam (MV-D107 Phase 3). The workbench frontend has no
 * telemetry sink today, so this is a dependency-free seam: events are emitted through an
 * injectable sink that DEFAULTS TO NO-OP. Wire `setOntologyTelemetrySink` to a real analytics
 * transport when one lands — no component code changes when it does.
 *
 * TODO(MV-D107 P3): connect `setOntologyTelemetrySink` to the workspace telemetry transport
 * once a first-party seam exists. Until then emit() is a safe no-op (no network, no dep).
 */
import type { NextActionKind } from "@/ontology/overviewModel"

// The four funnel milestones from the goal: land (view) → act (CTA) → first scan → first review.
export type OntologyFunnelEvent =
  | { name: "overview_view" }
  | { name: "overview_cta"; action: NextActionKind }
  | { name: "scan_start" }
  | { name: "review_decision"; kind: string; action: string }

type Sink = (event: OntologyFunnelEvent) => void

let sink: Sink | null = null

/** Install (or clear, with null) the telemetry sink. Used by a future transport and by tests. */
export function setOntologyTelemetrySink(next: Sink | null): void {
  sink = next
}

/**
 * Emit one funnel event. No-op until a sink is wired, and NEVER throws — instrumentation must
 * not break the surface it observes.
 */
export function emitOntologyEvent(event: OntologyFunnelEvent): void {
  try {
    sink?.(event)
  } catch {
    /* best-effort — swallow sink errors so telemetry can't take the UI down */
  }
}
