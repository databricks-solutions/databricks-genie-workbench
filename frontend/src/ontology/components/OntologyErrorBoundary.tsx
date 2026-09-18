/**
 * A minimal render-error boundary for the ontology write surfaces (Phase 5 / 17j).
 * Async failures in apply/undo are handled in-component via `phase` state; this catches
 * the render-time throw that phase state can't, replacing the apply/undo subtree with a
 * recoverable card instead of letting the exception unmount the whole page (playbook:
 * error boundaries are mandatory on these surfaces). Mirrors GraphErrorBoundary
 * (components/model/SemanticGraph.tsx) but scoped to the ontology apply surface.
 */
import { Component, type ReactNode } from "react"
import { AlertTriangle } from "lucide-react"

export class OntologyErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false }

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true }
  }

  componentDidCatch(error: unknown) {
    console.error("Ontology ErrorBoundary caught:", error)
  }

  render() {
    if (this.state.failed) {
      return (
        <div
          role="alert"
          className="flex items-start gap-2 rounded-lg border border-danger/30 bg-danger/5 px-3 py-2.5 text-sm text-danger-foreground"
        >
          <AlertTriangle aria-hidden className="mt-0.5 h-4 w-4 shrink-0" />
          <span>Something went wrong showing this. Close and reopen to try again.</span>
        </div>
      )
    }
    return this.props.children
  }
}
