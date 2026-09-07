/**
 * Ontology Map v2 (MV-D75) — docked right-rail inspector. Upgrades the old floating popover
 * into a persistent rail: plain-language facts (MV-D23, via `nodeFacts`), a drill action for
 * business areas, and a "View measures / Pages" expand action for metric views + sub-domains
 * (calls the §2.3 expand route). Every state is handled (loading / error / done, MV-D43).
 * Presentational — it holds no cytoscape or fetch state; the map owns those.
 */
import { ChevronRight, Loader2, Sparkles, X } from "lucide-react"
import type { NodeFacts } from "@/ontology/estateGraphModel"

export type ExpandState = "idle" | "loading" | "error" | "done"

export function GraphInspector({
  facts,
  canExpand,
  expandState,
  onExpand,
  onDrill,
  onClose,
}: {
  facts: NodeFacts | null
  canExpand: boolean
  expandState: ExpandState
  onExpand: () => void
  onDrill: (topId: string, name: string) => void
  onClose: () => void
}) {
  if (!facts) {
    return (
      <div className="flex h-full flex-col items-start justify-center gap-1 px-4 text-xs text-muted">
        <p className="font-medium text-secondary">Nothing selected</p>
        <p className="max-w-[13rem]">Tap a business area, table or metric to see what it is.</p>
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col gap-3 p-3.5">
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="text-sm font-semibold text-primary break-words">{facts.title}</p>
          <span className="mt-1 inline-block rounded-full bg-accent/15 px-2 py-0.5 text-xs text-accent">
            {facts.chip}
          </span>
        </div>
        <button onClick={onClose} aria-label="Close inspector" className="shrink-0 text-muted hover:text-primary">
          <X className="h-4 w-4" />
        </button>
      </div>

      {facts.lines.length > 0 && (
        <div className="space-y-1 text-xs text-secondary">
          {facts.lines.map((line, i) => (
            <p key={i}>{line}</p>
          ))}
        </div>
      )}

      <div className="mt-auto space-y-2">
        {facts.drillTopId && (
          <button
            onClick={() => onDrill(facts.drillTopId!, facts.title)}
            className="flex w-full items-center justify-center gap-1 rounded-lg bg-accent px-3 py-1.5 text-xs font-semibold text-white hover:opacity-90"
          >
            View its assets <ChevronRight className="h-3.5 w-3.5" />
          </button>
        )}

        {canExpand && (
          <div>
            <button
              onClick={onExpand}
              disabled={expandState === "loading" || expandState === "done"}
              className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-accent/40 bg-accent/10 px-3 py-1.5 text-xs font-semibold text-accent hover:bg-accent/20 disabled:opacity-60"
            >
              {expandState === "loading" ? (
                <>
                  <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading…
                </>
              ) : expandState === "done" ? (
                <>
                  <Sparkles className="h-3.5 w-3.5" /> Measures &amp; Pages shown
                </>
              ) : (
                <>
                  <Sparkles className="h-3.5 w-3.5" /> View measures &amp; Pages
                </>
              )}
            </button>
            {expandState === "error" && (
              <p className="mt-1.5 text-[11px] text-danger-foreground">
                Couldn&apos;t load the details right now. Try again in a moment.
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
