/**
 * Ontology Map north-star (MV-D81/D83) — docked right-rail inspector. Shows a selected
 * node's type pill, name, plain-language description, plain metadata, and a **navigable
 * Relationships list** (each row expands the path to + selects its target, §5/R15).
 * Technical detail (expressions / FQNs) lives behind an opt-in disclosure (MV-D23 / §9-C).
 * A selected proposal shows its confidence band + Approve / Dismiss (§3.5, Phase-5 apply).
 * Presentational — holds no layout or fetch state; the map owns those.
 */
import { ChevronRight, Sparkles, X } from "lucide-react"

export interface InspectorRelationship {
  targetId: string
  label: string
  /** "part of" | "contains" | a typed cross-link verb, etc. */
  verb: string
  xdom: boolean
  /**
   * Evidence key/values from the edge's pre-seed `detail` bag (MV-D88) — e.g.
   * `["shares key", "route_id"]`. Empty/absent for structural (part-of / contains) rows;
   * shown inline under the row so the relationship reads without hovering the arc.
   */
  detail?: [string, string][]
}

export interface InspectorData {
  title: string
  typeLabel: string
  description: string
  /** Plain-language metadata lines (no jargon). */
  facts: string[]
  /** Real key/value metadata from the snapshot (MV-D86) — rows/format/freshness/… */
  meta?: [string, string][]
  /** Documentation Pages attached to this node, hydrated on expand (MV-D73). */
  pages?: { id: string; label: string }[]
  /** Technical detail (Expression / Path / Source) — behind the disclosure (§9-C). */
  technical: string[]
  relationships: InspectorRelationship[]
  /**
   * A domain/org that has sub-containers AND gated directly-attached assets (#1): the explicit
   * "Show / Hide N direct assets" control. Null when there are none to reveal (asset-only
   * domains already reveal on open; leaves have no children).
   */
  directAssets?: { id: string; count: number; drilled: boolean } | null
  isProposal?: boolean
  band?: "High" | "Medium" | "Low" | null
}

export function GraphInspector({
  data,
  onSelectRelationship,
  onToggleDirectAssets,
  onClose,
  onApprove,
  onDismiss,
}: {
  data: InspectorData | null
  onSelectRelationship: (targetId: string) => void
  onToggleDirectAssets?: (id: string) => void
  onClose: () => void
  onApprove?: () => void
  onDismiss?: () => void
}) {
  if (!data) {
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
          <p className="text-sm font-semibold text-primary break-words">{data.title}</p>
          <span className="mt-1 inline-block rounded-full bg-accent/15 px-2 py-0.5 text-xs text-accent">
            {data.typeLabel}
          </span>
          {data.isProposal && data.band && (
            <span className="ml-1.5 mt-1 inline-block rounded-full border border-default px-2 py-0.5 text-[11px] text-secondary">
              {data.band} confidence
            </span>
          )}
        </div>
        <button onClick={onClose} aria-label="Close inspector" className="shrink-0 text-muted hover:text-primary">
          <X className="h-4 w-4" />
        </button>
      </div>

      {data.description && <p className="text-xs leading-snug text-secondary">{data.description}</p>}

      {data.facts.length > 0 && (
        <div className="space-y-1.5 text-xs text-secondary">
          {data.facts.map((line, i) => (
            <p key={i} className="flex gap-1.5 leading-snug">
              <span className="mt-[5px] h-1 w-1 shrink-0 rounded-full bg-accent/70" aria-hidden />
              <span>{line}</span>
            </p>
          ))}
        </div>
      )}

      {data.meta && data.meta.length > 0 && (
        <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
          {data.meta.map(([k, v]) => (
            <div key={k} className="contents">
              <dt className="text-muted">{k}</dt>
              <dd className="break-words text-right text-secondary">{v}</dd>
            </div>
          ))}
        </dl>
      )}

      {data.directAssets && onToggleDirectAssets && (
        <button
          onClick={() => onToggleDirectAssets(data.directAssets!.id)}
          className="flex w-full items-center justify-between gap-2 rounded-md border border-default px-2 py-1.5 text-xs font-medium text-secondary hover:bg-elevated hover:text-primary"
          aria-pressed={data.directAssets.drilled}
        >
          <span>
            {data.directAssets.drilled ? "Hide" : "Show"} {data.directAssets.count}{" "}
            {data.directAssets.count === 1 ? "direct asset" : "direct assets"}
          </span>
          <ChevronRight
            className={`h-3.5 w-3.5 shrink-0 text-muted transition-transform ${data.directAssets.drilled ? "rotate-90" : ""}`}
          />
        </button>
      )}

      {data.pages && data.pages.length > 0 && (
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-wide text-muted">
            Attached pages · {data.pages.length}
          </p>
          <ul className="space-y-0.5 text-xs text-secondary">
            {data.pages.slice(0, 8).map((p) => (
              <li key={p.id} className="truncate">{p.label}</li>
            ))}
            {data.pages.length > 8 && (
              <li className="text-muted">+{data.pages.length - 8} more</li>
            )}
          </ul>
        </div>
      )}

      {data.relationships.length > 0 && (
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-wide text-muted">Relationships</p>
          <ul className="space-y-0.5">
            {data.relationships.map((r, i) => (
              <li key={`${r.targetId}-${i}`}>
                <button
                  onClick={() => onSelectRelationship(r.targetId)}
                  className="flex w-full items-center justify-between gap-2 rounded-md px-1.5 py-1 text-left text-xs text-secondary hover:bg-elevated hover:text-primary"
                >
                  <span className="min-w-0 flex-1 truncate">
                    <span className="text-muted">{r.verb} </span>
                    {r.label}
                  </span>
                  {r.xdom && (
                    <span className="shrink-0 rounded bg-danger/15 px-1 text-[10px] font-semibold text-danger-foreground">
                      X-DOM
                    </span>
                  )}
                  <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted" />
                </button>
                {/* Evidence bag (MV-D88): the why behind the arc, inline so it reads without
                    hovering — e.g. "shares key · route_id". Absent for structural rows. */}
                {r.detail && r.detail.length > 0 && (
                  <dl className="ml-1.5 mt-0.5 flex flex-wrap gap-x-2 gap-y-0.5 pl-1.5 text-[11px] leading-tight">
                    {r.detail.map(([k, v]) => (
                      <span key={k} className="inline-flex gap-1">
                        <dt className="text-muted">{k}</dt>
                        <dd className="text-secondary">{v}</dd>
                      </span>
                    ))}
                  </dl>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

      {data.technical.length > 0 && (
        <details className="text-xs">
          <summary className="cursor-pointer select-none text-muted hover:text-secondary">
            Technical details
          </summary>
          <div className="mt-1.5 space-y-1 font-mono text-[11px] text-muted">
            {data.technical.map((line, i) => (
              <p key={i} className="break-all">{line}</p>
            ))}
          </div>
        </details>
      )}

      {data.isProposal && (onApprove || onDismiss) && (
        <div className="mt-auto flex gap-2">
          {onApprove && (
            <button
              onClick={onApprove}
              className="flex flex-1 items-center justify-center gap-1 rounded-lg bg-accent px-3 py-1.5 text-xs font-semibold text-white hover:opacity-90"
            >
              <Sparkles className="h-3.5 w-3.5" /> Approve
            </button>
          )}
          {onDismiss && (
            <button
              onClick={onDismiss}
              className="flex-1 rounded-lg border border-default px-3 py-1.5 text-xs font-semibold text-secondary hover:bg-elevated"
            >
              Dismiss
            </button>
          )}
        </div>
      )}
    </div>
  )
}
