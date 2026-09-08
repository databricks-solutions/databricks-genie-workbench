/**
 * Ontology Map — Domain show/hide panel (MV-D87, Lane P2, §5/R27). A right-rail list of the
 * estate's business areas (each expandable to its sub-areas) with checkboxes; unchecking one
 * hides that subtree — and its cross-links + legend counts — from the map. VIEW-ONLY: the
 * panel reports toggles via callbacks and never mutates the snapshot (R13); the map derives a
 * filtered model (`filterModelByVisibility`). Presentational — holds only its own row-expand
 * UI state. "Show all / Hide all" flips the whole set at once.
 */
import { useState } from "react"
import { ChevronDown, ChevronRight, Eye, X } from "lucide-react"

export interface VisibilityGroup {
  id: string
  label: string
  subdomains: { id: string; label: string }[]
}

export function DomainVisibilityPanel({
  groups,
  hidden,
  onToggle,
  onShowAll,
  onHideAll,
  onClose,
}: {
  groups: VisibilityGroup[]
  hidden: ReadonlySet<string>
  onToggle: (id: string) => void
  onShowAll: () => void
  onHideAll: () => void
  onClose?: () => void
}) {
  const [open, setOpen] = useState<Set<string>>(new Set())
  const toggleOpen = (id: string) =>
    setOpen((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <div className="flex flex-col gap-2 rounded-xl border border-default bg-elevated p-3">
      <div className="flex items-center justify-between gap-2">
        <p className="flex items-center gap-1.5 text-xs font-semibold text-primary">
          <Eye className="h-3.5 w-3.5" /> Show / hide areas
        </p>
        {onClose && (
          <button onClick={onClose} aria-label="Close visibility panel" className="shrink-0 text-muted hover:text-primary">
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      <div className="flex items-center gap-2 text-[11px]">
        <button onClick={onShowAll} className="rounded-md border border-default px-2 py-0.5 text-secondary hover:text-primary">
          Show all
        </button>
        <button onClick={onHideAll} className="rounded-md border border-default px-2 py-0.5 text-secondary hover:text-primary">
          Hide all
        </button>
      </div>

      {groups.length === 0 ? (
        <p className="text-[11px] text-muted">No business areas to filter.</p>
      ) : (
        <ul className="max-h-64 space-y-0.5 overflow-y-auto pr-1 text-xs">
          {groups.map((g) => {
            const groupHidden = hidden.has(g.id)
            const isOpen = open.has(g.id)
            return (
              <li key={g.id}>
                <div className="flex items-center gap-1">
                  {g.subdomains.length > 0 ? (
                    <button
                      onClick={() => toggleOpen(g.id)}
                      aria-label={isOpen ? `Collapse ${g.label}` : `Expand ${g.label}`}
                      aria-expanded={isOpen}
                      className="shrink-0 text-muted hover:text-primary"
                    >
                      {isOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                    </button>
                  ) : (
                    <span className="inline-block w-3.5 shrink-0" aria-hidden />
                  )}
                  <label className="flex min-w-0 flex-1 items-center gap-1.5 py-0.5">
                    <input
                      type="checkbox"
                      checked={!groupHidden}
                      onChange={() => onToggle(g.id)}
                      className="h-3.5 w-3.5 shrink-0 accent-current"
                    />
                    <span className="truncate text-secondary">{g.label}</span>
                  </label>
                </div>
                {isOpen && g.subdomains.length > 0 && (
                  <ul className="ml-5 space-y-0.5">
                    {g.subdomains.map((s) => {
                      const subHidden = groupHidden || hidden.has(s.id)
                      return (
                        <li key={s.id}>
                          <label className="flex items-center gap-1.5 py-0.5">
                            <input
                              type="checkbox"
                              checked={!subHidden}
                              disabled={groupHidden}
                              onChange={() => onToggle(s.id)}
                              className="h-3.5 w-3.5 shrink-0 accent-current disabled:opacity-40"
                            />
                            <span className={`truncate ${groupHidden ? "text-muted" : "text-secondary"}`}>{s.label}</span>
                          </label>
                        </li>
                      )
                    })}
                  </ul>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
