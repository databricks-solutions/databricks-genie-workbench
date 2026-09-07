/**
 * Ontology Map v2 (MV-D75) — search-to-focus box. Presentational + controlled: it owns no
 * cytoscape state, it just emits the query string; the map centers/selects the first
 * matching node. Zero jargon (MV-D23).
 */
import { Search, X } from "lucide-react"

export function GraphSearch({
  value,
  onChange,
  onSubmit,
  onClear,
  hint,
}: {
  value: string
  onChange: (next: string) => void
  onSubmit: () => void
  onClear: () => void
  hint?: string | null
}) {
  return (
    <div className="relative">
      <form
        onSubmit={(e) => {
          e.preventDefault()
          onSubmit()
        }}
        className="flex items-center gap-1.5 rounded-lg border border-default bg-elevated px-2.5 py-1"
      >
        <Search className="h-3.5 w-3.5 shrink-0 text-muted" />
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="Find a table, area or metric…"
          aria-label="Search the estate map"
          className="w-48 bg-transparent text-xs text-primary placeholder:text-muted focus:outline-none"
        />
        {value && (
          <button
            type="button"
            onClick={onClear}
            aria-label="Clear search"
            className="shrink-0 text-muted hover:text-primary"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </form>
      {hint && <p className="absolute left-1 top-full mt-1 text-[11px] text-muted">{hint}</p>}
    </div>
  )
}
