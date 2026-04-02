import { useState, useEffect, useCallback, useMemo } from "react"
import { X, Search, ChevronRight, ChevronDown, Check, Loader2, CheckSquare } from "lucide-react"
import { discoverCatalogs, discoverSchemas, discoverTables, searchTables } from "@/lib/api"

const MAX_TABLES = 30

interface TableBrowserDrawerProps {
  open: boolean
  onClose: () => void
  selectedTables: string[]
  onApplyChanges: (added: string[], removed: string[]) => void
}

interface CatalogNode {
  name: string
  comment?: string | null
}

interface SchemaNode {
  name: string
  catalog_name: string
  comment?: string | null
}

interface TableNode {
  full_name: string
  name: string
  comment?: string | null
  table_type?: string | null
}

export function TableBrowserDrawer({
  open,
  onClose,
  selectedTables,
  onApplyChanges,
}: TableBrowserDrawerProps) {
  // Pending selection (local draft — not committed until "Apply")
  const [pending, setPending] = useState<Set<string>>(new Set(selectedTables))

  // Sync pending with selectedTables when drawer opens or selectedTables changes
  useEffect(() => {
    setPending(new Set(selectedTables))
  }, [selectedTables, open])

  // Tree state
  const [catalogs, setCatalogs] = useState<CatalogNode[]>([])
  const [schemas, setSchemas] = useState<Record<string, SchemaNode[]>>({})
  const [tables, setTables] = useState<Record<string, TableNode[]>>({})
  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(new Set())
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState<Record<string, boolean>>({})

  // Search state
  const [searchQuery, setSearchQuery] = useState("")
  const [searchResults, setSearchResults] = useState<TableNode[] | null>(null)
  const [searchGrouped, setSearchGrouped] = useState<Record<string, TableNode[]>>({})
  const [searching, setSearching] = useState(false)

  // Diff from committed selection
  const diff = useMemo(() => {
    const committed = new Set(selectedTables)
    const added = [...pending].filter((t) => !committed.has(t))
    const removed = selectedTables.filter((t) => !pending.has(t))
    return { added, removed, hasChanges: added.length > 0 || removed.length > 0 }
  }, [pending, selectedTables])

  // Load catalogs when drawer opens
  useEffect(() => {
    if (open && catalogs.length === 0) {
      setLoading((l) => ({ ...l, _catalogs: true }))
      discoverCatalogs()
        .then((res) => setCatalogs(res.catalogs || []))
        .catch(() => {})
        .finally(() => setLoading((l) => ({ ...l, _catalogs: false })))
    }
  }, [open, catalogs.length])

  const toggleCatalog = useCallback(
    (catalogName: string) => {
      const next = new Set(expandedCatalogs)
      if (next.has(catalogName)) {
        next.delete(catalogName)
      } else {
        next.add(catalogName)
        if (!schemas[catalogName]) {
          setLoading((l) => ({ ...l, [catalogName]: true }))
          discoverSchemas(catalogName)
            .then((res) => setSchemas((s) => ({ ...s, [catalogName]: res.schemas || [] })))
            .catch(() => {})
            .finally(() => setLoading((l) => ({ ...l, [catalogName]: false })))
        }
      }
      setExpandedCatalogs(next)
    },
    [expandedCatalogs, schemas]
  )

  const toggleSchema = useCallback(
    (schemaKey: string, catalogName: string, schemaName: string) => {
      const next = new Set(expandedSchemas)
      if (next.has(schemaKey)) {
        next.delete(schemaKey)
      } else {
        next.add(schemaKey)
        if (!tables[schemaKey]) {
          setLoading((l) => ({ ...l, [schemaKey]: true }))
          discoverTables(catalogName, schemaName)
            .then((res) => setTables((t) => ({ ...t, [schemaKey]: res.tables || [] })))
            .catch(() => {})
            .finally(() => setLoading((l) => ({ ...l, [schemaKey]: false })))
        }
      }
      setExpandedSchemas(next)
    },
    [expandedSchemas, tables]
  )

  const handleSearch = useCallback(async () => {
    if (!searchQuery.trim()) {
      setSearchResults(null)
      setSearchGrouped({})
      return
    }
    setSearching(true)
    try {
      const keywords = searchQuery.trim().split(/\s+/)
      const res = await searchTables(keywords)
      const tableNodes: TableNode[] = (res.tables || []).map((t) => ({
        full_name: t.full_name,
        name: t.full_name.split(".").pop() || t.full_name,
        comment: t.comment || null,
      }))
      setSearchResults(tableNodes)
      const grouped: Record<string, TableNode[]> = {}
      for (const t of tableNodes) {
        const parts = t.full_name.split(".")
        const group = parts.length >= 2 ? `${parts[0]}.${parts[1]}` : "other"
        if (!grouped[group]) grouped[group] = []
        grouped[group].push(t)
      }
      setSearchGrouped(grouped)
    } catch {
      setSearchResults([])
      setSearchGrouped({})
    } finally {
      setSearching(false)
    }
  }, [searchQuery])

  const clearSearch = useCallback(() => {
    setSearchQuery("")
    setSearchResults(null)
    setSearchGrouped({})
  }, [])

  const isPending = useCallback((fullName: string) => pending.has(fullName), [pending])

  const atLimit = pending.size >= MAX_TABLES

  const toggleTable = useCallback(
    (fullName: string) => {
      setPending((prev) => {
        const next = new Set(prev)
        if (next.has(fullName)) {
          next.delete(fullName)
        } else if (next.size < MAX_TABLES) {
          next.add(fullName)
        }
        return next
      })
    },
    []
  )

  const selectAllInSchema = useCallback(
    (schemaKey: string) => {
      const schemaTables = tables[schemaKey] || []
      setPending((prev) => {
        const next = new Set(prev)
        const allSelected = schemaTables.every((t) => next.has(t.full_name))
        if (allSelected) {
          // Deselect all in schema
          for (const t of schemaTables) next.delete(t.full_name)
        } else {
          // Select all (up to limit)
          for (const t of schemaTables) {
            if (next.size >= MAX_TABLES) break
            next.add(t.full_name)
          }
        }
        return next
      })
    },
    [tables]
  )

  const handleApply = useCallback(() => {
    onApplyChanges(diff.added, diff.removed)
  }, [onApplyChanges, diff])

  if (!open) return null

  return (
    <div className="w-80 flex-shrink-0 border-r border-default bg-surface flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2.5 border-b border-default">
        <span className="text-xs font-semibold text-primary">Table Browser</span>
        <button onClick={onClose} className="text-muted hover:text-primary transition-colors">
          <X className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* Limit warning */}
      {atLimit && (
        <div className="px-3 py-1.5 bg-amber-500/10 text-amber-400 text-[10px] border-b border-default">
          Maximum {MAX_TABLES} tables reached. Remove a table to add more.
        </div>
      )}

      {/* Search bar */}
      <div className="px-3 py-2 border-b border-default">
        <div className="relative">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-muted" />
          <input
            className="w-full text-xs bg-elevated border border-default rounded px-2 py-1.5 pl-7 text-primary placeholder:text-muted focus:outline-none focus:border-accent"
            placeholder="Search tables, columns, comments..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          />
          {searching && (
            <Loader2 className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-muted animate-spin" />
          )}
        </div>
        {searchResults !== null && (
          <button onClick={clearSearch} className="text-[10px] text-accent hover:underline mt-1">
            Clear search — back to browse
          </button>
        )}
      </div>

      {/* Tree / Search results */}
      <div className="flex-1 overflow-y-auto text-xs">
        {searching ? (
          <div className="flex items-center justify-center py-8 text-muted">
            <Loader2 className="w-4 h-4 animate-spin mr-2" /> Searching...
          </div>
        ) : searchResults !== null ? (
          Object.keys(searchGrouped).length === 0 ? (
            <div className="px-3 py-6 text-center text-muted">No tables found</div>
          ) : (
            Object.entries(searchGrouped)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([group, groupTables]) => (
                <div key={group}>
                  <div className="px-3 py-1.5 text-[10px] font-medium text-muted bg-elevated sticky top-0">
                    {group} <span className="text-muted/60">({groupTables.length})</span>
                  </div>
                  {groupTables.map((t) => (
                    <TableRow
                      key={t.full_name}
                      table={t}
                      selected={isPending(t.full_name)}
                      disabled={atLimit && !isPending(t.full_name)}
                      onToggle={() => toggleTable(t.full_name)}
                      indent={1}
                    />
                  ))}
                </div>
              ))
          )
        ) : loading._catalogs ? (
          <div className="flex items-center justify-center py-8 text-muted">
            <Loader2 className="w-4 h-4 animate-spin mr-2" /> Loading catalogs...
          </div>
        ) : (
          catalogs.map((cat) => (
            <div key={cat.name}>
              <button
                onClick={() => toggleCatalog(cat.name)}
                className="flex items-center gap-1.5 w-full px-3 py-1.5 hover:bg-elevated transition-colors text-left"
              >
                {expandedCatalogs.has(cat.name) ? (
                  <ChevronDown className="w-3 h-3 text-muted flex-shrink-0" />
                ) : (
                  <ChevronRight className="w-3 h-3 text-muted flex-shrink-0" />
                )}
                <span className="text-primary font-medium truncate">{cat.name}</span>
              </button>

              {expandedCatalogs.has(cat.name) && (
                <div>
                  {loading[cat.name] ? (
                    <div className="pl-7 py-1.5 text-muted flex items-center gap-1.5">
                      <Loader2 className="w-3 h-3 animate-spin" /> Loading...
                    </div>
                  ) : (
                    (schemas[cat.name] || []).map((sch) => {
                      const schemaKey = `${cat.name}.${sch.name}`
                      const schemaTables = tables[schemaKey] || []
                      const allSelected = schemaTables.length > 0 && schemaTables.every((t) => pending.has(t.full_name))
                      return (
                        <div key={schemaKey}>
                          <div className="flex items-center w-full pl-7 pr-3 py-1.5 hover:bg-elevated transition-colors">
                            <button
                              onClick={() => toggleSchema(schemaKey, cat.name, sch.name)}
                              className="flex items-center gap-1.5 flex-1 min-w-0 text-left"
                            >
                              {expandedSchemas.has(schemaKey) ? (
                                <ChevronDown className="w-3 h-3 text-muted flex-shrink-0" />
                              ) : (
                                <ChevronRight className="w-3 h-3 text-muted flex-shrink-0" />
                              )}
                              <span className="text-secondary truncate">{sch.name}</span>
                            </button>
                            {/* Select all in schema */}
                            {expandedSchemas.has(schemaKey) && schemaTables.length > 0 && (
                              <button
                                onClick={() => selectAllInSchema(schemaKey)}
                                className="text-[10px] text-accent hover:underline flex items-center gap-0.5 flex-shrink-0"
                                title={allSelected ? "Deselect all" : "Select all"}
                              >
                                <CheckSquare className="w-3 h-3" />
                                {allSelected ? "None" : "All"}
                              </button>
                            )}
                          </div>

                          {expandedSchemas.has(schemaKey) && (
                            <div>
                              {loading[schemaKey] ? (
                                <div className="pl-14 py-1.5 text-muted flex items-center gap-1.5">
                                  <Loader2 className="w-3 h-3 animate-spin" /> Loading...
                                </div>
                              ) : (
                                schemaTables.map((t) => (
                                  <TableRow
                                    key={t.full_name}
                                    table={t}
                                    selected={isPending(t.full_name)}
                                    disabled={atLimit && !isPending(t.full_name)}
                                    onToggle={() => toggleTable(t.full_name)}
                                    indent={3}
                                  />
                                ))
                              )}
                            </div>
                          )}
                        </div>
                      )
                    })
                  )}
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* Footer: selected count + apply button */}
      <div className="border-t border-default px-3 py-2 bg-surface">
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-[10px] text-muted">
            {pending.size}/{MAX_TABLES} table{pending.size !== 1 ? "s" : ""}
          </span>
          {diff.hasChanges && (
            <span className="text-[10px] text-accent">
              {diff.added.length > 0 && `+${diff.added.length}`}
              {diff.added.length > 0 && diff.removed.length > 0 && " / "}
              {diff.removed.length > 0 && `-${diff.removed.length}`}
            </span>
          )}
        </div>

        {/* Selected table chips */}
        {pending.size > 0 && (
          <div className="flex flex-wrap gap-1 max-h-20 overflow-y-auto mb-2">
            {[...pending].map((t) => {
              const short = t.split(".").pop() || t
              return (
                <span
                  key={t}
                  className="inline-flex items-center gap-0.5 text-[10px] bg-accent/10 text-accent px-1.5 py-0.5 rounded"
                >
                  {short}
                  <button
                    onClick={() => toggleTable(t)}
                    className="hover:text-red-400 transition-colors"
                  >
                    <X className="w-2.5 h-2.5" />
                  </button>
                </span>
              )
            })}
          </div>
        )}

        {/* Apply button */}
        <button
          onClick={handleApply}
          disabled={!diff.hasChanges}
          className="w-full py-1.5 text-xs font-medium rounded-lg transition-colors disabled:opacity-30 disabled:cursor-not-allowed bg-accent text-white hover:bg-accent/90"
        >
          Apply Changes
        </button>
      </div>
    </div>
  )
}

// ── Table row with checkbox ──────────────────────────────────────

function TableRow({
  table,
  selected,
  disabled,
  onToggle,
  indent,
}: {
  table: TableNode
  selected: boolean
  disabled?: boolean
  onToggle: () => void
  indent: number
}) {
  const pl = indent === 1 ? "pl-4" : indent === 2 ? "pl-8" : "pl-14"
  return (
    <button
      onClick={onToggle}
      disabled={disabled}
      className={`flex items-center gap-2 w-full ${pl} pr-3 py-1.5 hover:bg-elevated transition-colors text-left ${disabled ? "opacity-40 cursor-not-allowed" : ""}`}
    >
      <div
        className={`w-3.5 h-3.5 rounded border flex items-center justify-center flex-shrink-0 ${
          selected
            ? "border-green-400/50 bg-green-500/15"
            : "border-default"
        }`}
      >
        {selected && <Check className="w-2.5 h-2.5 text-green-400" />}
      </div>
      <div className="min-w-0 flex-1">
        <div className="text-primary truncate">
          {table.name}
          {table.table_type && table.table_type !== "MANAGED" && table.table_type !== "TABLE" && (
            <span className="ml-1 text-[9px] text-muted">({table.table_type})</span>
          )}
        </div>
        {table.comment && (
          <div className="text-[10px] text-muted truncate">{table.comment}</div>
        )}
      </div>
    </button>
  )
}
