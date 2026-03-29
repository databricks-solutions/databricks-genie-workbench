/**
 * SpaceList - Org-wide Genie Space listing with IQ scores.
 */
import { useState, useEffect, useCallback } from "react"
import { Star, RefreshCw, Search, LayoutGrid, AlertTriangle, Zap, Plus, ExternalLink, Filter } from "lucide-react"
import { listSpaces, scanSpace, toggleStar } from "@/lib/api"
import { MATURITY_COLORS } from "@/lib/utils"
import type { SpaceListItem, ScanResult } from "@/types"
import { WelcomeHero } from "@/components/WelcomeHero"

interface SpaceListProps {
  onSelectSpace: (spaceId: string, displayName: string, spaceUrl?: string) => void
  onCreateSpace?: () => void
}

export function SpaceList({ onSelectSpace, onCreateSpace }: SpaceListProps) {
  const [spaces, setSpaces] = useState<SpaceListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState("")
  const [starredOnly, setStarredOnly] = useState(false)
  const [scanning, setScanning] = useState<Set<string>>(new Set())
  const [maturityFilter, setMaturityFilter] = useState<Set<string>>(new Set())

  const toggleMaturityFilter = (value: string) => {
    setMaturityFilter(prev => {
      const next = new Set(prev)
      if (next.has(value)) next.delete(value)
      else next.add(value)
      return next
    })
  }

  const filteredSpaces = maturityFilter.size === 0
    ? spaces
    : spaces.filter(s => maturityFilter.has(s.maturity ?? "Unscanned"))

  const loadSpaces = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await listSpaces({ search: search || undefined, starred_only: starredOnly })
      setSpaces(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load spaces")
    } finally {
      setLoading(false)
    }
  }, [search, starredOnly])

  useEffect(() => {
    loadSpaces()
  }, [loadSpaces])

  const handleScan = async (e: React.MouseEvent, spaceId: string) => {
    e.stopPropagation()
    setScanning(prev => new Set(prev).add(spaceId))
    try {
      const result: ScanResult = await scanSpace(spaceId)
      setSpaces(prev => prev.map(s =>
        s.space_id === spaceId
          ? { ...s, score: result.score, maturity: result.maturity, optimization_accuracy: result.optimization_accuracy, last_scanned: result.scanned_at }
          : s
      ))
    } catch (e) {
      console.error("Scan failed:", e)
    } finally {
      setScanning(prev => { const s = new Set(prev); s.delete(spaceId); return s })
    }
  }

  const handleToggleStar = async (e: React.MouseEvent, space: SpaceListItem) => {
    e.stopPropagation()
    const newStarred = !space.is_starred
    setSpaces(prev => prev.map(s => s.space_id === space.space_id ? { ...s, is_starred: newStarred } : s))
    try {
      await toggleStar(space.space_id, newStarred)
    } catch {
      // Revert
      setSpaces(prev => prev.map(s => s.space_id === space.space_id ? { ...s, is_starred: !newStarred } : s))
    }
  }

  return (
    <div className="space-y-6">
      {/* Hero */}
      <WelcomeHero spaces={spaces} loading={loading} onCreateSpace={onCreateSpace} />

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative flex-1 max-w-sm min-w-[200px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search spaces..."
            className="w-full pl-9 pr-3 py-2 rounded-lg border border-default bg-surface text-primary text-sm placeholder:text-muted focus:outline-none focus:ring-2 focus:ring-accent/50"
          />
        </div>
        <button
          onClick={() => setStarredOnly(!starredOnly)}
          className={`flex items-center gap-1.5 px-3 py-2 rounded-lg border text-sm transition-colors ${
            starredOnly
              ? "border-amber-500/50 bg-amber-500/10 text-amber-400"
              : "border-default bg-surface text-muted hover:text-secondary"
          }`}
        >
          <Star className="w-3.5 h-3.5" />
          Starred
        </button>

        {/* Maturity filters */}
        <div className="flex items-center gap-1.5 border-l border-default pl-2.5">
          <Filter className="w-3.5 h-3.5 text-muted" />
          {([
            { value: "Trusted", label: "Trusted", active: "border-emerald-500/50 bg-emerald-500/10 text-emerald-400" },
            { value: "Ready to Optimize", label: "Ready", active: "border-blue-500/50 bg-blue-500/10 text-blue-400" },
            { value: "Not Ready", label: "Not Ready", active: "border-red-500/50 bg-red-500/10 text-red-400" },
            { value: "Unscanned", label: "Unscanned", active: "border-slate-400/50 bg-slate-400/10 text-slate-400" },
          ] as const).map(f => (
            <button
              key={f.value}
              onClick={() => toggleMaturityFilter(f.value)}
              className={`px-2.5 py-1 rounded-md border text-xs font-medium transition-colors ${
                maturityFilter.has(f.value)
                  ? f.active
                  : "border-default bg-surface text-muted hover:text-secondary"
              }`}
            >
              {f.label}
            </button>
          ))}
        </div>

        <button
          onClick={loadSpaces}
          className="p-2 rounded-lg border border-default bg-surface hover:bg-elevated text-muted hover:text-secondary transition-colors ml-auto"
          title="Refresh"
        >
          <RefreshCw className="w-4 h-4" />
        </button>
      </div>

      {/* Content */}
      {loading ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="bg-surface border border-default rounded-xl p-4 animate-pulse">
              <div className="h-4 bg-elevated rounded w-3/4 mb-4" />
              <div className="flex gap-1.5 mb-3">
                <div className="h-5 w-28 bg-elevated rounded-full" />
                <div className="h-5 w-16 bg-elevated rounded-full" />
              </div>
              <div className="h-3 bg-elevated rounded w-1/2" />
            </div>
          ))}
        </div>
      ) : error ? (
        <div className="flex items-center gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-red-400">
          <AlertTriangle className="w-5 h-5 flex-shrink-0" />
          <span>{error}</span>
        </div>
      ) : filteredSpaces.length === 0 ? (
        <div className="text-center py-16 text-muted">
          <LayoutGrid className="w-12 h-12 mx-auto mb-4 opacity-30" />
          <p className="text-lg">{maturityFilter.size > 0 && spaces.length > 0 ? "No spaces match filters" : "No spaces found"}</p>
          {search && <p className="text-sm mt-1">Try a different search term</p>}
          {maturityFilter.size > 0 && spaces.length > 0 && (
            <button
              onClick={() => setMaturityFilter(new Set())}
              className="mt-4 inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border border-default hover:border-accent/40 hover:text-accent text-muted text-sm transition-colors"
            >
              Clear filters
            </button>
          )}
          {onCreateSpace && spaces.length === 0 && (
            <button
              onClick={onCreateSpace}
              className="mt-6 inline-flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-dashed border-default hover:border-accent/40 hover:text-accent text-muted transition-colors"
            >
              <Plus className="w-5 h-5" />
              Create Space
            </button>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 animate-stagger">
          {filteredSpaces.map(space => (
            <div
              key={space.space_id}
              onClick={() => onSelectSpace(space.space_id, space.display_name, space.space_url ?? undefined)}
              className="group bg-surface border border-default rounded-xl hover:border-accent/40 cursor-pointer transition-all duration-200 hover:shadow-md hover:-translate-y-0.5"
            >
              <div className="p-4">
                {/* Name + star */}
                <div className="flex items-center gap-2 mb-3">
                  <h3 className="text-sm font-semibold text-primary truncate flex-1">
                    {space.display_name}
                  </h3>
                  <button
                    onClick={(e) => handleToggleStar(e, space)}
                    className="transition-colors"
                  >
                    <Star className={`w-4 h-4 ${space.is_starred ? "fill-amber-400 text-amber-400" : "text-muted hover:text-amber-400"}`} />
                  </button>
                </div>

                {/* Status pills */}
                <div className="flex flex-wrap items-center gap-1.5 mb-3">
                  {space.score != null ? (
                    <>
                      <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full border ${MATURITY_COLORS[space.maturity ?? ""]?.badge ?? "bg-elevated text-muted border-default"}`}>
                        {space.score}/12 · {space.maturity}
                      </span>
                      <span className={`text-[10px] font-mono px-2 py-0.5 rounded-full border ${
                        space.optimization_accuracy != null
                          ? space.optimization_accuracy >= 0.85
                            ? "border-emerald-500/30 bg-emerald-500/20 text-emerald-400"
                            : space.optimization_accuracy >= 0.61
                              ? "border-amber-500/30 bg-amber-500/20 text-amber-400"
                              : "border-red-500/30 bg-red-500/20 text-red-400"
                          : "border-default bg-elevated text-muted"
                      }`}>
                        {space.optimization_accuracy != null
                          ? `${Math.round(space.optimization_accuracy * 100)}% acc.`
                          : "Not optimized"}
                      </span>
                    </>
                  ) : (
                    <span className="text-[10px] font-medium px-2 py-0.5 rounded-full border border-default bg-elevated text-muted">
                      Unscanned
                    </span>
                  )}
                </div>

                {/* Footer */}
                <div className="pt-3 border-t border-default flex items-center justify-between gap-2">
                  {space.space_url ? (
                    <a
                      href={space.space_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      className="text-xs text-muted font-mono min-w-0 truncate hover:text-accent transition-colors inline-flex items-center gap-1"
                    >
                      {space.space_id}
                      <ExternalLink className="w-3 h-3 flex-shrink-0" />
                    </a>
                  ) : (
                    <span className="text-xs text-muted font-mono min-w-0 truncate">{space.space_id}</span>
                  )}
                  <button
                    onClick={(e) => handleScan(e, space.space_id)}
                    disabled={scanning.has(space.space_id)}
                    className="flex items-center gap-1 text-xs px-2 py-1 rounded-md border border-default hover:border-accent/40 hover:text-accent text-muted transition-colors disabled:opacity-50"
                  >
                    {scanning.has(space.space_id) ? (
                      <RefreshCw className="w-3 h-3 animate-spin" />
                    ) : (
                      <Zap className="w-3 h-3" />
                    )}
                    {scanning.has(space.space_id) ? "Scanning..." : "Scan"}
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
