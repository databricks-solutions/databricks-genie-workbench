/**
 * SpaceList - Org-wide Genie Space listing with IQ scores.
 */
import { useState, useEffect, useCallback } from "react"
import { Star, RefreshCw, Search, LayoutGrid, AlertTriangle, Zap, Plus, ExternalLink } from "lucide-react"
import { listSpaces, scanSpace, toggleStar } from "@/lib/api"
import { getScoreHex, getOptimizationLabel } from "@/lib/utils"
import type { SpaceListItem, ScanResult } from "@/types"

interface SpaceListProps {
  onSelectSpace: (spaceId: string, displayName: string, spaceUrl?: string) => void
  onCreateSpace?: () => void
}

const CIRCLE_LABELS: Record<string, string> = {
  "Not Ready": "NOT\nREADY",
  "Ready to Optimize": "READY",
  "Trusted": "TRUSTED",
}

function StatusCircle({ maturity }: { maturity: string | null }) {
  if (!maturity) {
    return (
      <div className="w-14 h-14 rounded-full border-2 border-default flex items-center justify-center">
        <span className="text-xs text-muted">—</span>
      </div>
    )
  }

  const color = getScoreHex(maturity)
  const label = CIRCLE_LABELS[maturity] ?? maturity.toUpperCase()

  return (
    <div className="w-14 h-14 rounded-full border-2 flex items-center justify-center" style={{ borderColor: color }}>
      <span className="text-[9px] font-bold text-center leading-tight whitespace-pre-line" style={{ color }}>
        {label}
      </span>
    </div>
  )
}

export function SpaceList({ onSelectSpace, onCreateSpace }: SpaceListProps) {
  const [spaces, setSpaces] = useState<SpaceListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState("")
  const [starredOnly, setStarredOnly] = useState(false)
  const [scanning, setScanning] = useState<Set<string>>(new Set())

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
    } catch (e) {
      // Revert
      setSpaces(prev => prev.map(s => s.space_id === space.space_id ? { ...s, is_starred: !newStarred } : s))
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-primary">Genie Spaces</h2>
          <p className="text-muted mt-1">{spaces.length} spaces{starredOnly ? " (starred)" : ""}</p>
        </div>
        <button
          onClick={loadSpaces}
          className="flex items-center gap-2 px-3 py-2 rounded-lg border border-default bg-surface hover:bg-surface-secondary text-sm text-secondary transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
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
          className={`flex items-center gap-2 px-3 py-2 rounded-lg border text-sm transition-colors ${
            starredOnly
              ? "border-amber-500/50 bg-amber-500/10 text-amber-400"
              : "border-default bg-surface text-muted hover:text-secondary"
          }`}
        >
          <Star className="w-4 h-4" />
          Starred
        </button>
      </div>

      {/* Content */}
      {loading ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="bg-surface border border-default rounded-xl p-4 animate-pulse">
              <div className="h-4 bg-surface-secondary rounded w-3/4 mb-3" />
              <div className="h-3 bg-surface-secondary rounded w-1/2" />
            </div>
          ))}
        </div>
      ) : error ? (
        <div className="flex items-center gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-red-400">
          <AlertTriangle className="w-5 h-5 flex-shrink-0" />
          <span>{error}</span>
        </div>
      ) : spaces.length === 0 ? (
        <div className="text-center py-16 text-muted">
          <LayoutGrid className="w-12 h-12 mx-auto mb-4 opacity-30" />
          <p className="text-lg">No spaces found</p>
          {search && <p className="text-sm mt-1">Try a different search term</p>}
          {onCreateSpace && (
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
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {onCreateSpace && (
            <button
              onClick={onCreateSpace}
              className="group bg-surface border-2 border-dashed border-default rounded-xl p-4 hover:border-accent/40 hover:bg-surface-secondary/50 cursor-pointer transition-all flex flex-col items-center justify-center gap-3 min-h-[140px]"
            >
              <div className="w-14 h-14 rounded-full border-2 border-default group-hover:border-accent/40 flex items-center justify-center transition-colors">
                <Plus className="w-6 h-6 text-muted group-hover:text-accent transition-colors" />
              </div>
              <span className="text-sm font-semibold text-muted group-hover:text-accent transition-colors">Create Space</span>
            </button>
          )}
          {spaces.map(space => (
            <div
              key={space.space_id}
              onClick={() => onSelectSpace(space.space_id, space.display_name, space.space_url ?? undefined)}
              className="group bg-surface border border-default rounded-xl p-4 hover:border-accent/40 hover:bg-surface-secondary/50 cursor-pointer transition-all"
            >
              <div className="flex items-start gap-3">
                <StatusCircle maturity={space.maturity} />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <h3 className="text-sm font-semibold text-primary truncate flex-1">
                      {space.display_name}
                    </h3>
                    <button
                      onClick={(e) => handleToggleStar(e, space)}
                      className={`transition-opacity ${space.is_starred ? "opacity-100" : "opacity-0 group-hover:opacity-100"}`}
                    >
                      <Star className={`w-4 h-4 ${space.is_starred ? "fill-amber-400 text-amber-400" : "text-muted hover:text-amber-400"}`} />
                    </button>
                  </div>
                  <p className="text-xs text-muted mt-1.5">
                    {space.score != null ? (
                      <>
                        {space.score}/15 checks passed
                        {" · "}
                        {getOptimizationLabel(space.optimization_accuracy)}
                      </>
                    ) : (
                      "Not scanned"
                    )}
                  </p>
                </div>
              </div>
              <div className="mt-3 pt-3 border-t border-default flex items-center justify-between">
                {space.space_url ? (
                  <a
                    href={space.space_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    onClick={(e) => e.stopPropagation()}
                    className="text-xs text-muted font-mono break-all hover:text-accent transition-colors inline-flex items-center gap-1"
                  >
                    {space.space_id}
                    <ExternalLink className="w-3 h-3 flex-shrink-0" />
                  </a>
                ) : (
                  <span className="text-xs text-muted font-mono break-all">{space.space_id}</span>
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
          ))}
        </div>
      )}
    </div>
  )
}
