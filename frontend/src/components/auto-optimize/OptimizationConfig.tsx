import { useState } from "react"
import { Rocket } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { triggerAutoOptimize } from "@/lib/api"

interface OptimizationConfigProps {
  spaceId: string
  onStarted: (runId: string) => void
  hasActiveRun: boolean
}

const LEVERS = [
  { id: 1, name: "Tables & Columns", description: "Update table descriptions, column descriptions, and synonyms" },
  { id: 2, name: "Metric Views", description: "Update metric view column descriptions" },
  { id: 3, name: "Table-Valued Functions", description: "Remove underperforming TVFs" },
  { id: 4, name: "Join Specifications", description: "Add, update, or remove join relationships" },
  { id: 5, name: "Genie Space Instructions", description: "Rewrite global routing instructions" },
]

export function OptimizationConfig({ spaceId, onStarted, hasActiveRun }: OptimizationConfigProps) {
  const [selectedLevers, setSelectedLevers] = useState<Set<number>>(new Set(LEVERS.map((l) => l.id)))
  const [applyMode] = useState<"genie_config" | "both">("genie_config")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function toggleLever(id: number) {
    setSelectedLevers((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  async function handleStart() {
    setLoading(true)
    setError(null)
    try {
      const result = await triggerAutoOptimize({
        space_id: spaceId,
        apply_mode: applyMode,
        levers: Array.from(selectedLevers).sort(),
      })
      onStarted(result.runId)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to start optimization")
    } finally {
      setLoading(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Optimization Configuration</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Apply Mode */}
        <div>
          <h4 className="text-sm font-medium text-primary mb-3">Apply Mode</h4>
          <div className="flex gap-3">
            <button
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-accent/10 text-accent border border-accent/30"
            >
              Config Only
            </button>
            <button
              disabled
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-elevated text-muted border border-default cursor-not-allowed flex items-center gap-2"
            >
              Config + UC Write Backs
              <Badge variant="secondary">Coming soon</Badge>
            </button>
          </div>
        </div>

        {/* Lever Selection */}
        <div>
          <h4 className="text-sm font-medium text-primary mb-3">Optimization Levers</h4>
          <div className="space-y-3">
            {LEVERS.map((lever) => (
              <label key={lever.id} className="flex items-start gap-3 cursor-pointer">
                <Checkbox
                  checked={selectedLevers.has(lever.id)}
                  onCheckedChange={() => toggleLever(lever.id)}
                  className="mt-0.5"
                />
                <div>
                  <span className="text-sm font-medium text-primary">{lever.name}</span>
                  <p className="text-xs text-muted">{lever.description}</p>
                </div>
              </label>
            ))}
          </div>
        </div>

        {/* Error */}
        {error && (
          <div className="rounded-lg bg-danger/10 border border-danger/20 px-4 py-3 text-sm text-danger">
            {error}
          </div>
        )}

        {/* Start Button */}
        <button
          onClick={handleStart}
          disabled={loading || hasActiveRun || selectedLevers.size === 0}
          className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-accent text-white font-semibold hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          <Rocket className="w-4 h-4" />
          {loading ? "Starting..." : "Start Optimization"}
        </button>
      </CardContent>
    </Card>
  )
}
