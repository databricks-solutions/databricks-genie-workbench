import { useEffect, useState, useRef } from "react"
import { ExternalLink, Info } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { OptimizationConfig } from "@/components/auto-optimize/OptimizationConfig"
import { RunHistoryTable } from "@/components/auto-optimize/RunHistoryTable"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { RunDetailView } from "@/components/auto-optimize/RunDetailView"
import {
  getAutoOptimizeHealth,
  getAutoOptimizeStatus,
  getAutoOptimizeRunsForSpace,
} from "@/lib/api"
import type { GSORunStatus } from "@/types"

interface AutoOptimizeTabProps {
  spaceId: string
}

type View = "configure" | "monitoring" | "detail"

const TERMINAL_STATUSES = new Set([
  "CONVERGED",
  "STALLED",
  "MAX_ITERATIONS",
  "FAILED",
  "CANCELLED",
  "APPLIED",
  "DISCARDED",
])

const ACTIVE_STATUSES = new Set(["QUEUED", "IN_PROGRESS", "RUNNING"])

const STATUS_VARIANT: Record<string, "default" | "success" | "warning" | "danger" | "info" | "secondary"> = {
  CONVERGED: "success",
  APPLIED: "success",
  STALLED: "warning",
  MAX_ITERATIONS: "warning",
  FAILED: "danger",
  CANCELLED: "secondary",
  DISCARDED: "secondary",
  IN_PROGRESS: "info",
  RUNNING: "info",
  QUEUED: "secondary",
}

export function AutoOptimizeTab({ spaceId }: AutoOptimizeTabProps) {
  const [configured, setConfigured] = useState<boolean | null>(null)
  const [view, setView] = useState<View>("configure")
  const [activeRunId, setActiveRunId] = useState<string | null>(null)
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [runStatus, setRunStatus] = useState<GSORunStatus | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Health check on mount
  useEffect(() => {
    getAutoOptimizeHealth()
      .then((res) => setConfigured(res.configured))
      .catch(() => setConfigured(false))
  }, [])

  // Check for active runs on mount
  useEffect(() => {
    if (configured !== true) return
    getAutoOptimizeRunsForSpace(spaceId).then((runs) => {
      const active = runs.find((r) => ACTIVE_STATUSES.has(r.status))
      if (active) {
        setActiveRunId(active.run_id)
        setView("monitoring")
      }
    })
  }, [spaceId, configured])

  // Polling for active run status
  useEffect(() => {
    if (view !== "monitoring" || !activeRunId) return

    function poll() {
      getAutoOptimizeStatus(activeRunId!)
        .then((status) => {
          setRunStatus(status)
          if (TERMINAL_STATUSES.has(status.status)) {
            if (intervalRef.current) {
              clearInterval(intervalRef.current)
              intervalRef.current = null
            }
          }
        })
        .catch(() => {})
    }

    poll()
    intervalRef.current = setInterval(poll, 5000)

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }
  }, [view, activeRunId])

  // Loading state
  if (configured === null) {
    return <div className="py-8 text-center text-muted text-sm">Loading...</div>
  }

  // Not configured
  if (!configured) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Info className="w-10 h-10 text-muted mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-primary mb-2">Auto-Optimize is not configured</h3>
          <p className="text-muted text-sm">
            Contact your administrator to set GSO_CATALOG and GSO_JOB_ID for this deployment.
          </p>
        </CardContent>
      </Card>
    )
  }

  // Configure view
  if (view === "configure") {
    return (
      <div className="space-y-6">
        <OptimizationConfig
          spaceId={spaceId}
          hasActiveRun={!!activeRunId}
          onStarted={(runId) => {
            setActiveRunId(runId)
            setView("monitoring")
          }}
        />
        <RunHistoryTable
          spaceId={spaceId}
          onSelectRun={(runId) => {
            setSelectedRunId(runId)
            setView("detail")
          }}
        />
      </div>
    )
  }

  // Monitoring view
  if (view === "monitoring" && activeRunId) {
    const isTerminal = runStatus ? TERMINAL_STATUSES.has(runStatus.status) : false

    return (
      <div className="space-y-4">
        <button
          onClick={() => {
            setView("configure")
            if (isTerminal) setActiveRunId(null)
          }}
          className="text-sm text-accent hover:underline"
        >
          &larr; Back to configuration
        </button>

        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Optimization in Progress</CardTitle>
              {runStatus && (
                <Badge variant={STATUS_VARIANT[runStatus.status] ?? "secondary"}>
                  {runStatus.status}
                </Badge>
              )}
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {runStatus && (
              <ScoreSummary
                baselineScore={runStatus.baselineScore}
                optimizedScore={runStatus.optimizedScore}
              />
            )}

            {runStatus?.convergenceReason && (
              <p className="text-sm text-muted">
                Reason: {runStatus.convergenceReason}
              </p>
            )}

            {!isTerminal && (
              <p className="text-xs text-muted animate-pulse">Polling every 5 seconds...</p>
            )}

            <div className="flex gap-3">
              <button
                onClick={() => {
                  setSelectedRunId(activeRunId)
                  setView("detail")
                }}
                className="flex items-center gap-1.5 text-sm text-accent hover:underline"
              >
                <ExternalLink className="w-3.5 h-3.5" />
                View Details
              </button>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  // Detail view — placeholder for Layer 2
  if (view === "detail" && selectedRunId) {
    return (
      <div className="space-y-4">
        <RunDetailView runId={selectedRunId} onBack={() => setView("configure")} />
      </div>
    )
  }

  return null
}
