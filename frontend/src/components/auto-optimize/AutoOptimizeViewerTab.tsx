/**
 * The Optimize tab below Can Edit: the run list and the active-run status (both Can View).
 * The editor tab never mounts for a viewer, so its Edit-level requests cannot fire.
 */
import { useCallback, useEffect, useState } from "react"
import { Info, Lock } from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { getActiveRunForSpace, getAutoOptimizeHealth, getAutoOptimizeRunsForSpace } from "@/lib/api"
import { OPTIMIZE_VIEWER_NOTE } from "@/lib/space-access"
import { BenchmarkPolicyCell, RunMetadataCell, STATUS_VARIANT } from "@/components/auto-optimize/RunHistoryTable"
import { championAccuracyText, humanizeTerminalReason } from "@/components/auto-optimize/runHistory"
import type { GSORunSummary } from "@/types"

interface ViewProps {
  configured: boolean | null
  activeRunId: string | null
  runs: GSORunSummary[]
  loading: boolean
}

export function AutoOptimizeViewerView({ configured, activeRunId, runs, loading }: ViewProps) {
  if (configured === null) {
    return <div className="py-8 text-center text-muted text-sm">Loading...</div>
  }
  if (!configured) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Info className="w-10 h-10 text-muted mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-primary mb-2">Optimize is not configured</h3>
          <p className="text-muted text-sm">
            Contact your administrator to set GSO_CATALOG and GSO_JOB_ID for this deployment.
          </p>
        </CardContent>
      </Card>
    )
  }
  return (
    <div className="space-y-6">
      {activeRunId && (
        <Card className="border-blue-500/30 bg-blue-500/5">
          <CardContent className="py-4">
            <h3 className="text-sm font-semibold text-primary mb-1">Optimization in progress</h3>
            <p className="text-xs text-muted">An optimization run is currently running for this agent.</p>
          </CardContent>
        </Card>
      )}
      <Card>
        <CardHeader>
          <CardTitle>Optimization History</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="mb-3 flex items-center gap-2 text-xs text-muted">
            <Lock className="w-3.5 h-3.5 shrink-0" />
            {OPTIMIZE_VIEWER_NOTE}
          </p>
          {loading ? (
            <p className="text-muted text-sm py-4">Loading...</p>
          ) : runs.length === 0 ? (
            <p className="text-muted text-sm py-4">No optimization runs yet.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Run</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Outcome</TableHead>
                  <TableHead>Champion accuracy</TableHead>
                  <TableHead>Benchmark handling</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {runs.map((run) => (
                  <TableRow key={run.run_id}>
                    <TableCell className="align-top"><RunMetadataCell run={run} /></TableCell>
                    <TableCell className="align-top">
                      <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>{run.status}</Badge>
                    </TableCell>
                    <TableCell
                      className="max-w-[14rem] truncate align-top text-sm text-muted"
                      title={humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                    >
                      {humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                    </TableCell>
                    <TableCell className="align-top text-sm">{championAccuracyText(run.best_accuracy)}</TableCell>
                    <TableCell className="align-top"><BenchmarkPolicyCell run={run} /></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export function AutoOptimizeViewerTab({ spaceId }: { spaceId: string }) {
  const [configured, setConfigured] = useState<boolean | null>(null)
  const [activeRunId, setActiveRunId] = useState<string | null>(null)
  const [runs, setRuns] = useState<GSORunSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let live = true
    getAutoOptimizeHealth()
      .then((res) => { if (live) setConfigured(res.configured) })
      .catch(() => { if (live) setConfigured(false) })
    return () => { live = false }
  }, [])

  const loadSpaceReads = useCallback(async (isLive: () => boolean) => {
    setLoading(true)
    setActiveRunId(null)
    setRuns([])
    try {
      const [active, runList] = await Promise.all([
        getActiveRunForSpace(spaceId)
          .then((res) => (res.hasActiveRun ? res.activeRunId : null))
          .catch(() => null),
        getAutoOptimizeRunsForSpace(spaceId).catch(() => [] as GSORunSummary[]),
      ])
      if (!isLive()) return
      setActiveRunId(active)
      setRuns(runList)
    } finally {
      if (isLive()) setLoading(false)
    }
  }, [spaceId])

  useEffect(() => {
    if (configured !== true) return
    let live = true
    void loadSpaceReads(() => live)
    return () => { live = false }
  }, [configured, loadSpaceReads])

  return <AutoOptimizeViewerView configured={configured} activeRunId={activeRunId} runs={runs} loading={loading} />
}
