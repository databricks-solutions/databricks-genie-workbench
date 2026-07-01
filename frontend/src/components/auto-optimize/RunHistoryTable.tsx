import { useEffect, useState } from "react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table"
import { getAutoOptimizeRunsForSpace } from "@/lib/api"
import { championAccuracyText, humanizeTerminalReason } from "@/components/auto-optimize/runHistory"
import type { GSORunSummary } from "@/types"

interface RunHistoryTableProps {
  spaceId: string
  onSelectRun: (runId: string) => void
}

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

export function RunHistoryTable({ spaceId, onSelectRun }: RunHistoryTableProps) {
  const [runs, setRuns] = useState<GSORunSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getAutoOptimizeRunsForSpace(spaceId)
      .then(setRuns)
      .catch(() => setRuns([]))
      .finally(() => setLoading(false))
  }, [spaceId])

  return (
    <Card>
      <CardHeader>
        <CardTitle>Optimization History</CardTitle>
      </CardHeader>
      <CardContent>
        {loading ? (
          <p className="text-muted text-sm py-4">Loading...</p>
        ) : runs.length === 0 ? (
          <p className="text-muted text-sm py-4">No optimization runs yet.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Outcome</TableHead>
                <TableHead>Champion accuracy</TableHead>
                <TableHead>Model</TableHead>
                <TableHead>Triggered By</TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => (
                <TableRow key={run.run_id}>
                  <TableCell className="text-sm">
                    {run.started_at
                      ? new Date(run.started_at).toLocaleDateString(undefined, {
                          month: "short",
                          day: "numeric",
                          year: "numeric",
                          hour: "2-digit",
                          minute: "2-digit",
                        })
                      : "—"}
                  </TableCell>
                  <TableCell>
                    <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>
                      {run.status}
                    </Badge>
                  </TableCell>
                  <TableCell
                    className="text-sm text-muted max-w-[14rem] truncate"
                    title={humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                  >
                    {humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                  </TableCell>
                  <TableCell className="text-sm">
                    {championAccuracyText(run.best_accuracy)}
                  </TableCell>
                  <TableCell className="text-sm text-muted max-w-[12rem] truncate" title={run.llm_model ?? undefined}>
                    {run.llm_model ?? "—"}
                  </TableCell>
                  <TableCell className="text-sm text-muted">
                    {run.triggered_by ?? "—"}
                  </TableCell>
                  <TableCell>
                    <button
                      onClick={() => onSelectRun(run.run_id)}
                      className="text-sm text-accent hover:underline"
                    >
                      View Details
                    </button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  )
}
