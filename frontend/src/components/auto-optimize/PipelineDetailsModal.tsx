import { useEffect, useState } from "react"
import { X } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { PipelineStepCard } from "@/components/auto-optimize/PipelineStepCard"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { getAutoOptimizeRun } from "@/lib/api"
import type { GSOPipelineRun } from "@/types"

interface PipelineDetailsModalProps {
  runId: string
  isOpen: boolean
  onClose: () => void
}

const STEP_DESCRIPTIONS: Record<number, { name: string; description: string }> = {
  1: { name: "Preflight",            description: "Reads config and queries Unity Catalog for metadata" },
  2: { name: "Baseline Evaluation",  description: "Runs benchmarks through 9 judges" },
  3: { name: "Proactive Enrichment", description: "Enriches descriptions, joins, instructions" },
  4: { name: "Adaptive Optimization", description: "Applies optimization levers with 3-gate eval" },
  5: { name: "Finalization",         description: "Repeatability checks and model promotion" },
  6: { name: "Deploy",               description: "Deploys optimized config to target" },
}

export function PipelineDetailsModal({ runId, isOpen, onClose }: PipelineDetailsModalProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)

  useEffect(() => {
    if (!isOpen) return
    getAutoOptimizeRun(runId).then(setRun).catch(() => {})
  }, [runId, isOpen])

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="fixed inset-0 bg-black/50 backdrop-blur-sm" onClick={onClose} />

      {/* Modal content */}
      <Card className="relative z-50 w-full max-w-2xl mx-4 max-h-[85vh] overflow-y-auto">
        <CardContent className="p-6">
          {/* Header */}
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-lg font-semibold text-primary">Pipeline Details</h2>
            <button
              onClick={onClose}
              className="p-1.5 rounded-lg hover:bg-elevated text-muted hover:text-primary transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Pipeline steps */}
          {run ? (
            <div className="space-y-3">
              {Array.from({ length: 6 }, (_, i) => {
                const stepNum = i + 1
                const step = run.steps?.find((s) => s.stepNumber === stepNum)
                const meta = STEP_DESCRIPTIONS[stepNum]
                return (
                  <PipelineStepCard
                    key={stepNum}
                    stepNumber={stepNum}
                    name={step?.name ?? meta?.name ?? `Step ${stepNum}`}
                    status={step?.status ?? "pending"}
                    durationSeconds={step?.durationSeconds ?? null}
                    description={meta?.description ?? ""}
                  />
                )
              })}

              {/* Score summary */}
              <div className="pt-4 border-t border-default">
                <ScoreSummary
                  baselineScore={run.baselineScore}
                  optimizedScore={run.optimizedScore}
                />
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted py-4 text-center">Loading pipeline details...</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
