import { Check, Cog, Loader2, X } from "lucide-react"
import type { GSOTerminalReason } from "@/types"
import { buildTaskRail, RAIL_STEP_PREFIXES, type RailNode } from "@/components/auto-optimize/cockpit"

interface TaskRailProps {
  stepsCompleted?: number | null
  currentStepName?: string | null
  status?: string | null
  terminalReason?: GSOTerminalReason | null
  benchmarkUnrepairable?: boolean
  onShowDetails?: () => void
}

// The 4-task rail.
// 00 intake · 01 QC+repair · 02 optimize · 03 publish_and_audit.
// The 01 node branches to a BENCHMARK_UNREPAIRABLE hard-fail chip.
export function TaskRail({
  stepsCompleted,
  currentStepName,
  status,
  terminalReason,
  benchmarkUnrepairable,
  onShowDetails,
}: TaskRailProps) {
  const nodes = buildTaskRail({
    stepsCompleted,
    currentStepName,
    status,
    terminalReason,
    benchmarkUnrepairable,
  })
  const completedCount = nodes.filter((n) => n.state === "completed").length
  // Denominator is derived from the rendered node set (the 4-task DAG) — the
  // "never 6" invariant is local here and can't regress on a stale backend total.
  const total = nodes.length

  return (
    <div className="rounded-xl border border-default bg-surface p-4">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs font-semibold uppercase tracking-widest text-muted">
          Optimization pipeline
        </span>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted">
            {completedCount}/{total} tasks
          </span>
          {onShowDetails && (
            <button
              onClick={onShowDetails}
              className="rounded-lg border border-default p-1.5 text-muted transition-colors hover:bg-elevated hover:text-primary"
              title="Pipeline details"
            >
              <Cog className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      <ol className="flex items-stretch gap-1">
        {nodes.map((node, i) => (
          <RailStep key={node.stepNumber} node={node} isLast={i === nodes.length - 1} />
        ))}
      </ol>
    </div>
  )
}

function RailStep({ node, isLast }: { node: RailNode; isLast: boolean }) {
  const prefix = RAIL_STEP_PREFIXES[node.stepNumber] ?? ""

  const dot =
    node.state === "completed" ? (
      <span className="flex h-6 w-6 items-center justify-center rounded-full bg-emerald-500 text-white">
        <Check className="h-3.5 w-3.5" />
      </span>
    ) : node.state === "failed" ? (
      <span className="flex h-6 w-6 items-center justify-center rounded-full bg-danger text-white">
        <X className="h-3.5 w-3.5" />
      </span>
    ) : node.state === "current" ? (
      <span className="flex h-6 w-6 items-center justify-center rounded-full bg-accent text-white">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
      </span>
    ) : (
      <span className="flex h-6 w-6 items-center justify-center rounded-full border border-default bg-elevated text-[10px] font-semibold text-muted">
        {node.stepNumber}
      </span>
    )

  const nameColor =
    node.state === "completed"
      ? "text-primary"
      : node.state === "failed"
        ? "text-danger"
        : node.state === "current"
          ? "text-accent"
          : "text-muted"

  return (
    <li className="flex flex-1 flex-col gap-1.5">
      <div className="flex items-center gap-1.5">
        {dot}
        {!isLast && (
          <span
            className={`h-0.5 flex-1 rounded-full ${
              node.state === "completed" ? "bg-emerald-500" : "bg-elevated"
            }`}
          />
        )}
      </div>
      <div className="min-w-0 pr-1">
        <p className="truncate text-[11px] font-medium leading-tight">
          <span className="text-muted">{prefix && `${prefix} · `}</span>
          <span className={nameColor}>{node.name}</span>
        </p>
        {node.chip && (
          <span className="mt-1 inline-flex items-center rounded-full bg-danger/15 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wide text-danger">
            {node.chip}
          </span>
        )}
      </div>
    </li>
  )
}
