import { useState } from "react"
import { Badge } from "@/components/ui/badge"
import { ChevronDown, Wrench, CheckCircle2, XCircle, GitBranch } from "lucide-react"
import type { GSOLeverStatus, GSOPatchDetail, GSOLeverIteration } from "@/types"

interface OptimizationLeversProps {
  levers: GSOLeverStatus[]
}

const STATUS_BADGE: Record<string, { variant: "success" | "danger" | "warning" | "secondary" | "info"; label: string }> = {
  accepted: { variant: "success", label: "Accepted" },
  rolled_back: { variant: "danger", label: "Rolled Back" },
  failed: { variant: "danger", label: "Failed" },
  skipped: { variant: "secondary", label: "Skipped" },
  running: { variant: "info", label: "Running" },
  evaluating: { variant: "info", label: "Evaluating" },
  pending: { variant: "secondary", label: "Pending" },
}

function PatchCard({ patch }: { patch: GSOPatchDetail }) {
  const commandStr = patch.command
    ? typeof patch.command === "string"
      ? patch.command
      : JSON.stringify(patch.command)
    : null

  return (
    <div className="rounded-lg border border-default bg-surface p-3 space-y-1.5">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="inline-flex items-center rounded-md border border-accent/30 bg-accent/10 px-2 py-0.5 text-xs font-mono font-medium text-accent">
          {patch.patchType}
        </span>
        <span className="text-xs text-muted">scope: {patch.scope}</span>
        <span className="text-xs text-muted">risk: {patch.riskLevel}</span>
        {patch.rolledBack && (
          <Badge variant="danger" className="text-[10px] py-0 px-1.5">rolled back</Badge>
        )}
      </div>
      {patch.targetObject && (
        <p className="text-xs text-muted font-mono">target: {patch.targetObject}</p>
      )}
      {commandStr && (
        <div className="relative">
          <p className="text-xs font-mono text-muted/80 bg-elevated/50 rounded px-2 py-1.5 overflow-hidden max-h-16 line-clamp-3">
            {commandStr}
          </p>
          {commandStr.length > 200 && (
            <div className="mt-1 h-1.5 rounded-full bg-elevated overflow-hidden">
              <div
                className="h-full rounded-full bg-accent/60"
                style={{ width: `${Math.min(100, (200 / commandStr.length) * 100)}%` }}
              />
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function IterationRow({ iteration }: { iteration: GSOLeverIteration }) {
  const badge = STATUS_BADGE[iteration.status] ?? STATUS_BADGE.pending
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="inline-flex items-center rounded-md border border-default bg-elevated px-2 py-0.5 font-medium text-primary">
        Iteration {iteration.iteration}
      </span>
      <Badge variant={badge.variant} className="text-[10px] py-0 px-1.5">
        {badge.variant === "success" && <CheckCircle2 className="h-2.5 w-2.5 mr-0.5" />}
        {badge.label}
      </Badge>
      <span className="text-muted tabular-nums">{iteration.patchCount} patches</span>
    </div>
  )
}

function LeverCard({ lever }: { lever: GSOLeverStatus }) {
  const [open, setOpen] = useState(lever.status === "accepted")
  const [showProvenance, setShowProvenance] = useState(false)
  const badge = STATUS_BADGE[lever.status] ?? STATUS_BADGE.pending

  return (
    <div className="rounded-xl border border-default bg-surface overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex w-full items-center gap-3 px-4 py-3 text-left cursor-pointer hover:bg-elevated/30 transition-colors"
      >
        <span className="flex h-6 w-6 items-center justify-center rounded-full bg-elevated text-xs font-bold text-muted shrink-0">
          {lever.lever}
        </span>
        <span className="text-sm font-semibold text-primary flex-1">{lever.name}</span>
        <Badge variant={badge.variant} className="text-[10px]">
          {badge.variant === "success" && <CheckCircle2 className="h-3 w-3 mr-0.5" />}
          {badge.variant === "danger" && <XCircle className="h-3 w-3 mr-0.5" />}
          {badge.label}
        </Badge>
        <span className="text-xs text-muted tabular-nums">{lever.patchCount} patches</span>
        <ChevronDown className={`h-4 w-4 text-muted transition-transform duration-200 ${open ? "rotate-180" : ""}`} />
      </button>

      {open && (
        <div className="border-t border-dashed border-default px-4 py-3 space-y-3">
          {/* Patches — show top-level, or fall back to iteration-level patches */}
          {(() => {
            const allPatches = lever.patches.length > 0
              ? lever.patches
              : lever.iterations.flatMap((it) => it.patches ?? [])
            if (allPatches.length === 0) return null
            return (
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted">Changes</p>
                <div className="space-y-2">
                  {allPatches.map((patch, i) => (
                    <PatchCard key={i} patch={patch} />
                  ))}
                </div>
              </div>
            )
          })()}

          {/* Iteration history */}
          {lever.iterations.length > 0 && (
            <div className="space-y-1.5">
              <p className="text-xs font-medium text-muted">Iteration history</p>
              <div className="space-y-1">
                {lever.iterations.map((it) => (
                  <IterationRow key={it.iteration} iteration={it} />
                ))}
              </div>
            </div>
          )}

          {/* Provenance toggle */}
          <button
            type="button"
            onClick={() => setShowProvenance(!showProvenance)}
            className="flex items-center gap-1 text-xs text-muted hover:text-primary transition-colors"
          >
            <GitBranch className="h-3 w-3" />
            Provenance
            <ChevronDown className={`h-3 w-3 transition-transform duration-200 ${showProvenance ? "rotate-180" : ""}`} />
          </button>
          {showProvenance && lever.iterations.length > 0 && (
            <div className="text-xs text-muted bg-elevated/30 rounded-lg p-3 space-y-1">
              {lever.iterations.map((it) => (
                <div key={it.iteration}>
                  <span className="font-medium">Iteration {it.iteration}:</span>{" "}
                  {it.patchTypes.join(", ") || "—"}
                  {it.scoreDelta != null && (
                    <span className={it.scoreDelta > 0 ? "text-emerald-600 ml-1" : "text-red-500 ml-1"}>
                      ({it.scoreDelta > 0 ? "+" : ""}{it.scoreDelta.toFixed(1)}%)
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export function OptimizationLevers({ levers }: OptimizationLeversProps) {
  if (levers.length === 0) return null

  const acceptedCount = levers.filter((l) => l.status === "accepted").length
  const totalCount = levers.length

  return (
    <div className="mt-3 space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5 text-xs font-semibold text-muted uppercase tracking-wider">
          <Wrench className="h-3.5 w-3.5" />
          Optimization Levers
        </div>
        <span className="text-xs text-muted tabular-nums">
          {acceptedCount}/{totalCount} accepted
        </span>
      </div>
      <div className="space-y-2">
        {levers.map((lever) => (
          <LeverCard key={lever.lever} lever={lever} />
        ))}
      </div>
    </div>
  )
}
