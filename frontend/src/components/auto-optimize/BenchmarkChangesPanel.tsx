import { useEffect, useState } from "react"
import { PlusCircle, MinusCircle, RefreshCw, AlertTriangle, Wrench, Target, CheckCircle2 } from "lucide-react"
import { getAutoOptimizeBenchmarkChanges } from "@/lib/api"
import type { GSOBenchmarkChanges, GSOBenchmarkMutation, GSOBenchmarkQC } from "@/types"

interface BenchmarkChangesPanelProps {
  runId: string
  /**
   * Pre-fetched benchmark changes. When provided the panel skips its own fetch
   * (the live cockpit already polls ``/benchmark-changes`` for the rail chip).
   */
  changes?: GSOBenchmarkChanges | null
}

// The documented working-set window (D8 / §3.5): 30–40 questions.
const DEFAULT_WINDOW_MIN = 30
const DEFAULT_WINDOW_MAX = 40

/**
 * GSO v2 Phase 6 (§3.5) + Phase 13 (item 3) — surfaces the benchmark provenance
 * ledger (questions GSO added / removed / changed in the user's live Genie
 * Space) AND the 01_benchmark_qc_and_repair QC meta: the 30–40 working-set
 * window meter + the bounded repair-tries indicator. Promoted out of the buried
 * PipelineDetailsModal tab to a first-class surface under task 01.
 */
export function BenchmarkChangesPanel({ runId, changes: provided }: BenchmarkChangesPanelProps) {
  const [fetched, setFetched] = useState<GSOBenchmarkChanges | null>(null)
  const [loading, setLoading] = useState(provided === undefined)

  useEffect(() => {
    if (provided !== undefined) return
    let active = true
    getAutoOptimizeBenchmarkChanges(runId)
      .then((c) => {
        if (active) {
          setFetched(c)
          setLoading(false)
        }
      })
      .catch(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
  }, [runId, provided])

  const changes = provided !== undefined ? provided : fetched
  const qc = changes?.qc ?? null
  const total = changes?.counts.total ?? 0

  if (loading) {
    return (
      <PanelShell>
        <p className="text-sm text-muted animate-pulse text-center py-6">Loading benchmark changes…</p>
      </PanelShell>
    )
  }

  if (!qc && total === 0) {
    return (
      <PanelShell>
        <p className="text-sm text-muted text-center py-6">
          GSO made no changes to this space's benchmark set.
        </p>
      </PanelShell>
    )
  }

  return (
    <PanelShell counts={changes?.counts}>
      {qc && <QcMeter qc={qc} />}

      {total > 0 ? (
        <>
          <p className="text-xs text-muted">
            GSO pushes its EXPLAIN-validated benchmark questions into your live Genie Space (additive,
            merge-only) and recommends pruning invalid ones. Discarding the run reverts these from the
            intake snapshot.
          </p>

          <ChangeGroup
            title="Added"
            icon={<PlusCircle className="h-4 w-4 text-emerald-500" />}
            mutations={changes?.added ?? []}
          />
          <ChangeGroup
            title="Changed"
            icon={<RefreshCw className="h-4 w-4 text-blue-400" />}
            mutations={changes?.changed ?? []}
          />
          <ChangeGroup
            title="Removed"
            icon={<MinusCircle className="h-4 w-4 text-red-400" />}
            mutations={changes?.removed ?? []}
          />
          <ChangeGroup
            title="Prune recommended"
            icon={<AlertTriangle className="h-4 w-4 text-amber-500" />}
            mutations={changes?.pruneRecommended ?? []}
          />
        </>
      ) : (
        <p className="text-xs text-muted">
          GSO made no additive changes to this space's benchmark set.
        </p>
      )}
    </PanelShell>
  )
}

function PanelShell({
  children,
  counts,
}: {
  children: React.ReactNode
  counts?: GSOBenchmarkChanges["counts"]
}) {
  return (
    <div className="rounded-xl border border-default p-6 space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest text-muted">
            Task 01 · Benchmark QC &amp; repair
          </p>
          <h3 className="text-sm font-semibold text-primary">Benchmark Changes</h3>
        </div>
        {counts && counts.total > 0 && (
          <div className="flex items-center gap-1.5 text-xs text-muted">
            <span className="text-emerald-500">+{counts.added} added</span>
            <span>·</span>
            <span className="text-red-400">−{counts.removed} removed</span>
            <span>·</span>
            <span className="text-blue-400">{counts.changed} changed</span>
            {counts.pruneRecommended > 0 && (
              <>
                <span>·</span>
                <span className="text-amber-500">{counts.pruneRecommended} prune-recommended</span>
              </>
            )}
          </div>
        )}
      </div>
      {children}
    </div>
  )
}

/**
 * The 30–40 working-set window meter + the bounded repair-tries indicator
 * (Phase 13, item 3). ``persistedCount`` (questions persisted into the live
 * space) is the headline; the shaded band marks the target window.
 */
function QcMeter({ qc }: { qc: GSOBenchmarkQC }) {
  const min = qc.windowTargetMin ?? DEFAULT_WINDOW_MIN
  const max = qc.windowTargetMax ?? DEFAULT_WINDOW_MAX
  const count = qc.persistedCount ?? qc.validCount ?? null

  const inWindow = count != null && count >= min && count <= max
  const below = count != null && count < min

  const windowStatus = count == null
    ? { label: "—", tone: "text-muted" }
    : inWindow
      ? { label: "In window", tone: "text-emerald-600 dark:text-emerald-400" }
      : below
        ? { label: "Below window · top-up recommended", tone: "text-amber-600 dark:text-amber-400" }
        : { label: "Above window · prune recommended", tone: "text-amber-600 dark:text-amber-400" }

  // Meter track domain: fit the window and the count with headroom.
  const domainMax = Math.max(max, count ?? 0) * 1.15 || 1
  const pct = (v: number) => `${Math.max(0, Math.min(100, (v / domainMax) * 100))}%`

  const triesUsed = qc.repairTriesUsed ?? 0
  const triesMax = qc.repairMaxTries ?? 0
  const repairedCount = qc.repairedIds?.length ?? 0
  const stillInvalid = qc.stillInvalidIds?.length ?? 0

  return (
    <div className="space-y-4 rounded-lg border border-default bg-elevated/30 p-4">
      {/* Window meter */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <span className="flex items-center gap-1.5 text-xs font-semibold text-primary">
            <Target className="h-3.5 w-3.5 text-indigo-500" />
            Working-set window
          </span>
          <span className="text-xs text-muted">
            <span className="font-semibold text-primary">{count ?? "—"}</span> questions · target {min}–{max}
          </span>
        </div>
        <div className="relative h-2.5 rounded-full bg-elevated">
          {/* Target band */}
          <div
            className="absolute inset-y-0 rounded-full bg-emerald-500/25"
            style={{ left: pct(min), width: pct(max - min) }}
          />
          {/* Count marker */}
          {count != null && (
            <div
              className={`absolute inset-y-[-2px] w-0.5 rounded-full ${inWindow ? "bg-emerald-500" : "bg-amber-500"}`}
              style={{ left: pct(count) }}
            />
          )}
        </div>
        <p className={`text-[11px] font-medium ${windowStatus.tone}`}>{windowStatus.label}</p>
      </div>

      {/* Repair-tries indicator + validity */}
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-default pt-3 text-xs">
        <span className="flex items-center gap-1.5 text-muted">
          <Wrench className="h-3.5 w-3.5 text-blue-400" />
          Repair sweeps:
          <span className="font-semibold text-primary">
            {triesUsed}
            {triesMax > 0 ? ` / ${triesMax}` : ""}
          </span>
          {triesMax > 0 && (
            <span className="ml-1 flex items-center gap-0.5">
              {Array.from({ length: triesMax }, (_, i) => (
                <span
                  key={i}
                  className={`h-1.5 w-1.5 rounded-full ${i < triesUsed ? "bg-blue-400" : "bg-elevated border border-default"}`}
                />
              ))}
            </span>
          )}
        </span>
        {repairedCount > 0 && (
          <span className="text-muted">
            <span className="font-semibold text-primary">{repairedCount}</span> repaired
          </span>
        )}
        {qc.finalValidity != null && (
          <span
            className={`flex items-center gap-1 font-medium ${
              qc.finalValidity
                ? "text-emerald-600 dark:text-emerald-400"
                : "text-red-500"
            }`}
          >
            {qc.finalValidity ? <CheckCircle2 className="h-3.5 w-3.5" /> : <AlertTriangle className="h-3.5 w-3.5" />}
            {qc.finalValidity ? "Benchmark valid" : "Benchmark invalid"}
          </span>
        )}
        {stillInvalid > 0 && (
          <span className="text-amber-600 dark:text-amber-400">
            {stillInvalid} still invalid
          </span>
        )}
      </div>

      {qc.terminalReason === "BENCHMARK_UNREPAIRABLE" && (
        <p className="flex items-center gap-1.5 rounded-md border border-red-300 bg-red-50 px-2.5 py-1.5 text-[11px] font-medium text-red-600 dark:border-red-800 dark:bg-red-950/20 dark:text-red-400">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
          Benchmark could not be repaired into a valid evaluation set — the run stopped here.
        </p>
      )}
    </div>
  )
}

function ChangeGroup({
  title,
  icon,
  mutations,
}: {
  title: string
  icon: React.ReactNode
  mutations: GSOBenchmarkMutation[]
}) {
  if (mutations.length === 0) return null
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        {icon}
        <h4 className="text-xs font-semibold text-primary uppercase tracking-wider">
          {title} ({mutations.length})
        </h4>
      </div>
      <div className="space-y-2">
        {mutations.map((m, i) => (
          <MutationRow key={`${m.questionId ?? "q"}-${i}`} mutation={m} />
        ))}
      </div>
    </div>
  )
}

function MutationRow({ mutation }: { mutation: GSOBenchmarkMutation }) {
  const question = mutation.after?.question ?? mutation.before?.question ?? mutation.questionId ?? "—"
  const beforeSql = mutation.before?.sql
  const afterSql = mutation.after?.sql
  return (
    <div className="rounded-lg border border-default bg-elevated/30 px-3 py-2 text-xs space-y-1.5">
      <div className="flex items-start justify-between gap-2">
        <span className="text-primary">{question}</span>
        {mutation.reason && (
          <span className="shrink-0 rounded-full border border-default bg-surface px-2 py-0.5 text-[10px] font-medium text-muted" title={mutation.reason}>
            {mutation.reason}
          </span>
        )}
      </div>
      {mutation.op === "changed" && (beforeSql || afterSql) && (
        <div className="grid grid-cols-2 gap-2">
          <pre className="rounded border border-default bg-surface p-2 font-mono text-[11px] whitespace-pre-wrap overflow-x-auto">
            {beforeSql ?? "—"}
          </pre>
          <pre className="rounded border border-default bg-surface p-2 font-mono text-[11px] whitespace-pre-wrap overflow-x-auto">
            {afterSql ?? "—"}
          </pre>
        </div>
      )}
      {mutation.op !== "changed" && (afterSql || beforeSql) && (
        <pre className="rounded border border-default bg-surface p-2 font-mono text-[11px] whitespace-pre-wrap overflow-x-auto">
          {afterSql ?? beforeSql}
        </pre>
      )}
    </div>
  )
}
