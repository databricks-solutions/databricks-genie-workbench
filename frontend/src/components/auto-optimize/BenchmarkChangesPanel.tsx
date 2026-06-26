import { useEffect, useState } from "react"
import { PlusCircle, MinusCircle, RefreshCw, AlertTriangle } from "lucide-react"
import { getAutoOptimizeBenchmarkChanges } from "@/lib/api"
import type { GSOBenchmarkChanges, GSOBenchmarkMutation } from "@/types"

interface BenchmarkChangesPanelProps {
  runId: string
}

/**
 * GSO v2 Phase 6 (§3.5) — surfaces the benchmark provenance ledger: the
 * questions GSO added / removed / changed (and prune recommendations) in the
 * user's live Genie Space, with provenance. Makes the live-space mutation
 * transparent in the Workbench.
 */
export function BenchmarkChangesPanel({ runId }: BenchmarkChangesPanelProps) {
  const [changes, setChanges] = useState<GSOBenchmarkChanges | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let active = true
    getAutoOptimizeBenchmarkChanges(runId)
      .then((c) => {
        if (active) {
          setChanges(c)
          setLoading(false)
        }
      })
      .catch(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
  }, [runId])

  if (loading) {
    return (
      <div className="rounded-xl border border-default p-6">
        <h3 className="text-sm font-semibold text-primary mb-3">Benchmark Changes</h3>
        <p className="text-sm text-muted animate-pulse text-center py-6">Loading benchmark changes…</p>
      </div>
    )
  }

  if (!changes || changes.counts.total === 0) {
    return (
      <div className="rounded-xl border border-default p-6">
        <h3 className="text-sm font-semibold text-primary mb-3">Benchmark Changes</h3>
        <p className="text-sm text-muted text-center py-6">
          GSO made no changes to this space's benchmark set.
        </p>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-default p-6 space-y-5">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-primary">Benchmark Changes</h3>
        <div className="flex items-center gap-1.5 text-xs text-muted">
          <span className="text-emerald-500">+{changes.counts.added} added</span>
          <span>·</span>
          <span className="text-red-400">−{changes.counts.removed} removed</span>
          <span>·</span>
          <span className="text-blue-400">{changes.counts.changed} changed</span>
          {changes.counts.pruneRecommended > 0 && (
            <>
              <span>·</span>
              <span className="text-amber-500">{changes.counts.pruneRecommended} prune-recommended</span>
            </>
          )}
        </div>
      </div>

      <p className="text-xs text-muted">
        GSO pushes its EXPLAIN-validated benchmark questions into your live Genie Space (additive,
        merge-only) and recommends pruning invalid ones. Discarding the run reverts these from the
        preflight snapshot.
      </p>

      <ChangeGroup
        title="Added"
        icon={<PlusCircle className="h-4 w-4 text-emerald-500" />}
        mutations={changes.added}
      />
      <ChangeGroup
        title="Changed"
        icon={<RefreshCw className="h-4 w-4 text-blue-400" />}
        mutations={changes.changed}
      />
      <ChangeGroup
        title="Removed"
        icon={<MinusCircle className="h-4 w-4 text-red-400" />}
        mutations={changes.removed}
      />
      <ChangeGroup
        title="Prune recommended"
        icon={<AlertTriangle className="h-4 w-4 text-amber-500" />}
        mutations={changes.pruneRecommended}
      />
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
