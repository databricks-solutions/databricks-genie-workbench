import { useEffect, useId, useState } from "react"
import {
  PlusCircle,
  MinusCircle,
  RefreshCw,
  AlertTriangle,
  Wrench,
  Target,
  CheckCircle2,
  ChevronDown,
  ShieldCheck,
} from "lucide-react"
import { getAutoOptimizeBenchmarkChanges } from "@/lib/api"
import type {
  GSOBenchmarkChanges,
  GSOBenchmarkMutation,
  GSOBenchmarkQC,
  GSOBenchmarkQualityFinding,
} from "@/types"

interface BenchmarkChangesPanelProps {
  runId: string
  /**
   * Pre-fetched benchmark changes. When provided the panel skips its own fetch
   * (the live cockpit already polls ``/benchmark-changes`` for the rail chip).
   */
  changes?: GSOBenchmarkChanges | null
  /** Hide the card title when a run-level activity divider labels the panel. */
  showTitle?: boolean
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
export function BenchmarkChangesPanel({
  runId,
  changes: provided,
  showTitle = true,
}: BenchmarkChangesPanelProps) {
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
      <PanelShell showTitle={showTitle}>
        <p className="rounded-xl border border-default py-6 text-center text-sm text-muted animate-pulse">
          Loading benchmark changes…
        </p>
      </PanelShell>
    )
  }

  if (!qc && total === 0) {
    return (
      <PanelShell showTitle={showTitle}>
        <p className="rounded-xl border border-default py-6 text-center text-sm text-muted">
          GSO made no changes to this agent's benchmark set.
        </p>
      </PanelShell>
    )
  }

  return (
    <PanelShell showTitle={showTitle}>
      {qc && <QcMeter qc={qc} counts={changes?.counts} />}
      {qc && <BenchmarkQuality qc={qc} />}

      {changes && total > 0 ? (
        <BenchmarkRepairs changes={changes} />
      ) : (
        <p className="text-xs text-muted">
          {qc?.benchmarkPolicy === "review_only"
            ? "GSO reviewed the existing benchmarks without changing the live benchmark set."
            : "GSO made no additive changes to this agent's benchmark set."}
        </p>
      )}
    </PanelShell>
  )
}

function PanelShell({
  children,
  showTitle = true,
}: {
  children: React.ReactNode
  showTitle?: boolean
}) {
  return (
    <div className="space-y-4">
      {showTitle && (
        <h3 className="text-sm font-semibold text-primary">Benchmark QC &amp; Repairs</h3>
      )}
      {children}
    </div>
  )
}

/**
 * The 30–40 working-set window meter + the bounded repair-tries indicator
 * (Phase 13, item 3). ``persistedCount`` (questions persisted into the
 * optimization handoff) is the headline; the shaded band marks the target
 * window.
 */
function QcMeter({
  qc,
  counts,
}: {
  qc: GSOBenchmarkQC
  counts?: GSOBenchmarkChanges["counts"]
}) {
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
  const reviewOnly = qc.benchmarkPolicy === "review_only"
  const minimum = qc.minimumValidCount

  return (
    <div className="space-y-4 rounded-xl border border-default bg-elevated/30 p-4">
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
        {reviewOnly ? (
          <span className="flex items-center gap-1.5 font-medium text-cyan-600 dark:text-cyan-400">
            <ShieldCheck className="h-3.5 w-3.5" />
            Review only · live benchmarks preserved
          </span>
        ) : (
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
        )}
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
            {qc.finalValidity ? "SQL valid" : "SQL invalid"}
          </span>
        )}
        {stillInvalid > 0 && (
          <span className="text-amber-600 dark:text-amber-400">
            {stillInvalid} still invalid
          </span>
        )}
        {counts && counts.total > 0 && (
          <div className="flex flex-wrap items-center gap-1.5 text-[11px] text-muted sm:ml-auto">
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

      {qc.terminalReason === "BENCHMARK_UNREPAIRABLE" && (
        <p className="flex items-center gap-1.5 rounded-md border border-red-300 bg-red-50 px-2.5 py-1.5 text-[11px] font-medium text-red-600 dark:border-red-800 dark:bg-red-950/20 dark:text-red-400">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
          Benchmark could not be repaired into a valid evaluation set — the run stopped here.
        </p>
      )}
      {qc.optimizationEligible === false && qc.terminalReason === "INSUFFICIENT_VALID_BENCHMARKS" && (
        <p className="flex items-start gap-1.5 rounded-md border border-amber-300 bg-amber-50 px-2.5 py-1.5 text-[11px] font-medium text-amber-800 dark:border-amber-800 dark:bg-amber-950/20 dark:text-amber-300">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          Optimization was skipped: {qc.validCount ?? 0} valid benchmarks remained
          {minimum != null ? `; at least ${minimum} are required` : ""}. No evaluation or configuration patch ran.
        </p>
      )}
    </div>
  )
}

function BenchmarkRepairs({ changes }: { changes: GSOBenchmarkChanges }) {
  return (
    <div className="space-y-3 rounded-xl border border-default bg-elevated/20 p-4">
      <div>
        <div className="flex items-center gap-1.5 text-xs font-semibold text-primary">
          <Wrench className="h-3.5 w-3.5 text-blue-400" />
          Benchmark repairs
        </div>
        <p className="mt-1 text-[11px] leading-relaxed text-muted">
          GSO pushes its quality-reviewed, SQL-valid benchmark questions into your live Genie Agent
          (additive, merge-only) and excludes hard failures from evaluation. Discarding the run reverts
          additions and changes from the intake snapshot.
        </p>
      </div>

      <ChangeGroup
        title="Added"
        icon={<PlusCircle className="h-4 w-4 text-emerald-500" />}
        mutations={changes.added}
        defaultExpanded={false}
      />
      <ChangeGroup
        title="Changed"
        icon={<RefreshCw className="h-4 w-4 text-blue-400" />}
        mutations={changes.changed}
        defaultExpanded
      />
      <ChangeGroup
        title="Removed"
        icon={<MinusCircle className="h-4 w-4 text-red-400" />}
        mutations={changes.removed}
        defaultExpanded
      />
      <ChangeGroup
        title="Prune recommended"
        icon={<AlertTriangle className="h-4 w-4 text-amber-500" />}
        mutations={changes.pruneRecommended}
        defaultExpanded
      />
    </div>
  )
}

function BenchmarkQuality({ qc }: { qc: GSOBenchmarkQC }) {
  const findings = qc.qualityFindings ?? []
  const counts = qc.qualityCounts
  if (!counts && findings.length === 0 && !qc.qualityReviewStatus) return null

  const errors = findings.filter((f) => f.severity === "error")
  const warnings = findings.filter((f) => f.severity !== "error")
  const coverage = qc.semanticReviewCoverage == null
    ? null
    : Math.round(qc.semanticReviewCoverage * 100)

  return (
    <div className="space-y-3 rounded-xl border border-default bg-elevated/20 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-1.5 text-xs font-semibold text-primary">
            <ShieldCheck className="h-3.5 w-3.5 text-cyan-500" />
            Benchmark quality
          </div>
          <p className="mt-1 text-[11px] text-muted">
            Question clarity, ground-truth alignment, SQL validity, and data checks
          </p>
        </div>
        {counts && (
          <div className="flex flex-wrap items-center gap-1.5 text-[11px] font-medium">
            <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-emerald-600 dark:text-emerald-400">
              {counts.trusted} trusted
            </span>
            <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-amber-600 dark:text-amber-400">
              {counts.warnings} warnings
            </span>
            <span className="rounded-full border border-red-500/30 bg-red-500/10 px-2 py-0.5 text-red-600 dark:text-red-400">
              {counts.excluded} excluded
            </span>
          </div>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-default pt-3 text-[11px] text-muted">
        {coverage != null && <span>Semantic review coverage: <strong className="text-primary">{coverage}%</strong></span>}
        {qc.qualityReviewVersion && <span>Policy: {qc.qualityReviewVersion}</span>}
        {qc.qualityReviewStatus === "degraded" && (
          <span className="flex items-center gap-1 font-medium text-amber-600 dark:text-amber-400">
            <AlertTriangle className="h-3 w-3" />
            Review incomplete — treat the accuracy score with caution
          </span>
        )}
      </div>

      <QualityFindingGroup
        title="Excluded from evaluation"
        findings={errors}
        tone="error"
        defaultExpanded
      />
      <QualityFindingGroup
        title="Warnings"
        findings={warnings}
        tone="warning"
        defaultExpanded={false}
      />
    </div>
  )
}

function QualityFindingGroup({
  title,
  findings,
  tone,
  defaultExpanded,
}: {
  title: string
  findings: GSOBenchmarkQualityFinding[]
  tone: "error" | "warning"
  defaultExpanded: boolean
}) {
  const [expanded, setExpanded] = useState(defaultExpanded)
  const contentId = useId()
  if (findings.length === 0) return null

  return (
    <div className="border-t border-default pt-2">
      <button
        type="button"
        className="flex w-full items-center justify-between gap-2 rounded px-1 py-1 text-left hover:bg-elevated/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cyan-500/60"
        aria-expanded={expanded}
        aria-controls={contentId}
        onClick={() => setExpanded((value) => !value)}
      >
        <span className={`text-[11px] font-semibold uppercase tracking-wider ${tone === "error" ? "text-red-500" : "text-amber-600 dark:text-amber-400"}`}>
          {title} ({findings.length})
        </span>
        <ChevronDown className={`h-3.5 w-3.5 text-muted transition-transform ${expanded ? "rotate-180" : ""}`} />
      </button>
      {expanded && (
        <div id={contentId} className="mt-2 space-y-2">
          {findings.map((finding, index) => (
            <QualityFindingRow key={`${finding.question_id}-${finding.code}-${index}`} finding={finding} />
          ))}
        </div>
      )}
    </div>
  )
}

function QualityFindingRow({ finding }: { finding: GSOBenchmarkQualityFinding }) {
  const code = finding.code.toLowerCase().replaceAll("_", " ")
  const currentSql = finding.before?.sql
  return (
    <div className="rounded-md border border-default bg-surface/70 px-3 py-2 text-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <span className="font-medium text-primary">{finding.question || finding.question_id}</span>
        <span className="rounded-full border border-default bg-elevated px-2 py-0.5 text-[10px] font-medium capitalize text-muted">
          {code}
        </span>
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-muted">{finding.explanation}</p>
      {currentSql && (
        <div className={`mt-2 grid gap-2 ${finding.proposed_sql ? "md:grid-cols-2" : ""}`}>
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-wider text-muted">Current ground truth</p>
            <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap rounded bg-elevated p-2 font-mono text-[10px] text-primary">
              {currentSql}
            </pre>
          </div>
          {finding.proposed_sql && (
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-wider text-cyan-600 dark:text-cyan-400">Suggested ground truth</p>
              <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap rounded bg-cyan-500/5 p-2 font-mono text-[10px] text-primary ring-1 ring-cyan-500/20">
                {finding.proposed_sql}
              </pre>
            </div>
          )}
        </div>
      )}
      {(finding.proposed_question || (finding.proposed_sql && !currentSql)) && (
        <div className="mt-2 border-l-2 border-cyan-500/50 pl-2">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-cyan-600 dark:text-cyan-400">Suggested repair</p>
          {finding.proposed_question && <p className="mt-1 text-[11px] text-primary">{finding.proposed_question}</p>}
          {finding.proposed_sql && !currentSql && (
            <pre className="mt-1 overflow-x-auto whitespace-pre-wrap rounded bg-elevated p-2 font-mono text-[10px] text-primary">
              {finding.proposed_sql}
            </pre>
          )}
        </div>
      )}
    </div>
  )
}

function ChangeGroup({
  title,
  icon,
  mutations,
  defaultExpanded,
}: {
  title: string
  icon: React.ReactNode
  mutations: GSOBenchmarkMutation[]
  defaultExpanded: boolean
}) {
  const [expanded, setExpanded] = useState(defaultExpanded)
  const contentId = useId()
  if (mutations.length === 0) return null
  return (
    <div className="space-y-2">
      <button
        type="button"
        className="flex w-full items-center justify-between gap-2 rounded-md px-1 py-1 text-left hover:bg-elevated/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cyan-500/60"
        aria-expanded={expanded}
        aria-controls={contentId}
        onClick={() => setExpanded((value) => !value)}
      >
        <span className="flex items-center gap-2">
          {icon}
          <span className="text-xs font-semibold uppercase tracking-wider text-primary">
            {title} ({mutations.length})
          </span>
        </span>
        <ChevronDown className={`h-4 w-4 text-muted transition-transform ${expanded ? "rotate-180" : ""}`} />
      </button>
      {expanded && (
        <div id={contentId} className="space-y-2">
          {mutations.map((m, i) => (
            <MutationRow key={`${m.questionId ?? "q"}-${i}`} mutation={m} />
          ))}
        </div>
      )}
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
          <div className="min-w-0">
            <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">Before</p>
            <pre className="rounded border border-default bg-surface p-2 font-mono text-[11px] whitespace-pre-wrap overflow-x-auto">
              {beforeSql ?? "—"}
            </pre>
          </div>
          <div className="min-w-0">
            <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-blue-500">After</p>
            <pre className="rounded border border-default bg-surface p-2 font-mono text-[11px] whitespace-pre-wrap overflow-x-auto">
              {afterSql ?? "—"}
            </pre>
          </div>
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
