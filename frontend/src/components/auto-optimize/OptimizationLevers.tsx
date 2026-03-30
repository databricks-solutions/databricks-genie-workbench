import { useState, useMemo } from "react"
import { Badge } from "@/components/ui/badge"
import { ChevronDown, Wrench, CheckCircle2, XCircle, GitBranch } from "lucide-react"
import { ProactiveEnrichmentView } from "@/components/auto-optimize/ProactiveEnrichmentView"
import { SmartPatchCard } from "@/components/auto-optimize/SmartPatchCard"
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

function parseCommand(raw: Record<string, unknown> | string | null): Record<string, unknown> {
  if (!raw) return {}
  if (typeof raw === "object") return raw
  if (typeof raw === "string") {
    try {
      let parsed = JSON.parse(raw)
      if (typeof parsed === "string") parsed = JSON.parse(parsed)
      return typeof parsed === "object" && parsed !== null ? parsed : {}
    } catch { return {} }
  }
  return {}
}

function isExampleSqlPatch(patch: GSOPatchDetail): boolean {
  if (patch.patchType === "add_example_sql" || patch.patchType === "update_example_sql") return true
  const cmd = parseCommand(patch.command)
  return cmd.section === "example_question_sqls"
}

function isSqlExpressionPatch(patch: GSOPatchDetail): boolean {
  return patch.patchType.includes("sql_snippet") || patch.patchType.includes("sql_expression")
}

function isDescriptionPatch(patch: GSOPatchDetail): boolean {
  return patch.patchType.includes("description")
}

function ExampleSqlTable({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="rounded-lg border border-default overflow-hidden">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default bg-elevated/30">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Question</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">SQL</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const cmd = parseCommand(p.command)
            const question = String(cmd.question || "")
            const sql = String(cmd.sql || cmd.new_sql || "")
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums align-top">{i + 1}</td>
                <td className="px-3 py-2 text-primary align-top max-w-[300px]">{question || "—"}</td>
                <td className="px-3 py-2 align-top">
                  {sql ? (
                    <code className="text-[11px] font-mono text-primary bg-elevated/50 rounded px-1.5 py-0.5 block whitespace-pre-wrap leading-relaxed">
                      {sql}
                    </code>
                  ) : "—"}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SqlExpressionTable({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="rounded-lg border border-default overflow-hidden">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default bg-elevated/30">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-24">Type</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Display Name</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">SQL</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-40">Target Table</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const patchData = parseCommand(p.patch)
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums">{i + 1}</td>
                <td className="px-3 py-2">
                  <span className="inline-flex items-center rounded-md border border-teal-500/30 bg-teal-500/10 px-1.5 py-0.5 text-[10px] font-medium text-teal-700 dark:text-teal-400">
                    {String(patchData.snippet_type || "expression")}
                  </span>
                </td>
                <td className="px-3 py-2 text-primary">{String(patchData.display_name || "—")}</td>
                <td className="px-3 py-2">
                  {patchData.sql ? (
                    <code className="text-[11px] font-mono bg-elevated/50 rounded px-1.5 py-0.5 text-primary block whitespace-pre-wrap leading-relaxed">
                      {String(patchData.sql)}
                    </code>
                  ) : "—"}
                </td>
                <td className="px-3 py-2 text-muted font-mono truncate max-w-[160px]" title={p.targetObject ?? undefined}>
                  {p.targetObject || "—"}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function DescriptionTable({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="rounded-lg border border-default overflow-hidden">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default bg-elevated/30">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-16">Op</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-48">Target</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Description</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const patchData = parseCommand(p.patch)
            const cmdData = parseCommand(p.command)
            const desc = String(patchData.description || cmdData.description || cmdData.new_text || patchData.new_text || "—")
            const op = String(cmdData.op || p.patchType.split("_")[0] || "update")
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums align-top">{i + 1}</td>
                <td className="px-3 py-2 align-top">
                  <span className={`inline-flex items-center rounded-md border px-1.5 py-0.5 text-[10px] font-mono font-medium ${
                    op === "add" ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400"
                    : op === "remove" ? "border-red-500/30 bg-red-500/10 text-red-700 dark:text-red-400"
                    : "border-blue-500/30 bg-blue-500/10 text-blue-700 dark:text-blue-400"
                  }`}>
                    {op}
                  </span>
                </td>
                <td className="px-3 py-2 text-primary font-mono truncate max-w-[200px] align-top" title={p.targetObject ?? undefined}>
                  {p.targetObject || "—"}
                </td>
                <td className="px-3 py-2 text-muted align-top">{desc}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function classifyPatches(patches: GSOPatchDetail[]) {
  const exampleSql: GSOPatchDetail[] = []
  const sqlExpression: GSOPatchDetail[] = []
  const description: GSOPatchDetail[] = []
  const other: GSOPatchDetail[] = []

  for (const p of patches) {
    if (isExampleSqlPatch(p)) exampleSql.push(p)
    else if (isSqlExpressionPatch(p)) sqlExpression.push(p)
    else if (isDescriptionPatch(p)) description.push(p)
    else other.push(p)
  }
  return { exampleSql, sqlExpression, description, other }
}

function PatchGroup({ label, count, children }: { label: string; count: number; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-2">
        <h4 className="text-xs font-semibold text-primary">{label}</h4>
        <span className="text-[10px] text-muted tabular-nums">({count})</span>
      </div>
      {children}
    </div>
  )
}

function renderClassifiedPatches(patches: GSOPatchDetail[]) {
  const { exampleSql, sqlExpression, description, other } = classifyPatches(patches)
  const sections: React.ReactNode[] = []

  if (description.length > 0) {
    sections.push(
      <PatchGroup key="desc" label="Descriptions" count={description.length}>
        <DescriptionTable patches={description} />
      </PatchGroup>
    )
  }
  if (other.length > 0) {
    sections.push(
      <PatchGroup key="other" label={sections.length > 0 ? "Other Changes" : "Changes"} count={other.length}>
        <div className="space-y-2">
          {other.map((patch, i) => <SmartPatchCard key={i} patch={patch} />)}
        </div>
      </PatchGroup>
    )
  }
  if (sqlExpression.length > 0) {
    sections.push(
      <PatchGroup key="sqlexpr" label="SQL Expressions" count={sqlExpression.length}>
        <SqlExpressionTable patches={sqlExpression} />
      </PatchGroup>
    )
  }
  if (exampleSql.length > 0) {
    sections.push(
      <PatchGroup key="exsql" label="SQL Queries" count={exampleSql.length}>
        <ExampleSqlTable patches={exampleSql} />
      </PatchGroup>
    )
  }

  return sections
}

function LeverPatchContent({ lever, allPatches }: { lever: GSOLeverStatus; allPatches: GSOPatchDetail[] }) {
  if (allPatches.length === 0) return null

  if (lever.lever === 0) {
    return <ProactiveEnrichmentView patches={allPatches} />
  }

  // If there are multiple iterations with patches, group by iteration
  const iterationsWithPatches = lever.iterations.filter((it) => (it.patches?.length ?? 0) > 0)

  if (iterationsWithPatches.length > 1) {
    return (
      <div className="space-y-4">
        {iterationsWithPatches.map((it) => {
          const badge = STATUS_BADGE[it.status] ?? STATUS_BADGE.pending
          return (
            <div key={it.iteration} className="space-y-2">
              <div className="flex items-center gap-2">
                <span className="inline-flex items-center rounded-md border border-default bg-elevated px-2 py-0.5 text-xs font-medium text-primary">
                  Iteration {it.iteration}
                </span>
                <Badge variant={badge.variant} className="text-[10px] py-0 px-1.5">
                  {badge.variant === "success" && <CheckCircle2 className="h-2.5 w-2.5 mr-0.5" />}
                  {badge.label}
                </Badge>
                {it.scoreDelta != null && (
                  <span className={`text-[11px] tabular-nums ${Number(it.scoreDelta) > 0 ? "text-emerald-600" : "text-red-500"}`}>
                    {Number(it.scoreDelta) > 0 ? "+" : ""}{Number(it.scoreDelta).toFixed(1)}%
                  </span>
                )}
              </div>
              {renderClassifiedPatches(it.patches ?? [])}
            </div>
          )
        })}
      </div>
    )
  }

  // Single iteration or flat patches — render without iteration headers
  return (
    <div className="space-y-4">
      <p className="text-xs font-medium text-muted">Changes</p>
      {renderClassifiedPatches(allPatches)}
    </div>
  )
}

function IterationRow({ iteration, lever }: { iteration: GSOLeverIteration; lever: number }) {
  const badge = STATUS_BADGE[iteration.status] ?? STATUS_BADGE.pending
  // Exclude example SQL patches from displayed count
  const filteredCount = iteration.patches.length > 0
    ? iteration.patches.filter((p) => !isExampleSqlPatch(p)).length
    : iteration.patchCount
  // For lever 5 (Text Instructions), show instruction text preview
  const instructionPreview = lever === 5 ? (() => {
    const instrPatches = iteration.patches.filter((p) => p.patchType.includes("instruction"))
    if (instrPatches.length === 0) return null
    const cmd = parseCommand(instrPatches[0].command)
    const patchData = parseCommand(instrPatches[0].patch)
    return String(cmd.new_text || patchData.new_text || patchData.proposed_value || "")
  })() : null
  return (
    <div className="space-y-1">
      <div className="flex items-center gap-2 text-xs">
        <span className="inline-flex items-center rounded-md border border-default bg-elevated px-2 py-0.5 font-medium text-primary">
          Iteration {iteration.iteration}
        </span>
        <Badge variant={badge.variant} className="text-[10px] py-0 px-1.5">
          {badge.variant === "success" && <CheckCircle2 className="h-2.5 w-2.5 mr-0.5" />}
          {badge.label}
        </Badge>
        {filteredCount > 0 && (
          <span className="text-muted tabular-nums">{filteredCount} patches</span>
        )}
      </div>
      {instructionPreview && (
        <p className="text-[11px] text-muted leading-relaxed pl-2 border-l-2 border-default truncate max-w-full">
          {instructionPreview.slice(0, 120)}{instructionPreview.length > 120 ? "…" : ""}
        </p>
      )}
    </div>
  )
}

function LeverCard({ lever }: { lever: GSOLeverStatus }) {
  const [open, setOpen] = useState(lever.status === "accepted")
  const [showProvenance, setShowProvenance] = useState(false)
  const badge = STATUS_BADGE[lever.status] ?? STATUS_BADGE.pending

  const allPatches = lever.patches.length > 0
    ? lever.patches
    : lever.iterations.flatMap((it) => it.patches ?? [])

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
          <LeverPatchContent lever={lever} allPatches={allPatches} />

          {lever.iterations.length > 0 && lever.iterations.filter((it) => (it.patches?.length ?? 0) > 0).length <= 1 && (
            <div className="space-y-1.5">
              <p className="text-xs font-medium text-muted">Iteration history</p>
              <div className="space-y-1">
                {lever.iterations.map((it) => (
                  <IterationRow key={it.iteration} iteration={it} lever={lever.lever} />
                ))}
              </div>
            </div>
          )}

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
            <div className="text-xs bg-elevated/30 rounded-lg overflow-hidden">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-default">
                    <th className="text-left px-3 py-1.5 text-muted font-medium w-24">Iteration</th>
                    <th className="text-left px-3 py-1.5 text-muted font-medium w-20">Status</th>
                    <th className="text-left px-3 py-1.5 text-muted font-medium w-16">Score</th>
                    <th className="text-left px-3 py-1.5 text-muted font-medium">Targets modified</th>
                  </tr>
                </thead>
                <tbody>
                  {lever.iterations.map((it) => {
                    const itBadge = STATUS_BADGE[it.status] ?? STATUS_BADGE.pending
                    const targets = (it.patches ?? [])
                      .map((p) => p.targetObject?.split(".").slice(-1)[0])
                      .filter(Boolean)
                    const uniqueTargets = [...new Set(targets)]
                    return (
                      <tr key={it.iteration} className="border-b border-default last:border-0">
                        <td className="px-3 py-1.5 text-primary font-medium">Iteration {it.iteration}</td>
                        <td className="px-3 py-1.5">
                          <Badge variant={itBadge.variant} className="text-[10px] py-0 px-1.5">
                            {itBadge.label}
                          </Badge>
                        </td>
                        <td className="px-3 py-1.5 tabular-nums">
                          {it.scoreDelta != null ? (
                            <span className={Number(it.scoreDelta) > 0 ? "text-emerald-600" : "text-red-500"}>
                              {Number(it.scoreDelta) > 0 ? "+" : ""}{Number(it.scoreDelta).toFixed(1)}%
                            </span>
                          ) : "—"}
                        </td>
                        <td className="px-3 py-1.5 text-muted font-mono">
                          {uniqueTargets.length > 0
                            ? uniqueTargets.join(", ")
                            : `${it.patchCount} patch${it.patchCount !== 1 ? "es" : ""}`}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const ALL_LEVERS: { lever: number; name: string }[] = [
  { lever: 1, name: "Tables & Columns" },
  { lever: 2, name: "Metric Views" },
  { lever: 3, name: "SQL Queries & Functions" },
  { lever: 4, name: "Join Specifications" },
  { lever: 5, name: "Text Instructions" },
  { lever: 6, name: "SQL Expressions" },
]

function regroupLevers(levers: GSOLeverStatus[]): GSOLeverStatus[] {
  const result = levers.map((l) => ({
    ...l,
    patches: [...l.patches],
    iterations: l.iterations.map((it) => ({ ...it, patches: [...(it.patches ?? [])] })),
  }))

  const exampleSqlPatches: GSOPatchDetail[] = []
  for (const lever of result) {
    if (lever.lever === 3 || lever.lever === 0) continue

    const topLevel = lever.patches.filter(isExampleSqlPatch)
    const iterLevel = lever.iterations.flatMap((it) => (it.patches ?? []).filter(isExampleSqlPatch))
    const toMove = [...topLevel, ...iterLevel]
    if (toMove.length === 0) continue

    exampleSqlPatches.push(...toMove)
    const moveSet = new Set(toMove)
    lever.patches = lever.patches.filter((p) => !moveSet.has(p))
    for (const it of lever.iterations) {
      if (it.patches) {
        const before = it.patches.length
        it.patches = it.patches.filter((p) => !moveSet.has(p))
        it.patchCount = Math.max(0, it.patchCount - (before - it.patches.length))
      }
    }
    lever.patchCount = lever.patches.length + lever.iterations.reduce((s, it) => s + (it.patches?.length ?? 0), 0)
  }

  if (exampleSqlPatches.length > 0) {
    let lever3 = result.find((l) => l.lever === 3)
    if (!lever3) {
      lever3 = {
        lever: 3,
        name: "SQL Queries & Functions",
        status: "accepted",
        patchCount: 0,
        scoreBefore: null,
        scoreAfter: null,
        scoreDelta: null,
        rollbackReason: null,
        patches: [],
        iterations: [],
      }
      result.push(lever3)
    }
    lever3.patches.push(...exampleSqlPatches)
    lever3.patchCount = lever3.patches.length + lever3.iterations.reduce((s, it) => s + (it.patches?.length ?? 0), 0)
  }

  const byLever = new Map(result.map((l) => [l.lever, l]))
  const full: GSOLeverStatus[] = []
  for (const def of ALL_LEVERS) {
    const existing = byLever.get(def.lever)
    if (existing) {
      full.push(existing)
    } else {
      full.push({
        lever: def.lever,
        name: def.name,
        status: "skipped",
        patchCount: 0,
        scoreBefore: null,
        scoreAfter: null,
        scoreDelta: null,
        rollbackReason: null,
        patches: [],
        iterations: [],
      })
    }
  }

  const lever0 = result.find((l) => l.lever === 0)
  if (lever0) full.unshift(lever0)

  return full
}

export function OptimizationLevers({ levers }: OptimizationLeversProps) {
  const regrouped = useMemo(() => regroupLevers(levers), [levers])

  if (levers.length === 0) return null

  const acceptedCount = regrouped.filter((l) => l.status === "accepted").length
  const totalCount = regrouped.length

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
        {regrouped.map((lever) => (
          <LeverCard key={lever.lever} lever={lever} />
        ))}
      </div>
    </div>
  )
}
