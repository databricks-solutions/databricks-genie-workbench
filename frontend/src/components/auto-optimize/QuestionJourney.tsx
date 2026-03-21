import { useEffect, useMemo, useState } from "react"
import { CheckCircle, XCircle, Filter } from "lucide-react"
import { getAutoOptimizeQuestionResults } from "@/lib/api"
import type { GSOIterationResult, GSOQuestionDetail } from "@/types"

interface QuestionJourneyProps {
  runId: string
  iterations: GSOIterationResult[]
}

type FilterType = "all" | "failing" | "fixed" | "regressed" | "persistent"

interface QuestionRow {
  question_id: string
  question: string
  results: Map<number, boolean> // iteration → passed
  status: "passing" | "failing" | "fixed" | "regressed" | "persistent"
}

export function QuestionJourney({ runId, iterations }: QuestionJourneyProps) {
  const [questionData, setQuestionData] = useState<Map<number, GSOQuestionDetail[]>>(new Map())
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<FilterType>("all")

  const fullIterations = useMemo(
    () => iterations.filter((it) => it.eval_scope === "full").sort((a, b) => a.iteration - b.iteration),
    [iterations]
  )

  useEffect(() => {
    if (fullIterations.length === 0) {
      setLoading(false)
      return
    }

    setLoading(true)
    Promise.all(
      fullIterations.map((it) =>
        getAutoOptimizeQuestionResults(runId, it.iteration).then(
          (results) => [it.iteration, results] as const
        )
      )
    )
      .then((pairs) => {
        const m = new Map<number, GSOQuestionDetail[]>()
        for (const [iter, results] of pairs) m.set(iter, results)
        setQuestionData(m)
      })
      .finally(() => setLoading(false))
  }, [runId, fullIterations])

  const rows = useMemo(() => {
    const questionMap = new Map<string, QuestionRow>()

    for (const [iter, details] of questionData) {
      for (const q of details) {
        if (!questionMap.has(q.question_id)) {
          questionMap.set(q.question_id, {
            question_id: q.question_id,
            question: q.question,
            results: new Map(),
            status: "passing",
          })
        }
        questionMap.get(q.question_id)!.results.set(iter, q.passed ?? false)
      }
    }

    // Determine status based on baseline → final trajectory
    const iterNums = fullIterations.map((it) => it.iteration)
    const baselineIter = iterNums[0]
    const finalIter = iterNums[iterNums.length - 1]

    for (const row of questionMap.values()) {
      const baselinePassed = row.results.get(baselineIter)
      const finalPassed = row.results.get(finalIter)

      if (baselinePassed && finalPassed) {
        row.status = "passing"
      } else if (!baselinePassed && finalPassed) {
        row.status = "fixed"
      } else if (baselinePassed && !finalPassed) {
        row.status = "regressed"
      } else if (!baselinePassed && !finalPassed) {
        row.status = "persistent"
      } else {
        row.status = finalPassed ? "passing" : "failing"
      }
    }

    return Array.from(questionMap.values())
  }, [questionData, fullIterations])

  const filtered = useMemo(() => {
    if (filter === "all") return rows
    if (filter === "failing") return rows.filter((r) => r.status !== "passing")
    return rows.filter((r) => r.status === filter)
  }, [rows, filter])

  const iterNums = fullIterations.map((it) => it.iteration)

  const FILTER_OPTIONS: { key: FilterType; label: string }[] = [
    { key: "all", label: "All" },
    { key: "failing", label: "Failing" },
    { key: "fixed", label: "Fixed" },
    { key: "regressed", label: "Regressed" },
    { key: "persistent", label: "Persistent" },
  ]

  if (loading) {
    return (
      <div className="rounded-xl border border-default p-6">
        <h3 className="text-sm font-semibold text-primary mb-3">Question Journey</h3>
        <p className="text-sm text-muted animate-pulse text-center py-6">Loading question data...</p>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-default p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-primary">
          Question Journey ({filtered.length})
        </h3>
        <div className="flex items-center gap-1">
          {FILTER_OPTIONS.map((opt) => (
            <button
              key={opt.key}
              onClick={() => setFilter(opt.key)}
              className={`flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium transition-colors ${
                filter === opt.key
                  ? "bg-accent text-white"
                  : "bg-elevated text-muted hover:text-primary"
              }`}
            >
              <Filter className="h-3 w-3" />
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {filtered.length === 0 ? (
        <p className="text-sm text-muted text-center py-6">No questions match this filter</p>
      ) : (
        <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-surface">
              <tr className="border-b border-default">
                <th className="text-left px-3 py-2 text-xs font-medium text-muted">Question</th>
                {iterNums.map((iter) => (
                  <th key={iter} className="text-center px-3 py-2 text-xs font-medium text-muted whitespace-nowrap">
                    {iter === 0 ? "Baseline" : `Iter ${iter}`}
                  </th>
                ))}
                <th className="text-center px-3 py-2 text-xs font-medium text-muted">Status</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((row) => (
                <tr key={row.question_id} className="border-b border-default last:border-0 hover:bg-elevated/50">
                  <td className="px-3 py-2 text-primary max-w-[400px] truncate" title={row.question}>
                    {row.question || row.question_id}
                  </td>
                  {iterNums.map((iter) => {
                    const passed = row.results.get(iter)
                    return (
                      <td key={iter} className="text-center px-3 py-2">
                        {passed == null ? (
                          <span className="text-muted">\u2014</span>
                        ) : passed ? (
                          <CheckCircle className="h-4 w-4 text-emerald-500 inline" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500 inline" />
                        )}
                      </td>
                    )
                  })}
                  <td className="text-center px-3 py-2">
                    <StatusBadge status={row.status} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    passing: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-400",
    fixed: "bg-blue-100 text-blue-700 dark:bg-blue-950/40 dark:text-blue-400",
    regressed: "bg-red-100 text-red-700 dark:bg-red-950/40 dark:text-red-400",
    persistent: "bg-amber-100 text-amber-700 dark:bg-amber-950/40 dark:text-amber-400",
    failing: "bg-red-100 text-red-700 dark:bg-red-950/40 dark:text-red-400",
  }
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase ${styles[status] ?? "bg-elevated text-muted"}`}>
      {status}
    </span>
  )
}
