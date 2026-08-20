import { Badge } from "@/components/ui/badge"
import { DataTable } from "@/components/DataTable"
import type { GSOQuestionDetail, SqlExecutionColumn } from "@/types"
import { questionState, questionStateLabel, formatAssessmentReason } from "@/lib/assessment"
import { useMemo } from "react"

const STATE_BADGE: Record<string, "success" | "danger" | "warning" | "secondary"> = {
  passing: "success",
  failing: "danger",
  needs_review: "warning",
  excluded: "secondary",
}

interface QuestionDetailProps {
  question: GSOQuestionDetail | null
}

/** Parse a CSV sample string (from comparison.genie_sample / gt_sample) into DataTable-compatible format. */
function parseCsvSample(csv: string | null | undefined, columnNames?: string[]): {
  columns: SqlExecutionColumn[]
  data: (string | number | boolean | null)[][]
} {
  if (!csv?.trim()) return { columns: [], data: [] }

  const lines = csv.trim().split("\n")
  if (lines.length === 0) return { columns: [], data: [] }

  // First line is the header
  const headers = lines[0].split(",").map((h) => h.trim())
  const columns: SqlExecutionColumn[] = (columnNames?.length ? columnNames : headers).map((name) => ({
    name,
    type_name: "",
  }))

  const data: (string | number | boolean | null)[][] = []
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue
    const cells = lines[i].split(",").map((cell) => {
      const trimmed = cell.trim()
      if (trimmed === "" || trimmed === "None" || trimmed === "null") return null
      const num = Number(trimmed)
      if (!isNaN(num) && trimmed !== "") return num
      return trimmed
    })
    data.push(cells)
  }

  return { columns, data }
}

export function QuestionDetail({ question }: QuestionDetailProps) {
  // Hooks must run unconditionally (Rules of Hooks). parseCsvSample already
  // no-ops on null/undefined, so it is safe to compute before the early return.
  const genieParsed = useMemo(
    () => parseCsvSample(question?.genie_sample, question?.genie_columns),
    [question?.genie_sample, question?.genie_columns],
  )
  const gtParsed = useMemo(
    () => parseCsvSample(question?.gt_sample, question?.gt_columns),
    [question?.gt_sample, question?.gt_columns],
  )

  if (!question) {
    return (
      <div className="flex items-center justify-center h-64 text-muted text-sm">
        Select a question to view details
      </div>
    )
  }

  const hasResultTables = genieParsed.data.length > 0 || gtParsed.data.length > 0
  const state = questionState(question)
  const reasons = question.assessment_reasons ?? []

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center gap-3 flex-wrap">
        <Badge variant={STATE_BADGE[state] ?? "secondary"}>
          {questionStateLabel(state)}
        </Badge>
        {question.match_type && (
          <span className="text-xs text-muted font-mono">{question.match_type}</span>
        )}
      </div>

      {/* Assessment reasons (replaces the retired per-judge verdicts) */}
      {reasons.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">Assessment reasons</h4>
          <div className="flex flex-wrap gap-1.5">
            {reasons.map((r) => (
              <span
                key={r}
                title={r}
                className="inline-flex items-center rounded-full border border-default bg-elevated/50 px-2.5 py-0.5 text-xs font-medium text-primary"
              >
                {formatAssessmentReason(r)}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Question text */}
      <div>
        <h4 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">Question</h4>
        <div className="rounded-lg border border-default bg-elevated px-4 py-3 text-sm text-primary">
          {question.question || question.question_id}
        </div>
      </div>

      {/* SQL comparison */}
      <div>
        <h4 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">Response</h4>
        <div className="grid grid-cols-2 gap-3">
          {/* Genie Response */}
          <div className="rounded-lg border border-default overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 bg-elevated border-b border-default">
              <span className="text-xs font-medium text-muted">Genie Response</span>
              <span className="text-xs text-muted/60 font-mono">SQL</span>
            </div>
            <pre className="p-3 text-xs font-mono text-primary overflow-x-auto whitespace-pre-wrap min-h-[80px] bg-surface">
              {question.generated_sql ?? "—"}
            </pre>
          </div>

          {/* Ground truth */}
          <div className="rounded-lg border border-default overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 bg-elevated border-b border-default">
              <span className="text-xs font-medium text-muted">Ground truth SQL answer</span>
              <span className="text-xs text-muted/60 font-mono">SQL</span>
            </div>
            <pre className="p-3 text-xs font-mono text-primary overflow-x-auto whitespace-pre-wrap min-h-[80px] bg-surface">
              {question.expected_sql ?? "—"}
            </pre>
          </div>
        </div>
      </div>

      {/* Query result tables */}
      {hasResultTables && (
        <div>
          <h4 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">Query Results</h4>
          <div className="grid grid-cols-2 gap-3">
            {/* Genie results */}
            <div>
              {question.genie_rows != null && (
                <div className="text-xs text-muted mb-1.5">{question.genie_rows} rows</div>
              )}
              <DataTable
                columns={genieParsed.columns}
                data={genieParsed.data}
                maxHeight="200px"
                truncated={genieParsed.data.length < (question.genie_rows ?? 0)}
              />
            </div>

            {/* GT results */}
            <div>
              {question.gt_rows != null && (
                <div className="text-xs text-muted mb-1.5">{question.gt_rows} rows</div>
              )}
              <DataTable
                columns={gtParsed.columns}
                data={gtParsed.data}
                maxHeight="200px"
                truncated={gtParsed.data.length < (question.gt_rows ?? 0)}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
