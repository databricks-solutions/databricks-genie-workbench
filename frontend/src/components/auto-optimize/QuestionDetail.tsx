import { Badge } from "@/components/ui/badge"
import { DataTable } from "@/components/DataTable"
import type { GSOQuestionDetail, SqlExecutionColumn } from "@/types"
import { useMemo } from "react"

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
  if (!question) {
    return (
      <div className="flex items-center justify-center h-64 text-muted text-sm">
        Select a question to view details
      </div>
    )
  }

  const genieParsed = useMemo(
    () => parseCsvSample(question.genie_sample, question.genie_columns),
    [question.genie_sample, question.genie_columns],
  )
  const gtParsed = useMemo(
    () => parseCsvSample(question.gt_sample, question.gt_columns),
    [question.gt_sample, question.gt_columns],
  )
  const hasResultTables = genieParsed.data.length > 0 || gtParsed.data.length > 0

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center gap-3">
        {question.excluded ? (
          <Badge variant="secondary">Excluded</Badge>
        ) : (
          <Badge variant={question.passed ? "success" : "danger"}>
            {question.passed ? "Pass" : "Fail"}
          </Badge>
        )}
        {question.match_type && (
          <span className="text-xs text-muted font-mono">{question.match_type}</span>
        )}
      </div>

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
