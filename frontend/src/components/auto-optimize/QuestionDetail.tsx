import { Badge } from "@/components/ui/badge"
import type { GSOQuestionResult } from "@/types"

interface QuestionDetailProps {
  question: GSOQuestionResult | null
}

export function QuestionDetail({ question }: QuestionDetailProps) {
  if (!question) {
    return (
      <div className="flex items-center justify-center h-64 text-muted text-sm">
        Select a question to view details
      </div>
    )
  }

  const pass = question.failure_type == null || question.failure_type === ""

  return (
    <div className="space-y-5">
      {/* Header */}
      <div>
        <h3 className="text-sm font-semibold text-primary mb-2">{question.question_id}</h3>
        <div className="flex items-center gap-3">
          <Badge variant={pass ? "success" : "danger"}>
            {pass ? "Pass" : "Fail"}
          </Badge>
          <span className="text-xs text-muted">Judge: {question.judge}</span>
          {question.confidence != null && (
            <span className="text-xs text-muted">
              Confidence: {(question.confidence * 100).toFixed(0)}%
            </span>
          )}
        </div>
      </div>

      {/* Failure type */}
      {question.failure_type && (
        <div>
          <h4 className="text-xs font-medium text-muted mb-1">Failure Type</h4>
          <p className="text-sm text-primary">{question.failure_type}</p>
        </div>
      )}

      {/* Value (SQL or text) */}
      <div>
        <h4 className="text-xs font-medium text-muted mb-2">Value</h4>
        <pre className="rounded-lg bg-elevated border border-default p-4 text-xs font-mono text-primary overflow-x-auto whitespace-pre-wrap">
          {question.value}
        </pre>
      </div>
    </div>
  )
}
