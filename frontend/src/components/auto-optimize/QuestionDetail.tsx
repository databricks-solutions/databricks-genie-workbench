import { Badge } from "@/components/ui/badge"
import type { GSOQuestionDetail } from "@/types"

interface QuestionDetailProps {
  question: GSOQuestionDetail | null
}

export function QuestionDetail({ question }: QuestionDetailProps) {
  if (!question) {
    return (
      <div className="flex items-center justify-center h-64 text-muted text-sm">
        Select a question to view details
      </div>
    )
  }

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Badge variant={question.passed ? "success" : "danger"}>
          {question.passed ? "Pass" : "Fail"}
        </Badge>
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
          {/* Model output */}
          <div className="rounded-lg border border-default overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 bg-elevated border-b border-default">
              <span className="text-xs font-medium text-muted">Model output</span>
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
    </div>
  )
}
