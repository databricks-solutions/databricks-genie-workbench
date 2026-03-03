/**
 * Labeling page for reviewing benchmark questions one at a time.
 * Questions are pre-processed before arriving at this page.
 */

import { useState, useMemo } from "react"
import { ArrowLeft, ChevronLeft, ChevronRight, ChevronDown, GitCompare, Check, AlertCircle } from "lucide-react"
import { format as formatSqlLib } from "sql-formatter"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { SqlCodeBlock } from "@/components/SqlCodeBlock"
import { SqlDiffView } from "@/components/SqlDiffView"
import { DataTable } from "@/components/DataTable"
import { getSelectedBenchmarkQuestions, getExpectedSql } from "@/lib/benchmarkUtils"
import type { SqlExecutionResult } from "@/types"

interface LabelingPageProps {
  genieSpaceId: string
  spaceData: Record<string, unknown>
  selectedQuestions: string[]
  // Lifted state (persists across navigation)
  currentIndex: number
  generatedSql: Record<string, string>
  genieResults: Record<string, SqlExecutionResult | null>
  expectedResults: Record<string, SqlExecutionResult | null>
  correctAnswers: Record<string, boolean | null>
  feedbackTexts: Record<string, string>
  processingErrors: Record<string, string>
  // Actions
  onSetCurrentIndex: (index: number) => void
  onSetCorrectAnswer: (questionId: string, answer: boolean | null) => void
  onSetFeedbackText: (questionId: string, text: string) => void
  onBack: () => void
  onFinish: () => void
}

export function LabelingPage({
  genieSpaceId,
  spaceData,
  selectedQuestions,
  // Lifted state
  currentIndex,
  generatedSql,
  genieResults,
  expectedResults,
  correctAnswers,
  feedbackTexts,
  processingErrors,
  // Actions
  onSetCurrentIndex,
  onSetCorrectAnswer,
  onSetFeedbackText,
  onBack,
  onFinish,
}: LabelingPageProps) {
  // Transient UI state (local only)
  const [showDiff, setShowDiff] = useState(false)

  // Get the selected questions in order
  const questions = useMemo(
    () => getSelectedBenchmarkQuestions(spaceData, selectedQuestions),
    [spaceData, selectedQuestions]
  )

  const totalQuestions = questions.length
  const currentQuestion = questions[currentIndex]
  // Count questions that have been labeled (have Yes/No feedback)
  const labeledCount = questions.filter(q => correctAnswers[q.id] !== undefined && correctAnswers[q.id] !== null).length
  const allLabeled = labeledCount === totalQuestions && totalQuestions > 0

  const handlePrev = () => {
    onSetCurrentIndex(Math.max(0, currentIndex - 1))
  }

  const handleNext = () => {
    onSetCurrentIndex(Math.min(totalQuestions - 1, currentIndex + 1))
  }

  const handleSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onSetCurrentIndex(Number(e.target.value))
  }

  const handleSegmentClick = (idx: number) => {
    onSetCurrentIndex(idx)
  }

  // Get expected SQL from answer (look for SQL format first, then fall back to first answer)
  const expectedSql = useMemo(() => getExpectedSql(currentQuestion), [currentQuestion])

  // Get current question's generated SQL
  const currentGeneratedSql = currentQuestion ? generatedSql[currentQuestion.id] : null

  // Get current question's error (if any)
  const currentError = currentQuestion ? processingErrors[currentQuestion.id] : null

  // Get current question's execution results and feedback
  const genieResult = currentQuestion ? genieResults[currentQuestion.id] ?? null : null
  const expectedResult = currentQuestion ? expectedResults[currentQuestion.id] ?? null : null
  const isCorrect = currentQuestion ? correctAnswers[currentQuestion.id] ?? null : null
  const feedbackText = currentQuestion ? feedbackTexts[currentQuestion.id] ?? '' : ''

  // Format SQL using sql-formatter library for consistent display
  const formatSql = (sql: string): string => {
    try {
      return formatSqlLib(sql, {
        language: "sql",
        indentStyle: "standard",
        keywordCase: "upper",
        linesBetweenQueries: 1,
      })
    } catch {
      // If formatting fails, return original
      return sql
    }
  }

  // Format the generated SQL for display
  const formattedGeneratedSql = currentGeneratedSql ? formatSql(currentGeneratedSql) : null

  if (!currentQuestion) {
    return (
      <div className="space-y-6 animate-slide-up">
        <div className="flex items-center gap-4">
          <Button variant="outline" onClick={onBack}>
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Benchmarks
          </Button>
        </div>
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted">No questions selected.</p>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-primary">
            Labeling Session
          </h1>
          <p className="text-muted">
            Space ID: <span className="font-mono text-secondary">{genieSpaceId}</span>
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" onClick={onBack}>
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Benchmarks
          </Button>
        </div>
      </div>

      {/* Segmented Progress Bar with Navigation */}
      <div className="space-y-2">
        <div className="flex items-center gap-3">
          <Button
            variant="outline"
            size="sm"
            onClick={handlePrev}
            disabled={currentIndex === 0}
          >
            <ChevronLeft className="w-4 h-4 mr-1" />
            Prev
          </Button>

          {/* Segmented Bar */}
          <div className="flex-1 flex gap-1">
            {questions.map((q, idx) => {
              const labelValue = correctAnswers[q.id]
              const isLabeled = labelValue !== undefined && labelValue !== null
              const isCurrent = idx === currentIndex
              const hasError = processingErrors[q.id]
              const hasResult = generatedSql[q.id]

              // Determine segment color
              let bgClass = "bg-elevated hover:bg-secondary/30"
              let title = `Question ${idx + 1}`

              if (isLabeled) {
                bgClass = labelValue === true
                  ? "bg-green-500 dark:shadow-[0_0_8px_rgba(34,197,94,0.5)]"
                  : "bg-red-500 dark:shadow-[0_0_8px_rgba(239,68,68,0.5)]"
                title += labelValue ? " (correct)" : " (incorrect)"
              } else if (hasError) {
                bgClass = "bg-amber-500 dark:shadow-[0_0_8px_rgba(245,158,11,0.5)]"
                title += " (error)"
              } else if (hasResult) {
                bgClass = "bg-secondary/50"
                title += " (ready to label)"
              } else if (isCurrent) {
                bgClass = "bg-secondary/40"
              }

              return (
                <button
                  key={q.id}
                  onClick={() => handleSegmentClick(idx)}
                  className={`
                    flex-1 h-2.5 rounded-full transition-all cursor-pointer
                    ${bgClass}
                    ${isCurrent ? "ring-2 ring-accent ring-offset-2 ring-offset-surface" : ""}
                  `}
                  title={title}
                />
              )
            })}
          </div>

          <Button
            variant="outline"
            size="sm"
            onClick={handleNext}
            disabled={currentIndex === totalQuestions - 1}
          >
            Next
            <ChevronRight className="w-4 h-4 ml-1" />
          </Button>

          <Button
            size="sm"
            disabled={!allLabeled}
            onClick={onFinish}
          >
            <Check className="w-4 h-4 mr-1" />
            Finish
          </Button>
        </div>

        {/* Status text */}
        <p className="text-center text-sm text-muted">
          {labeledCount} of {totalQuestions} labeled
        </p>
      </div>

      {/* Question Display with Selector */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-muted">Question</h3>
            <div className="relative">
              <select
                value={currentIndex}
                onChange={handleSelectChange}
                className="appearance-none text-sm text-secondary bg-elevated hover:bg-sunken pl-3 pr-8 py-1.5 rounded-lg cursor-pointer focus:outline-none focus:ring-2 focus:ring-accent/50 transition-colors"
              >
                {questions.map((q, idx) => {
                  const text = q.question.join(" ")
                  const truncated = text.length > 40 ? text.slice(0, 40) + "..." : text
                  return (
                    <option key={q.id} value={idx}>
                      {idx + 1}. {truncated}
                    </option>
                  )
                })}
              </select>
              <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-4 h-4 text-muted pointer-events-none" />
            </div>
          </div>
          <p className="text-primary text-lg">
            {currentQuestion.question.join(" ")}
          </p>
        </CardContent>
      </Card>

      {/* Error display if processing failed for this question */}
      {currentError && (
        <Card className="border-amber-500/50 bg-amber-500/5">
          <CardContent className="py-4">
            <div className="flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-amber-500 flex-shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-amber-600 dark:text-amber-400">
                  Failed to process this question
                </p>
                <p className="text-sm text-muted mt-1">{currentError}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Labeling Feedback Box */}
      <Card className={!genieResult && !currentError ? 'opacity-50' : ''}>
        <CardContent className="py-4 px-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-primary">Label This Question</h3>
            {!genieResult && !currentError && (
              <span className="text-xs text-muted">No results available</span>
            )}
          </div>
          <div className="flex flex-col md:flex-row gap-4">
            {/* Question 1: Was Genie's output correct? */}
            <div>
              <label className="text-sm text-secondary mb-2 block">Was Genie's output correct?</label>
              <div className="flex gap-2">
                <button
                  onClick={() => currentQuestion && onSetCorrectAnswer(currentQuestion.id, true)}
                  disabled={!genieResult && !currentError}
                  className={`px-3 py-1.5 text-sm rounded-lg border transition-colors ${
                    isCorrect === true
                      ? 'border-accent bg-accent/10 text-accent'
                      : 'border-default text-secondary hover:border-strong'
                  } disabled:cursor-not-allowed disabled:hover:border-default`}
                >
                  Yes
                </button>
                <button
                  onClick={() => currentQuestion && onSetCorrectAnswer(currentQuestion.id, false)}
                  disabled={!genieResult && !currentError}
                  className={`px-3 py-1.5 text-sm rounded-lg border transition-colors ${
                    isCorrect === false
                      ? 'border-accent bg-accent/10 text-accent'
                      : 'border-default text-secondary hover:border-strong'
                  } disabled:cursor-not-allowed disabled:hover:border-default`}
                >
                  No
                </button>
              </div>
            </div>

            {/* Question 2: What did Genie get wrong? */}
            <div className="flex-1">
              <label className="text-sm text-secondary mb-2 block">If not, what did Genie get wrong?</label>
              <textarea
                value={feedbackText}
                onChange={(e) => currentQuestion && onSetFeedbackText(currentQuestion.id, e.target.value)}
                placeholder="Describe what was incorrect..."
                disabled={!genieResult && !currentError}
                className="w-full p-2 text-sm rounded-lg border border-default bg-surface text-primary placeholder:text-muted resize-none focus:outline-none focus:ring-2 focus:ring-accent/50 transition-colors disabled:cursor-not-allowed disabled:bg-elevated"
                rows={2}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Side-by-side Output Display */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-6">
        {/* Genie's Output (left) */}
        <div className="space-y-3">
          <h3 className="text-sm font-medium text-muted">Genie's Output</h3>
          <Card>
            <CardContent className="py-4">
              {genieResult ? (
                genieResult.error ? (
                  <div className="text-danger text-sm py-4 text-center">{genieResult.error}</div>
                ) : (
                  <DataTable
                    columns={genieResult.columns}
                    data={genieResult.data}
                    truncated={genieResult.truncated}
                  />
                )
              ) : currentError ? (
                <div className="text-center py-8">
                  <AlertCircle className="w-8 h-8 text-amber-500 mx-auto mb-2" />
                  <p className="text-amber-500 text-sm">Processing failed</p>
                </div>
              ) : (
                <p className="text-muted text-sm text-center py-8">
                  No results available
                </p>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Expected Output (right) */}
        <div className="space-y-3">
          <h3 className="text-sm font-medium text-muted">Expected Output</h3>
          <Card>
            <CardContent className="py-4">
              {expectedResult ? (
                expectedResult.error ? (
                  <div className="text-danger text-sm py-4 text-center">{expectedResult.error}</div>
                ) : (
                  <DataTable
                    columns={expectedResult.columns}
                    data={expectedResult.data}
                    truncated={expectedResult.truncated}
                  />
                )
              ) : expectedSql ? (
                <p className="text-muted text-sm text-center py-8">
                  No results available
                </p>
              ) : (
                <p className="text-muted text-sm text-center py-8">
                  No expected SQL available
                </p>
              )}
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Side-by-side SQL Display */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Generated SQL (left) */}
        <div className="space-y-3">
          <h3 className="text-sm font-medium text-muted">Genie's Generated SQL</h3>
          {formattedGeneratedSql ? (
            <SqlCodeBlock code={formattedGeneratedSql} maxLines={15} />
          ) : currentError ? (
            <Card>
              <CardContent className="py-8 text-center">
                <AlertCircle className="w-6 h-6 text-amber-500 mx-auto mb-2" />
                <p className="text-amber-500 text-sm">Processing failed</p>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-8 text-center">
                <p className="text-muted text-sm">No SQL generated</p>
              </CardContent>
            </Card>
          )}
        </div>

        {/* Expected SQL (right) */}
        <div className="space-y-3">
          <h3 className="text-sm font-medium text-muted">Expected SQL</h3>
          {expectedSql ? (
            <SqlCodeBlock code={expectedSql} maxLines={15} />
          ) : (
            <Card>
              <CardContent className="py-8 text-center">
                <p className="text-muted text-sm">No expected SQL available</p>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Collapsible Diff View - only shown when both SQLs exist */}
      {formattedGeneratedSql && expectedSql && (
        <div className="space-y-3">
          <button
            onClick={() => setShowDiff(!showDiff)}
            className="flex items-center gap-2 text-sm font-medium text-secondary hover:text-primary transition-colors"
          >
            <GitCompare className="w-4 h-4" />
            <span>Show Diff View</span>
            <ChevronDown className={`w-4 h-4 transition-transform ${showDiff ? "rotate-180" : ""}`} />
          </button>

          {showDiff && (
            <SqlDiffView
              generatedSql={formattedGeneratedSql}
              expectedSql={expectedSql}
            />
          )}
        </div>
      )}

    </div>
  )
}
