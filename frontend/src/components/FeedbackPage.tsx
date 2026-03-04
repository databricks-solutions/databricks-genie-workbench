/**
 * Feedback page showing summary of labeling session results.
 */

import { useMemo } from "react"
import { ArrowLeft, Sparkles, Check, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { getSelectedBenchmarkQuestions } from "@/lib/benchmarkUtils"

interface FeedbackPageProps {
  spaceData: Record<string, unknown>
  selectedQuestions: string[]
  correctAnswers: Record<string, boolean | null>
  feedbackTexts: Record<string, string>
  onBack: () => void
  onBeginOptimization: () => void
}

export function FeedbackPage({
  spaceData,
  selectedQuestions,
  correctAnswers,
  feedbackTexts,
  onBack,
  onBeginOptimization,
}: FeedbackPageProps) {
  // Get the selected questions in order
  const questions = useMemo(
    () => getSelectedBenchmarkQuestions(spaceData, selectedQuestions),
    [spaceData, selectedQuestions]
  )

  const correctCount = questions.filter(q => correctAnswers[q.id] === true).length
  const incorrectCount = questions.filter(q => correctAnswers[q.id] === false).length

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-primary">
            Labeling Feedback Summary
          </h1>
          <p className="text-muted">
            {questions.length} questions reviewed
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" onClick={onBack}>
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Labeling
          </Button>
          <Button
            onClick={onBeginOptimization}
            className="bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-600 hover:to-purple-600 text-white"
          >
            <Sparkles className="w-4 h-4 mr-2" />
            Begin Optimization
          </Button>
        </div>
      </div>

      {/* Summary Stats */}
      <div className="flex gap-4">
        <div className="flex items-center gap-2 text-sm">
          <div className="w-3 h-3 rounded-full bg-green-500" />
          <span className="text-secondary">{correctCount} correct</span>
        </div>
        <div className="flex items-center gap-2 text-sm">
          <div className="w-3 h-3 rounded-full bg-red-500" />
          <span className="text-secondary">{incorrectCount} incorrect</span>
        </div>
      </div>

      {/* Questions List */}
      <div className="space-y-4">
        {questions.map((question, index) => {
          const isCorrect = correctAnswers[question.id]
          const feedback = feedbackTexts[question.id]
          const questionText = question.question.join(" ")

          return (
            <Card key={question.id}>
              <CardContent className="py-4">
                <div className="space-y-2">
                  <div className="flex items-start justify-between gap-4">
                    <p className="text-primary">
                      <span className="text-muted font-mono text-sm mr-2">
                        {index + 1}.
                      </span>
                      {questionText}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    {isCorrect === true ? (
                      <>
                        <div className="flex items-center gap-1.5 text-green-600 dark:text-green-400">
                          <Check className="w-4 h-4" />
                          <span className="text-sm font-medium">Correct</span>
                        </div>
                      </>
                    ) : isCorrect === false ? (
                      <>
                        <div className="flex items-center gap-1.5 text-red-600 dark:text-red-400">
                          <X className="w-4 h-4" />
                          <span className="text-sm font-medium">Incorrect</span>
                        </div>
                      </>
                    ) : (
                      <span className="text-sm text-muted">Not labeled</span>
                    )}
                  </div>
                  {isCorrect === false && feedback && (
                    <div className="mt-2 pl-4 border-l-2 border-red-300 dark:border-red-700">
                      <p className="text-sm text-secondary">
                        <span className="text-muted">Feedback:</span> {feedback}
                      </p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}
