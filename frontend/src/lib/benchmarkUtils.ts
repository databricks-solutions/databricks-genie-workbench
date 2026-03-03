/**
 * Shared utilities for working with benchmark questions.
 */

import type { BenchmarkQuestion } from "@/types"

/**
 * Extract benchmark questions from Genie Space data.
 *
 * @param spaceData - The full Genie Space configuration
 * @returns Array of benchmark questions, or empty array if none exist
 */
export function getBenchmarkQuestions(
  spaceData: Record<string, unknown> | null
): BenchmarkQuestion[] {
  if (!spaceData) return []
  const benchmarks = spaceData.benchmarks as { questions?: BenchmarkQuestion[] } | undefined
  return benchmarks?.questions || []
}

/**
 * Get expected SQL from a benchmark question's answer.
 * Looks for SQL format first, then falls back to first answer.
 *
 * @param question - The benchmark question
 * @returns The expected SQL string, or null if not available
 */
export function getExpectedSql(question: BenchmarkQuestion | undefined): string | null {
  if (!question?.answer?.length) return null
  const sqlAnswer = question.answer.find(a => a.format.toLowerCase() === "sql")
  const answer = sqlAnswer || question.answer[0]
  // Content array elements already include \n, so join with empty string
  return answer?.content?.join("") || null
}

/**
 * Filter benchmark questions by their IDs, preserving the order of the IDs.
 *
 * @param spaceData - The full Genie Space configuration
 * @param selectedIds - Array of question IDs to filter by
 * @returns Array of benchmark questions in the order of selectedIds
 */
export function getSelectedBenchmarkQuestions(
  spaceData: Record<string, unknown> | null,
  selectedIds: string[]
): BenchmarkQuestion[] {
  const allQuestions = getBenchmarkQuestions(spaceData)
  return selectedIds
    .map(id => allQuestions.find(q => q.id === id))
    .filter((q): q is BenchmarkQuestion => q !== undefined)
}
