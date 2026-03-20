/**
 * Custom hook for managing the Genie Space workspace state.
 * Handles space data loading and the optimization workflow.
 */

import { useState, useCallback, useRef } from "react"
import type {
  OptimizeView,
  SqlExecutionResult,
  OptimizationSuggestion,
  GenieCreateResponse,
  ComparisonResult,
  FailureDiagnosis,
  FetchSpaceResponse,
} from "@/types"
import {
  fetchSpace,
  parseSpaceJson,
  streamOptimizations,
  mergeConfig,
  queryGenie,
  executeSql,
  compareResults,
  createGenieSpace as createGenieSpaceApi,
} from "@/lib/api"
import { getBenchmarkQuestions, getExpectedSql } from "@/lib/benchmarkUtils"

export interface AnalysisState {
  genieSpaceId: string
  spaceData: Record<string, unknown> | null
  isLoading: boolean
  error: string | null
  // Optimize flow
  optimizeView: OptimizeView | null
  selectedQuestions: string[]
  hasLabelingSession: boolean
  // Labeling session state (persists across navigation)
  labelingCurrentIndex: number
  labelingGeneratedSql: Record<string, string>
  labelingGenieResults: Record<string, SqlExecutionResult | null>
  labelingExpectedResults: Record<string, SqlExecutionResult | null>
  labelingCorrectAnswers: Record<string, boolean | null>
  labelingFeedbackTexts: Record<string, string>
  labelingProcessingErrors: Record<string, string>
  // Auto-comparison state (QW1)
  labelingComparisons: Record<string, ComparisonResult | null>
  // Benchmark processing state (upfront processing before labeling)
  isProcessingBenchmarks: boolean
  benchmarkProcessingProgress: { current: number; total: number } | null
  // Optimization state
  optimizationSuggestions: OptimizationSuggestion[] | null
  optimizationSummary: string | null
  optimizationDiagnosis: FailureDiagnosis[]
  isOptimizing: boolean
  // Preview state
  selectedSuggestions: Set<number>  // Original indices of selected suggestions
  previewConfig: Record<string, unknown> | null
  previewSummary: string | null
  isGeneratingPreview: boolean
  // Genie creation state
  isCreatingGenie: boolean
  genieCreateError: string | null
  createdGenieResult: GenieCreateResponse | null
}

const initialState: AnalysisState = {
  genieSpaceId: "",
  spaceData: null,
  isLoading: false,
  error: null,
  // Optimize flow
  optimizeView: null,
  selectedQuestions: [],
  hasLabelingSession: false,
  // Labeling session state
  labelingCurrentIndex: 0,
  labelingGeneratedSql: {},
  labelingGenieResults: {},
  labelingExpectedResults: {},
  labelingCorrectAnswers: {},
  labelingFeedbackTexts: {},
  labelingProcessingErrors: {},
  // Auto-comparison state (QW1)
  labelingComparisons: {},
  // Benchmark processing state
  isProcessingBenchmarks: false,
  benchmarkProcessingProgress: null,
  // Optimization state
  optimizationSuggestions: null,
  optimizationSummary: null,
  optimizationDiagnosis: [],
  isOptimizing: false,
  // Preview state
  selectedSuggestions: new Set<number>(),
  previewConfig: null,
  previewSummary: null,
  isGeneratingPreview: false,
  // Genie creation state
  isCreatingGenie: false,
  genieCreateError: null,
  createdGenieResult: null,
}

export function useAnalysis() {
  const [state, setState] = useState<AnalysisState>(initialState)
  const benchmarkProcessingCancelledRef = useRef(false)

  const setError = useCallback((error: string | null) => {
    setState((prev) => ({ ...prev, error, isLoading: false }))
  }, [])

  const setLoading = useCallback((isLoading: boolean) => {
    setState((prev) => ({ ...prev, isLoading }))
  }, [])

  const handleFetchSpace = useCallback(async (spaceId: string) => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }))

    try {
      const response: FetchSpaceResponse = await fetchSpace(spaceId)
      setState((prev) => ({
        ...prev,
        genieSpaceId: response.genie_space_id,
        spaceData: response.space_data,
        isLoading: false,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Failed to fetch space",
        isLoading: false,
      }))
    }
  }, [])

  const handleParseJson = useCallback(async (jsonContent: string) => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }))

    try {
      const response: FetchSpaceResponse = await parseSpaceJson(jsonContent)
      setState((prev) => ({
        ...prev,
        genieSpaceId: response.genie_space_id,
        spaceData: response.space_data,
        isLoading: false,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Failed to parse JSON",
        isLoading: false,
      }))
    }
  }, [])

  const goToBenchmarks = useCallback(() => {
    setState((prev) => ({ ...prev, optimizeView: "benchmarks" }))
  }, [])

  const toggleQuestionSelection = useCallback((questionId: string) => {
    setState((prev) => {
      const isSelected = prev.selectedQuestions.includes(questionId)
      return {
        ...prev,
        selectedQuestions: isSelected
          ? prev.selectedQuestions.filter((id) => id !== questionId)
          : [...prev.selectedQuestions, questionId],
      }
    })
  }, [])

  const selectAllQuestions = useCallback((questionIds: string[]) => {
    setState((prev) => ({ ...prev, selectedQuestions: questionIds }))
  }, [])

  const deselectAllQuestions = useCallback(() => {
    setState((prev) => ({ ...prev, selectedQuestions: [] }))
  }, [])

  const goToLabeling = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "labeling",
      hasLabelingSession: true,
    }))
  }, [])

  const goToFeedback = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "feedback",
    }))
  }, [])

  const goToOptimization = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "optimization",
    }))
  }, [])

  const goToPreview = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "preview",
    }))
  }, [])

  const toggleSuggestionSelection = useCallback((index: number) => {
    setState((prev) => {
      const newSelected = new Set(prev.selectedSuggestions)
      if (newSelected.has(index)) {
        newSelected.delete(index)
      } else {
        newSelected.add(index)
      }
      return { ...prev, selectedSuggestions: newSelected }
    })
  }, [])

  const selectAllSuggestions = useCallback(() => {
    setState((prev) => {
      if (!prev.optimizationSuggestions) return prev
      const allIndices = new Set(prev.optimizationSuggestions.map((_, i) => i))
      return { ...prev, selectedSuggestions: allIndices }
    })
  }, [])

  const deselectAllSuggestions = useCallback(() => {
    setState((prev) => ({ ...prev, selectedSuggestions: new Set<number>() }))
  }, [])

  const startOptimization = useCallback(() => {
    const { genieSpaceId, spaceData, selectedQuestions, labelingCorrectAnswers, labelingFeedbackTexts, labelingComparisons } = state
    if (!spaceData) return

    setState((prev) => ({ ...prev, isOptimizing: true, error: null, optimizeView: "optimization" }))

    // Get benchmark questions and build labeling feedback
    const allQuestions = getBenchmarkQuestions(spaceData)

    // Build feedback items from selected questions with auto-comparison context (QW1)
    const labelingFeedback = selectedQuestions.map(id => {
      const question = allQuestions.find(q => q.id === id)
      const comparison = labelingComparisons[id]
      const autoLabel = comparison?.auto_label ?? null
      const userAnswer = labelingCorrectAnswers[id] ?? null

      // User overrode if auto-label exists and doesn't match user's answer
      const userOverrode = autoLabel !== null && userAnswer !== null && autoLabel !== userAnswer

      return {
        question_text: question?.question.join(" ") || "",
        is_correct: userAnswer,
        feedback_text: labelingFeedbackTexts[id] || null,
        auto_label: autoLabel,
        user_overrode_auto_label: userOverrode,
        auto_comparison_summary: comparison?.summary || null,
      }
    })

    // Use streaming API to avoid proxy timeouts
    streamOptimizations(
      genieSpaceId,
      spaceData,
      labelingFeedback,
      // onProgress - heartbeats to keep connection alive (no UI update needed)
      () => {},
      // onComplete
      (response) => {
        setState((prev) => ({
          ...prev,
          optimizationSuggestions: response.suggestions,
          optimizationSummary: response.summary,
          optimizationDiagnosis: response.diagnosis || [],
          isOptimizing: false,
        }))
      },
      // onError
      (err) => {
        setState((prev) => ({
          ...prev,
          error: err instanceof Error ? err.message : "Optimization failed",
          isOptimizing: false,
        }))
      }
    )
  }, [state.genieSpaceId, state.spaceData, state.selectedQuestions, state.labelingCorrectAnswers, state.labelingFeedbackTexts, state.labelingComparisons])

  const generatePreviewConfig = useCallback(async () => {
    const { spaceData, optimizationSuggestions, selectedSuggestions } = state
    if (!spaceData || !optimizationSuggestions || selectedSuggestions.size === 0) return

    setState((prev) => ({
      ...prev,
      isGeneratingPreview: true,
      error: null,
      optimizeView: "preview",
      // Reset creation state when generating new preview
      isCreatingGenie: false,
      genieCreateError: null,
      createdGenieResult: null,
    }))

    // Get selected suggestions by their original indices
    const selectedSuggestionsList = Array.from(selectedSuggestions)
      .sort((a, b) => a - b)
      .map((i) => optimizationSuggestions[i])
      .filter(Boolean)

    try {
      const response = await mergeConfig(spaceData, selectedSuggestionsList)
      setState((prev) => ({
        ...prev,
        previewConfig: response.merged_config,
        previewSummary: response.summary,
        isGeneratingPreview: false,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Preview generation failed",
        isGeneratingPreview: false,
      }))
    }
  }, [state.spaceData, state.optimizationSuggestions, state.selectedSuggestions])

  const createGenieSpace = useCallback(async (displayName: string) => {
    const { previewConfig } = state
    if (!previewConfig) return

    setState((prev) => ({
      ...prev,
      isCreatingGenie: true,
      genieCreateError: null,
    }))

    try {
      const response = await createGenieSpaceApi({
        display_name: displayName,
        merged_config: previewConfig,
      })
      setState((prev) => ({
        ...prev,
        isCreatingGenie: false,
        createdGenieResult: response,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        isCreatingGenie: false,
        genieCreateError: err instanceof Error ? err.message : "Failed to create Genie Space",
      }))
    }
  }, [state.previewConfig])

  const clearSpaceData = useCallback(() => {
    setState((prev) => ({
      ...prev,
      genieSpaceId: "",
      spaceData: null,
    }))
  }, [])

  // Labeling session actions
  const setLabelingCurrentIndex = useCallback((index: number) => {
    setState((prev) => ({ ...prev, labelingCurrentIndex: index }))
  }, [])

  const setLabelingGeneratedSql = useCallback((questionId: string, sql: string) => {
    setState((prev) => ({
      ...prev,
      labelingGeneratedSql: { ...prev.labelingGeneratedSql, [questionId]: sql },
    }))
  }, [])

  const setLabelingGenieResult = useCallback(
    (questionId: string, result: SqlExecutionResult | null) => {
      setState((prev) => ({
        ...prev,
        labelingGenieResults: { ...prev.labelingGenieResults, [questionId]: result },
      }))
    },
    []
  )

  const setLabelingExpectedResult = useCallback(
    (questionId: string, result: SqlExecutionResult | null) => {
      setState((prev) => ({
        ...prev,
        labelingExpectedResults: { ...prev.labelingExpectedResults, [questionId]: result },
      }))
    },
    []
  )

  const setLabelingCorrectAnswer = useCallback(
    (questionId: string, answer: boolean | null) => {
      setState((prev) => ({
        ...prev,
        labelingCorrectAnswers: { ...prev.labelingCorrectAnswers, [questionId]: answer },
      }))
    },
    []
  )

  const setLabelingFeedbackText = useCallback((questionId: string, text: string) => {
    setState((prev) => ({
      ...prev,
      labelingFeedbackTexts: { ...prev.labelingFeedbackTexts, [questionId]: text },
    }))
  }, [])

  const clearLabelingSession = useCallback(() => {
    setState((prev) => ({
      ...prev,
      hasLabelingSession: false,
      labelingCurrentIndex: 0,
      labelingGeneratedSql: {},
      labelingGenieResults: {},
      labelingExpectedResults: {},
      labelingCorrectAnswers: {},
      labelingFeedbackTexts: {},
      labelingProcessingErrors: {},
      labelingComparisons: {},
    }))
  }, [])

  // Benchmark processing - process all questions upfront before labeling
  const processBenchmarksAndGoToLabeling = useCallback(async () => {
    const { genieSpaceId, spaceData, selectedQuestions } = state
    if (!spaceData || selectedQuestions.length === 0) return

    // Get all benchmark questions
    const allQuestions = getBenchmarkQuestions(spaceData)

    // Reset cancellation flag
    benchmarkProcessingCancelledRef.current = false

    setState((prev) => ({
      ...prev,
      isProcessingBenchmarks: true,
      benchmarkProcessingProgress: { current: 0, total: selectedQuestions.length },
      labelingProcessingErrors: {},
    }))

    // Process each question sequentially
    for (let i = 0; i < selectedQuestions.length; i++) {
      if (benchmarkProcessingCancelledRef.current) {
        // User cancelled - stop processing but don't navigate
        setState((prev) => ({
          ...prev,
          isProcessingBenchmarks: false,
          benchmarkProcessingProgress: null,
        }))
        return
      }

      const questionId = selectedQuestions[i]
      const question = allQuestions.find(q => q.id === questionId)

      // Update progress
      setState((prev) => ({
        ...prev,
        benchmarkProcessingProgress: { current: i + 1, total: selectedQuestions.length },
      }))

      if (!question) continue

      // Skip if already processed
      if (state.labelingGeneratedSql[questionId]) continue

      try {
        const questionText = question.question.join(" ")
        const response = await queryGenie(genieSpaceId, questionText)

        if (benchmarkProcessingCancelledRef.current) continue

        if (response.status === "COMPLETED" && response.sql) {
          // Store generated SQL
          setState((prev) => ({
            ...prev,
            labelingGeneratedSql: { ...prev.labelingGeneratedSql, [questionId]: response.sql! },
          }))

          // Execute both SQLs in parallel
          const expectedSql = getExpectedSql(question)
          const [genieExec, expectedExec] = await Promise.allSettled([
            executeSql(response.sql),
            expectedSql ? executeSql(expectedSql) : Promise.resolve(null),
          ])

          if (benchmarkProcessingCancelledRef.current) continue

          // Store Genie result
          if (genieExec.status === "fulfilled" && genieExec.value) {
            setState((prev) => ({
              ...prev,
              labelingGenieResults: { ...prev.labelingGenieResults, [questionId]: genieExec.value },
            }))
          } else if (genieExec.status === "rejected") {
            setState((prev) => ({
              ...prev,
              labelingGenieResults: {
                ...prev.labelingGenieResults,
                [questionId]: {
                  columns: [],
                  data: [],
                  row_count: 0,
                  truncated: false,
                  error: genieExec.reason?.message || "Failed to execute Genie SQL",
                },
              },
            }))
          }

          // Store Expected result
          if (expectedExec.status === "fulfilled" && expectedExec.value) {
            setState((prev) => ({
              ...prev,
              labelingExpectedResults: { ...prev.labelingExpectedResults, [questionId]: expectedExec.value },
            }))
          } else if (expectedExec.status === "rejected") {
            setState((prev) => ({
              ...prev,
              labelingExpectedResults: {
                ...prev.labelingExpectedResults,
                [questionId]: {
                  columns: [],
                  data: [],
                  row_count: 0,
                  truncated: false,
                  error: expectedExec.reason?.message || "Failed to execute Expected SQL",
                },
              },
            }))
          }

          // Auto-compare results (QW1) — semantic LLM comparison with SQL + question context
          const genieRes = genieExec.status === "fulfilled" ? genieExec.value : null
          const expectedRes = expectedExec.status === "fulfilled" ? expectedExec.value : null
          if (genieRes && expectedRes) {
            try {
              const comparison = await compareResults(
                genieRes,
                expectedRes,
                response.sql,
                expectedSql ?? undefined,
                questionText,
              )
              if (benchmarkProcessingCancelledRef.current) continue
              setState((prev) => ({
                ...prev,
                labelingComparisons: { ...prev.labelingComparisons, [questionId]: comparison },
                // Pre-fill the correct answer with auto-label
                labelingCorrectAnswers: {
                  ...prev.labelingCorrectAnswers,
                  [questionId]: prev.labelingCorrectAnswers[questionId] ?? comparison.auto_label,
                },
              }))
            } catch {
              // Comparison failed — not critical, just skip auto-label
            }
          }
        } else {
          // Genie failed to generate SQL
          const errorMsg = response.error || "Genie did not generate SQL for this question"
          setState((prev) => ({
            ...prev,
            labelingProcessingErrors: { ...prev.labelingProcessingErrors, [questionId]: errorMsg },
          }))
        }
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : "Failed to process question"
        setState((prev) => ({
          ...prev,
          labelingProcessingErrors: { ...prev.labelingProcessingErrors, [questionId]: errorMsg },
        }))
      }
    }

    // Processing complete - navigate to labeling
    if (!benchmarkProcessingCancelledRef.current) {
      setState((prev) => ({
        ...prev,
        isProcessingBenchmarks: false,
        benchmarkProcessingProgress: null,
        optimizeView: "labeling",
        hasLabelingSession: true,
      }))
    }
  }, [state.genieSpaceId, state.spaceData, state.selectedQuestions, state.labelingGeneratedSql])

  const cancelBenchmarkProcessing = useCallback(() => {
    benchmarkProcessingCancelledRef.current = true
    setState((prev) => ({
      ...prev,
      isProcessingBenchmarks: false,
      benchmarkProcessingProgress: null,
    }))
  }, [])

  const resetOptimizeFlow = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: null,
      selectedQuestions: [],
      hasLabelingSession: false,
      labelingCurrentIndex: 0,
      labelingGeneratedSql: {},
      labelingGenieResults: {},
      labelingExpectedResults: {},
      labelingCorrectAnswers: {},
      labelingFeedbackTexts: {},
      labelingProcessingErrors: {},
      labelingComparisons: {},
      isProcessingBenchmarks: false,
      benchmarkProcessingProgress: null,
      optimizationSuggestions: null,
      optimizationSummary: null,
      optimizationDiagnosis: [],
      isOptimizing: false,
      selectedSuggestions: new Set<number>(),
      previewConfig: null,
      previewSummary: null,
      isGeneratingPreview: false,
      isCreatingGenie: false,
      genieCreateError: null,
      createdGenieResult: null,
      error: null,
    }))
  }, [])

  const reset = useCallback(() => {
    setState(initialState)
  }, [])

  return {
    state,
    actions: {
      setError,
      setLoading,
      handleFetchSpace,
      handleParseJson,
      goToBenchmarks,
      goToLabeling,
      goToFeedback,
      goToOptimization,
      goToPreview,
      startOptimization,
      generatePreviewConfig,
      createGenieSpace,
      toggleSuggestionSelection,
      selectAllSuggestions,
      deselectAllSuggestions,
      toggleQuestionSelection,
      selectAllQuestions,
      deselectAllQuestions,
      clearSpaceData,
      // Labeling session actions
      setLabelingCurrentIndex,
      setLabelingGeneratedSql,
      setLabelingGenieResult,
      setLabelingExpectedResult,
      setLabelingCorrectAnswer,
      setLabelingFeedbackText,
      clearLabelingSession,
      // Benchmark processing actions
      processBenchmarksAndGoToLabeling,
      cancelBenchmarkProcessing,
      reset,
      resetOptimizeFlow,
    },
  }
}
