/**
 * Custom hook for managing the Genie Space analysis state.
 */

import { useState, useCallback, useRef } from "react"
import type {
  Phase,
  AppMode,
  OptimizeView,
  SectionInfo,
  SectionAnalysis,
  FetchSpaceResponse,
  SqlExecutionResult,
  OptimizationSuggestion,
  SynthesisResult,
  GenieCreateResponse,
} from "@/types"
import {
  fetchSpace,
  parseSpaceJson,
  analyzeSection,
  analyzeAllSections as analyzeAllSectionsApi,
  streamOptimizations,
  mergeConfig,
  queryGenie,
  executeSql,
  createGenieSpace as createGenieSpaceApi,
} from "@/lib/api"
import { getBenchmarkQuestions, getExpectedSql } from "@/lib/benchmarkUtils"

export interface AnalysisState {
  mode: AppMode | null
  phase: Phase
  optimizeView: OptimizeView | null
  genieSpaceId: string
  spaceData: Record<string, unknown> | null
  sections: SectionInfo[]
  currentSectionIndex: number
  sectionAnalyses: SectionAnalysis[]
  allSectionsAnalyzed: boolean
  showChecklist: boolean
  showSettings: boolean
  isLoading: boolean
  error: string | null
  expandedSections: Record<string, boolean>
  analysisProgress: { completed: number; total: number } | null
  analyzingSection: number | null
  selectedSections: number[]  // Indices of selected sections for analysis
  analysisViewIndex: number  // Index within filtered analyzed sections list
  selectedQuestions: string[]
  hasLabelingSession: boolean
  // Cross-sectional analysis state
  synthesis: SynthesisResult | null
  isFullAnalysis: boolean
  // Labeling session state (persists across navigation)
  labelingCurrentIndex: number
  labelingGeneratedSql: Record<string, string>
  labelingGenieResults: Record<string, SqlExecutionResult | null>
  labelingExpectedResults: Record<string, SqlExecutionResult | null>
  labelingCorrectAnswers: Record<string, boolean | null>
  labelingFeedbackTexts: Record<string, string>
  labelingProcessingErrors: Record<string, string>
  // Benchmark processing state (upfront processing before labeling)
  isProcessingBenchmarks: boolean
  benchmarkProcessingProgress: { current: number; total: number } | null
  // Optimization state
  optimizationSuggestions: OptimizationSuggestion[] | null
  optimizationSummary: string | null
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
  mode: null,
  phase: "input",
  optimizeView: null,
  genieSpaceId: "",
  spaceData: null,
  sections: [],
  currentSectionIndex: 0,
  sectionAnalyses: [],
  allSectionsAnalyzed: false,
  showChecklist: false,
  showSettings: false,
  isLoading: false,
  error: null,
  expandedSections: {},
  analysisProgress: null,
  analyzingSection: null,
  selectedSections: [],  // Will be populated when sections are loaded
  analysisViewIndex: 0,
  selectedQuestions: [],
  hasLabelingSession: false,
  // Cross-sectional analysis state
  synthesis: null,
  isFullAnalysis: false,
  // Labeling session state
  labelingCurrentIndex: 0,
  labelingGeneratedSql: {},
  labelingGenieResults: {},
  labelingExpectedResults: {},
  labelingCorrectAnswers: {},
  labelingFeedbackTexts: {},
  labelingProcessingErrors: {},
  // Benchmark processing state
  isProcessingBenchmarks: false,
  benchmarkProcessingProgress: null,
  // Optimization state
  optimizationSuggestions: null,
  optimizationSummary: null,
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

  const setMode = useCallback((mode: AppMode | null) => {
    setState((prev) => {
      // If clearing mode or no space data, just update mode
      if (!mode || !prev.spaceData) {
        return { ...prev, mode }
      }
      // Space data loaded + mode selected → transition to ingest
      return {
        ...prev,
        mode,
        phase: "ingest",
        optimizeView: mode === "optimize" ? "benchmarks" : null,
      }
    })
  }, [])

  const setPhase = useCallback((phase: Phase) => {
    setState((prev) => ({ ...prev, phase, showChecklist: false, showSettings: false, optimizeView: null }))
  }, [])

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
      // Initialize selectedSections with all configured sections (where has_data is true)
      const configuredIndices = response.sections
        .map((s, i) => (s.has_data ? i : -1))
        .filter((i) => i !== -1)
      setState((prev) => ({
        ...prev,
        genieSpaceId: response.genie_space_id,
        spaceData: response.space_data,
        sections: response.sections,
        selectedSections: configuredIndices,
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
      // Initialize selectedSections with all configured sections (where has_data is true)
      const configuredIndices = response.sections
        .map((s, i) => (s.has_data ? i : -1))
        .filter((i) => i !== -1)
      setState((prev) => ({
        ...prev,
        genieSpaceId: response.genie_space_id,
        spaceData: response.space_data,
        sections: response.sections,
        selectedSections: configuredIndices,
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

  const startAnalysis = useCallback(() => {
    setState((prev) => ({
      ...prev,
      phase: "analysis",
      currentSectionIndex: 0,
      sectionAnalyses: [],
      allSectionsAnalyzed: false,
    }))
  }, [])

  const analyzeAllSections = useCallback(async () => {
    const { sections, spaceData, selectedSections } = state
    if (!spaceData || selectedSections.length === 0) return

    // Filter sections to only analyze selected ones
    const sectionsToAnalyze = selectedSections.map((i) => sections[i])

    setState((prev) => ({
      ...prev,
      isLoading: true,
      error: null,
      analysisProgress: { completed: 0, total: sectionsToAnalyze.length },
    }))

    try {
      const response = await analyzeAllSectionsApi(
        sectionsToAnalyze,
        spaceData,
        (completed, total) =>
          setState((prev) => ({ ...prev, analysisProgress: { completed, total } }))
      )

      // Map results back to original section indices (sparse array)
      const sparseAnalyses: SectionAnalysis[] = []
      selectedSections.forEach((sectionIndex, resultIndex) => {
        sparseAnalyses[sectionIndex] = response.analyses[resultIndex]
      })

      setState((prev) => ({
        ...prev,
        sectionAnalyses: sparseAnalyses,
        synthesis: response.synthesis,
        isFullAnalysis: response.is_full_analysis,
        allSectionsAnalyzed: true,
        phase: "summary",
        isLoading: false,
        analysisProgress: null,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Analysis failed",
        isLoading: false,
        analysisProgress: null,
      }))
    }
  }, [state.sections, state.spaceData, state.selectedSections])

  const analyzeSingleSection = useCallback(
    async (index: number) => {
      const { sections, spaceData } = state
      if (!spaceData || index >= sections.length) return

      setState((prev) => ({ ...prev, analyzingSection: index, error: null }))

      try {
        const section = sections[index]
        const analysis = await analyzeSection({
          section_name: section.name,
          section_data: section.data,
          full_space: spaceData,
        })

        setState((prev) => {
          const newAnalyses = [...prev.sectionAnalyses]
          newAnalyses[index] = analysis
          return {
            ...prev,
            sectionAnalyses: newAnalyses,
            currentSectionIndex: index,
            phase: "analysis",
            analyzingSection: null,
          }
        })
      } catch (err) {
        setState((prev) => ({
          ...prev,
          error: err instanceof Error ? err.message : "Analysis failed",
          analyzingSection: null,
        }))
      }
    },
    [state.sections, state.spaceData]
  )

  const analyzeCurrentSection = useCallback(async () => {
    const { sections, currentSectionIndex, spaceData, sectionAnalyses } = state

    if (!spaceData || currentSectionIndex >= sections.length) return

    const section = sections[currentSectionIndex]

    // Check if already analyzed
    if (sectionAnalyses.length > currentSectionIndex) return

    setState((prev) => ({ ...prev, isLoading: true, error: null }))

    try {
      const analysis = await analyzeSection({
        section_name: section.name,
        section_data: section.data,
        full_space: spaceData,
      })

      setState((prev) => {
        const newAnalyses = [...prev.sectionAnalyses, analysis]
        const allDone = newAnalyses.length === prev.sections.length

        return {
          ...prev,
          sectionAnalyses: newAnalyses,
          allSectionsAnalyzed: allDone,
          isLoading: false,
        }
      })
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Analysis failed",
        isLoading: false,
      }))
    }
  }, [state])

  const goToSection = useCallback((index: number) => {
    setState((prev) => {
      // Compute the view index within analyzed sections
      const analyzedIndices = prev.sectionAnalyses
        .map((a, i) => (a !== undefined ? i : -1))
        .filter((i) => i !== -1)
      const viewIndex = analyzedIndices.indexOf(index)
      return {
        ...prev,
        phase: "analysis",
        currentSectionIndex: index,
        analysisViewIndex: viewIndex >= 0 ? viewIndex : 0,
        showChecklist: false,
        showSettings: false,
      }
    })
  }, [])

  const setAnalysisViewIndex = useCallback((index: number) => {
    setState((prev) => {
      // Get the list of analyzed section indices
      const analyzedIndices = prev.sectionAnalyses
        .map((a, i) => (a !== undefined ? i : -1))
        .filter((i) => i !== -1)
      // Clamp index to valid range
      const clampedIndex = Math.max(0, Math.min(analyzedIndices.length - 1, index))
      const originalIndex = analyzedIndices[clampedIndex] ?? prev.currentSectionIndex
      return {
        ...prev,
        analysisViewIndex: clampedIndex,
        currentSectionIndex: originalIndex,
      }
    })
  }, [])

  const goToAnalysis = useCallback(() => {
    setState((prev) => {
      const analyzedIndices = prev.sectionAnalyses
        .map((a, i) => (a !== undefined ? i : -1))
        .filter((i) => i !== -1)
      const firstAnalyzedIndex = analyzedIndices[0] ?? 0
      return {
        ...prev,
        phase: "analysis",
        currentSectionIndex: firstAnalyzedIndex,
        analysisViewIndex: 0,
        showChecklist: false,
        showSettings: false,
        optimizeView: null,
      }
    })
  }, [])

  const nextSection = useCallback(() => {
    setState((prev) => {
      const nextIndex = prev.currentSectionIndex + 1
      if (nextIndex >= prev.sections.length) {
        return { ...prev, phase: "summary" }
      }
      return { ...prev, currentSectionIndex: nextIndex }
    })
  }, [])

  const prevSection = useCallback(() => {
    setState((prev) => ({
      ...prev,
      currentSectionIndex: Math.max(0, prev.currentSectionIndex - 1),
    }))
  }, [])

  const goToSummary = useCallback(() => {
    setState((prev) => ({ ...prev, phase: "summary", showChecklist: false, showSettings: false, optimizeView: null }))
  }, [])

  const goToIngest = useCallback(() => {
    setState((prev) => ({ ...prev, phase: "ingest", showChecklist: false, showSettings: false, optimizeView: null }))
  }, [])

  const toggleChecklist = useCallback(() => {
    setState((prev) => ({ ...prev, showChecklist: !prev.showChecklist, showSettings: false }))
  }, [])

  const toggleSettings = useCallback(() => {
    setState((prev) => ({ ...prev, showSettings: !prev.showSettings, showChecklist: false }))
  }, [])

  const toggleSectionExpanded = useCallback((sectionName: string) => {
    setState((prev) => ({
      ...prev,
      expandedSections: {
        ...prev.expandedSections,
        [sectionName]: !prev.expandedSections[sectionName],
      },
    }))
  }, [])

  const expandAllSections = useCallback(() => {
    setState((prev) => ({
      ...prev,
      expandedSections: Object.fromEntries(
        prev.sectionAnalyses.map((a) => [a.section_name, true])
      ),
    }))
  }, [])

  const collapseAllSections = useCallback(() => {
    setState((prev) => ({
      ...prev,
      expandedSections: {},
    }))
  }, [])

  const toggleSectionSelection = useCallback((index: number) => {
    setState((prev) => {
      const isSelected = prev.selectedSections.includes(index)
      return {
        ...prev,
        selectedSections: isSelected
          ? prev.selectedSections.filter((i) => i !== index)
          : [...prev.selectedSections, index].sort((a, b) => a - b),
      }
    })
  }, [])

  const selectAllSections = useCallback(() => {
    setState((prev) => {
      const configuredIndices = prev.sections
        .map((s, i) => (s.has_data ? i : -1))
        .filter((i) => i !== -1)
      return { ...prev, selectedSections: configuredIndices }
    })
  }, [])

  const deselectAllSections = useCallback(() => {
    setState((prev) => ({ ...prev, selectedSections: [] }))
  }, [])

  const goToBenchmarks = useCallback(() => {
    setState((prev) => ({ ...prev, optimizeView: "benchmarks", showChecklist: false, showSettings: false }))
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
      showChecklist: false,
      showSettings: false,
      hasLabelingSession: true,
    }))
  }, [])

  const goToFeedback = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "feedback",
      showChecklist: false,
      showSettings: false,
    }))
  }, [])

  const goToOptimization = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "optimization",
      showChecklist: false,
      showSettings: false,
    }))
  }, [])

  const goToPreview = useCallback(() => {
    setState((prev) => ({
      ...prev,
      optimizeView: "preview",
      showChecklist: false,
      showSettings: false,
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

  const selectAllByPriority = useCallback((priority: string) => {
    setState((prev) => {
      if (!prev.optimizationSuggestions) return prev
      const indices = prev.optimizationSuggestions
        .map((s, i) => (s.priority === priority ? i : -1))
        .filter((i) => i !== -1)
      return { ...prev, selectedSuggestions: new Set([...prev.selectedSuggestions, ...indices]) }
    })
  }, [])

  const deselectAllByPriority = useCallback((priority: string) => {
    setState((prev) => {
      if (!prev.optimizationSuggestions) return prev
      const indicesToRemove = new Set(
        prev.optimizationSuggestions
          .map((s, i) => (s.priority === priority ? i : -1))
          .filter((i) => i !== -1)
      )
      return {
        ...prev,
        selectedSuggestions: new Set([...prev.selectedSuggestions].filter((i) => !indicesToRemove.has(i))),
      }
    })
  }, [])

  const startOptimization = useCallback(() => {
    const { genieSpaceId, spaceData, selectedQuestions, labelingCorrectAnswers, labelingFeedbackTexts } = state
    if (!spaceData) return

    setState((prev) => ({ ...prev, isOptimizing: true, error: null, optimizeView: "optimization" }))

    // Get benchmark questions and build labeling feedback
    const allQuestions = getBenchmarkQuestions(spaceData)

    // Build feedback items from selected questions
    const labelingFeedback = selectedQuestions.map(id => {
      const question = allQuestions.find(q => q.id === id)
      return {
        question_text: question?.question.join(" ") || "",
        is_correct: labelingCorrectAnswers[id] ?? null,
        feedback_text: labelingFeedbackTexts[id] || null,
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
  }, [state.genieSpaceId, state.spaceData, state.selectedQuestions, state.labelingCorrectAnswers, state.labelingFeedbackTexts])

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
      sections: [],
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

  const reset = useCallback(() => {
    setState(initialState)
  }, [])

  return {
    state,
    actions: {
      setMode,
      setPhase,
      setError,
      setLoading,
      handleFetchSpace,
      handleParseJson,
      startAnalysis,
      analyzeAllSections,
      analyzeSingleSection,
      analyzeCurrentSection,
      goToSection,
      setAnalysisViewIndex,
      goToAnalysis,
      nextSection,
      prevSection,
      goToSummary,
      goToIngest,
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
      selectAllByPriority,
      deselectAllByPriority,
      toggleChecklist,
      toggleSettings,
      toggleSectionExpanded,
      expandAllSections,
      collapseAllSections,
      toggleSectionSelection,
      selectAllSections,
      deselectAllSections,
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
    },
  }
}

