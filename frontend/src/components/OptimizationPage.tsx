/**
 * OptimizationPage component displaying AI-generated optimization suggestions.
 * Organized by optimization lever (QW5) with failure diagnosis (QW4).
 */

import { useMemo } from "react"
import { ArrowLeft, Loader2, Sparkles, AlertTriangle, Eye, Stethoscope } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { AccordionItem } from "@/components/ui/accordion"
import { SuggestionCard } from "@/components/SuggestionCard"
import type { OptimizationSuggestion, FailureDiagnosis } from "@/types"

interface OptimizationPageProps {
  suggestions: OptimizationSuggestion[] | null
  summary: string | null
  diagnosis: FailureDiagnosis[]
  isLoading: boolean
  error: string | null
  selectedSuggestions: Set<number>
  onBack: () => void
  onToggleSuggestionSelection: (index: number) => void
  onCreateNewGenie: () => void
}

type SuggestionItem = { suggestion: OptimizationSuggestion; originalIndex: number }

// Lever-based grouping (QW5)
const LEVER_ORDER = [
  {
    key: "data_model",
    label: "Data Model",
    description: "Fix data model issues before adjusting instructions",
    categories: ["description", "synonym", "column_discovery"],
  },
  {
    key: "joins",
    label: "Joins & Relationships",
    description: "Ensure tables are properly connected",
    categories: ["join_spec"],
  },
  {
    key: "sql_assets",
    label: "SQL Assets",
    description: "Improve filters, expressions, measures, and examples",
    categories: ["filter", "expression", "measure", "sql_example"],
  },
  {
    key: "instructions",
    label: "Instructions",
    description: "Fine-tune text instructions last — structural fixes have more impact",
    categories: ["instruction"],
  },
] as const

interface LeverGroup {
  items: SuggestionItem[]
  highCount: number
  mediumCount: number
  lowCount: number
  selectedCount: number
}

const PRIORITY_CONFIG = {
  high: {
    label: "High",
    dotColor: "bg-red-500",
    badgeClass: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
  },
  medium: {
    label: "Medium",
    dotColor: "bg-amber-500",
    badgeClass: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400",
  },
  low: {
    label: "Low",
    dotColor: "bg-blue-500",
    badgeClass: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
  },
} as const

// Failure category color scheme — each category maps to a shared color
const FAILURE_COLORS = {
  table_column: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
  join: "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400",
  logic: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400",
  interpretation: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400",
  config: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
} as const

// Failure type display labels
const FAILURE_TYPE_LABELS: Record<string, { label: string; color: string }> = {
  wrong_table: { label: "Wrong Table", color: FAILURE_COLORS.table_column },
  wrong_column: { label: "Wrong Column", color: FAILURE_COLORS.table_column },
  missing_column: { label: "Missing Column", color: FAILURE_COLORS.table_column },
  wrong_table_for_metric: { label: "Wrong Table for Metric", color: FAILURE_COLORS.table_column },
  missing_join_spec: { label: "Missing Join", color: FAILURE_COLORS.join },
  wrong_join: { label: "Wrong Join", color: FAILURE_COLORS.join },
  cartesian_product: { label: "Cartesian Product", color: FAILURE_COLORS.join },
  wrong_aggregation: { label: "Wrong Aggregation", color: FAILURE_COLORS.logic },
  wrong_filter: { label: "Wrong Filter", color: FAILURE_COLORS.logic },
  missing_filter: { label: "Missing Filter", color: FAILURE_COLORS.logic },
  wrong_grouping: { label: "Wrong Grouping", color: FAILURE_COLORS.logic },
  wrong_date_handling: { label: "Date Handling", color: FAILURE_COLORS.interpretation },
  entity_mismatch: { label: "Entity Mismatch", color: FAILURE_COLORS.interpretation },
  ambiguous_query: { label: "Ambiguous Query", color: FAILURE_COLORS.interpretation },
  missing_description: { label: "Missing Description", color: FAILURE_COLORS.config },
  missing_synonym: { label: "Missing Synonym", color: FAILURE_COLORS.config },
  misleading_instruction: { label: "Misleading Instruction", color: FAILURE_COLORS.config },
}

export function OptimizationPage({
  suggestions,
  summary,
  diagnosis,
  isLoading,
  error,
  selectedSuggestions,
  onBack,
  onToggleSuggestionSelection,
  onCreateNewGenie,
}: OptimizationPageProps) {
  // Group suggestions by lever then priority (QW5)
  const leverGroups = useMemo(() => {
    const result: Record<string, LeverGroup> = {}
    for (const lever of LEVER_ORDER) {
      result[lever.key] = { items: [], highCount: 0, mediumCount: 0, lowCount: 0, selectedCount: 0 }
    }
    // "other" for categories not matching any lever
    result["other"] = { items: [], highCount: 0, mediumCount: 0, lowCount: 0, selectedCount: 0 }

    if (!suggestions) return result

    // Build a category -> lever key map
    const categoryToLever: Record<string, string> = {}
    for (const lever of LEVER_ORDER) {
      for (const cat of lever.categories) {
        categoryToLever[cat] = lever.key
      }
    }

    suggestions.forEach((suggestion, index) => {
      const leverKey = categoryToLever[suggestion.category] || "other"
      const group = result[leverKey]
      const item: SuggestionItem = { suggestion, originalIndex: index }
      group.items.push(item)
      if (suggestion.priority === "high") group.highCount++
      else if (suggestion.priority === "medium") group.mediumCount++
      else group.lowCount++
      if (selectedSuggestions.has(index)) group.selectedCount++
    })

    // Sort items within each lever by priority (high > medium > low)
    const priorityOrder = { high: 0, medium: 1, low: 2 }
    for (const group of Object.values(result)) {
      group.items.sort((a, b) =>
        (priorityOrder[a.suggestion.priority as keyof typeof priorityOrder] ?? 3) -
        (priorityOrder[b.suggestion.priority as keyof typeof priorityOrder] ?? 3)
      )
    }

    return result
  }, [suggestions, selectedSuggestions])

  // Derive priority counts from the already-memoized lever groups (avoids .filter() in render)
  const priorityCounts = useMemo(() => {
    const counts = { high: 0, medium: 0, low: 0 }
    for (const group of Object.values(leverGroups)) {
      counts.high += group.highCount
      counts.medium += group.mediumCount
      counts.low += group.lowCount
    }
    return counts
  }, [leverGroups])

  const totalCount = suggestions?.length || 0
  const selectedCount = selectedSuggestions.size

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-primary">
            Optimization Suggestions
          </h1>
          <p className="text-muted">
            {isLoading
              ? "Analyzing your configuration..."
              : `${totalCount} suggestions generated`}
          </p>
        </div>
        <Button variant="outline" onClick={onBack}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Feedback
        </Button>
      </div>

      {/* Loading state */}
      {isLoading && (
        <Card className="border-accent/30 bg-accent/5">
          <CardContent className="py-12 flex flex-col items-center justify-center gap-4">
            <div className="relative flex items-center justify-center">
              <Loader2 className="w-12 h-12 text-accent animate-spin" />
              <Sparkles className="w-5 h-5 text-accent absolute animate-pulse" />
            </div>
            <div className="text-center">
              <p className="text-lg font-medium text-primary">
                Generating optimization suggestions
              </p>
              <p className="text-sm text-muted mt-1">
                AI is analyzing your Genie Space configuration and labeling feedback...
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error state */}
      {error && !isLoading && (
        <Card className="border-red-300 dark:border-red-800 bg-red-50 dark:bg-red-950/30">
          <CardContent className="py-6 flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-red-700 dark:text-red-400">
                Failed to generate suggestions
              </p>
              <p className="text-sm text-red-600 dark:text-red-500 mt-1">{error}</p>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Summary card */}
      {summary && !isLoading && (
        <Card className="border-accent/30 bg-gradient-to-r from-indigo-50 to-purple-50 dark:from-indigo-950/30 dark:to-purple-950/30">
          <CardContent className="py-4">
            <div className="flex items-start gap-3">
              <Sparkles className="w-5 h-5 text-accent flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-primary mb-1">Optimization Strategy</p>
                <p className="text-secondary text-sm">{summary}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Failure Diagnosis section (QW4) */}
      {diagnosis && diagnosis.length > 0 && !isLoading && (
        <Card className="border-amber-200 dark:border-amber-800/50">
          <CardContent className="py-4">
            <div className="flex items-center gap-2 mb-3">
              <Stethoscope className="w-4 h-4 text-amber-600 dark:text-amber-400" />
              <h3 className="font-medium text-primary text-sm">Failure Diagnosis</h3>
              <span className="text-xs text-muted">({diagnosis.length} question{diagnosis.length !== 1 ? "s" : ""} analyzed)</span>
            </div>
            <div className="space-y-3">
              {diagnosis.map((d, i) => (
                <div key={i} className="p-3 rounded-lg bg-elevated">
                  <p className="text-sm font-medium text-primary mb-2">
                    {d.question}
                  </p>
                  <div className="flex flex-wrap gap-1.5 mb-2">
                    {d.failure_types.map((ft) => {
                      const config = FAILURE_TYPE_LABELS[ft] || { label: ft, color: "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300" }
                      return (
                        <span key={ft} className={`text-xs px-2 py-0.5 rounded-full font-medium ${config.color}`}>
                          {config.label}
                        </span>
                      )
                    })}
                  </div>
                  <p className="text-xs text-muted">{d.explanation}</p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Stats and Create button */}
      {suggestions && suggestions.length > 0 && !isLoading && (
        <div className="space-y-4">
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex gap-4 flex-wrap">
              {Object.entries(PRIORITY_CONFIG).map(([priority, config]) => {
                const count = priorityCounts[priority as keyof typeof priorityCounts]
                if (count === 0) return null
                return (
                  <div key={priority} className="flex items-center gap-2 text-sm">
                    <div className={`w-3 h-3 rounded-full ${config.dotColor}`} />
                    <span className="text-secondary">
                      {count} {priority} priority
                    </span>
                  </div>
                )
              })}
            </div>

            {/* Selection count and Create button */}
            <div className="flex items-center gap-3">
              <span className="text-sm text-muted">
                {selectedCount} of {totalCount} selected
              </span>
              {selectedCount > 0 && (
                <Button onClick={onCreateNewGenie}>
                  <Eye className="w-4 h-4 mr-2" />
                  Preview Changes
                </Button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Suggestions grouped by lever (QW5) */}
      {suggestions && suggestions.length > 0 && !isLoading && (
        <div className="space-y-4">
          {LEVER_ORDER.map((lever) => {
            const group = leverGroups[lever.key]
            if (!group || group.items.length === 0) return null

            return (
              <AccordionItem
                key={lever.key}
                defaultOpen={true}
                icon={<div className="w-2 h-2 rounded-full bg-accent" />}
                title={
                  <span className="text-primary">
                    {lever.label} ({group.items.length})
                    {group.selectedCount > 0 && (
                      <span className="ml-2 text-xs font-normal text-muted">
                        {group.selectedCount} selected
                      </span>
                    )}
                  </span>
                }
                action={
                  <div
                    className="flex items-center gap-1"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => {
                        // Select all items in this lever
                        group.items.forEach(({ originalIndex }) => {
                          if (!selectedSuggestions.has(originalIndex)) {
                            onToggleSuggestionSelection(originalIndex)
                          }
                        })
                      }}
                    >
                      Select All
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => {
                        // Deselect all items in this lever
                        group.items.forEach(({ originalIndex }) => {
                          if (selectedSuggestions.has(originalIndex)) {
                            onToggleSuggestionSelection(originalIndex)
                          }
                        })
                      }}
                    >
                      Deselect All
                    </Button>
                  </div>
                }
              >
                <div className="space-y-1">
                  <p className="text-xs text-muted mb-3">{lever.description}</p>
                  <div className="space-y-3">
                    {group.items.map(({ suggestion, originalIndex }) => (
                      <SuggestionCard
                        key={`${lever.key}-${originalIndex}`}
                        suggestion={suggestion}
                        selectionEnabled={true}
                        isSelected={selectedSuggestions.has(originalIndex)}
                        onToggleSelection={() => onToggleSuggestionSelection(originalIndex)}
                      />
                    ))}
                  </div>
                </div>
              </AccordionItem>
            )
          })}

          {/* "Other" lever for uncategorized suggestions */}
          {leverGroups["other"] && leverGroups["other"].items.length > 0 && (
            <AccordionItem
              key="other"
              defaultOpen={true}
              icon={<div className="w-2 h-2 rounded-full bg-gray-400" />}
              title={
                <span className="text-primary">
                  Other ({leverGroups["other"].items.length})
                  {leverGroups["other"].selectedCount > 0 && (
                    <span className="ml-2 text-xs font-normal text-muted">
                      {leverGroups["other"].selectedCount} selected
                    </span>
                  )}
                </span>
              }
            >
              <div className="space-y-3">
                {leverGroups["other"].items.map(({ suggestion, originalIndex }) => (
                  <SuggestionCard
                    key={`other-${originalIndex}`}
                    suggestion={suggestion}
                    selectionEnabled={true}
                    isSelected={selectedSuggestions.has(originalIndex)}
                    onToggleSelection={() => onToggleSuggestionSelection(originalIndex)}
                  />
                ))}
              </div>
            </AccordionItem>
          )}
        </div>
      )}

      {/* Empty state */}
      {suggestions && suggestions.length === 0 && !isLoading && (
        <Card>
          <CardContent className="py-12 text-center">
            <Sparkles className="w-12 h-12 text-muted mx-auto mb-4" />
            <p className="text-lg font-medium text-primary">
              No optimization suggestions generated
            </p>
            <p className="text-sm text-muted mt-1">
              Your Genie Space configuration looks good based on the labeling feedback.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
