/**
 * OptimizationPage component displaying AI-generated optimization suggestions.
 * Organized in a two-level collapsible hierarchy: Priority > Category.
 */

import { useMemo } from "react"
import { ArrowLeft, Loader2, Sparkles, AlertTriangle, Eye } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { AccordionItem } from "@/components/ui/accordion"
import { SuggestionCard } from "@/components/SuggestionCard"
import type { OptimizationSuggestion } from "@/types"

interface OptimizationPageProps {
  suggestions: OptimizationSuggestion[] | null
  summary: string | null
  isLoading: boolean
  error: string | null
  selectedSuggestions: Set<number>
  onBack: () => void
  onToggleSuggestionSelection: (index: number) => void
  onSelectAllByPriority: (priority: string) => void
  onDeselectAllByPriority: (priority: string) => void
  onCreateNewGenie: () => void
}

type SuggestionItem = { suggestion: OptimizationSuggestion; originalIndex: number }
type CategoryGroup = { category: string; items: SuggestionItem[] }
type PriorityGroup = { categories: CategoryGroup[]; count: number; selectedCount: number }

const PRIORITY_CONFIG = {
  high: {
    label: "High Priority",
    dotColor: "bg-red-500",
    textColor: "text-red-700 dark:text-red-400",
    defaultOpen: true,
  },
  medium: {
    label: "Medium Priority",
    dotColor: "bg-amber-500",
    textColor: "text-amber-700 dark:text-amber-400",
    defaultOpen: true,
  },
  low: {
    label: "Low Priority",
    dotColor: "bg-blue-500",
    textColor: "text-blue-700 dark:text-blue-400",
    defaultOpen: false,
  },
} as const

export function OptimizationPage({
  suggestions,
  summary,
  isLoading,
  error,
  selectedSuggestions,
  onBack,
  onToggleSuggestionSelection,
  onSelectAllByPriority,
  onDeselectAllByPriority,
  onCreateNewGenie,
}: OptimizationPageProps) {
  // Group suggestions by priority then category with original indices
  const groupedSuggestions = useMemo(() => {
    if (!suggestions) {
      return {
        high: { categories: [], count: 0, selectedCount: 0 },
        medium: { categories: [], count: 0, selectedCount: 0 },
        low: { categories: [], count: 0, selectedCount: 0 },
      }
    }

    const priorities = ["high", "medium", "low"] as const
    const result: Record<(typeof priorities)[number], PriorityGroup> = {
      high: { categories: [], count: 0, selectedCount: 0 },
      medium: { categories: [], count: 0, selectedCount: 0 },
      low: { categories: [], count: 0, selectedCount: 0 },
    }

    // Build category maps for each priority
    const categoryMaps = {
      high: new Map<string, SuggestionItem[]>(),
      medium: new Map<string, SuggestionItem[]>(),
      low: new Map<string, SuggestionItem[]>(),
    }

    suggestions.forEach((suggestion, index) => {
      const priority = suggestion.priority as (typeof priorities)[number]
      const category = suggestion.category || "Other"
      const map = categoryMaps[priority]

      if (!map.has(category)) map.set(category, [])
      map.get(category)!.push({ suggestion, originalIndex: index })
      result[priority].count++
      if (selectedSuggestions.has(index)) result[priority].selectedCount++
    })

    // Convert maps to sorted arrays
    for (const priority of priorities) {
      result[priority].categories = Array.from(categoryMaps[priority].entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([category, items]) => ({ category, items }))
    }

    return result
  }, [suggestions, selectedSuggestions])

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

      {/* Stats and Create button */}
      {suggestions && suggestions.length > 0 && !isLoading && (
        <div className="space-y-4">
          {/* Priority stats */}
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex gap-4 flex-wrap">
              {groupedSuggestions.high.count > 0 && (
                <div className="flex items-center gap-2 text-sm">
                  <div className="w-3 h-3 rounded-full bg-red-500" />
                  <span className="text-secondary">
                    {groupedSuggestions.high.count} high priority
                  </span>
                </div>
              )}
              {groupedSuggestions.medium.count > 0 && (
                <div className="flex items-center gap-2 text-sm">
                  <div className="w-3 h-3 rounded-full bg-amber-500" />
                  <span className="text-secondary">
                    {groupedSuggestions.medium.count} medium priority
                  </span>
                </div>
              )}
              {groupedSuggestions.low.count > 0 && (
                <div className="flex items-center gap-2 text-sm">
                  <div className="w-3 h-3 rounded-full bg-blue-500" />
                  <span className="text-secondary">
                    {groupedSuggestions.low.count} low priority
                  </span>
                </div>
              )}
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

      {/* Suggestions grouped by priority then category - collapsible accordions */}
      {suggestions && suggestions.length > 0 && !isLoading && (
        <div className="space-y-4">
          {(["high", "medium", "low"] as const).map((priority) => {
            const group = groupedSuggestions[priority]
            if (group.count === 0) return null

            const config = PRIORITY_CONFIG[priority]

            return (
              <AccordionItem
                key={priority}
                defaultOpen={config.defaultOpen}
                icon={<div className={`w-3 h-3 rounded-full ${config.dotColor}`} />}
                title={
                  <span className={config.textColor}>
                    {config.label} ({group.count})
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
                      onClick={() => onSelectAllByPriority(priority)}
                    >
                      Select All
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => onDeselectAllByPriority(priority)}
                    >
                      Deselect All
                    </Button>
                  </div>
                }
              >
                <div className="space-y-2">
                  {group.categories.map(({ category, items }) => (
                    <AccordionItem
                      key={`${priority}-${category}`}
                      defaultOpen={true}
                      title={
                        <span className="text-sm text-secondary">
                          {category} ({items.length})
                        </span>
                      }
                      className="border-transparent bg-black/5 dark:bg-white/5"
                    >
                      <div className="space-y-3">
                        {items.map(({ suggestion, originalIndex }) => (
                          <SuggestionCard
                            key={`${priority}-${category}-${originalIndex}`}
                            suggestion={suggestion}
                            selectionEnabled={true}
                            isSelected={selectedSuggestions.has(originalIndex)}
                            onToggleSelection={() => onToggleSuggestionSelection(originalIndex)}
                          />
                        ))}
                      </div>
                    </AccordionItem>
                  ))}
                </div>
              </AccordionItem>
            )
          })}
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
