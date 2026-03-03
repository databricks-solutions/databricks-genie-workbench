/**
 * Summary phase component showing overall results.
 * Supports both full analysis (with synthesis) and partial analysis layouts.
 */

import {
  ChevronDown,
  ChevronUp,
  Info,
  Sparkles,
  CheckCircle2,
  Lightbulb,
  ArrowRight,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ChecklistProgress } from "@/components/ChecklistProgress"
import { AssessmentBadge } from "@/components/AssessmentBadge"
import { ScoreGauge } from "@/components/ScoreGauge"
import { cn } from "@/lib/utils"
import type { SectionAnalysis, SynthesisResult } from "@/types"

interface SummaryPhaseProps {
  genieSpaceId: string
  sectionAnalyses: SectionAnalysis[]
  selectedSections: number[]
  expandedSections: Record<string, boolean>
  onToggleSection: (sectionName: string) => void
  onExpandAll: () => void
  onCollapseAll: () => void
  synthesis?: SynthesisResult | null
  isFullAnalysis?: boolean
}

function formatSectionName(sectionName: string): string {
  return sectionName
    .replace(/_/g, " ")
    .replace(/\./g, " -> ")
    .replace(/\b\w/g, (l) => l.toUpperCase())
    .replace(/Sql/g, "SQL")
}

function getScoreColorClasses(percentage: number): string {
  if (percentage === 100) {
    return "bg-success/10 text-success dark:bg-success/20"
  }
  if (percentage >= 60) {
    return "bg-warning/10 text-warning dark:bg-warning/20"
  }
  return "bg-danger/10 text-danger dark:bg-danger/20"
}

export function SummaryPhase({
  genieSpaceId,
  sectionAnalyses,
  selectedSections,
  expandedSections,
  onToggleSection,
  onExpandAll,
  onCollapseAll,
  synthesis,
  isFullAnalysis = false,
}: SummaryPhaseProps) {
  // Filter analyses to only show selected sections
  const filteredAnalyses = selectedSections
    .map((index) => sectionAnalyses[index])
    .filter((a): a is SectionAnalysis => a !== undefined)

  // Calculate totals from filtered analyses
  const totalChecklist = filteredAnalyses.reduce(
    (sum, a) => sum + a.checklist.length,
    0
  )
  const passedChecklist = filteredAnalyses.reduce(
    (sum, a) => sum + a.checklist.filter((c) => c.passed).length,
    0
  )

  // Full analysis layout (with synthesis)
  if (isFullAnalysis && synthesis) {
    return (
      <div className="space-y-8 animate-stagger">
        {/* Header */}
        <div>
          <h2 className="text-lg font-display font-semibold text-primary">
            Analysis Summary
          </h2>
          <p className="text-sm text-muted">
            Space ID:{" "}
            <code className="px-1.5 py-0.5 bg-elevated rounded text-secondary font-mono text-xs">
              {genieSpaceId}
            </code>
          </p>
        </div>

        {/* Assessment Badge - Hero element */}
        <div className="flex justify-center py-6">
          <AssessmentBadge
            assessment={synthesis.assessment}
            rationale={synthesis.assessment_rationale}
          />
        </div>

        {/* Celebration Points */}
        {synthesis.celebration_points.length > 0 && (
          <Card className="border-success/30 bg-success/5 dark:bg-success/10">
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2 text-success">
                <CheckCircle2 className="w-5 h-5" />
                What's Working Well
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2">
                {synthesis.celebration_points.map((point, index) => (
                  <li
                    key={index}
                    className="flex items-start gap-2 text-sm text-secondary"
                  >
                    <CheckCircle2 className="w-4 h-4 text-success flex-shrink-0 mt-0.5" />
                    <span>{point}</span>
                  </li>
                ))}
              </ul>
            </CardContent>
          </Card>
        )}

        {/* Quick Wins */}
        {synthesis.top_quick_wins.length > 0 && (
          <Card className="border-warning/30 bg-warning/5 dark:bg-warning/10">
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2 text-warning">
                <Lightbulb className="w-5 h-5" />
                Quick Wins
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2">
                {synthesis.top_quick_wins.map((win, index) => (
                  <li
                    key={index}
                    className="flex items-start gap-2 text-sm text-secondary"
                  >
                    <ArrowRight className="w-4 h-4 text-warning flex-shrink-0 mt-0.5" />
                    <span>{win}</span>
                  </li>
                ))}
              </ul>
            </CardContent>
          </Card>
        )}

        {/* Compensating Strengths */}
        {synthesis.compensating_strengths.length > 0 && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <Sparkles className="w-5 h-5 text-accent" />
                Cross-Section Insights
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {synthesis.compensating_strengths.map((strength, index) => (
                  <div
                    key={index}
                    className="p-3 bg-elevated rounded-lg text-sm"
                  >
                    <div className="flex items-center gap-2 text-muted mb-1">
                      <span className="font-medium text-secondary">
                        {formatSectionName(strength.covering_section)}
                      </span>
                      <ArrowRight className="w-3 h-3" />
                      <span>{formatSectionName(strength.covered_section)}</span>
                    </div>
                    <p className="text-secondary">{strength.explanation}</p>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Section Results (collapsible, secondary) */}
        <Card>
          <CardHeader className="pb-3 flex flex-row items-center justify-between">
            <CardTitle className="text-base">Section Details</CardTitle>
            <div className="flex items-center gap-2">
              <Button variant="ghost" size="sm" onClick={onExpandAll}>
                Expand All
              </Button>
              <Button variant="ghost" size="sm" onClick={onCollapseAll}>
                Collapse All
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {filteredAnalyses.map((analysis, index) => {
                const displayName = formatSectionName(analysis.section_name)
                const passed = analysis.checklist.filter((c) => c.passed).length
                const total = analysis.checklist.length
                const isExpanded = expandedSections[analysis.section_name] ?? false
                const percentage = total > 0 ? (passed / total) * 100 : 100

                return (
                  <div
                    key={analysis.section_name}
                    className="border border-default rounded-lg overflow-hidden dark:card-glow"
                    style={{ animationDelay: `${index * 50}ms` }}
                  >
                    <button
                      type="button"
                      onClick={() => onToggleSection(analysis.section_name)}
                      className="flex w-full items-center justify-between px-4 py-3 text-left bg-surface hover:bg-elevated transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <span className="font-medium text-primary">
                          {displayName}
                        </span>
                        <span
                          className={cn(
                            "text-sm px-2.5 py-0.5 rounded-full font-medium",
                            getScoreColorClasses(percentage)
                          )}
                        >
                          {passed}/{total}
                        </span>
                      </div>
                      {isExpanded ? (
                        <ChevronUp className="w-5 h-5 text-muted" />
                      ) : (
                        <ChevronDown className="w-5 h-5 text-muted" />
                      )}
                    </button>

                    <div
                      className={cn(
                        "grid transition-all duration-200 ease-in-out",
                        isExpanded
                          ? "grid-rows-[1fr] opacity-100"
                          : "grid-rows-[0fr] opacity-0"
                      )}
                    >
                      <div className="overflow-hidden">
                        <div className="px-4 py-4 bg-elevated border-t border-default space-y-4">
                          {analysis.summary && (
                            <div className="flex items-start gap-3 p-3 bg-info/10 dark:bg-info/15 rounded-lg">
                              <Info className="w-5 h-5 text-info flex-shrink-0 mt-0.5" />
                              <p className="text-sm text-secondary">
                                {analysis.summary}
                              </p>
                            </div>
                          )}

                          {analysis.checklist.length > 0 ? (
                            <ChecklistProgress checklist={analysis.checklist} />
                          ) : (
                            <p className="text-sm text-muted text-center py-4">
                              No checklist items for this section.
                            </p>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  // Partial analysis layout (no synthesis)
  return (
    <div className="space-y-8 animate-stagger">
      {/* Header */}
      <div>
        <h2 className="text-lg font-display font-semibold text-primary">
          Analysis Summary
        </h2>
        <p className="text-sm text-muted">
          Space ID:{" "}
          <code className="px-1.5 py-0.5 bg-elevated rounded text-secondary font-mono text-xs">
            {genieSpaceId}
          </code>
        </p>
      </div>

      {/* Score Gauge - Hero element for partial analysis */}
      <div className="flex justify-center py-4">
        <ScoreGauge
          passed={passedChecklist}
          total={totalChecklist}
          size={220}
          animationDuration={1200}
        />
      </div>

      {/* Prompt for full analysis */}
      <Card className="border-accent/30 bg-accent/5 dark:bg-accent/10">
        <CardContent className="py-4">
          <div className="flex items-start gap-3">
            <Info className="w-5 h-5 text-accent flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-sm text-secondary">
                <span className="font-medium">Partial analysis:</span> Analyze
                all your configured sections to unlock cross-sectional insights,
                compensating strengths, and personalized quick wins.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Section Results (expanded by default, primary focus) */}
      <Card>
        <CardHeader className="pb-3 flex flex-row items-center justify-between">
          <CardTitle className="text-base">Section Results</CardTitle>
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="sm" onClick={onExpandAll}>
              Expand All
            </Button>
            <Button variant="ghost" size="sm" onClick={onCollapseAll}>
              Collapse All
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {filteredAnalyses.map((analysis, index) => {
              const displayName = formatSectionName(analysis.section_name)
              const passed = analysis.checklist.filter((c) => c.passed).length
              const total = analysis.checklist.length
              const isExpanded = expandedSections[analysis.section_name] ?? false
              const percentage = total > 0 ? (passed / total) * 100 : 100

              return (
                <div
                  key={analysis.section_name}
                  className="border border-default rounded-lg overflow-hidden dark:card-glow"
                  style={{ animationDelay: `${index * 50}ms` }}
                >
                  <button
                    type="button"
                    onClick={() => onToggleSection(analysis.section_name)}
                    className="flex w-full items-center justify-between px-4 py-3 text-left bg-surface hover:bg-elevated transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      <span className="font-medium text-primary">
                        {displayName}
                      </span>
                      <span
                        className={cn(
                          "text-sm px-2.5 py-0.5 rounded-full font-medium",
                          getScoreColorClasses(percentage)
                        )}
                      >
                        {passed}/{total}
                      </span>
                    </div>
                    {isExpanded ? (
                      <ChevronUp className="w-5 h-5 text-muted" />
                    ) : (
                      <ChevronDown className="w-5 h-5 text-muted" />
                    )}
                  </button>

                  <div
                    className={cn(
                      "grid transition-all duration-200 ease-in-out",
                      isExpanded
                        ? "grid-rows-[1fr] opacity-100"
                        : "grid-rows-[0fr] opacity-0"
                    )}
                  >
                    <div className="overflow-hidden">
                      <div className="px-4 py-4 bg-elevated border-t border-default space-y-4">
                        {analysis.summary && (
                          <div className="flex items-start gap-3 p-3 bg-info/10 dark:bg-info/15 rounded-lg">
                            <Info className="w-5 h-5 text-info flex-shrink-0 mt-0.5" />
                            <p className="text-sm text-secondary">
                              {analysis.summary}
                            </p>
                          </div>
                        )}

                        {analysis.checklist.length > 0 ? (
                          <ChecklistProgress checklist={analysis.checklist} />
                        ) : (
                          <p className="text-sm text-muted text-center py-4">
                            No checklist items for this section.
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
