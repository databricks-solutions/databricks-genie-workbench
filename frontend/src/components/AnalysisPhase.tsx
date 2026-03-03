/**
 * Analysis phase component showing section-by-section analysis.
 * Uses LabelingPage-style cycling navigation for analyzed sections only.
 */

import { useMemo } from "react"
import { ChevronLeft, ChevronRight, ChevronDown, AlertTriangle, Info, BarChart3 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ChecklistProgress } from "@/components/ChecklistProgress"
import type { SectionInfo, SectionAnalysis } from "@/types"

interface AnalysisPhaseProps {
  genieSpaceId: string
  sections: SectionInfo[]
  sectionAnalyses: SectionAnalysis[]
  analysisViewIndex: number
  isLoading: boolean
  error: string | null
  onAnalyzeSection: () => Promise<void>
  onSetAnalysisViewIndex: (index: number) => void
  onGoToSummary: () => void
}

function formatSectionName(sectionName: string): string {
  return sectionName
    .replace(/_/g, " ")
    .replace(/\./g, " → ")
    .replace(/\b\w/g, (l) => l.toUpperCase())
    .replace(/Sql/g, "SQL")
}

function getShortSectionName(sectionName: string): string {
  return sectionName
    .split(".")
    .pop()!
    .replace(/_/g, " ")
    .replace(/\b\w/g, (l) => l.toUpperCase())
    .replace(/Sql/g, "SQL")
}

// Get score color class based on score percentage
function getScoreColorClass(score: number): string {
  if (score >= 80) return "bg-green-500 dark:shadow-[0_0_8px_rgba(34,197,94,0.5)]"
  if (score >= 60) return "bg-amber-500 dark:shadow-[0_0_8px_rgba(245,158,11,0.5)]"
  return "bg-red-500 dark:shadow-[0_0_8px_rgba(239,68,68,0.5)]"
}

export function AnalysisPhase({
  genieSpaceId,
  sections,
  sectionAnalyses,
  analysisViewIndex,
  isLoading,
  error,
  onAnalyzeSection,
  onSetAnalysisViewIndex,
  onGoToSummary,
}: AnalysisPhaseProps) {
  // Compute the list of analyzed sections (only sections that have analysis results)
  const analyzedSections = useMemo(() => {
    return sectionAnalyses
      .map((analysis, originalIndex) =>
        analysis !== undefined
          ? { analysis, originalIndex, section: sections[originalIndex] }
          : null
      )
      .filter((item): item is NonNullable<typeof item> => item !== null)
  }, [sectionAnalyses, sections])

  const totalAnalyzed = analyzedSections.length
  const currentItem = analyzedSections[analysisViewIndex]
  const currentSection = currentItem?.section
  const currentAnalysis = currentItem?.analysis

  // Navigation handlers
  const handlePrev = () => onSetAnalysisViewIndex(Math.max(0, analysisViewIndex - 1))
  const handleNext = () => onSetAnalysisViewIndex(Math.min(totalAnalyzed - 1, analysisViewIndex + 1))
  const handleSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onSetAnalysisViewIndex(Number(e.target.value))
  }
  const handleSegmentClick = (idx: number) => onSetAnalysisViewIndex(idx)

  const displayName = currentSection ? formatSectionName(currentSection.name) : ""

  // Handle case when no sections are analyzed
  if (totalAnalyzed === 0) {
    return (
      <div className="space-y-6 animate-slide-up">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-display font-bold text-primary">
              Analysis Results
            </h1>
            <p className="text-muted">
              Space ID: <span className="font-mono text-secondary">{genieSpaceId}</span>
            </p>
          </div>
        </div>
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted">No sections have been analyzed yet.</p>
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
            Analysis Results
          </h1>
          <p className="text-muted">
            Space ID: <span className="font-mono text-secondary">{genieSpaceId}</span>
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Button onClick={onGoToSummary}>
            <BarChart3 className="w-4 h-4 mr-2" />
            View Summary
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
            disabled={analysisViewIndex === 0}
          >
            <ChevronLeft className="w-4 h-4 mr-1" />
            Prev
          </Button>

          {/* Segmented Bar */}
          <div className="flex-1 flex gap-1">
            {analyzedSections.map((item, idx) => {
              const isCurrent = idx === analysisViewIndex
              const score = item.analysis.score
              const colorClass = getScoreColorClass(score)
              const shortName = getShortSectionName(item.section.name)

              return (
                <button
                  key={item.section.name}
                  onClick={() => handleSegmentClick(idx)}
                  className={`
                    flex-1 h-2.5 rounded-full transition-all cursor-pointer
                    ${colorClass}
                    ${isCurrent ? "ring-2 ring-accent ring-offset-2 ring-offset-surface" : ""}
                  `}
                  title={`${shortName} (${score}%)`}
                />
              )
            })}
          </div>

          <Button
            variant="outline"
            size="sm"
            onClick={handleNext}
            disabled={analysisViewIndex === totalAnalyzed - 1}
          >
            Next
            <ChevronRight className="w-4 h-4 ml-1" />
          </Button>
        </div>

        {/* Status text */}
        <p className="text-center text-sm text-muted">
          {analysisViewIndex + 1} of {totalAnalyzed} analyzed sections
        </p>
      </div>

      {/* Section Selector Dropdown */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-muted">Section</h3>
            <div className="relative">
              <select
                value={analysisViewIndex}
                onChange={handleSelectChange}
                className="appearance-none text-sm text-secondary bg-elevated hover:bg-sunken pl-3 pr-8 py-1.5 rounded-lg cursor-pointer focus:outline-none focus:ring-2 focus:ring-accent/50 transition-colors"
              >
                {analyzedSections.map((item, idx) => {
                  const shortName = getShortSectionName(item.section.name)
                  return (
                    <option key={item.section.name} value={idx}>
                      {idx + 1}. {shortName} ({item.analysis.score}%)
                    </option>
                  )
                })}
              </select>
              <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-4 h-4 text-muted pointer-events-none" />
            </div>
          </div>
          <h2 className="text-lg font-display font-semibold text-primary">
            {displayName}
          </h2>
        </CardContent>
      </Card>

      {/* Loading state */}
      {isLoading && (
        <Card>
          <CardContent className="py-12 flex flex-col items-center justify-center">
            {/* Scanning animation */}
            <div className="relative w-16 h-16 mb-6">
              <div className="absolute inset-0 rounded-full border-4 border-elevated" />
              <div className="absolute inset-0 rounded-full border-4 border-accent border-t-transparent animate-spin" />
              <div className="absolute inset-2 rounded-full bg-accent/10 dark:bg-accent/20 animate-pulse" />
            </div>
            <p className="text-lg font-display font-medium text-primary">
              Analyzing {displayName}...
            </p>
            <p className="text-sm text-muted">This may take a moment</p>
            {/* Scanning bar */}
            <div className="w-48 h-1 mt-4 rounded-full bg-elevated overflow-hidden">
              <div className="h-full animate-scan opacity-60" />
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error state */}
      {error && (
        <Card className="border-danger/30 bg-danger/5 dark:bg-danger/10">
          <CardContent className="py-6 flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-danger flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-danger">Analysis Failed</p>
              <p className="text-sm text-secondary mt-1">{error}</p>
              <Button
                variant="outline"
                size="sm"
                className="mt-3"
                onClick={onAnalyzeSection}
              >
                Retry
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Analysis content */}
      {currentAnalysis && currentSection && !isLoading && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-stretch animate-fade-in">
          {/* Section Data */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3 flex-shrink-0">
              <CardTitle className="text-base">Section Data</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col min-h-0">
              {!currentSection.has_data ? (
                <div className="flex items-start gap-3 p-4 bg-warning/10 dark:bg-warning/15 rounded-lg text-warning">
                  <AlertTriangle className="w-5 h-5 flex-shrink-0 mt-0.5" />
                  <p className="text-sm">
                    This section is not configured in your Genie Space.
                  </p>
                </div>
              ) : (
                <pre className="text-xs font-mono overflow-auto flex-1 p-4 bg-elevated rounded-lg border border-default">
                  {JSON.stringify(currentSection.data, null, 2)}
                </pre>
              )}
            </CardContent>
          </Card>

          {/* Checklist */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3 flex-shrink-0">
              <CardTitle className="text-base">Checklist</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto">
              {currentAnalysis.summary && (
                <div className="flex items-start gap-3 p-4 bg-info/10 dark:bg-info/15 rounded-lg mb-4">
                  <Info className="w-5 h-5 text-info flex-shrink-0 mt-0.5" />
                  <p className="text-sm text-secondary">{currentAnalysis.summary}</p>
                </div>
              )}

              {currentAnalysis.checklist.length > 0 ? (
                <ChecklistProgress checklist={currentAnalysis.checklist} />
              ) : (
                <p className="text-sm text-muted text-center py-8">
                  No checklist items for this section.
                </p>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  )
}
