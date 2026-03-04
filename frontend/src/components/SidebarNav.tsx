/**
 * Sidebar navigation component showing analysis progress.
 */

import { useState } from "react"
import {
  Download,
  BarChart3,
  ClipboardCheck,
  RotateCcw,
  Check,
  Search,
  ChevronDown,
  MessageSquare,
  Tag,
  FileText,
  Settings,
  Sparkles,
  GitCompare,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import type { Phase, OptimizeView } from "@/types"

interface SidebarNavProps {
  phase: Phase
  optimizeView: OptimizeView | null
  hasAnalyzedSections: boolean
  analyzedCount: number
  allSectionsAnalyzed: boolean
  showChecklist: boolean
  showSettings: boolean
  hasLabelingSession: boolean
  hasOptimizationResults: boolean
  hasPreviewResults: boolean
  onGoToIngest: () => void
  onGoToAnalysis: () => void
  onGoToSummary: () => void
  onGoToBenchmarks: () => void
  onGoToLabeling: () => void
  onGoToFeedback: () => void
  onGoToOptimization: () => void
  onGoToPreview: () => void
  onToggleChecklist: () => void
  onToggleSettings: () => void
  onReset: () => void
}

export function SidebarNav({
  phase,
  optimizeView,
  hasAnalyzedSections,
  analyzedCount,
  allSectionsAnalyzed,
  showChecklist,
  showSettings,
  hasLabelingSession,
  hasOptimizationResults,
  hasPreviewResults,
  onGoToIngest,
  onGoToAnalysis,
  onGoToSummary,
  onGoToBenchmarks,
  onGoToLabeling,
  onGoToFeedback,
  onGoToOptimization,
  onGoToPreview,
  onToggleChecklist,
  onToggleSettings,
  onReset,
}: SidebarNavProps) {
  const [isAnalysisExpanded, setIsAnalysisExpanded] = useState(true)
  const [isOptimizeExpanded, setIsOptimizeExpanded] = useState(true)

  if (phase === "input") {
    return (
      <aside className="w-64 bg-surface border-r border-default p-4 min-h-screen">
        <div className="mb-6">
          <h2 className="text-sm font-semibold text-primary mb-1">Progress</h2>
          <p className="text-xs text-muted">
            Enter a Genie Space ID to begin
          </p>
        </div>
      </aside>
    )
  }

  return (
    <aside className="w-64 bg-surface border-r border-default p-4 min-h-screen flex flex-col relative overflow-hidden">
      {/* Subtle inner shadow for depth */}
      <div className="absolute inset-y-0 right-0 w-px bg-gradient-to-b from-transparent via-border-strong to-transparent opacity-50" />

      <nav className="space-y-2 flex-1">
        {/* Ingest step */}
        <button
          onClick={onGoToIngest}
          className={cn(
            "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
            phase === "ingest" && !showChecklist && !showSettings
              ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
              : "bg-success/10 text-success hover:bg-success/20 dark:bg-success/15"
          )}
        >
          {phase === "ingest" && !showChecklist && !showSettings ? (
            <Download className="w-4 h-4" />
          ) : (
            <Check className="w-4 h-4" />
          )}
          Ingested Serialized Space
        </button>

        <hr className="border-default my-3" />

        {/* Analysis group */}
        <div>
          <button
            type="button"
            onClick={() => setIsAnalysisExpanded(!isAnalysisExpanded)}
            className="w-full flex items-center justify-between px-2 py-1.5 text-xs font-semibold text-muted uppercase tracking-wider hover:text-secondary transition-colors"
          >
            <span>Analyze</span>
            <ChevronDown
              className={cn(
                "w-4 h-4 transition-transform duration-200",
                isAnalysisExpanded && "rotate-180"
              )}
            />
          </button>

          <div
            className={cn(
              "grid transition-all duration-200 ease-in-out",
              isAnalysisExpanded ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0"
            )}
          >
            <div className="overflow-hidden">
              {/* Single Analysis button */}
              <div className="space-y-1 pl-2 mt-1">
                <button
                  onClick={onGoToAnalysis}
                  disabled={!hasAnalyzedSections}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    phase === "analysis" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : hasAnalyzedSections
                      ? "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                      : "bg-elevated text-muted cursor-not-allowed opacity-60"
                  )}
                >
                  <Search className="w-4 h-4" />
                  Analysis
                  {hasAnalyzedSections && (
                    <span className="ml-auto text-xs opacity-70">{analyzedCount}</span>
                  )}
                </button>
              </div>

              {/* Summary step - inside Analysis group */}
              <div className="pl-2 mt-2">
                <button
                  onClick={onGoToSummary}
                  disabled={!allSectionsAnalyzed}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    phase === "summary" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : allSectionsAnalyzed
                      ? "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                      : "bg-elevated text-muted cursor-not-allowed opacity-60"
                  )}
                >
                  <BarChart3 className="w-4 h-4" />
                  Summary
                </button>
              </div>
            </div>
          </div>
        </div>

        <hr className="border-default my-3" />

        {/* Optimize group */}
        <div>
          <button
            type="button"
            onClick={() => setIsOptimizeExpanded(!isOptimizeExpanded)}
            className="w-full flex items-center justify-between px-2 py-1.5 text-xs font-semibold text-muted uppercase tracking-wider hover:text-secondary transition-colors"
          >
            <span>Optimize</span>
            <ChevronDown
              className={cn(
                "w-4 h-4 transition-transform duration-200",
                isOptimizeExpanded && "rotate-180"
              )}
            />
          </button>

          <div
            className={cn(
              "grid transition-all duration-200 ease-in-out",
              isOptimizeExpanded ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0"
            )}
          >
            <div className="overflow-hidden">
              <div className="space-y-1 pl-2 mt-1">
                {/* Benchmarks button */}
                <button
                  onClick={onGoToBenchmarks}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    optimizeView === "benchmarks" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                  )}
                >
                  <MessageSquare className="w-4 h-4" />
                  Benchmarks
                </button>

                {/* Labeling button */}
                <button
                  onClick={onGoToLabeling}
                  disabled={!hasLabelingSession}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    optimizeView === "labeling" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : hasLabelingSession
                      ? "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                      : "bg-elevated text-muted cursor-not-allowed opacity-60"
                  )}
                >
                  <Tag className="w-4 h-4" />
                  Labeling
                </button>

                {/* Feedback button */}
                <button
                  onClick={onGoToFeedback}
                  disabled={!hasLabelingSession}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    optimizeView === "feedback" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : hasLabelingSession
                      ? "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                      : "bg-elevated text-muted cursor-not-allowed opacity-60"
                  )}
                >
                  <FileText className="w-4 h-4" />
                  Feedback
                </button>

                {/* Optimization button */}
                <button
                  onClick={onGoToOptimization}
                  disabled={!hasOptimizationResults}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    optimizeView === "optimization" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : hasOptimizationResults
                      ? "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                      : "bg-elevated text-muted cursor-not-allowed opacity-60"
                  )}
                >
                  <Sparkles className="w-4 h-4" />
                  Optimization
                </button>

                {/* Preview button */}
                <button
                  onClick={onGoToPreview}
                  disabled={!hasPreviewResults}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all text-left",
                    optimizeView === "preview" && !showChecklist && !showSettings
                      ? "gradient-accent text-white shadow-lg shadow-accent/20 dark:glow-accent"
                      : hasPreviewResults
                      ? "bg-elevated text-secondary hover:bg-sunken cursor-pointer"
                      : "bg-elevated text-muted cursor-not-allowed opacity-60"
                  )}
                >
                  <GitCompare className="w-4 h-4" />
                  Preview
                </button>
              </div>
            </div>
          </div>
        </div>
      </nav>

      {/* Bottom actions */}
      <div className="mt-6 space-y-2">
        <hr className="border-default mb-3" />

        <Button
          variant="ghost"
          className={cn(
            "w-full justify-start",
            showChecklist ? "text-accent bg-accent/10" : "text-secondary"
          )}
          onClick={onToggleChecklist}
        >
          <ClipboardCheck className="w-4 h-4 mr-2" />
          Checklist
        </Button>

        <Button
          variant="ghost"
          className={cn(
            "w-full justify-start",
            showSettings ? "text-accent bg-accent/10" : "text-secondary"
          )}
          onClick={onToggleSettings}
        >
          <Settings className="w-4 h-4 mr-2" />
          Settings
        </Button>

        <Button
          variant="ghost"
          className="w-full justify-start text-muted hover:text-danger"
          onClick={onReset}
        >
          <RotateCcw className="w-4 h-4 mr-2" />
          Start Over
        </Button>
      </div>
    </aside>
  )
}
