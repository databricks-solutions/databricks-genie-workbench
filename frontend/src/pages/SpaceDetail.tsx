/**
 * SpaceDetail - Unified 4-tab detail view for a Genie Space.
 * Tabs: Score | Analysis | Optimize | History
 */
import { useState, useEffect } from "react"
import { ArrowLeft, Star, RefreshCw, Zap, Eye, BarChart2, Brain, Settings2, Clock, ExternalLink } from "lucide-react"
import { scanSpace, toggleStar, getSpaceHistory } from "@/lib/api"
import { getScoreColor } from "@/lib/utils"
import type { ScanResult, ScoreHistoryPoint } from "@/types"
import { IQScoreTab } from "./IQScoreTab"
import { HistoryTab } from "./HistoryTab"
// GenieRx components used in Analysis/Optimize tabs
import { IngestPhase } from "@/components/IngestPhase"
import { AnalysisPhase } from "@/components/AnalysisPhase"
import { SummaryPhase } from "@/components/SummaryPhase"
import { BenchmarksPage } from "@/components/BenchmarksPage"
import { LabelingPage } from "@/components/LabelingPage"
import { FeedbackPage } from "@/components/FeedbackPage"
import { OptimizationPage } from "@/components/OptimizationPage"
import { PreviewPage } from "@/components/PreviewPage"
import { useAnalysis } from "@/hooks/useAnalysis"
import { SpaceOverview } from "@/components/SpaceOverview"

type Tab = "overview" | "score" | "analysis" | "optimize" | "history"

interface SpaceDetailProps {
  spaceId: string
  displayName: string
  spaceUrl?: string
  onBack: () => void
}

export function SpaceDetail({ spaceId, displayName, spaceUrl, onBack }: SpaceDetailProps) {
  const [activeTab, setActiveTab] = useState<Tab>("overview")
  const [scanResult, setScanResult] = useState<ScanResult | null>(null)
  const [isStarred, setIsStarred] = useState(false)
  const [isScanning, setIsScanning] = useState(false)
  const [history, setHistory] = useState<ScoreHistoryPoint[]>([])

  const { state, actions } = useAnalysis()

  useEffect(() => {
    // Preload the space in the analysis hook
    if (spaceId) {
      actions.handleFetchSpace(spaceId)
    }
  }, [spaceId])

  const handleScan = async () => {
    setIsScanning(true)
    try {
      const result = await scanSpace(spaceId)
      setScanResult(result)
    } catch (e) {
      console.error("Scan failed:", e)
    } finally {
      setIsScanning(false)
    }
  }

  const handleToggleStar = async () => {
    const newStarred = !isStarred
    setIsStarred(newStarred)
    try {
      await toggleStar(spaceId, newStarred)
    } catch {
      setIsStarred(!newStarred)
    }
  }

  useEffect(() => {
    if (activeTab === "history") {
      getSpaceHistory(spaceId).then(setHistory).catch(console.error)
    }
  }, [activeTab, spaceId])

  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: "overview", label: "Overview", icon: <Eye className="w-4 h-4" /> },
    { id: "score", label: "Score", icon: <BarChart2 className="w-4 h-4" /> },
    { id: "analysis", label: "Analysis", icon: <Brain className="w-4 h-4" /> },
    { id: "optimize", label: "Optimize", icon: <Settings2 className="w-4 h-4" /> },
    { id: "history", label: "History", icon: <Clock className="w-4 h-4" /> },
  ]

  const scoreColor = getScoreColor(scanResult?.score)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start gap-4">
        <button
          onClick={onBack}
          className="mt-1 p-2 rounded-lg border border-default hover:bg-surface-secondary text-muted hover:text-secondary transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-display font-bold text-primary">{displayName}</h2>
            <button onClick={handleToggleStar}>
              <Star className={`w-5 h-5 ${isStarred ? "fill-amber-400 text-amber-400" : "text-muted hover:text-amber-400"} transition-colors`} />
            </button>
          </div>
          <div className="flex items-center gap-3 mt-2">
            {scanResult ? (
              <>
                <span className={`text-3xl font-bold ${scoreColor}`}>{scanResult.score}</span>
                <span className="text-muted text-sm">/100</span>
                <span className="text-xs px-2 py-0.5 rounded-full border border-current opacity-70 font-medium">
                  {scanResult.maturity}
                </span>
              </>
            ) : (
              <span className="text-muted text-sm">Not scanned yet</span>
            )}
            <button
              onClick={handleScan}
              disabled={isScanning}
              className="flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-lg border border-default hover:border-accent/40 hover:text-accent text-muted transition-colors disabled:opacity-50 ml-2"
            >
              {isScanning ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <Zap className="w-3.5 h-3.5" />}
              {isScanning ? "Scanning..." : "Run IQ Scan"}
            </button>
          </div>
          {spaceUrl ? (
            <a
              href={spaceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-muted mt-1 font-mono hover:text-accent transition-colors inline-flex items-center gap-1"
            >
              {spaceId}
              <ExternalLink className="w-3 h-3 flex-shrink-0" />
            </a>
          ) : (
            <p className="text-xs text-muted mt-1 font-mono">{spaceId}</p>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-default">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.id
                ? "border-accent text-accent"
                : "border-transparent text-muted hover:text-secondary"
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div>
        {activeTab === "overview" && (
          <SpaceOverview spaceData={state.spaceData} isLoading={state.isLoading} />
        )}

        {activeTab === "score" && (
          <IQScoreTab
            scanResult={scanResult}
            onScan={handleScan}
            isScanning={isScanning}
            spaceId={spaceId}
            spaceConfig={state.spaceData ?? undefined}
          />
        )}

        {activeTab === "analysis" && (
          <div className="space-y-4">
            {(state.phase === "input" || state.phase === "ingest") && !state.spaceData ? (
              <div className="text-center py-12 text-muted">
                {state.isLoading ? (
                  <><RefreshCw className="w-8 h-8 mx-auto mb-3 animate-spin opacity-40" /><p>Loading space configuration...</p></>
                ) : state.error ? (
                  <p className="text-red-400">{state.error}</p>
                ) : (
                  <p>Space data unavailable.</p>
                )}
              </div>
            ) : (state.phase === "input" || state.phase === "ingest") ? (
              <IngestPhase
                genieSpaceId={spaceId}
                spaceData={state.spaceData!}
                sections={state.sections}
                sectionAnalyses={state.sectionAnalyses}
                isLoading={state.isLoading}
                analysisProgress={state.analysisProgress}
                selectedSections={state.selectedSections}
                onToggleSectionSelection={actions.toggleSectionSelection}
                onSelectAllSections={actions.selectAllSections}
                onDeselectAllSections={actions.deselectAllSections}
                onAnalyzeAllSections={actions.analyzeAllSections}
                onGoToSection={actions.goToSection}
              />
            ) : state.phase === "analysis" ? (
              <AnalysisPhase
                genieSpaceId={spaceId}
                sections={state.sections}
                sectionAnalyses={state.sectionAnalyses}
                analysisViewIndex={state.analysisViewIndex}
                isLoading={state.isLoading}
                error={state.error}
                onAnalyzeSection={actions.analyzeCurrentSection}
                onSetAnalysisViewIndex={actions.setAnalysisViewIndex}
                onGoToSummary={actions.goToSummary}
              />
            ) : (
              <SummaryPhase
                genieSpaceId={spaceId}
                sectionAnalyses={state.sectionAnalyses}
                selectedSections={state.selectedSections}
                expandedSections={state.expandedSections}
                onToggleSection={actions.toggleSectionExpanded}
                onExpandAll={actions.expandAllSections}
                onCollapseAll={actions.collapseAllSections}
                synthesis={state.synthesis}
                isFullAnalysis={state.isFullAnalysis}
              />
            )}
          </div>
        )}

        {activeTab === "optimize" && (
          <div className="space-y-4">
            {state.optimizeView === "benchmarks" && state.spaceData ? (
              <BenchmarksPage
                genieSpaceId={spaceId}
                spaceData={state.spaceData}
                selectedQuestions={state.selectedQuestions}
                onToggleSelection={actions.toggleQuestionSelection}
                onSelectAll={actions.selectAllQuestions}
                onDeselectAll={actions.deselectAllQuestions}
                isProcessingBenchmarks={state.isProcessingBenchmarks}
                benchmarkProcessingProgress={state.benchmarkProcessingProgress}
                onProcessBenchmarksAndGoToLabeling={actions.processBenchmarksAndGoToLabeling}
                onCancelBenchmarkProcessing={actions.cancelBenchmarkProcessing}
              />
            ) : state.optimizeView === "labeling" && state.spaceData ? (
              <LabelingPage
                genieSpaceId={spaceId}
                spaceData={state.spaceData}
                selectedQuestions={state.selectedQuestions}
                currentIndex={state.labelingCurrentIndex}
                generatedSql={state.labelingGeneratedSql}
                genieResults={state.labelingGenieResults}
                expectedResults={state.labelingExpectedResults}
                correctAnswers={state.labelingCorrectAnswers}
                feedbackTexts={state.labelingFeedbackTexts}
                processingErrors={state.labelingProcessingErrors}
                onSetCurrentIndex={actions.setLabelingCurrentIndex}
                onSetCorrectAnswer={actions.setLabelingCorrectAnswer}
                onSetFeedbackText={actions.setLabelingFeedbackText}
                onBack={actions.goToBenchmarks}
                onFinish={actions.goToFeedback}
              />
            ) : state.optimizeView === "feedback" && state.spaceData ? (
              <FeedbackPage
                spaceData={state.spaceData}
                selectedQuestions={state.selectedQuestions}
                correctAnswers={state.labelingCorrectAnswers}
                feedbackTexts={state.labelingFeedbackTexts}
                onBack={actions.goToLabeling}
                onBeginOptimization={actions.startOptimization}
              />
            ) : state.optimizeView === "optimization" && state.spaceData ? (
              <OptimizationPage
                suggestions={state.optimizationSuggestions}
                summary={state.optimizationSummary}
                isLoading={state.isOptimizing}
                error={state.error}
                selectedSuggestions={state.selectedSuggestions}
                onBack={actions.goToFeedback}
                onToggleSuggestionSelection={actions.toggleSuggestionSelection}
                onSelectAllByPriority={actions.selectAllByPriority}
                onDeselectAllByPriority={actions.deselectAllByPriority}
                onCreateNewGenie={actions.generatePreviewConfig}
              />
            ) : state.optimizeView === "preview" && state.spaceData ? (
              <PreviewPage
                currentConfig={state.spaceData}
                previewConfig={state.previewConfig}
                summary={state.previewSummary}
                isLoading={state.isGeneratingPreview}
                error={state.error}
                selectedCount={state.selectedSuggestions.size}
                onBack={actions.goToOptimization}
                isCreating={state.isCreatingGenie}
                createError={state.genieCreateError}
                createdResult={state.createdGenieResult}
                onCreateGenieSpace={actions.createGenieSpace}
              />
            ) : (
              <div className="text-center py-12 text-muted">
                <Settings2 className="w-12 h-12 mx-auto mb-4 opacity-30" />
                <p>Run IQ Scan first, then come back to optimize based on findings.</p>
                <button onClick={() => { actions.resetOptimizeFlow(); actions.goToBenchmarks() }} className="mt-4 px-4 py-2 rounded-lg border border-accent/40 text-accent text-sm hover:bg-accent/10 transition-colors">
                  Start Optimize Flow
                </button>
              </div>
            )}
          </div>
        )}

        {activeTab === "history" && (
          <HistoryTab history={history} />
        )}
      </div>
    </div>
  )
}
