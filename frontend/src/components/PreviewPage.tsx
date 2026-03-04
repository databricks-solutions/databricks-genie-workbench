/**
 * PreviewPage component displaying side-by-side JSON diff of config changes.
 */

import { useState } from "react"
import { ArrowLeft, Loader2, Sparkles, AlertTriangle, Expand, Minimize2, Plus, ExternalLink, CheckCircle2 } from "lucide-react"
import { cn } from "@/lib/utils"
import { diffViewerStyles } from "@/lib/diffViewerStyles"
import ReactDiffViewer, { DiffMethod } from "react-diff-viewer-continued"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { useTheme } from "@/hooks/useTheme"
import type { GenieCreateResponse } from "@/types"

interface PreviewPageProps {
  currentConfig: Record<string, unknown> | null
  previewConfig: Record<string, unknown> | null
  summary: string | null
  isLoading: boolean
  error: string | null
  selectedCount: number
  onBack: () => void
  // Genie creation props
  isCreating?: boolean
  createError?: string | null
  createdResult?: GenieCreateResponse | null
  onCreateGenieSpace?: (displayName: string) => void
}

export function PreviewPage({
  currentConfig,
  previewConfig,
  summary,
  isLoading,
  error,
  selectedCount,
  onBack,
  isCreating = false,
  createError = null,
  createdResult = null,
  onCreateGenieSpace,
}: PreviewPageProps) {
  const { isDark } = useTheme()
  const [showFullDiff, setShowFullDiff] = useState(false)
  const [newSpaceName, setNewSpaceName] = useState("")

  const handleCreate = () => {
    if (onCreateGenieSpace && newSpaceName.trim()) {
      onCreateGenieSpace(newSpaceName.trim())
    }
  }

  const currentJson = currentConfig ? JSON.stringify(currentConfig, null, 2) : ""
  const previewJson = previewConfig ? JSON.stringify(previewConfig, null, 2) : ""

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-primary">
            Configuration Preview
          </h1>
          <p className="text-muted">
            {isLoading
              ? "Generating merged configuration..."
              : `Preview of ${selectedCount} applied suggestion${selectedCount !== 1 ? "s" : ""}`}
          </p>
        </div>
        <Button variant="outline" onClick={onBack}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Suggestions
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
                Merging configuration changes
              </p>
              <p className="text-sm text-muted mt-1">
                AI is applying your selected suggestions to create the new configuration...
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
                Failed to generate preview
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
                <p className="font-medium text-primary mb-1">Changes Applied</p>
                <p className="text-secondary text-sm">{summary}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Creation success */}
      {createdResult && (
        <Card className="border-green-300 dark:border-green-800 bg-green-50 dark:bg-green-950/30">
          <CardContent className="py-6">
            <div className="flex items-start gap-3">
              <CheckCircle2 className="w-6 h-6 text-green-600 dark:text-green-400 flex-shrink-0" />
              <div className="flex-1">
                <p className="font-medium text-green-700 dark:text-green-400 text-lg">
                  Genie Space Created Successfully
                </p>
                <p className="text-sm text-green-600 dark:text-green-500 mt-1">
                  Your new Genie Space "{createdResult.display_name}" is ready to use.
                </p>
                <div className="mt-4">
                  <a
                    href={createdResult.space_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-green-600 hover:bg-green-700 text-white font-medium transition-colors"
                  >
                    <ExternalLink className="w-4 h-4" />
                    Open in Databricks
                  </a>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Create Genie Space section */}
      {previewConfig && !isLoading && onCreateGenieSpace && !createdResult && (
        <Card className="border-accent/30">
          <CardContent className="py-6">
            <div className="flex flex-col gap-4">
              <div>
                <h3 className="text-lg font-medium text-primary mb-1">
                  Create New Genie Space
                </h3>
                <p className="text-sm text-muted">
                  Create a new Genie Space with these optimizations applied.
                  The original space will not be modified.
                </p>
              </div>
              <div className="flex items-center gap-3">
                <input
                  type="text"
                  value={newSpaceName}
                  onChange={(e) => setNewSpaceName(e.target.value)}
                  placeholder="Enter a name for the new Genie Space"
                  className="flex-1 px-3 py-2 rounded-lg border border-default bg-surface text-primary placeholder:text-muted focus:outline-none focus:ring-2 focus:ring-accent/50"
                  disabled={isCreating}
                />
                <Button
                  onClick={handleCreate}
                  disabled={!newSpaceName.trim() || isCreating}
                >
                  {isCreating ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Creating...
                    </>
                  ) : (
                    <>
                      <Plus className="w-4 h-4 mr-2" />
                      Create New Genie Space
                    </>
                  )}
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Creation error */}
      {createError && !isCreating && !createdResult && (
        <Card className="border-red-300 dark:border-red-800 bg-red-50 dark:bg-red-950/30">
          <CardContent className="py-6 flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-red-700 dark:text-red-400">
                Failed to create Genie Space
              </p>
              <p className="text-sm text-red-600 dark:text-red-500 mt-1">{createError}</p>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Diff viewer */}
      {previewConfig && !isLoading && (
        <div className="rounded-lg overflow-hidden border border-default">
          <div className="flex border-b border-default">
            <div className="flex-1 px-4 py-2 bg-elevated text-sm font-medium text-secondary">
              Current Configuration
            </div>
            <div className="flex-1 px-4 py-2 bg-elevated text-sm font-medium text-secondary border-l border-default flex items-center justify-between">
              <span>New Configuration</span>
              <div className="flex items-center gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowFullDiff(false)}
                  className={cn("h-7 px-2", !showFullDiff && "bg-sunken")}
                >
                  <Minimize2 className="w-3.5 h-3.5 mr-1.5" />
                  Changes Only
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowFullDiff(true)}
                  className={cn("h-7 px-2", showFullDiff && "bg-sunken")}
                >
                  <Expand className="w-3.5 h-3.5 mr-1.5" />
                  Show All
                </Button>
              </div>
            </div>
          </div>
          <ReactDiffViewer
            oldValue={currentJson}
            newValue={previewJson}
            splitView={true}
            useDarkTheme={isDark}
            compareMethod={DiffMethod.WORDS}
            styles={diffViewerStyles}
            hideLineNumbers={false}
            showDiffOnly={!showFullDiff}
            extraLinesSurroundingDiff={3}
          />
        </div>
      )}
    </div>
  )
}
