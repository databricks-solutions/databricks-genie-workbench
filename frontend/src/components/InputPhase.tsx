/**
 * Input phase component for entering Genie Space ID or pasting JSON.
 */

import { useState } from "react"
import { Search, FileJson, Loader2, AlertCircle, Sparkles, ClipboardCheck, Zap, ArrowLeft } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent, CardDescription, CardHeader } from "@/components/ui/card"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import type { AppMode } from "@/types"
import { cn } from "@/lib/utils"

interface InputPhaseProps {
  spaceData: Record<string, unknown> | null
  onSelectMode: (mode: AppMode | null) => void
  onClearSpaceData: () => void
  onFetchSpace: (spaceId: string) => Promise<void>
  onParseJson: (jsonContent: string) => Promise<void>
  isLoading: boolean
  error: string | null
}

export function InputPhase({
  spaceData,
  onSelectMode,
  onClearSpaceData,
  onFetchSpace,
  onParseJson,
  isLoading,
  error,
}: InputPhaseProps) {
  const [spaceId, setSpaceId] = useState("")
  const [jsonContent, setJsonContent] = useState("")

  const handleFetch = async () => {
    if (spaceId.trim()) {
      await onFetchSpace(spaceId.trim())
    }
  }

  const handleParse = async () => {
    if (jsonContent.trim()) {
      await onParseJson(jsonContent.trim())
    }
  }

  // Input form view (shown first, before space is loaded)
  if (!spaceData) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] animate-stagger">
        {/* Welcome intro */}
        <div className="text-center mb-10 max-w-2xl">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl gradient-accent shadow-xl shadow-accent/25 dark:glow-accent mb-6">
            <Sparkles className="w-8 h-8 text-white" />
          </div>

          <h1 className="text-4xl font-display font-extrabold text-primary mb-4 tracking-tight">
            Welcome to Genie<span className="text-accent">Rx</span>
          </h1>
          <p className="text-lg text-secondary leading-relaxed">
            Getting the most out of your Genie Space starts with a solid configuration.
            Enter your Space ID to get started.
          </p>
        </div>

        <Card className="w-full max-w-2xl shadow-xl dark:card-glow">
          <CardHeader className="text-center pb-2">
            <CardDescription className="text-base">
              <span className="font-semibold text-primary">Get Started:</span>{" "}
              Enter your Genie Space ID or paste the JSON configuration
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Tabs defaultValue="fetch" className="w-full">
              <TabsList className="grid w-full grid-cols-2 mb-6">
                <TabsTrigger value="fetch">
                  <Search className="w-4 h-4 mr-2" />
                  Fetch by ID
                </TabsTrigger>
                <TabsTrigger value="paste">
                  <FileJson className="w-4 h-4 mr-2" />
                  Paste JSON
                </TabsTrigger>
              </TabsList>

              <TabsContent value="fetch">
                <div className="space-y-4">
                  <div>
                    <label
                      htmlFor="space-id"
                      className="block text-sm font-medium text-secondary mb-2"
                    >
                      Genie Space ID
                    </label>
                    <div className="flex gap-3">
                      <Input
                        id="space-id"
                        placeholder="e.g., 01f0627099691651968d0a92a26b06e9"
                        value={spaceId}
                        onChange={(e) => setSpaceId(e.target.value)}
                        onKeyDown={(e) => e.key === "Enter" && handleFetch()}
                        disabled={isLoading}
                        className="font-mono"
                      />
                      <Button
                        onClick={handleFetch}
                        disabled={!spaceId.trim() || isLoading}
                      >
                        {isLoading ? (
                          <Loader2 className="w-4 h-4 animate-spin" />
                        ) : (
                          <>
                            <Search className="w-4 h-4 mr-2" />
                            Fetch
                          </>
                        )}
                      </Button>
                    </div>
                  </div>
                  <p className="text-sm text-muted">
                    The unique identifier for your Databricks Genie Space. You can find
                    this in the Genie Space URL.
                  </p>
                </div>
              </TabsContent>

              <TabsContent value="paste">
                <div className="space-y-4">
                  <div>
                    <label
                      htmlFor="json-content"
                      className="block text-sm font-medium text-secondary mb-2"
                    >
                      Genie Space JSON
                    </label>
                    <Textarea
                      id="json-content"
                      placeholder='{"serialized_space": "{\\"config\\": {...}, \\"data_sources\\": {...}, ...}", ...}'
                      value={jsonContent}
                      onChange={(e) => setJsonContent(e.target.value)}
                      disabled={isLoading}
                      className="min-h-[200px] font-mono text-sm"
                    />
                  </div>
                  <div className="flex justify-end">
                    <Button
                      onClick={handleParse}
                      disabled={!jsonContent.trim() || isLoading}
                    >
                      {isLoading ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <>
                          <FileJson className="w-4 h-4 mr-2" />
                          Load JSON
                        </>
                      )}
                    </Button>
                  </div>

                  <details className="text-sm text-muted cursor-pointer group">
                    <summary className="font-medium hover:text-primary transition-colors">
                      How to get the serialized JSON
                    </summary>
                    <pre className="mt-3 p-4 bg-elevated rounded-lg overflow-x-auto text-xs border border-default">
{`import requests

your_workspace_url = "https://your-workspace.cloud.databricks.com"
your_genie_space_id = ""

response = requests.get(
    f"{your_workspace_url}/api/2.0/genie/spaces/{your_genie_space_id}",
    headers={
        "Authorization": f"Bearer {token}",  # enter PAT token
        "Content-Type": "application/json",
    },
    params={
        "include_serialized_space": "true",
    },
)

response.json()`}
                    </pre>
                  </details>
                </div>
              </TabsContent>
            </Tabs>

            {error && (
              <div className="mt-4 flex items-start gap-2 p-4 bg-danger/10 dark:bg-danger/15 rounded-lg text-danger border border-danger/20">
                <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5" />
                <p className="text-sm">{error}</p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    )
  }

  // Mode selection view (shown after space is loaded)
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] animate-stagger">
      {/* Back button */}
      <div className="w-full max-w-3xl mb-6">
        <button
          onClick={onClearSpaceData}
          className="inline-flex items-center gap-2 text-sm text-muted hover:text-primary transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to input
        </button>
      </div>

      {/* Intro */}
      <div className="text-center mb-10 max-w-2xl">
        <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl gradient-accent shadow-xl shadow-accent/25 dark:glow-accent mb-6">
          <Sparkles className="w-8 h-8 text-white" />
        </div>

        <h1 className="text-4xl font-display font-extrabold text-primary mb-4 tracking-tight">
          Space Loaded
        </h1>
        <p className="text-lg text-secondary leading-relaxed">
          Your Genie Space configuration is ready.
          Choose how you want to work with it.
        </p>
      </div>

      {/* Mode selection cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-3xl">
        {/* Analyze option */}
        <button
          onClick={() => onSelectMode("analyze")}
          className={cn(
            "group relative p-6 rounded-2xl border-2 border-default bg-surface cursor-pointer",
            "hover:border-accent hover:shadow-xl hover:shadow-accent/10 dark:hover:glow-accent",
            "transition-all duration-300 text-left"
          )}
        >
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-accent/10 dark:bg-accent/20 flex items-center justify-center">
              <ClipboardCheck className="w-6 h-6 text-accent" />
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-primary mb-1">
                Analyze Genie Space
              </h3>
              <p className="text-sm text-secondary leading-relaxed">
                Review your configuration across{" "}
                <span className="font-semibold text-primary">10 key areas</span>,
                identify issues, and get clear guidance on how to fix them.
              </p>
            </div>
          </div>
        </button>

        {/* Optimize option */}
        <button
          onClick={() => onSelectMode("optimize")}
          className={cn(
            "group relative p-6 rounded-2xl border-2 border-default bg-surface cursor-pointer",
            "hover:border-accent hover:shadow-xl hover:shadow-accent/10 dark:hover:glow-accent",
            "transition-all duration-300 text-left"
          )}
        >
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-accent/10 dark:bg-accent/20 flex items-center justify-center">
              <Zap className="w-6 h-6 text-accent" />
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-primary mb-1">
                Optimize Genie Space
              </h3>
              <p className="text-sm text-secondary leading-relaxed">
                Run benchmark questions, measure accuracy, and optimize your space
                for better query performance.
              </p>
            </div>
          </div>
        </button>
      </div>
    </div>
  )
}
