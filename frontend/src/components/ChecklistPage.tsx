/**
 * Checklist reference page component.
 */

import { useState, useEffect } from "react"
import { ArrowLeft, Loader2 } from "lucide-react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { getChecklist } from "@/lib/api"

interface ChecklistPageProps {
  onBack: () => void
}

export function ChecklistPage({ onBack }: ChecklistPageProps) {
  const [content, setContent] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function loadChecklist() {
      try {
        const markdown = await getChecklist()
        setContent(markdown)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load checklist")
      } finally {
        setIsLoading(false)
      }
    }
    loadChecklist()
  }, [])

  return (
    <div className="space-y-6 animate-slide-up">
      <div className="flex items-center gap-4">
        <Button variant="ghost" onClick={onBack}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Analysis
        </Button>
      </div>

      <div>
        <h1 className="text-2xl font-display font-bold text-primary">
          Checklist Reference
        </h1>
        <p className="text-muted">
          Genie Space configuration checklist organized by schema
        </p>
      </div>

      <Card>
        <CardContent className="p-6">
          {isLoading && (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-6 h-6 animate-spin text-muted" />
            </div>
          )}

          {error && (
            <div className="text-center py-12 text-danger">
              <p>{error}</p>
            </div>
          )}

          {content && (
            <div className="prose max-w-none">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {content}
              </ReactMarkdown>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
