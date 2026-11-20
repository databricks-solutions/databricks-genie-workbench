import { useEffect, useState } from "react"
import { Sparkles, ExternalLink } from "lucide-react"
import { getSettings } from "@/lib/api"
import { genieAgentsListUrl, GENIE_AGENTS_SETUP_DOCS_URL } from "@/lib/genie-agents"

interface GenieCodeSetupBannerViewProps {
  workspaceHost: string | null
  /** One-line variant for the home page; the full variant is used on the Create page. */
  compact?: boolean
}

const SETUP_LINK_CLASS = "inline-flex items-center gap-1 text-xs font-medium text-accent hover:underline"

/**
 * Tip shown on the Create page: Genie Agents can also be set up directly in
 * Databricks with Genie Code, then scored and optimized in the Workbench.
 */
export function GenieCodeSetupBannerView({ workspaceHost, compact = false }: GenieCodeSetupBannerViewProps) {
  if (compact) {
    return (
      <div
        role="note"
        data-testid="genie-code-setup-banner"
        className="flex flex-wrap items-center gap-x-4 gap-y-2 rounded-lg border border-accent/30 bg-accent/5 px-4 py-2.5"
      >
        <Sparkles className="h-4 w-4 flex-shrink-0 text-accent" />
        <p className="flex-1 min-w-[240px] text-sm text-secondary">
          <span className="font-semibold text-primary">Set up Genie Agents with Genie Code.</span>{" "}
          Create an agent directly in Databricks, then score and optimize it here.
        </p>
        <a href={genieAgentsListUrl(workspaceHost)} target="_blank" rel="noopener noreferrer" className={SETUP_LINK_CLASS}>
          Open Genie Agents
          <ExternalLink className="h-3 w-3" />
        </a>
        <a href={GENIE_AGENTS_SETUP_DOCS_URL} target="_blank" rel="noopener noreferrer" className={SETUP_LINK_CLASS}>
          How it works
          <ExternalLink className="h-3 w-3" />
        </a>
      </div>
    )
  }

  return (
    <div
      role="note"
      data-testid="genie-code-setup-banner"
      className="flex items-start gap-3 rounded-lg border border-accent/30 bg-accent/5 px-4 py-3"
    >
      <Sparkles className="mt-0.5 h-4 w-4 flex-shrink-0 text-accent" />
      <div className="flex-1 space-y-2">
        <p className="text-sm font-semibold text-primary">
          Set up Genie Agents with Genie Code
        </p>
        <p className="text-sm text-muted">
          You can also create Genie Agents directly in Databricks. Open Genie Agents in your
          workspace, click New, and describe what the agent should do, or select your tables and
          choose Create with guided setup. Genie Code then walks you through the agent details,
          relevant data, business context, and initial benchmarks. After you create the agent,
          come back here to score and optimize it.
        </p>
        <div className="flex flex-wrap items-center gap-4 pt-1">
          <a
            href={genieAgentsListUrl(workspaceHost)}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-white hover:bg-accent/90 transition-colors"
          >
            Open Genie Agents
            <ExternalLink className="h-3 w-3" />
          </a>
          <a
            href={GENIE_AGENTS_SETUP_DOCS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className={SETUP_LINK_CLASS}
          >
            How to set up an agent with Genie Code
            <ExternalLink className="h-3 w-3" />
          </a>
        </div>
      </div>
    </div>
  )
}

/** Banner wrapper that looks up the workspace host for the Genie Agents link. */
export function GenieCodeSetupBanner({ compact = false }: { compact?: boolean }) {
  const [workspaceHost, setWorkspaceHost] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    getSettings()
      .then((s) => { if (!cancelled) setWorkspaceHost(s.databricks_host) })
      .catch(() => { /* fall back to a relative link */ })
    return () => { cancelled = true }
  }, [])

  return <GenieCodeSetupBannerView workspaceHost={workspaceHost} compact={compact} />
}
