/** Databricks docs page for creating a Genie Agent with Genie Code. */
export const GENIE_AGENTS_SETUP_DOCS_URL = "https://docs.databricks.com/genie-agents/set-up"

/**
 * Build the Databricks UI URL for the Genie Agents list page, or null when the
 * workspace host is unknown (a relative link would resolve against the app's
 * own origin, not the workspace).
 */
export function genieAgentsListUrl(workspaceHost: string | null): string | null {
  const host = workspaceHost?.trim().replace(/\/+$/, "")
  return host ? `${host}/genie/rooms` : null
}
