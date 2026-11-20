/** Databricks docs page for creating a Genie Agent with Genie Code. */
export const GENIE_AGENTS_SETUP_DOCS_URL = "https://docs.databricks.com/genie-agents/set-up"

/** Build the Databricks UI URL for the Genie Agents list page. */
export function genieAgentsListUrl(workspaceHost: string | null): string {
  if (!workspaceHost) return "/genie/rooms"
  return `${workspaceHost.replace(/\/+$/, "")}/genie/rooms`
}
