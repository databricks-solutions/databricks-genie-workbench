/** Build the Databricks UI URL for a Genie Space. */
export function genieSpaceUrl(spaceId: string, workspaceHost: string | null): string {
  const id = encodeURIComponent(spaceId)
  if (workspaceHost) {
    const host = workspaceHost.replace(/\/+$/, '')
    return `${host}/genie/rooms/${id}`
  }
  return `/genie/rooms/${id}`
}
