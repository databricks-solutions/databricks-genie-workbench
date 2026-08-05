export type AppView = "list" | "detail" | "admin" | "create" | "how-it-works"
export type SpaceTab = "score" | "optimize" | "history"

export interface AppRoute {
  view: AppView
  spaceId?: string
  tab?: SpaceTab
  runId?: string
}

const APP_QUERY_KEYS = ["view", "space", "tab", "run"] as const
const APP_VIEWS = new Set<AppView>(["list", "detail", "admin", "create", "how-it-works"])
const SPACE_TABS = new Set<SpaceTab>(["score", "optimize", "history"])

export const LIST_ROUTE: AppRoute = { view: "list" }

export function isSpaceTab(value: string | null | undefined): value is SpaceTab {
  return value != null && SPACE_TABS.has(value as SpaceTab)
}

export function parseAppRoute(search: string): AppRoute {
  const params = new URLSearchParams(search)
  const rawView = params.get("view")
  const spaceId = params.get("space")?.trim()
  const view = rawView && APP_VIEWS.has(rawView as AppView)
    ? rawView as AppView
    : spaceId
      ? "detail"
      : "list"

  if (view !== "detail" || !spaceId) {
    return { view: view === "detail" ? "list" : view }
  }

  const rawTab = params.get("tab")
  const tab = isSpaceTab(rawTab) ? rawTab : "score"
  const runId = tab === "optimize" ? params.get("run")?.trim() || undefined : undefined
  return { view: "detail", spaceId, tab, runId }
}

export function buildAppRouteUrl(route: AppRoute, currentHref: string): string {
  const url = new URL(currentHref)
  for (const key of APP_QUERY_KEYS) url.searchParams.delete(key)

  if (route.view !== "list") url.searchParams.set("view", route.view)
  if (route.view === "detail" && route.spaceId) {
    url.searchParams.set("space", route.spaceId)
    url.searchParams.set("tab", route.tab ?? "score")
    if (route.tab === "optimize" && route.runId) {
      url.searchParams.set("run", route.runId)
    }
  }

  return `${url.pathname}${url.search}${url.hash}`
}

export function routesEqual(left: AppRoute, right: AppRoute): boolean {
  return left.view === right.view
    && left.spaceId === right.spaceId
    && left.tab === right.tab
    && left.runId === right.runId
}
