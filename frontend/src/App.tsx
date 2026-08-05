/**
 * App - Genie Workbench root component.
 * Supports five top-level views: SpaceList, SpaceDetail, AdminDashboard, CreateSpace, HowItWorks.
 */
import { useCallback, useEffect, useState, Component } from "react"
import type { ReactNode, ErrorInfo } from "react"
import { LayoutGrid, BarChart2, BookOpen } from "lucide-react"
import { ThemeToggle } from "@/components/ThemeToggle"
import { useTheme } from "@/hooks/useTheme"
import { SpaceList } from "@/pages/SpaceList"
import { SpaceDetail } from "@/pages/SpaceDetail"
import { AdminDashboard } from "@/pages/AdminDashboard"
import { HowItWorks } from "@/pages/HowItWorks"
import { CreateAgentChat } from "@/components/CreateAgentChat"
import { getSpaceDetail } from "@/lib/api"
import {
  LIST_ROUTE,
  buildAppRouteUrl,
  isSpaceTab,
  parseAppRoute,
  routesEqual,
} from "@/lib/navigation"
import type { AppRoute, SpaceTab } from "@/lib/navigation"

class ErrorBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null }
  static getDerivedStateFromError(error: Error) { return { error } }
  componentDidCatch(error: Error, info: ErrorInfo) { console.error("React ErrorBoundary caught:", error, info) }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 32, fontFamily: "monospace" }}>
          <h2 style={{ color: "red" }}>Something went wrong</h2>
          <pre style={{ whiteSpace: "pre-wrap", marginTop: 8 }}>{this.state.error.message}</pre>
          <pre style={{ whiteSpace: "pre-wrap", marginTop: 8, fontSize: 12, opacity: 0.7 }}>{this.state.error.stack}</pre>
          <button onClick={() => this.setState({ error: null })} style={{ marginTop: 16, padding: "8px 16px" }}>Try Again</button>
        </div>
      )
    }
    return this.props.children
  }
}

interface DetailState {
  spaceId: string
  displayName: string
  spaceUrl?: string
  autoScan?: boolean
}

interface DetailError {
  spaceId: string
  message: string
}

function initialRoute(): AppRoute {
  return typeof window === "undefined" ? LIST_ROUTE : parseAppRoute(window.location.search)
}

function stringField(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined
}

export default function App() {
  useTheme()
  const [route, setRoute] = useState<AppRoute>(initialRoute)
  const [detailState, setDetailState] = useState<DetailState | null>(null)
  const [detailError, setDetailError] = useState<DetailError | null>(null)
  const currentView = route.view
  const routeSpaceId = route.view === "detail" ? route.spaceId : undefined
  const detailReady = Boolean(routeSpaceId && detailState?.spaceId === routeSpaceId)
  const detailErrorMessage = detailError && detailError.spaceId === routeSpaceId ? detailError.message : null
  const detailLoading = currentView === "detail" && !detailReady && !detailErrorMessage

  const navigate = useCallback((nextRoute: AppRoute, replace = false) => {
    if (routesEqual(route, nextRoute)) return
    if (typeof window !== "undefined") {
      const nextUrl = buildAppRouteUrl(nextRoute, window.location.href)
      window.history[replace ? "replaceState" : "pushState"](window.history.state, "", nextUrl)
    }
    setRoute(nextRoute)
  }, [route])

  useEffect(() => {
    const handlePopState = () => setRoute(parseAppRoute(window.location.search))
    window.addEventListener("popstate", handlePopState)
    return () => window.removeEventListener("popstate", handlePopState)
  }, [])

  useEffect(() => {
    if (route.view !== "detail" || !route.spaceId) return
    if (detailState?.spaceId === route.spaceId) return

    const spaceId = route.spaceId
    let cancelled = false
    getSpaceDetail(spaceId)
      .then((detail) => {
        if (cancelled) return
        const title = stringField(detail.space.title)
          ?? stringField(detail.space.display_name)
          ?? spaceId
        setDetailState({
          spaceId,
          displayName: title,
          spaceUrl: stringField(detail.space.space_url),
        })
        setDetailError(null)
      })
      .catch((error) => {
        if (!cancelled) {
          setDetailError({
            spaceId,
            message: error instanceof Error ? error.message : "Failed to load this Agent",
          })
        }
      })

    return () => { cancelled = true }
  }, [route, detailState])

  const handleSelectSpace = (spaceId: string, displayName: string, spaceUrl?: string, initialTab?: string, autoScan?: boolean) => {
    const tab = isSpaceTab(initialTab) ? initialTab : "score"
    setDetailState({ spaceId, displayName, spaceUrl, autoScan })
    setDetailError(null)
    navigate({ view: "detail", spaceId, tab })
  }

  const handleBack = () => {
    navigate(LIST_ROUTE)
  }

  const handleNavList = () => {
    navigate(LIST_ROUTE)
  }

  const handleNavAdmin = () => {
    navigate({ view: "admin" })
  }

  const handleNavCreate = () => {
    navigate({ view: "create" })
  }

  const handleNavHowItWorks = () => {
    navigate({ view: "how-it-works" })
  }

  const handleDetailNavigate = (tab: SpaceTab, runId?: string) => {
    if (route.view !== "detail" || !route.spaceId) return
    navigate({ view: "detail", spaceId: route.spaceId, tab, runId })
  }

  const handleCreated = (spaceId: string, displayName: string, spaceUrl?: string, initialTab?: string) => {
    handleSelectSpace(spaceId, displayName, spaceUrl, initialTab, initialTab === "score")
  }

  return (
    <ErrorBoundary>
    <div className="min-h-screen bg-background text-primary">
      {/* Top header */}
      <header className="sticky top-0 z-50 bg-surface/80 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center justify-between">
          {/* Logo + title */}
          <div className="flex items-center gap-3">
            <div className="w-7 h-7 rounded-lg bg-accent flex items-center justify-center flex-shrink-0">
              <svg viewBox="0 0 32 32" className="w-5 h-5">
                <svg x="6" y="6" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/>
                </svg>
                <path d="M7 2l1 3 3 1-3 1-1 3-1-3-3-1 3-1z" fill="white"/>
              </svg>
            </div>
            <span className="text-base font-display font-bold text-primary">Genie Workbench</span>
          </div>

          {/* Nav links */}
          <nav className="flex items-center gap-1">
            <button
              onClick={handleNavList}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                currentView === "list" || currentView === "detail"
                  ? "bg-accent/10 text-accent"
                  : "text-muted hover:text-secondary hover:bg-surface-secondary"
              }`}
            >
              <LayoutGrid className="w-4 h-4" />
              Agents
            </button>
            <button
              onClick={handleNavAdmin}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                currentView === "admin"
                  ? "bg-accent/10 text-accent"
                  : "text-muted hover:text-secondary hover:bg-surface-secondary"
              }`}
            >
              <BarChart2 className="w-4 h-4" />
              Admin
            </button>
            <button
              onClick={handleNavHowItWorks}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                currentView === "how-it-works"
                  ? "bg-accent/10 text-accent"
                  : "text-muted hover:text-secondary hover:bg-surface-secondary"
              }`}
            >
              <BookOpen className="w-4 h-4" />
              How It Works
            </button>
          </nav>

          <ThemeToggle />
        </div>
        <div className="header-accent-line" />
      </header>

      {/* Main content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {currentView === "list" && (
          <SpaceList onSelectSpace={handleSelectSpace} onCreateSpace={handleNavCreate} />
        )}

        {currentView === "detail" && detailLoading && (
          <div className="py-16 text-center text-sm text-muted">Loading Agent...</div>
        )}

        {currentView === "detail" && !detailLoading && detailErrorMessage && (
          <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-5 text-red-400">
            <p>{detailErrorMessage}</p>
            <button onClick={handleNavList} className="mt-3 text-sm text-accent hover:underline">
              Return to Agents
            </button>
          </div>
        )}

        {currentView === "detail" && !detailLoading && !detailErrorMessage && detailState && route.view === "detail" && detailState.spaceId === route.spaceId && (
          <SpaceDetail
            spaceId={detailState.spaceId}
            displayName={detailState.displayName}
            spaceUrl={detailState.spaceUrl}
            activeTab={route.tab ?? "score"}
            runId={route.runId}
            autoScan={detailState.autoScan}
            onBack={handleBack}
            onNavigate={handleDetailNavigate}
          />
        )}

        {currentView === "admin" && (
          <AdminDashboard onSelectSpace={handleSelectSpace} />
        )}

        {currentView === "how-it-works" && (
          <HowItWorks />
        )}

        {/* CreateAgentChat stays mounted (hidden when inactive) so SSE streams
            and component state survive navigation to other pages. */}
        <div className={currentView === "create" ? undefined : "hidden"}>
          <div className="mb-4">
            <h1 className="text-2xl font-bold text-primary">Create Genie Agent</h1>
            <p className="text-muted text-sm mt-1">
              AI-guided creation with live progress tracking — describe what you need and fill in details as you go
            </p>
          </div>
          <CreateAgentChat onCreated={handleCreated} />
        </div>
      </main>
    </div>
    </ErrorBoundary>
  )
}
