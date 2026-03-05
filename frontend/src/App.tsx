/**
 * App - Genie Workbench root component.
 * Supports four top-level views: SpaceList, SpaceDetail, AdminDashboard, CreateSpace.
 */
import { useState } from "react"
import { LayoutGrid, BarChart2, PlusCircle } from "lucide-react"
import { ThemeToggle } from "@/components/ThemeToggle"
import { useTheme } from "@/hooks/useTheme"
import { SpaceList } from "@/pages/SpaceList"
import { SpaceDetail } from "@/pages/SpaceDetail"
import { AdminDashboard } from "@/pages/AdminDashboard"
import { CreateAgentChat } from "@/components/CreateAgentChat"

type View = "list" | "detail" | "admin" | "create"

interface DetailState {
  spaceId: string
  displayName: string
}

export default function App() {
  useTheme()
  const [currentView, setCurrentView] = useState<View>("list")
  const [detailState, setDetailState] = useState<DetailState | null>(null)

  const handleSelectSpace = (spaceId: string, displayName: string) => {
    setDetailState({ spaceId, displayName })
    setCurrentView("detail")
  }

  const handleBack = () => {
    setCurrentView("list")
    setDetailState(null)
  }

  const handleNavList = () => {
    setCurrentView("list")
    setDetailState(null)
  }

  const handleNavAdmin = () => {
    setCurrentView("admin")
    setDetailState(null)
  }

  const handleNavCreate = () => {
    setCurrentView("create")
    setDetailState(null)
  }

  const handleCreated = (spaceId: string, displayName: string) => {
    handleSelectSpace(spaceId, displayName)
  }

  return (
    <div className="min-h-screen bg-background text-primary">
      {/* Top header */}
      <header className="sticky top-0 z-50 border-b border-default bg-surface/80 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center justify-between">
          {/* Logo + title */}
          <div className="flex items-center gap-3">
            <div className="w-7 h-7 rounded-lg bg-accent flex items-center justify-center flex-shrink-0">
              <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4 text-white" stroke="currentColor" strokeWidth="2">
                <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" strokeLinecap="round" strokeLinejoin="round" />
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
              Spaces
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
              onClick={handleNavCreate}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                currentView === "create"
                  ? "bg-accent/10 text-accent"
                  : "text-muted hover:text-secondary hover:bg-surface-secondary"
              }`}
            >
              <PlusCircle className="w-4 h-4" />
              Create Space
            </button>
          </nav>

          <ThemeToggle />
        </div>
      </header>

      {/* Main content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {currentView === "list" && (
          <SpaceList onSelectSpace={handleSelectSpace} />
        )}

        {currentView === "detail" && detailState && (
          <SpaceDetail
            spaceId={detailState.spaceId}
            displayName={detailState.displayName}
            onBack={handleBack}
          />
        )}

        {currentView === "admin" && (
          <AdminDashboard onSelectSpace={handleSelectSpace} />
        )}

        {currentView === "create" && (
          <div>
            <div className="mb-4">
              <h1 className="text-2xl font-bold text-primary">Create Genie Space</h1>
              <p className="text-muted text-sm mt-1">
                AI-guided creation with live progress tracking — describe what you need and fill in details as you go
              </p>
            </div>
            <CreateAgentChat onCreated={handleCreated} />
          </div>
        )}
      </main>
    </div>
  )
}
