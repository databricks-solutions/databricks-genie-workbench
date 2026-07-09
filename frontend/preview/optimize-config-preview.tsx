// TEMP isolated-render harness for visually verifying auto-optimize UI.
// Not part of the app build. Mocks the single API call (getModels) so no
// Databricks resources are needed. Includes a light/dark toggle.
import { useState } from "react"
import { createRoot } from "react-dom/client"
import "../src/index.css"
import { OptimizationConfig } from "../src/components/auto-optimize/OptimizationConfig"
import { AttemptLedger } from "../src/components/auto-optimize/AttemptLedger"
import { AttemptLadder } from "../src/components/auto-optimize/AttemptLadder"
import type { GSOPermissionCheck, GSOAttempt } from "../src/types"

// Stub the /models fetch so the harness needs no backend.
const realFetch = window.fetch.bind(window)
window.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
  const url = typeof input === "string" ? input : input.toString()
  if (url.includes("/models")) {
    return Promise.resolve(
      new Response(
        JSON.stringify([
          { name: "databricks-claude-sonnet-4-6", displayName: "Claude Sonnet 4.6", isDefault: true },
          { name: "databricks-claude-opus-4-1", displayName: "Claude Opus 4.1", isDefault: false },
        ]),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    )
  }
  return realFetch(input, init)
}) as typeof window.fetch

const permissions: GSOPermissionCheck = {
  sp_display_name: "sp-genie-workbench",
  sp_application_id: "00000000-0000-0000-0000-000000000000",
  sp_has_manage: true,
  schemas: [],
  can_start: true,
  errors: [],
}

// Mirrors the reported screenshot: baseline 50%, three patch attempts climbing
// to 80%, attempt 3 is the champion + highest + terminal.
const attempts: GSOAttempt[] = [
  { attemptNo: 1, attemptMode: "llm_patch", iteration: 1, evalScope: "full", lever: null, accuracy: 60, bestAccuracy: 60, decision: "accept", decisionReason: null, rolledBack: false, rollbackReason: null, isChampion: false, currentHypothesis: null, terminalReason: null },
  { attemptNo: 2, attemptMode: "llm_patch", iteration: 2, evalScope: "full", lever: null, accuracy: 73.3, bestAccuracy: 73.3, decision: "accept", decisionReason: null, rolledBack: false, rollbackReason: null, isChampion: false, currentHypothesis: null, terminalReason: null },
  { attemptNo: 3, attemptMode: "llm_patch", iteration: 3, evalScope: "full", lever: null, accuracy: 80, bestAccuracy: 80, decision: "terminal", decisionReason: null, rolledBack: false, rollbackReason: null, isChampion: true, currentHypothesis: null, terminalReason: "TARGET_REACHED" },
]

function Harness() {
  const [dark, setDark] = useState(false)

  function toggle() {
    const next = !dark
    setDark(next)
    document.documentElement.classList.toggle("dark", next)
    document.documentElement.classList.toggle("light", !next)
  }

  return (
    <div style={{ minHeight: "100vh", padding: 32 }}>
      <div style={{ maxWidth: 1200, margin: "0 auto" }}>
        <button
          onClick={toggle}
          style={{
            marginBottom: 16,
            padding: "6px 12px",
            borderRadius: 8,
            border: "1px solid var(--color-border-default, #ccc)",
            fontSize: 13,
            cursor: "pointer",
          }}
        >
          Toggle {dark ? "light" : "dark"} mode
        </button>

        {/* Ladder + Ledger side by side, exactly as RunDetailView / AutoOptimizeTab render them. */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2" style={{ marginBottom: 32 }}>
          <AttemptLadder baselineAccuracy={50} attempts={attempts} targetUnit={0.9} />
          <AttemptLedger baselineAccuracy={50} attempts={attempts} baselineIsChampion={false} />
        </div>

        <OptimizationConfig
          spaceId="preview-space"
          onStarted={() => {}}
          hasActiveRun={false}
          permissions={permissions}
          permsLoading={false}
          healthIssues={[]}
        />
      </div>
    </div>
  )
}

document.documentElement.classList.add("light")
createRoot(document.getElementById("root")!).render(<Harness />)
