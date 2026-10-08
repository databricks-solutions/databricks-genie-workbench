/**
 * PR #332 M1c-2 fidelity frames — the REAL agent-page surfaces in each access state
 * (M1c-D5/D6): notices, the read-only Score tab, locked sections, the Optimize viewer
 * list, the no-access page state, and a space-list card with a refused scan.
 */
import { Rocket } from "lucide-react"
import { IQScoreTab } from "@/pages/IQScoreTab"
import { SpaceCard } from "@/pages/SpaceList"
import { AutoOptimizeViewerView } from "@/components/auto-optimize/AutoOptimizeViewerTab"
import { CheckingAccess, LockedSection, SpaceAccessNotice, SpaceNoAccessState } from "@/components/space-access/SpaceAccessChrome"
import { CONFIG_NEEDS_EDIT, MODEL_NEEDS_EDIT, SCAN_NEEDS_EDIT } from "@/lib/space-access"
import type { GSORunSummary, ScanResult, SpaceListItem } from "@/types"

const noop = () => {}
const scan: ScanResult = {
  space_id: "s1", score: 7, total: 12, maturity: "Ready to Optimize", optimization_accuracy: null,
  checks: [], findings: ["6/20 visible columns look internal/noisy"],
  next_steps: ["Hide noisy internal, audit, raw, ingestion, and opaque technical columns"],
  warnings: ["SQL patterns found in text instructions — move to Example SQLs or SQL Expressions."],
  warning_next_steps: ["Restructure text instructions for optimal LLM context usage"],
  scanned_at: "2026-09-20T10:00:00Z",
}

const run: GSORunSummary = {
  run_id: "6f1c2a54-5b4e-4d7e-9c1f-2a3b4c5d6e7f",
  space_id: "s",
  status: "CONVERGED",
  started_at: "2026-09-20T10:00:00Z",
  completed_at: null,
  best_accuracy: 82,
  best_iteration: null,
  convergence_reason: null,
  llm_model: "databricks-claude-sonnet-4-6",
  triggered_by: "ana@example.com",
  benchmark_policy: "review_only",
}

const space: SpaceListItem = {
  space_id: "s1",
  display_name: "Sales Agent",
  score: 7,
  maturity: "Ready to Optimize",
  optimization_accuracy: null,
  is_starred: false,
  last_scanned: "2026-09-20T10:00:00Z",
  space_url: null,
}

/** Copied from SpaceDetail Ready-to-Optimize actionProps (live actionLabel/actionDescription). */
const editorActionProps = {
  onAction: noop,
  actionLabel: "Run Optimization",
  actionIcon: <Rocket className="w-4 h-4" />,
  actionDescription: (
    <>
      This agent passed the configuration checks. Auto-Optimize will benchmark real
      questions, tune the selected levers, and apply only changes that improve the
      measured result.
    </>
  ),
}

export function ScoreEditorFrame() {
  return (
    <div className="space-y-4">
      <IQScoreTab
        scanResult={scan}
        isScanning={false}
        spaceId="s1"
        onScan={noop}
        onNavigateToOptimize={noop}
        {...editorActionProps}
      />
    </div>
  )
}

export function ScoreViewerFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice access="view" reason={null} />
      <IQScoreTab scanResult={scan} isScanning={false} spaceId="s1" />
      <LockedSection title="Agent Configuration" message={CONFIG_NEEDS_EDIT} />
    </div>
  )
}

export function ScoreViewerUnscannedFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice access="view" reason={null} />
      <IQScoreTab scanResult={null} isScanning={false} spaceId="s1" />
      <LockedSection title="Agent Configuration" message={CONFIG_NEEDS_EDIT} />
    </div>
  )
}

export function ScoreUnknownFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice
        access="unknown"
        reason="Could not verify your access to this Genie Agent. Try again shortly."
      />
      <IQScoreTab scanResult={scan} isScanning={false} spaceId="s1" />
    </div>
  )
}

export function NoAccessPageFrame() {
  return (
    <div className="space-y-4">
      <SpaceNoAccessState reason="You need Can View permission on this Genie Agent." />
    </div>
  )
}

export function ModelViewerFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice access="view" reason={null} />
      <LockedSection title="Model" message={MODEL_NEEDS_EDIT} />
    </div>
  )
}

export function CheckingFrame() {
  return (
    <div className="space-y-4">
      <CheckingAccess />
    </div>
  )
}

export function OptimizeViewerFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice access="view" reason={null} />
      <AutoOptimizeViewerView configured activeRunId="r-active" runs={[run]} loading={false} />
    </div>
  )
}

export function OptimizeViewerEmptyFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice access="view" reason={null} />
      <AutoOptimizeViewerView configured activeRunId={null} runs={[]} loading={false} />
    </div>
  )
}

export function SpaceCardRefusedFrame() {
  return (
    <div className="space-y-4">
      <div className="max-w-sm">
        <SpaceCard
          space={space}
          scanning={false}
          scanError={SCAN_NEEDS_EDIT}
          onSelect={noop}
          onToggleStar={noop}
          onScan={noop}
        />
      </div>
    </div>
  )
}
