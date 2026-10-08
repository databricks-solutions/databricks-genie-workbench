/**
 * PR #332 M7b fidelity frames (MV-D119) — the REAL viewer Score tab when a finding and a
 * warning have no viewer-safe form (blanked, remediation kept), and the two deep-link
 * states: Genie's no-access answer and a failed load.
 */
import { IQScoreTab } from "@/pages/IQScoreTab"
import { LockedSection, SpaceAccessNotice, SpaceDetailLoadError } from "@/components/space-access/SpaceAccessChrome"
import { CONFIG_NEEDS_EDIT, SPACE_NO_ACCESS_FALLBACK } from "@/lib/space-access"
import type { ScanResult } from "@/types"

const noop = () => {}
const scan: ScanResult = {
  space_id: "s1", score: 7, total: 12, maturity: "Ready to Optimize", optimization_accuracy: null,
  checks: [
    { label: "Text instructions (>50 chars)", passed: true, detail: "1 instruction(s), 240 chars total — 1 warning(s)", severity: "warning" },
    { label: "Column visibility / noise control", passed: false, detail: "6/20 visible columns look internal/noisy (30%)", severity: "fail" },
    { label: "Optimization workflow completed", passed: false, detail: null, severity: "fail" },
  ],
  findings: ["", "6/20 visible columns look internal/noisy"],
  next_steps: [
    "Add text instructions to explain business context and terminology",
    "Hide noisy internal, audit, raw, ingestion, and opaque technical columns",
  ],
  warnings: ["", "Tables with row-level security — entity matching is silently disabled for these"],
  warning_next_steps: [
    "Restructure text instructions for optimal LLM context usage",
    "Entity matching won't work on tables with row filters or column masks",
  ],
  scanned_at: "2026-09-20T10:00:00Z",
}

export function ScoreViewerAllowlistFrame() {
  return (
    <div className="space-y-4">
      <SpaceAccessNotice access="view" reason={null} />
      <IQScoreTab scanResult={scan} isScanning={false} spaceId="s1" />
      <LockedSection title="Agent Configuration" message={CONFIG_NEEDS_EDIT} />
    </div>
  )
}

export function DeepLinkNoAccessFrame() {
  return (
    <div className="space-y-4">
      <SpaceDetailLoadError status={403} message={SPACE_NO_ACCESS_FALLBACK} onBack={noop} />
    </div>
  )
}

export function DeepLinkLoadFailedFrame() {
  return (
    <div className="space-y-4">
      <SpaceDetailLoadError status={500} message="Failed to get agent detail" onBack={noop} />
    </div>
  )
}
