/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Single registry of every mockup frame, consumed by both mockups.test.tsx
 * (copy assertions) and frontend/scripts/mockups/emit.tsx (static HTML export).
 * Frames graduate to production and drop out here as Prompts 11 / 13 / 13.5 land
 * the real panels (frames 4–5 are gone; the output panels ship in production).
 */
import type { ReactElement } from "react"
import {
  DenialConfigFrame,
  FirstRunConfigFrame,
  RerunConfigFrame,
} from "./MvRunConfigMockups"
import {
  ModelNodeDetailFrame,
  ModelTabEmptyFrame,
  ModelTabPopulatedFrame,
  ModelTabProposalOverlayFrame,
} from "./MvSemanticModelFrame"
import {
  IqScanAdvisoryEmptyFrame,
  IqScanAdvisoryFoundFrame,
  IqScanAdvisoryNotEntitledFrame,
} from "./MvIqScanAdvisoryMockups"
import {
  ByoEntryPointsFrame,
  ByoRefusedFrame,
  ByoVerifiedFrame,
} from "./MvByoRegistrationMockups"
import { Iq158CardFrame, RunOutput158Frame } from "./Mv158FidelityFrames"

export interface MockupFrame {
  /** Stable slug used for the exported HTML filename. */
  id: string
  /** Human title shown in the HTML export header. */
  title: string
  element: ReactElement
}

export const MOCKUP_FRAMES: MockupFrame[] = [
  { id: "1-runconfig-first-run", title: "1 · Run config — first run (Create and attach disabled)", element: <FirstRunConfigFrame /> },
  { id: "2-runconfig-rerun", title: "2 · Run config — re-run (approved for this Agent, granted)", element: <RerunConfigFrame /> },
  { id: "3-runconfig-denial", title: "3 · Run config — denial", element: <DenialConfigFrame /> },
  // Frames 4–5 (run output panels) graduated to production at Prompt 13 and were
  // removed from this registry (see MvSuggestOnlyPanel / MvCreateAttachPanel).
  // Model tab (Prompt 12.0). Frame 6 was retired into these — 9c is its
  // descendant (proposal overlay). Kept in frame 6's old array slot so the
  // emitter's export ordering is unchanged (MOCKUP_FRAMES is order-driven, not
  // id-sorted); the 9-family numbering follows frame 8 (BYO).
  { id: "9a-model-populated", title: "9a · Model tab — populated (governance ladder, joins)", element: <ModelTabPopulatedFrame /> },
  { id: "9b-model-empty", title: "9b · Model tab — never optimized (empty, honest ladder)", element: <ModelTabEmptyFrame /> },
  { id: "9c-model-proposal-overlay", title: "9c · Model tab — proposal overlay ON", element: <ModelTabProposalOverlayFrame /> },
  { id: "9d-model-node-detail", title: "9d · Model tab — node detail (measure + join)", element: <ModelNodeDetailFrame /> },
  { id: "7a-iqscan-found", title: "7a · IQ Scan — proposals found", element: <IqScanAdvisoryFoundFrame /> },
  { id: "7b-iqscan-empty", title: "7b · IQ Scan — empty (authored copy, needs review)", element: <IqScanAdvisoryEmptyFrame /> },
  { id: "7c-iqscan-not-entitled", title: "7c · IQ Scan — not entitled", element: <IqScanAdvisoryNotEntitledFrame /> },
  { id: "8a-byo-entry-points", title: "8a · BYO registration — entry points (MV-D24)", element: <ByoEntryPointsFrame /> },
  { id: "8b-byo-verified", title: "8b · BYO registration — verified (USER_CREATED, no Drop)", element: <ByoVerifiedFrame /> },
  { id: "8c-byo-refused", title: "8c · BYO registration — refused (not a metric view / not visible)", element: <ByoRefusedFrame /> },
  // Prompt 15.8 fidelity-gate exports — the REAL production surfaces (facts row,
  // one shared [Create this metric view] accept flow, no "%"/"confidence").
  { id: "15.8a-iq-scan-card", title: "15.8a · IQ scan — facts-lead card + accept flow (production)", element: <Iq158CardFrame /> },
  { id: "15.8b-run-output", title: "15.8b · Run output — suggest-only panel, count truth + ranked (production)", element: <RunOutput158Frame /> },
]
