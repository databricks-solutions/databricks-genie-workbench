/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Single registry of every mockup frame, consumed by both mockups.test.tsx
 * (copy assertions) and frontend/scripts/mockups/emit.tsx (static HTML export).
 * Deleted alongside the frames as Prompts 11 / 13 / 13.5 land the real panels.
 */
import type { ReactElement } from "react"
import {
  DenialConfigFrame,
  FirstRunConfigFrame,
  RerunConfigFrame,
} from "./MvRunConfigMockups"
import { CreateAndAttachOutputFrame, SuggestOnlyOutputFrame } from "./MvOutputMockups"
import { SemanticModelFrame } from "./MvSemanticModelFrame"
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
  { id: "4-output-suggest-only", title: "4 · Output — suggest only (Lift not measured)", element: <SuggestOnlyOutputFrame /> },
  { id: "5-output-create-attach", title: "5 · Output — create and attach (DETACHED regression)", element: <CreateAndAttachOutputFrame /> },
  { id: "6-semantic-model", title: "6 · Semantic model (static preview)", element: <SemanticModelFrame /> },
  { id: "7a-iqscan-found", title: "7a · IQ Scan — proposals found", element: <IqScanAdvisoryFoundFrame /> },
  { id: "7b-iqscan-empty", title: "7b · IQ Scan — empty (authored copy, needs review)", element: <IqScanAdvisoryEmptyFrame /> },
  { id: "7c-iqscan-not-entitled", title: "7c · IQ Scan — not entitled", element: <IqScanAdvisoryNotEntitledFrame /> },
  { id: "8a-byo-entry-points", title: "8a · BYO registration — entry points (MV-D24)", element: <ByoEntryPointsFrame /> },
  { id: "8b-byo-verified", title: "8b · BYO registration — verified (USER_CREATED, no Drop)", element: <ByoVerifiedFrame /> },
  { id: "8c-byo-refused", title: "8c · BYO registration — refused (not a metric view / not visible)", element: <ByoRefusedFrame /> },
]
