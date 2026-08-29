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
import { AttachedProposalCardFrame } from "./MvAttachAtApprovalFidelityFrames"
import { ModelV7ContractFrame } from "./MvSemanticV7ContractFrame"
import { RealModel3Frame, RealModel10Frame, RealModel30Frame, RealModelOverlayFrame, RealModelV7Frame } from "./Mv12fFidelityFrames"
import {
  BlueprintScale30Frame,
  BlueprintStarColumnsFrame,
  BlueprintStarMeasureLineageFrame,
  BlueprintStarMvSelectedFrame,
  BlueprintStarOverviewFrame,
  BlueprintStarStandardFrame,
  BlueprintUnknownRolesFrame,
  BlueprintWideTableFrame,
} from "./SemanticBlueprintFidelityFrames"
import {
  OntologyDomainDraftFrame,
  OntologyEmptyFrame,
  OntologyEnrichmentFailedFrame,
  OntologyGrantGateFrame,
  OntologyPageDraftFrame,
  OntologyTagsLensFrame,
  OntologyTaxonomyFrame,
} from "./OntologyPageMockups"

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
  // Prompt 12f step 0 — the committed v7 semantic-canvas CONTRACT (the frame the
  // v3 note called "the visual contract" but that was never committed). Prompt
  // 12f step 1 reconciles the deployed SemanticGraph to this.
  { id: "9e-model-v7-contract", title: "9e · Model tab — v7 contract (dedup canvas, boxed measures, curator inset)", element: <ModelV7ContractFrame /> },
  // Prompt 12f step 1 — fidelity-gate exports of the REAL SemanticGraph against
  // the 9e contract: the selected-MV scenario + the 3/10/30 scale fixtures.
  { id: "9f-model-real-v7", title: "9f · Model tab — REAL component, v7 scenario (Revenue selected)", element: <RealModelV7Frame /> },
  { id: "9g-model-real-3", title: "9g · Model tab — REAL component, 3 tables (expanded)", element: <RealModel3Frame /> },
  { id: "9h-model-real-10", title: "9h · Model tab — REAL component, 10 tables", element: <RealModel10Frame /> },
  { id: "9i-model-real-30", title: "9i · Model tab — REAL component, 30 tables (collapsed)", element: <RealModel30Frame /> },
  // Round-6 — REAL component with the proposal overlay ON (keep-measures + a
  // dashed "would govern →" link to a visible ghost proposed-MV card).
  { id: "9j-model-real-overlay", title: "9j · Model tab — REAL component, proposal overlay ON (keep + link)", element: <RealModelOverlayFrame /> },
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
  // Attach-at-approval (MV-D34) — the REAL card for a proposal already shelved on
  // the Agent config: the "Attached" header badge + the accept flow's attached
  // terminal with the SP grant an optimization run needs to read it.
  { id: "15.10-attached-proposal", title: "15.10 · IQ scan — already attached (create-and-attach-at-approval)", element: <AttachedProposalCardFrame /> },
  // Semantic Blueprint (v4) Phase-1 fidelity frames — the visual contract for the
  // blueprint rebuild (semantic-graph-v4-blueprint-note.md §5.9 / §11.4), static
  // captures of the north-star prototype's states through the pure reference math
  // in blueprintMath.ts. Gated by mockups.test.tsx before SemanticBlueprint.tsx.
  { id: "11a-blueprint-star", title: "11a · Blueprint — star, Standard, fact-center (crow's-foot, hops, callouts, headline)", element: <BlueprintStarStandardFrame /> },
  { id: "11b-blueprint-columns", title: "11b · Blueprint — star, Columns LOD (join-key rows, column-accurate ports)", element: <BlueprintStarColumnsFrame /> },
  { id: "11c-blueprint-measure-lineage", title: "11c · Blueprint — Space-config measure selected (dashed lineage → sources)", element: <BlueprintStarMeasureLineageFrame /> },
  { id: "11d-blueprint-mv-selected", title: "11d · Blueprint — metric view selected (member boundary, dotted uses-lineage)", element: <BlueprintStarMvSelectedFrame /> },
  { id: "11e-blueprint-unknown-roles", title: "11e · Blueprint — unknown roles (neutral TABLE, connectivity headers)", element: <BlueprintUnknownRolesFrame /> },
  { id: "11f-blueprint-wide-table", title: "11f · Blueprint — single wide table (no joins is a valid model)", element: <BlueprintWideTableFrame /> },
  { id: "11g-blueprint-30-tables", title: "11g · Blueprint — 30-table snowflake (bridges at density)", element: <BlueprintScale30Frame /> },
  { id: "11h-blueprint-overview", title: "11h · Blueprint — star, Overview band (no measure chips)", element: <BlueprintStarOverviewFrame /> },
  // Ontology track (Prompt 17.0, RE-SCOPED by MV-D36 + MV-D37) — the STANDALONE,
  // admin-gated, workspace/account-level page (NOT a SpaceDetail tab). Domains/
  // Sub-Domains ARE governed tags (MV-D37): a tiered permission banner, a Tags/
  // dedupe lens, and a Domain draft with an optional consented SET TAG apply.
  // Pages + Discover card stay copy-ready. Concept→Agent link is a Page Related asset.
  { id: "17.0a-ontology-permission-banner", title: "17.0a · Ontology — tiered permission banner (capability→permission matrix, 5 tiers incl. external enrichment)", element: <OntologyGrantGateFrame /> },
  { id: "17.0b-ontology-taxonomy", title: "17.0b · Ontology — proposed Domain → Sub-Domain → Page taxonomy (estate-wide)", element: <OntologyTaxonomyFrame /> },
  { id: "17.0c-ontology-tags-lens", title: "17.0c · Ontology — Governed-Tags / dedupe lens (reuse-vs-create, collisions, orphans)", element: <OntologyTagsLensFrame /> },
  { id: "17.0d-ontology-domain-draft", title: "17.0d · Ontology — Domain draft (plain new-vs-reuse, why, Apply-for-me + do-it-yourself; zero-burden)", element: <OntologyDomainDraftFrame /> },
  { id: "17.0e-ontology-page-draft", title: "17.0e · Ontology — Page draft (why we're suggesting this, synonyms, Related/Sources, certify, copy)", element: <OntologyPageDraftFrame /> },
  { id: "17.0f-ontology-enrichment-failed", title: "17.0f · Ontology — enrichment failed (draft complete, Recent-context left out not failed)", element: <OntologyEnrichmentFailedFrame /> },
  { id: "17.0g-ontology-empty", title: "17.0g · Ontology — empty (plain clean result, no system-table names)", element: <OntologyEmptyFrame /> },
]
