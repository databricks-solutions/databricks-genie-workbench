// Ontology top-level tab model (MV-D107 Phase 2). Kept in a pure module so the order/labels
// are unit-testable and so OntologyPage stays a component-only file (react-refresh). The IA is
// land → orient → act: Overview (land) · Review (act on suggestions) · Map + Estate (orient) ·
// Settings. Review = the drafts surface; Map = the estate graph; Estate = taxonomy + tags merged.
export type OntologyTab = "overview" | "review" | "map" | "estate" | "settings"

export const ONTOLOGY_TABS: { id: OntologyTab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "review", label: "Review" },
  { id: "map", label: "Map" },
  { id: "estate", label: "Estate" },
  { id: "settings", label: "Settings" },
]
