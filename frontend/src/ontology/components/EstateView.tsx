// Estate tab (MV-D107 Phase 2) — taxonomy + tags merged behind one segmented control, so
// "what exists in the estate" is a single orient surface. Reuses TaxonomyView (navigable +
// provenant) and TagsLensView unchanged; this wrapper only holds the segment state.
import { useState } from "react"
import { FolderTree, Tags } from "lucide-react"
import type { DomainDraft, OntologyInventory, OntologyTaxonomy, TagLens } from "@/ontology/types"
import { TaxonomyView } from "@/ontology/components/TaxonomyView"
import { TagsLensView } from "@/ontology/components/TagsLens"

type EstateSegment = "taxonomy" | "tags"

const SEGMENTS: { id: EstateSegment; label: string; icon: React.ReactNode }[] = [
  { id: "taxonomy", label: "Taxonomy", icon: <FolderTree className="h-4 w-4" aria-hidden="true" /> },
  { id: "tags", label: "Tags", icon: <Tags className="h-4 w-4" aria-hidden="true" /> },
]

export function EstateView({
  taxonomy,
  tags,
  inventory,
  drafts = [],
  onReview,
}: {
  taxonomy: OntologyTaxonomy
  tags: TagLens | null
  inventory: OntologyInventory | null
  drafts?: DomainDraft[]
  onReview?: (proposalId?: string) => void
}) {
  const [segment, setSegment] = useState<EstateSegment>("taxonomy")

  return (
    <div className="space-y-4">
      <div
        role="tablist"
        aria-label="Estate view"
        className="inline-flex items-center gap-1 rounded-lg border border-default bg-elevated p-1"
      >
        {SEGMENTS.map((s) => (
          <button
            key={s.id}
            role="tab"
            aria-selected={segment === s.id}
            onClick={() => setSegment(s.id)}
            className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
              segment === s.id
                ? "bg-surface text-primary shadow-sm"
                : "text-muted hover:text-secondary"
            }`}
          >
            {s.icon}
            {s.label}
          </button>
        ))}
      </div>

      {segment === "taxonomy" ? (
        <TaxonomyView
          taxonomy={taxonomy}
          inventory={inventory}
          tags={tags}
          drafts={drafts}
          onReview={onReview}
        />
      ) : tags ? (
        <TagsLensView lens={tags} />
      ) : (
        <p className="rounded-xl border border-default bg-elevated px-4 py-6 text-center text-sm text-muted">
          Governed tags aren&rsquo;t loaded yet.
        </p>
      )}
    </div>
  )
}
