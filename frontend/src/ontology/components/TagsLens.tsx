// Frame 17.0c — the Governed-Tags / dedupe lens: existing governed tags,
// reuse-vs-create collisions (exact + fuzzy: case / plural / token — no
// embeddings), and cleanup flags. Driven by GET /api/ontology/tags.
import { useState } from "react"
import { AlertTriangle, BadgeCheck, ChevronRight, ListChecks, Search, Tags } from "lucide-react"
import type { CleanupFlag, CollisionKind, TagLens } from "@/ontology/types"
import { filterTags } from "@/ontology/estateModel"

const COLLISION_LABEL: Record<CollisionKind, string> = {
  exact: "exact",
  fuzzy_case: "case",
  fuzzy_plural: "plural",
  fuzzy_token: "token",
}

const CLEANUP_LABEL: Record<CleanupFlag, string> = {
  orphan: "orphan",
  near_empty: "near-empty",
  deprecated_but_assigned: "deprecated-but-assigned",
}

export function TagsLensView({ lens }: { lens: TagLens }) {
  const [query, setQuery] = useState("")
  const visible = filterTags(lens.tags, query)
  const domainCount = lens.tags.filter((t) => t.acts_as_domain || t.acts_as_subdomain).length

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Tags className="h-4 w-4 text-accent" />
        <h3 className="text-sm font-semibold text-primary">Governed tags</h3>
        <span className="text-xs text-muted">
          — every governed tag already in your estate ({domainCount} act as domains/sub-domains)
        </span>
      </div>
      <p className="max-w-prose text-xs text-muted">
        The full vocabulary of tags already defined in Unity Catalog — the reuse/dedupe reference so a
        new proposal reuses an existing tag instead of minting a near-duplicate. Read from{" "}
        <span className="font-mono text-secondary">system.tags.governed_tags</span> +{" "}
        <span className="font-mono text-secondary">information_schema.*_tags</span>; read-only, nothing
        here is written.
      </p>

      {lens.tags.length === 0 ? (
        <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center text-sm text-muted">
          No governed tags in scope.
        </div>
      ) : (
        <div className="space-y-2">
          <label className="relative block max-w-sm">
            <span className="sr-only">Search governed tags</span>
            <Search
              className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted"
              aria-hidden="true"
            />
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search tag keys & allowed values…"
              className="w-full rounded-lg border border-default bg-surface py-1.5 pl-8 pr-3 text-sm text-primary placeholder:text-muted focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/40"
            />
          </label>
          {visible.length === 0 ? (
            <p className="rounded-xl border border-default bg-elevated px-4 py-6 text-center text-sm text-muted">
              No governed tags match “{query}”.
            </p>
          ) : (
            <div className="overflow-hidden rounded-xl border border-default">
              <div className="grid grid-cols-[1fr_1fr_auto_auto] items-center gap-x-3 border-b border-default bg-sunken px-4 py-2 text-xs font-semibold uppercase tracking-wide text-secondary">
                <span>Tag key</span>
                <span>Allowed values</span>
                <span>Assigns</span>
                <span>Domain?</span>
              </div>
              {visible.map((t) => (
            <div
              key={t.tag_key}
              className="grid grid-cols-[1fr_1fr_auto_auto] items-center gap-x-3 border-b border-default bg-surface px-4 py-2 text-xs last:border-b-0"
            >
              <span className="font-mono text-secondary">{t.tag_key}</span>
              <span className="text-muted">
                {t.allowed_values.length > 0 ? t.allowed_values.join(", ") : "—"}
              </span>
              <span className="font-mono text-secondary">{t.assignment_count}</span>
              <span>
                {t.acts_as_domain || t.acts_as_subdomain ? (
                  <BadgeCheck className="h-4 w-4 text-success-foreground" />
                ) : (
                  <span className="text-muted">—</span>
                )}
              </span>
            </div>
              ))}
            </div>
          )}
        </div>
      )}

      {lens.collisions.length > 0 && (
        <div className="rounded-xl border border-warning/30 bg-warning/5 p-3">
          <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-warning-foreground">
            <AlertTriangle className="h-3.5 w-3.5" />
            Collisions — near-duplicate tags to reuse, not duplicate
          </p>
          <div className="mt-2 space-y-1.5">
            {lens.collisions.map((c) => (
              <div key={c.members.join("|")} className="flex flex-wrap items-center gap-2 text-xs">
                {c.members.map((m, i) => (
                  <span key={m} className="flex items-center gap-2">
                    {i > 0 && <ChevronRight className="h-3 w-3 text-muted" />}
                    <span className="rounded-full bg-warning/10 px-2 py-0.5 font-mono text-warning-foreground">
                      {m}
                    </span>
                  </span>
                ))}
                <span className="rounded-full bg-elevated px-2 py-0.5 text-muted">
                  {COLLISION_LABEL[c.kind]}
                </span>
                <span className="text-muted">· {c.suggestion}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {lens.cleanup.length > 0 && (
        <div className="rounded-xl border border-default bg-sunken p-3">
          <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
            <ListChecks className="h-3.5 w-3.5 text-accent" />
            Cleanup — orphans, near-empty, deprecated-but-assigned
          </p>
          <ul className="mt-2 space-y-1 text-xs text-muted">
            {lens.cleanup.map((c) => (
              <li key={`${c.tag_key}:${c.flag}`} className="flex flex-wrap gap-2">
                <span className="font-mono text-secondary">{c.tag_key}</span>
                <span className="rounded-full bg-elevated px-2 py-0.5">{CLEANUP_LABEL[c.flag]}</span>
                <span>· {c.detail}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
