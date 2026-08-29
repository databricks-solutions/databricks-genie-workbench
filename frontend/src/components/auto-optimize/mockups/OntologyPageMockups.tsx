/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Ontology track (Prompt 17.0, RE-SCOPED by MV-D36). Ontology is NO LONGER a
 * per-Agent SpaceDetail tab. It is a STANDALONE, admin-gated, top-level page that
 * proposes a Domain → Sub-Domain → Page taxonomy across the whole estate:
 *   • every Genie Agent in the workspace (genie_client.list_spaces), and
 *   • every metric view in the account, enumerated under OBO via
 *     system.information_schema.tables WHERE table_type = 'METRIC_VIEW'
 *     (privilege-auto-filtered, no explicit grant), enriched by
 *     DESCRIBE TABLE EXTENDED … AS JSON on the matched shortlist.
 *
 * Identity is HYBRID: OBO for the UC inventory above; the service principal for
 * the system-table usage/lineage/cost signals that WEIGHT domain clustering
 * (system.access.audit / .table_lineage / billing.usage / query.history) — the
 * GenieWatch pattern. Those SP reads need explicit grants, so the page opens on a
 * grant preflight. A Settings-stored company name adds business context.
 *
 * Governed-tag substrate (MV-D37): a Domain IS a governed tag and a Sub-Domain is
 * a `{parent}/{child}` governed tag, so domain/sub-domain MEMBERSHIP is DDL-
 * writable (`SET TAG`) — an OPTIONAL, consented, dry-run-first apply (default
 * OFF). The Discover card and Pages stay copy-ready-only. Because a domain
 * proposal is really "reuse or create a governed tag," a Tags/dedupe lens is
 * core. The concept→Agent link is the Page's Related-assets field.
 *
 * Seven frames: (a) TIERED permission banner (capability→permission matrix);
 * (b) proposed Domain→Sub-Domain→Page taxonomy; (c) Governed-Tags / dedupe lens
 * (reuse-vs-create, collisions, orphans); (d) a Domain draft (reuse-vs-create tag
 * decision + SET TAG DDL preview + manual path); (e) a Page draft (synonyms,
 * Related/Sources chips, certify, Recent context, copy + checklist); (f)
 * enrichment-failed; (g) empty state.
 *
 * ⚠ COPY REVIEW REQUIRED — empty/enrichment/grant copy and the checklists are
 * AUTHORED NEW for this branch; sign off before ship. Prompt 17c builds these for
 * real and deletes this file. Fixtures are LOCAL (MV-D26 persistence OPEN); ids
 * match mvMockData (finance.sales.order_revenue, space 01ef9a2b3c4d5e6f).
 */
import {
  AlertTriangle,
  BadgeCheck,
  Building2,
  Check,
  ChevronRight,
  Copy,
  Database,
  FlaskConical,
  FolderTree,
  Info,
  Layers,
  Link2,
  ListChecks,
  Lock,
  ShieldAlert,
  Sparkles,
  Tags,
  X,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"

// ── Local fixtures (no backend model yet — MV-D26 OPEN) ──────────────────────
const AGENT_SPACE_ID = "01ef9a2b3c4d5e6f"
const COMPANY = "Northwind Trading Co."
const ESTATE = { agents: 12, metricViews: 47, catalogs: 5 }

interface PageNode {
  archetype: string
  name: string
  certify: boolean
}

interface SubDomainNode {
  name: string
  agents: number
  metricViews: number
  evidence: string[]
  pages: PageNode[]
}

interface DomainNode {
  name: string
  description: string
  subdomains: SubDomainNode[]
}

const taxonomy: DomainNode[] = [
  {
    name: "Commercial",
    description: "Revenue-generating go-to-market: sales, marketing, and partner performance.",
    subdomains: [
      {
        name: "Sales",
        agents: 3,
        metricViews: 6,
        evidence: ["41% of Genie query volume (30d)", "shared finance.sales spine (lineage)"],
        pages: [
          { archetype: "[Routing]", name: "Discounted revenue", certify: true },
          { archetype: "[Guardrail]", name: "Rate measures are non-additive", certify: true },
        ],
      },
      {
        name: "Marketing",
        agents: 2,
        metricViews: 4,
        evidence: ["campaign attribution joins", "8 curated funnel queries"],
        pages: [{ archetype: "[Taxonomy]", name: "Campaign channel codes", certify: false }],
      },
    ],
  },
  {
    name: "Finance",
    description: "Recognized revenue, billing, and margin reporting.",
    subdomains: [
      {
        name: "Revenue & Billing",
        agents: 2,
        metricViews: 9,
        evidence: ["gross vs net split recurs 11×", "billing.usage lineage"],
        pages: [{ archetype: "[Method]", name: "Recognized vs booked revenue", certify: true }],
      },
    ],
  },
  {
    name: "Operations",
    description: "Fulfillment, inventory, and supply-chain throughput.",
    subdomains: [
      {
        name: "Fulfillment",
        agents: 1,
        metricViews: 3,
        evidence: ["`orders.status` · 6 distinct codes", "column comment + profiling"],
        pages: [{ archetype: "[Taxonomy]", name: "Order status codes", certify: false }],
      },
    ],
  },
]

const commercialDomainDraft = {
  kind: "Domain" as const,
  name: "Commercial",
  description: "Revenue-generating go-to-market: sales, marketing, and partner performance.",
  subdomains: ["Sales", "Marketing", "Partnerships"],
  members: {
    agents: [`Genie Agent · Sales performance · ${AGENT_SPACE_ID}`, "Genie Agent · Marketing analytics · 01ef77aa22bb33cc"],
    metricViews: ["finance.sales.order_revenue", "marketing.campaigns.channel_performance"],
  },
  rationale:
    "These 3 Agents and 6 metric views all draw on the same core sales tables and make up 41% of the questions people ask Genie. Based on your business (\u201C" +
    COMPANY +
    "\u201D), we grouped them as Commercial rather than a generic \u201CSales\u201D.",
}

interface PageDraftFixture {
  archetype: string
  name: string
  domain: string
  reason: string
  synonyms: string[]
  description: string
  definition: string
  rules: string[]
  related: { kind: "agent" | "page"; label: string }[]
  sources: string[]
  certify: boolean
  recentContext?: { asOf: string; body: string; sources: string[] }
}

const routingDraft: PageDraftFixture = {
  archetype: "[Routing]",
  name: "Discounted revenue",
  domain: "Commercial / Sales",
  reason:
    "\u201CRevenue\u201D can mean several different things across your Agents, so Genie sometimes answers it inconsistently. This Page pins the one definition your team means, so answers line up every time.",
  synonyms: ["net revenue", "revenue after discount", "discounted sales", "how much did we actually make", "disc rev"],
  description: "Revenue net of line-item discounts — the default revenue across the Commercial domain.",
  definition:
    "For revenue, net revenue, or how much did we make, answer from the governed metric view `finance.sales.order_revenue` using its `total_revenue` measure — never from a raw SUM over `finance.sales.order_items`. `total_revenue` is SUM(items.quantity * items.unit_price), evaluated inside the metric view so its join to `finance.sales.orders` and its discount handling stay consistent across every question.",
  rules: [
    "Route revenue for the Commercial domain to `finance.sales.order_revenue.total_revenue`; do not hand-write a SUM over `finance.sales.order_items`.",
  ],
  related: [
    { kind: "agent", label: `Genie Agent · ${AGENT_SPACE_ID}` },
    { kind: "page", label: "[Guardrail] Rate measures are non-additive" },
  ],
  sources: ["finance.sales.order_revenue", "finance.sales.orders", "finance.sales.order_items"],
  certify: true,
  recentContext: {
    asOf: "2026-08-28",
    body:
      "Industry reporting increasingly separates gross bookings from net revenue after discounts and cancellations; net-of-discount revenue is the comparable figure across marketplace peers.",
    sources: ["https://www.sec.gov/ (peer 10-K revenue-recognition notes)"],
  },
}

// ── Standalone page chrome (NOT the SpaceDetail tab strip — MV-D36) ──────────
function OntologyPageChrome({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-default bg-surface">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-default px-5 py-4">
        <div className="flex items-center gap-2.5">
          <FolderTree className="h-5 w-5 text-accent" />
          <div>
            <h2 className="text-base font-semibold text-primary">Ontology</h2>
            <p className="text-xs text-muted">
              Proposed Domains, Sub-Domains &amp; Pages for Databricks Discover — across the estate
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="inline-flex items-center gap-1.5 rounded-full border border-default bg-elevated px-2.5 py-1 text-xs text-secondary">
            <Building2 className="h-3.5 w-3.5 text-accent" />
            {COMPANY}
          </span>
          <Badge variant="secondary">Admin</Badge>
        </div>
      </div>
      <div className="space-y-4 p-5">{children}</div>
    </div>
  )
}

function EstateReadLine() {
  return (
    <p className="text-xs text-muted">
      Read <span className="font-mono text-secondary">{ESTATE.agents}</span> Genie Agents (workspace) ·{" "}
      <span className="font-mono text-secondary">{ESTATE.metricViews}</span> metric views across{" "}
      <span className="font-mono text-secondary">{ESTATE.catalogs}</span> catalogs (account, via{" "}
      <span className="font-mono text-secondary">system.information_schema</span>, OBO)
    </p>
  )
}

// ── Frame a — tiered permission banner (capability → permission matrix) ──────
interface PermTier {
  capability: string
  detail: string
  permission: string
  identity: "OBO" | "SP" | "Batch"
  granted: boolean
}

const PERMISSION_TIERS: PermTier[] = [
  {
    capability: "Metric-view + tag inventory",
    detail: "system.information_schema auto-filters to your grants",
    permission: "none — no explicit grant",
    identity: "OBO",
    granted: true,
  },
  {
    capability: "Usage / lineage / cost ranking",
    detail: "weights domain clustering by real signal",
    permission: "USE CATALOG system + SELECT on access.audit, access.table_lineage, query.history, billing.usage",
    identity: "SP",
    granted: true,
  },
  {
    capability: "Governed-tag graph (dedupe)",
    detail: "reuse-vs-create needs the existing tags",
    permission: "SELECT on system.tags.governed_tags",
    identity: "SP",
    granted: false,
  },
  {
    capability: "Membership write (optional apply)",
    detail: "SET TAG on assets — default OFF, dry-run-first",
    permission: "MANAGE DISCOVERY + ASSIGN on each tag + APPLY TAG / USE SCHEMA / USE CATALOG",
    identity: "OBO",
    granted: false,
  },
  {
    capability: "Context sources (external enrichment)",
    detail: "naming & Recent-context only — never structural (MV-D38); disabled when no source is available",
    permission: "EXECUTE on the enabled Unity AI Gateway MCP services — opt-in, default OFF",
    identity: "Batch",
    granted: false,
  },
]

// ── Context Sources registry (MV-D46/D47) — the opt-in AI Gateway MCP panel ───
interface ContextSource {
  name: string
  cls: "internal" | "external"
  tier: "T0" | "T1" | "T2" | "T3"
  influence: string
  granted: boolean
}

const CONTEXT_SOURCES: ContextSource[] = [
  { name: "system.ai.web_search", cls: "external", tier: "T3", influence: "naming · Recent context", granted: false },
  { name: "You.com (myyoumcp)", cls: "external", tier: "T3", influence: "naming · Recent context (fallback)", granted: false },
  { name: "Confluence · Google Drive · Microsoft 365", cls: "external", tier: "T1", influence: "naming · description", granted: false },
  { name: "Genie One · Databricks SQL", cls: "internal", tier: "T0", influence: "structural signal + validation", granted: true },
]

export function OntologyGrantGateFrame() {
  const grantedCount = PERMISSION_TIERS.filter((t) => t.granted).length
  return (
    <OntologyPageChrome>
      <div className="flex items-start gap-2.5 rounded-xl border border-warning/40 bg-warning/5 px-4 py-3.5">
        <ShieldAlert className="mt-0.5 h-6 w-6 shrink-0 text-warning-foreground" />
        <div>
          <p className="text-base font-semibold text-primary">
            Ontology access — Tier {grantedCount} of {PERMISSION_TIERS.length} unlocked
          </p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            Each tier below unlocks more of the ontology. Read tiers degrade gracefully; the optional write tier is
            never required to view. Grant the missing rows to reach full visualization.
          </p>
        </div>
      </div>

      <div className="overflow-hidden rounded-xl border border-default">
        <div className="grid grid-cols-[auto_1fr_auto] items-center gap-x-3 border-b border-default bg-sunken px-4 py-2 text-xs font-semibold uppercase tracking-wide text-secondary">
          <span>Status</span>
          <span>Capability &amp; permission</span>
          <span>Identity</span>
        </div>
        {PERMISSION_TIERS.map((t) => (
          <div
            key={t.capability}
            className="grid grid-cols-[auto_1fr_auto] items-center gap-x-3 border-b border-default bg-surface px-4 py-3 last:border-b-0"
          >
            <span
              className={
                t.granted
                  ? "inline-flex h-6 w-6 items-center justify-center rounded-full bg-success/15 text-success-foreground"
                  : "inline-flex h-6 w-6 items-center justify-center rounded-full bg-warning/15 text-warning-foreground"
              }
            >
              {t.granted ? <Check className="h-3.5 w-3.5" /> : <X className="h-3.5 w-3.5" />}
            </span>
            <div>
              <p className="flex items-center gap-1.5 text-sm font-medium text-primary">
                {t.capability}
                {t.permission.startsWith("MANAGE") && <Lock className="h-3 w-3 text-muted" />}
              </p>
              <p className="text-xs text-muted">{t.detail}</p>
              <p className="mt-0.5 font-mono text-xs text-secondary">{t.permission}</p>
              {!t.granted && (
                <div className="mt-1.5 flex items-center gap-2">
                  <Button size="sm" variant="secondary">
                    <Copy className="mr-1 h-3 w-3" />
                    {t.identity === "SP" ? "Copy GRANT SQL" : "Copy entitlement request"}
                  </Button>
                </div>
              )}
            </div>
            <Badge variant={t.identity === "SP" ? "secondary" : "default"}>{t.identity}</Badge>
          </div>
        ))}
      </div>

      <div className="overflow-hidden rounded-xl border border-default bg-surface">
        <div className="flex items-center gap-2 border-b border-default bg-sunken px-4 py-2.5">
          <Tags className="h-4 w-4 text-accent" />
          <p className="text-sm font-semibold text-primary">
            Context sources — Unity AI Gateway MCP (opt-in, default OFF)
          </p>
        </div>
        {CONTEXT_SOURCES.map((s) => (
          <div key={s.name} className="flex items-center gap-3 border-b border-default px-4 py-2.5 last:border-b-0">
            <span
              className={
                s.granted
                  ? "inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-success/15 text-success-foreground"
                  : "inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-warning/15 text-warning-foreground"
              }
            >
              {s.granted ? <Check className="h-3.5 w-3.5" /> : <X className="h-3.5 w-3.5" />}
            </span>
            <div className="min-w-0 flex-1">
              <p className="truncate font-mono text-sm font-medium text-primary">{s.name}</p>
              <p className="text-xs text-muted">{s.influence}</p>
            </div>
            <Badge variant={s.cls === "internal" ? "default" : "secondary"}>{s.cls}</Badge>
            <Badge variant="secondary">{s.tier}</Badge>
            {!s.granted && (
              <Button size="sm" variant="secondary">
                <Copy className="mr-1 h-3 w-3" />
                Copy GRANT EXECUTE
              </Button>
            )}
          </div>
        ))}
        <p className="border-t border-default px-4 py-2 text-xs text-muted">
          External sources influence naming &amp; description only — never structure (MV-D38). Internal Genie / SQL MCPs
          add structural signal + validation. Governed by EXECUTE grants + service policies (MV-D47).
        </p>
      </div>

      <div className="flex items-start gap-2 rounded-lg border border-info/30 bg-info/5 px-3 py-2.5">
        <Info className="mt-0.5 h-4 w-4 text-info-foreground" />
        <p className="text-xs text-secondary">
          Company name set to <span className="font-medium text-primary">{COMPANY}</span> (Settings → Ontology) —
          proposals are named in your business&rsquo;s terms.
        </p>
      </div>

      <div className="flex flex-wrap gap-2">
        <Button size="sm" variant="secondary">Re-check permissions</Button>
        <Button size="sm">View taxonomy (Tier 2)</Button>
      </div>
    </OntologyPageChrome>
  )
}

// ── Frame b — the proposed Domain → Sub-Domain → Page taxonomy ───────────────
export function OntologyTaxonomyFrame() {
  return (
    <OntologyPageChrome>
      <div className="flex items-center gap-2">
        <Sparkles className="h-4 w-4 text-accent" />
        <h3 className="text-sm font-semibold text-primary">Proposed taxonomy</h3>
        <span className="text-xs text-muted">— copy-ready for Discover; nothing is written automatically</span>
      </div>
      <EstateReadLine />

      <div className="space-y-3">
        {taxonomy.map((domain) => (
          <div key={domain.name} className="rounded-xl border border-default bg-elevated p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <Layers className="h-4 w-4 text-accent" />
                <span className="text-sm font-semibold text-primary">{domain.name}</span>
                <span className="text-xs text-muted">Domain</span>
              </div>
              <Button size="sm" variant="secondary">View draft</Button>
            </div>
            <p className="mt-1 max-w-prose text-xs text-muted">{domain.description}</p>

            <div className="mt-3 space-y-2 border-l border-default pl-3">
              {domain.subdomains.map((sub) => (
                <div key={sub.name} className="rounded-lg border border-default bg-sunken p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="flex items-center gap-1.5">
                      <ChevronRight className="h-3.5 w-3.5 text-muted" />
                      <span className="text-sm font-medium text-primary">{sub.name}</span>
                      <span className="text-xs text-muted">Sub-Domain</span>
                    </div>
                    <div className="flex items-center gap-2 text-xs text-muted">
                      <span className="inline-flex items-center gap-1">
                        <Sparkles className="h-3 w-3" />
                        {sub.agents} Agents
                      </span>
                      <span className="inline-flex items-center gap-1">
                        <Database className="h-3 w-3" />
                        {sub.metricViews} metric views
                      </span>
                    </div>
                  </div>

                  <div className="mt-2 flex flex-wrap gap-1.5">
                    {sub.evidence.map((e) => (
                      <span key={e} className="inline-flex items-center gap-1 rounded-full bg-accent/10 px-2 py-0.5 text-xs text-accent">
                        <FlaskConical className="h-3 w-3" />
                        {e}
                      </span>
                    ))}
                  </div>

                  <div className="mt-2.5 space-y-1">
                    <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Pages</p>
                    <div className="flex flex-wrap gap-1.5">
                      {sub.pages.map((p) => (
                        <span
                          key={p.name}
                          className="inline-flex items-center gap-1.5 rounded-full border border-default bg-elevated px-2.5 py-0.5 text-xs text-secondary"
                        >
                          <span className="font-mono text-accent">{p.archetype}</span>
                          {p.name}
                          {p.certify && <BadgeCheck className="h-3 w-3 text-success-foreground" />}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </OntologyPageChrome>
  )
}

// ── Frame c — Governed-Tags / dedupe lens (reuse-vs-create, MV-D37) ──────────
const existingTags: { key: string; values: string; assigned: number; isDomain: boolean }[] = [
  { key: "Commercial", values: "—", assigned: 24, isDomain: true },
  { key: "Commercial/Sales", values: "—", assigned: 11, isDomain: true },
  { key: "Finance", values: "—", assigned: 18, isDomain: true },
  { key: "sensitivity", values: "public, internal, restricted", assigned: 63, isDomain: false },
  { key: "cost_center", values: "—", assigned: 9, isDomain: false },
]

const collisions: { proposed: string; existing: string; verdict: string }[] = [
  { proposed: "Sales", existing: "Commercial/Sales", verdict: "REUSE — already a sub-domain of Commercial" },
  { proposed: "Marketing", existing: "marketing (ungoverned tag, 4 assigns)", verdict: "PROMOTE — govern the existing tag, don't duplicate" },
]

const orphans: { key: string; issue: string }[] = [
  { key: "Ops_legacy", issue: "governed tag, no Domain — deprecated-but-assigned (3 assets)" },
  { key: "Finance/Audit", issue: "Sub-Domain tag with 0 assets — near-empty" },
]

export function OntologyTagsLensFrame() {
  return (
    <OntologyPageChrome>
      <div className="flex items-center gap-2">
        <Tags className="h-4 w-4 text-accent" />
        <h3 className="text-sm font-semibold text-primary">Governed tags</h3>
        <span className="text-xs text-muted">— Domains &amp; Sub-Domains ARE governed tags; dedupe before proposing</span>
      </div>
      <p className="max-w-prose text-xs text-muted">
        Read from <span className="font-mono text-secondary">system.tags.governed_tags</span> +{" "}
        <span className="font-mono text-secondary">information_schema.*_tags</span>. Every Domain proposal is a
        reuse-vs-create decision against these.
      </p>

      <div className="overflow-hidden rounded-xl border border-default">
        <div className="grid grid-cols-[1fr_1fr_auto_auto] items-center gap-x-3 border-b border-default bg-sunken px-4 py-2 text-xs font-semibold uppercase tracking-wide text-secondary">
          <span>Tag key</span>
          <span>Allowed values</span>
          <span>Assigns</span>
          <span>Domain?</span>
        </div>
        {existingTags.map((t) => (
          <div key={t.key} className="grid grid-cols-[1fr_1fr_auto_auto] items-center gap-x-3 border-b border-default bg-surface px-4 py-2 text-xs last:border-b-0">
            <span className="font-mono text-secondary">{t.key}</span>
            <span className="text-muted">{t.values}</span>
            <span className="font-mono text-secondary">{t.assigned}</span>
            <span>{t.isDomain ? <BadgeCheck className="h-4 w-4 text-success-foreground" /> : <span className="text-muted">—</span>}</span>
          </div>
        ))}
      </div>

      <div className="rounded-xl border border-warning/30 bg-warning/5 p-3">
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-warning-foreground">
          <AlertTriangle className="h-3.5 w-3.5" />
          Collisions — proposals that must reuse, not duplicate
        </p>
        <div className="mt-2 space-y-1.5">
          {collisions.map((c) => (
            <div key={c.proposed} className="flex flex-wrap items-center gap-2 text-xs">
              <span className="rounded-full bg-warning/10 px-2 py-0.5 font-mono text-warning-foreground">{c.proposed}</span>
              <ChevronRight className="h-3 w-3 text-muted" />
              <span className="font-mono text-secondary">{c.existing}</span>
              <span className="text-muted">· {c.verdict}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="rounded-xl border border-default bg-sunken p-3">
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
          <ListChecks className="h-3.5 w-3.5 text-accent" />
          Cleanup — orphans, near-empty, deprecated-but-assigned
        </p>
        <ul className="mt-2 space-y-1 text-xs text-muted">
          {orphans.map((o) => (
            <li key={o.key} className="flex gap-2">
              <span className="font-mono text-secondary">{o.key}</span>
              <span>· {o.issue}</span>
            </li>
          ))}
        </ul>
      </div>
    </OntologyPageChrome>
  )
}

// ── Frame d — a Domain copy-ready draft (reuse-vs-create + SET TAG DDL) ──────
function AssetChip({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full border border-default bg-elevated px-2.5 py-0.5 font-mono text-xs text-secondary">
      {children}
    </span>
  )
}

export function OntologyDomainDraftFrame() {
  const d = commercialDomainDraft
  return (
    <OntologyPageChrome>
      <div className="space-y-4 rounded-xl border border-default bg-surface p-5">
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div>
            <p className="text-sm font-semibold text-primary">{d.name}</p>
            <p className="mt-0.5 text-xs text-muted">Suggested {d.kind} · not created yet</p>
          </div>
          <Button size="sm">
            <Copy className="mr-1 h-3.5 w-3.5" />
            Copy Domain for Discover
          </Button>
        </div>

        {/* Plain-language new-vs-reuse — the outcome and the reason, not the mechanism. */}
        <div className="flex items-start gap-2 rounded-lg border border-info/30 bg-info/5 px-3 py-2.5">
          <Tags className="mt-0.5 h-4 w-4 text-info-foreground" />
          <p className="text-xs text-secondary">
            <span className="font-semibold text-primary">New domain</span> — we didn&rsquo;t find an existing one like
            this. If a similar domain already existed, we&rsquo;d suggest reusing it instead of creating a duplicate.
          </p>
        </div>

        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Description</p>
          <p className="mt-1 max-w-prose text-sm text-primary">{d.description}</p>
        </div>

        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Proposed Sub-Domains</p>
          <div className="mt-1.5 flex flex-wrap gap-1.5">
            {d.subdomains.map((s) => (
              <span key={s} className="rounded-full bg-accent/10 px-2.5 py-0.5 text-xs text-accent">
                {s}
              </span>
            ))}
          </div>
        </div>

        <div>
          <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
            <Link2 className="h-3.5 w-3.5 text-accent" />
            Member assets
          </p>
          <div className="mt-1.5 flex flex-wrap gap-1.5">
            {d.members.agents.map((a) => (
              <AssetChip key={a}>
                <Sparkles className="h-3 w-3 text-accent" />
                {a}
              </AssetChip>
            ))}
            {d.members.metricViews.map((m) => (
              <AssetChip key={m}>
                <Database className="h-3 w-3 text-muted" />
                {m}
              </AssetChip>
            ))}
          </div>
        </div>

        <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
          <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Why we&rsquo;re suggesting this</p>
          <p className="mt-1 max-w-prose text-sm text-primary">{d.rationale}</p>
        </div>

        {/* Zero-burden: a plain choice — we do it (with a preview), or you do it. No DDL, no grants. */}
        <div className="grid gap-3 md:grid-cols-2">
          <div className="rounded-lg border border-default bg-sunken px-3 py-2.5">
            <div className="flex items-center justify-between">
              <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
                <Sparkles className="h-3.5 w-3.5 text-accent" />
                Apply for me
              </p>
              <Badge variant="secondary">Preview first</Badge>
            </div>
            <p className="mt-2 text-xs text-secondary">
              We can set this domain up for you and file its {d.members.agents.length + d.members.metricViews.length}{" "}
              assets under it. You&rsquo;ll see exactly what changes and confirm — nothing happens until you say so.
            </p>
            <Button size="sm" className="mt-2.5">Preview changes</Button>
          </div>

          <div className="rounded-lg border border-default bg-sunken px-3 py-2.5">
            <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
              <ListChecks className="h-3.5 w-3.5 text-accent" />
              Prefer to do it yourself?
            </p>
            <ol className="mt-1.5 list-decimal space-y-1 pl-5 text-xs text-muted">
              <li>Create the &ldquo;{d.name}&rdquo; domain in Discover.</li>
              <li>Add the {d.subdomains.length} sub-domains listed above.</li>
              <li>Add the member Agents and metric views to each one.</li>
              <li>File the Page suggestions under each sub-domain.</li>
            </ol>
          </div>
        </div>
      </div>
    </OntologyPageChrome>
  )
}

// ── Shared: the Page draft panel (frames d and e) ───────────────────────────
function FieldBlock({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-secondary">{label}</p>
      <div className="mt-1 text-sm text-primary">{children}</div>
    </div>
  )
}

function PageDraftPanel({ draft }: { draft: PageDraftFixture }) {
  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-5">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="font-mono text-sm font-medium text-primary">
            {draft.archetype} {draft.name}
          </p>
          <p className="mt-0.5 text-xs text-muted">Page draft · {draft.domain} · Draft (publish in Discover)</p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {draft.certify ? (
            <Badge variant="success">
              <BadgeCheck className="mr-1 h-3 w-3" />
              Certify: Yes
            </Badge>
          ) : (
            <Badge variant="secondary">Certify: No</Badge>
          )}
          <Button size="sm">
            <Copy className="mr-1 h-3.5 w-3.5" />
            Copy Page for Discover
          </Button>
        </div>
      </div>

      <div className="rounded-lg border border-accent/30 bg-accent/5 px-3 py-2.5">
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-accent">
          <Sparkles className="h-3.5 w-3.5" />
          Why we&rsquo;re suggesting this
        </p>
        <p className="mt-1 max-w-prose text-sm text-primary">{draft.reason}</p>
      </div>

      <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
        <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Synonyms · the words people use, so Genie can find this</p>
        <div className="mt-1.5 flex flex-wrap gap-1.5">
          {draft.synonyms.map((s) => (
            <span key={s} className="rounded-full bg-accent/10 px-2.5 py-0.5 text-xs text-accent">
              {s}
            </span>
          ))}
        </div>
      </div>

      <FieldBlock label="Description">{draft.description}</FieldBlock>
      <FieldBlock label="Definition">
        <p className="max-w-prose leading-relaxed">{draft.definition}</p>
      </FieldBlock>
      <FieldBlock label="Rules">
        <ul className="list-disc space-y-1 pl-5">
          {draft.rules.map((r) => (
            <li key={r}>{r}</li>
          ))}
        </ul>
      </FieldBlock>

      <div>
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
          <Link2 className="h-3.5 w-3.5 text-accent" />
          Related assets · what this connects to
        </p>
        <div className="mt-1.5 flex flex-wrap gap-1.5">
          {draft.related.map((r) => (
            <AssetChip key={r.label}>
              {r.kind === "agent" ? <Sparkles className="h-3 w-3 text-accent" /> : <Layers className="h-3 w-3 text-muted" />}
              {r.label}
            </AssetChip>
          ))}
        </div>
      </div>

      <div>
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
          <Layers className="h-3.5 w-3.5 text-accent" />
          Sources · where the definition comes from
        </p>
        <div className="mt-1.5 flex flex-wrap gap-1.5">
          {draft.sources.map((s) => (
            <AssetChip key={s}>{s}</AssetChip>
          ))}
        </div>
      </div>

      {draft.recentContext && (
        <div className="rounded-lg border border-info/30 bg-info/5 px-3 py-2.5">
          <p className="text-xs font-semibold uppercase tracking-wide text-info-foreground">
            Recent context (informational, as of {draft.recentContext.asOf})
          </p>
          <p className="mt-1 max-w-prose text-sm text-primary">{draft.recentContext.body}</p>
          <p className="mt-2 text-xs text-muted">Sources: {draft.recentContext.sources.join("; ")}</p>
          <p className="mt-1 text-xs italic text-muted">
            Informational context summarised by an LLM from public sources as of {draft.recentContext.asOf}. Not
            certified operational data.
          </p>
        </div>
      )}

      <div className="rounded-lg border border-default bg-sunken px-3 py-2.5">
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-secondary">
          <ListChecks className="h-3.5 w-3.5 text-accent" />
          Publish in Discover · a few quick steps
        </p>
        <ol className="mt-1.5 list-decimal space-y-1 pl-5 text-xs text-muted">
          <li>Create the Page under {draft.domain}.</li>
          <li>Paste in the synonyms above.</li>
          <li>Add the Related assets (the Agent + sibling Pages).</li>
          <li>Add the Sources (the metric view + source tables).</li>
          <li>
            {draft.certify
              ? "Mark it Certified — this tells Genie to trust it above anything it might guess."
              : "Leave it uncertified — this is helpful context, not an official definition."}
          </li>
        </ol>
      </div>
    </div>
  )
}

// ── Frame d — the Page draft (enrichment succeeded) ─────────────────────────
export function OntologyPageDraftFrame() {
  return (
    <OntologyPageChrome>
      <PageDraftPanel draft={routingDraft} />
    </OntologyPageChrome>
  )
}

// ── Frame e — enrichment failed (draft complete, Recent-context absent) ─────
export function OntologyEnrichmentFailedFrame() {
  const withoutRecent: PageDraftFixture = { ...routingDraft, recentContext: undefined }
  return (
    <OntologyPageChrome>
      <div className="flex items-start gap-2 rounded-lg border border-warning/30 bg-warning/5 px-3 py-2.5">
        <Info className="mt-0.5 h-4 w-4 text-warning-foreground" />
        <p className="text-xs text-secondary">
          We couldn&rsquo;t add public context this time. The draft below is complete from your own data — the optional
          Recent context section is just left out, not failed.
        </p>
      </div>
      <PageDraftPanel draft={withoutRecent} />
    </OntologyPageChrome>
  )
}

// ── Frame f — empty state (honest: what was read, what would change it) ─────
export function OntologyEmptyFrame() {
  return (
    <OntologyPageChrome>
      <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
        <p className="text-sm font-medium text-primary">Nothing to suggest yet</p>
        <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
          We looked across your {ESTATE.agents} Genie Agents, {ESTATE.metricViews} metric views, and your existing
          domains, and didn&rsquo;t find a grouping strong enough to suggest yet. That&rsquo;s a clean result — nothing
          needs your attention. As your data grows and more questions get asked, check back and suggestions will appear
          here.
        </p>
      </div>
    </OntologyPageChrome>
  )
}
