/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE.
 *
 * Disposal (see docs/design/mockups/README.md): these frames exist only for the
 * Prompt 10 design review and are deleted as the real panels land — Prompt 11
 * removed the run-config frames, Prompt 13 removed the output frames (4–5, now
 * MvSuggestOnlyPanel / MvCreateAttachPanel), Prompt 13.5 the IQ-Scan frames (and
 * graduates or deletes MvProposalCard). tables_freed was CUT, never shipped (no
 * producer). Do not wire them to the backend and do not grow a dependency on them.
 *
 * Fixture shapes mirror the backend Pydantic models so Prompt 13/13.5 can feed
 * the same components from a real payload without a shape change:
 *   MvProposal          backend/models.py:273
 *   MvDdlArtifact       backend/models.py:310
 *   MvConsentPayload    backend/models.py:261
 *   MvCreatedObject     backend/models.py:355
 * These live local to the mockups on purpose — the gap report records there is
 * "no TS mirror yet" for the Mv* models, and this prompt adds no wiring, so
 * nothing here is promoted into frontend/src/types/index.ts.
 *
 * MV-D23: run_id is PRESENTATIONAL ONLY in every fixture below. No frame keys
 * state, fetch, or identity on it — Prompt 13.5 supplies the same ScoredProposal
 * shape from a space-scoped source.
 */

export interface MvProposalFixture {
  suggestion_id: string
  dedup_fingerprint: string
  target_space_id: string
  /** Presentational only (MV-D23) — never a state/fetch key. */
  run_id: string | null
  candidate_type: string
  confidence_score: number | null
  tier: "HIGH" | "MEDIUM" | "LOW" | null
  proposed_object: string | null
  evidence: {
    recurrence_count: number
    benchmark_question_ids: string[]
    source_tables: string[]
  } | null
  /**
   * Drives the join-strategy badge. The "nested" rung is intentionally NOT a
   * member: it is unreachable on every compute today (no EXACT uniqueness
   * evidence, so the in-job render always lands on rung 3 — MV-D14/D15).
   * Fixtures must depict only reachable states. Omit entirely for a single
   * direct join (no ladder to report).
   */
  join_strategy?: "denormalized" | "subquery-source"
}

export interface MvDdlFixture {
  suggestion_id: string
  proposed_object: string
  /** Omitted for a single direct join — no rung ladder to report (MV-D14/D15). */
  join_strategy?: "denormalized" | "subquery-source"
  /** Immutable rendered body (MV-D22). */
  yaml_text: string
  /**
   * Render-time CREATE VIEW wrapper — the full, EXECUTABLE statement
   * (CREATE VIEW … WITH METRICS LANGUAGE YAML AS $$ … $$). The DDL panel shows
   * this, not the bare YAML: what a user copies must run (POV §7.5).
   */
  ddl: string
  /** Copy-ready GRANT SELECT checklist — never auto-applied. */
  grant_sql: string
}

export interface MvConsentFixture {
  granted_by: string
  granted_at: string
  probe_id: string
  target: string
}

export interface MvCreatedObjectFixture {
  run_id: string
  suggestion_id: string
  full_name: string
  created_by: string
  status: "CREATED" | "ATTACHED" | "DETACHED" | "DROPPED"
  baseline_eval_run_id: string
  post_attach_eval_run_id: string
  on_regression_action: string | null
}

// ── Proposal (the recurring "revenue per order" measure) ────────────────────
export const proposalRevenue: MvProposalFixture = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  target_space_id: "01ef9a2b3c4d5e6f",
  run_id: "run_5c1e",
  candidate_type: "PROPOSE",
  confidence_score: 88,
  tier: "HIGH",
  proposed_object: "finance.sales.order_revenue",
  // No join_strategy: a single direct join needs no rung ladder, and the
  // "nested" rung is unreachable on every compute today (nothing produces EXACT
  // uniqueness evidence, so the in-job render always lands on rung 3 —
  // MV-D14/D15). Fixtures must depict only reachable states; Prompts 11–13 copy
  // from here.
  evidence: {
    recurrence_count: 14,
    benchmark_question_ids: ["bq_0007", "bq_0019", "bq_0022", "bq_0041"],
    source_tables: ["finance.sales.orders", "finance.sales.order_items"],
  },
}

export const proposalMargin: MvProposalFixture = {
  suggestion_id: "sug_3b77e0aa91d2",
  dedup_fingerprint: "3b77e0aa91d2f5c1",
  target_space_id: "01ef9a2b3c4d5e6f",
  run_id: "run_5c1e",
  candidate_type: "PROPOSE",
  confidence_score: 71,
  tier: "MEDIUM",
  proposed_object: "finance.sales.gross_margin",
  join_strategy: "subquery-source",
  evidence: {
    recurrence_count: 6,
    benchmark_question_ids: ["bq_0011", "bq_0033"],
    source_tables: ["finance.sales.orders", "finance.ref.product_cost"],
  },
}

const REVENUE_YAML = `version: "1.1"
source: finance.sales.orders
joins:
  - name: items
    source: finance.sales.order_items
    "on": orders.order_id = items.order_id
dimensions:
  - name: order_date
    expr: orders.order_date
measures:
  - name: total_revenue
    expr: SUM(items.quantity * items.unit_price)
    format: number
comment: |
  PURPOSE: Governed revenue for the sales Agent.
  BEST FOR: revenue over time, revenue by dimension.
  NOT FOR: per-line profitability (see gross_margin).`

export const ddlRevenue: MvDdlFixture = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  proposed_object: "finance.sales.order_revenue",
  yaml_text: REVENUE_YAML,
  // Full, executable statement — the panel's contract is that copying it runs.
  ddl: `CREATE VIEW \`finance\`.\`sales\`.\`order_revenue\`
WITH METRICS
LANGUAGE YAML
AS $$
${REVENUE_YAML}
$$;`,
  grant_sql: `GRANT SELECT ON VIEW \`finance\`.\`sales\`.\`order_revenue\` TO \`sales-analysts\`;`,
}

export const consentGranted: MvConsentFixture = {
  granted_by: "prashanth@example.com",
  granted_at: "2026-08-23T09:14:22Z",
  probe_id: "probe_7f21",
  target: "finance.sales",
}

export const createdAttached: MvCreatedObjectFixture = {
  run_id: "run_5c1e",
  suggestion_id: "sug_9f2a1c7d4e0b",
  full_name: "finance.sales.order_revenue",
  created_by: "prashanth@example.com",
  status: "ATTACHED",
  baseline_eval_run_id: "eval_a1",
  post_attach_eval_run_id: "eval_b2",
  on_regression_action: null,
}

export const createdDetached: MvCreatedObjectFixture = {
  ...createdAttached,
  status: "DETACHED",
  on_regression_action: "snapshot_revert",
}

// ── BYO registration (MV-D24) ───────────────────────────────────────────────
// A metric view the USER created themselves (from copied DDL) and reported back.
// provenance distinguishes it from OBO_CREATED views; the app never drops it.
export interface MvByoRegistrationFixture {
  identifier: string
  verified_by: string
  provenance: "USER_CREATED"
  type_confirmed: boolean
  validation_passed: boolean
}

export const byoVerified: MvByoRegistrationFixture = {
  identifier: "finance.sales.net_revenue",
  verified_by: "prashanth@example.com",
  provenance: "USER_CREATED",
  type_confirmed: true,
  validation_passed: true,
}

// ── Semantic model graph (Model tab — Prompt 12.0 review mockups) ────────────
// PRESENTATIONAL ONLY. The real nodes/edges JSON now LANDED at Prompt 12:
// GET /api/auto-optimize/spaces/{space_id}/semantic-graph
// (backend/routers/auto_optimize.py:1869), typed by SemanticGraphResponse in
// frontend/src/types/index.ts and rendered by the production Model tab
// (components/model/SemanticModelTab.tsx). These fixtures stay LOCAL to the
// mockups as the approved design record until the scaffold is disposed at
// Prompt 13.5; the production path does not read them.
//
// Governance ladder is a TRAFFIC LIGHT on the theme's semantic tokens
// (governed=success, curated=warning, ungoverned=danger) — the 12.0 correction,
// NOT the Prompt 10 confidence trio (high/medium/low), which is a different axis.
// Ungoverned is the state the whole feature exists to fix, so it draws the eye
// (danger), never muted. Presentation pairs every rung with an icon/label so it
// never relies on hue alone; the color mapping itself lives in
// MvSemanticModelFrame.tsx (data here carries only the rung + a text origin).

export type MvGovernance = "governed" | "curated" | "ungoverned"

export type MvNodeKind = "table" | "metric_view" | "measure"

export interface MvGraphNodeFixture {
  id: string
  kind: MvNodeKind
  label: string
  /** Layered layout position: column 0 = source/fact … 3 = field chips. */
  col: number
  row: number
  /** Governance rung — measure concepts (kind="measure") only. */
  governance?: MvGovernance
  /** Where the concept is defined — the chip's non-color label discriminator. */
  origin?: string
  /** Ghosted proposed-MV overlay node (frame 9c). */
  proposed?: boolean
}

export interface MvGraphEdgeFixture {
  from: string
  to: string
  kind: "join" | "membership" | "replaces"
  /** Join predicate (kind="join"). */
  on?: string
  relationship?: "many-to-one" | "one-to-one"
  /** SCD2 guard present in the predicate (is_current). */
  scd2?: boolean
  /**
   * Reachable strategies only (MV-D14/D15) — never "nested". Omit for a single
   * direct join. Mirrors MvProposalFixture.join_strategy above.
   */
  join_strategy?: "denormalized" | "subquery-source"
  cardinality?: string
}

export interface MvSemanticGraphFixture {
  nodes: MvGraphNodeFixture[]
  edges: MvGraphEdgeFixture[]
}

// Populated space (9a base, and the base layer under the 9c overlay): two source
// tables, one SCD2 dimension join, one existing governed MV, and three measure
// concepts — one per governance rung, the advisor's story in one glance.
export const semanticGraphPopulated: MvSemanticGraphFixture = {
  nodes: [
    { id: "orders", kind: "table", label: "orders", col: 0, row: 0 },
    { id: "order_items", kind: "table", label: "order_items", col: 0, row: 1 },
    { id: "customer", kind: "table", label: "customer", col: 1, row: 0 },
    { id: "orders_metrics", kind: "metric_view", label: "orders_metrics", col: 2, row: 0 },
    {
      id: "order_count",
      kind: "measure",
      label: "order_count",
      col: 3,
      row: 0,
      governance: "governed",
      origin: "orders_metrics (attached MV)",
    },
    {
      id: "gross_margin",
      kind: "measure",
      label: "gross_margin",
      col: 3,
      row: 1,
      governance: "curated",
      origin: "sql_snippets.measures",
    },
    {
      id: "discounted_revenue",
      kind: "measure",
      label: "discounted_revenue",
      col: 3,
      row: 2,
      governance: "ungoverned",
      origin: "proposal evidence · 14×",
    },
  ],
  edges: [
    { from: "order_items", to: "orders", kind: "join", on: "orders.order_id = items.order_id", relationship: "many-to-one" },
    {
      from: "customer",
      to: "orders",
      kind: "join",
      on: "orders.customer_id = customer.id AND customer.is_current = true",
      relationship: "many-to-one",
      scd2: true,
      join_strategy: "subquery-source",
    },
    { from: "order_count", to: "orders_metrics", kind: "membership" },
    { from: "gross_margin", to: "orders", kind: "membership" },
    { from: "discounted_revenue", to: "orders", kind: "membership" },
  ],
}

// Never-optimized space (9b): tables the space uses but which join nothing, all
// in the first column. No joins, and NO measure concepts — the honesty rule cuts
// both ways (frame-7b): nothing green because nothing is governed, and nothing
// red because no ungoverned measure has been *found* yet. An empty ladder, not an
// alarming one.
export const semanticGraphEmpty: MvSemanticGraphFixture = {
  nodes: [
    { id: "orders", kind: "table", label: "orders", col: 0, row: 0 },
    { id: "order_items", kind: "table", label: "order_items", col: 0, row: 1 },
    { id: "customer", kind: "table", label: "customer", col: 0, row: 2 },
  ],
  edges: [],
}

// Proposal overlay (9c): the ghosted proposed MV that would govern the ungoverned
// `discounted_revenue`, with dashed "replaces" edges from the raw tables it would
// cover (the tables_freed story). Rendered on top of semanticGraphPopulated.
export const semanticGraphProposalOverlay: MvSemanticGraphFixture = {
  nodes: [
    { id: "order_revenue", kind: "metric_view", label: "order_revenue", col: 2, row: 1, proposed: true },
  ],
  edges: [
    { from: "order_revenue", to: "orders", kind: "replaces" },
    { from: "order_revenue", to: "order_items", kind: "replaces" },
  ],
}

// Node-detail panel fixtures (9d). The measure is the ungoverned protagonist —
// evidence (recurrence + benchmark questions) exists only for concepts the
// advisor surfaced, and it reuses proposalRevenue.evidence unchanged. The join is
// the SCD2 customer join, with a reachable strategy only (MV-D14/D15).
export const measureDetail = {
  name: "discounted_revenue",
  governance: "ungoverned" as MvGovernance,
  expr: "SUM(items.quantity * items.unit_price * (1 - items.discount))",
  format: "number",
  synonyms: ["revenue", "net sales"],
}

// ── Semantic Blueprint (v4) fixtures — P1 fidelity frame ─────────────────────
// Ported verbatim from the north-star prototype's data model
// (docs/design/mockups/10-blueprint-prototype.html). Three robustness scenarios
// per blueprint §5.11 — star (roles proven), unknown roles (neutral, never
// guessed), single wide table (no joins, still valid) — plus a deterministic
// 30-table snowflake generator for the scale frame. `rank`/`order` are the
// AUTHORED source-left placement; fact-center re-ranking derives its own
// (blueprint §5.12) without mutating these fixtures.

export type BlueprintGov = "governed" | "curated" | "ungoverned"

export interface BlueprintMeasureFixture {
  name: string
  gov: BlueprintGov
  expr: string
  /** Lineage → source tables (already emitted by the backend, §5.10). */
  src: string[]
  /** Name collision: the governed MV that already exposes this name. */
  overlaps?: string
}

export interface BlueprintTableFixture {
  kind: "table"
  id: string
  /** Proven by a metric view only — absent means neutral "TABLE" (§5.11). */
  role?: "FACT" | "DIM"
  rank: number
  order: number
  coverage: number
  columnCount?: number
  island?: boolean
  unmodeled?: boolean
  cold?: boolean
  /** Participating columns (join keys + bindings) — the Columns-LOD rows. */
  cols: string[]
}

export interface BlueprintMvFixture {
  kind: "mv" | "config"
  id: string
  rank: number
  order: number
  mv_filter?: string
  materialization?: string
  dimensions?: { binding: string; name: string }[]
  measures: BlueprintMeasureFixture[]
}

export type BlueprintNodeFixture = BlueprintTableFixture | BlueprintMvFixture

export interface BlueprintJoinFixture {
  from: string
  fromCol: string
  to: string
  toCol: string
  rel: "N:1" | "1:1"
}

export interface BlueprintScenarioFixture {
  nodes: BlueprintNodeFixture[]
  joins: BlueprintJoinFixture[]
  /** MV id → member tables it sources (the proven `uses` set). */
  uses: Record<string, string[]>
}

export const blueprintStar: BlueprintScenarioFixture = {
  nodes: [
    { kind: "table", id: "fact_booking_detail", role: "FACT", rank: 0, order: 0, coverage: 3, columnCount: 41,
      cols: ["booking_id", "user_id", "property_id", "booking_date_id", "spend"] },
    { kind: "table", id: "fact_booking_daily", role: "FACT", rank: 0, order: 1, coverage: 2, columnCount: 9,
      cols: ["date_id", "property_id", "bookings"] },
    { kind: "table", id: "dim_property", role: "DIM", rank: 1, order: 0, coverage: 1, columnCount: 12,
      cols: ["property_id", "destination_id", "host_id"] },
    { kind: "table", id: "dim_user", role: "DIM", rank: 1, order: 1, coverage: 1, columnCount: 8,
      cols: ["user_id", "country", "is_business"] },
    { kind: "table", id: "dim_date", role: "DIM", rank: 1, order: 2, coverage: 1, columnCount: 6,
      cols: ["date_id", "month"] },
    { kind: "table", id: "dim_campaign", role: "DIM", rank: 1, order: 3, coverage: 0, columnCount: 7, island: true,
      cols: ["campaign_id", "channel", "budget"] },
    { kind: "table", id: "dim_destination", role: "DIM", rank: 2, order: 0, coverage: 1, unmodeled: true,
      cols: ["destination_id", "region"] },
    { kind: "table", id: "dim_host", role: "DIM", rank: 2, order: 1, coverage: 0, unmodeled: true, cold: true,
      cols: ["host_id", "host_type"] },
    { kind: "mv", id: "customer_analytics_metrics", rank: 3, order: 0,
      mv_filter: "booking_status != 'CANCELLED'",
      materialization: "2 materializations · EVERY 1 DAY",
      dimensions: [
        { binding: "dim_user", name: "country" },
        { binding: "dim_user", name: "segment" },
        { binding: "dim_property", name: "region" },
      ],
      measures: [
        { name: "customer_count", gov: "governed", expr: "COUNT(DISTINCT dim_user.user_id)", src: ["fact_booking_detail", "dim_user"] },
        { name: "business_count", gov: "governed", expr: "COUNT(DISTINCT CASE WHEN dim_user.is_business THEN dim_user.user_id END)", src: ["dim_user"] },
        { name: "booking_count", gov: "governed", expr: "COUNT(fact_booking_detail.booking_id)", src: ["fact_booking_detail"] },
        { name: "total_spend", gov: "governed", expr: "SUM(fact_booking_detail.spend)", src: ["fact_booking_detail"] },
        { name: "avg_booking_value", gov: "governed", expr: "SUM(fact_booking_detail.spend) / COUNT(fact_booking_detail.booking_id)", src: ["fact_booking_detail"] },
      ] },
    { kind: "config", id: "Space config", rank: 3, order: 1,
      measures: [
        { name: "bookings_per_customer", gov: "curated", expr: "booking_count / customer_count", src: ["fact_booking_detail", "dim_user"] },
        { name: "speed_per_customer", gov: "ungoverned", expr: "AVG(fact_booking_detail.response_ms)", src: ["fact_booking_detail"] },
        { name: "business_rate", gov: "ungoverned", overlaps: "customer_analytics_metrics", expr: "business_count / customer_count", src: ["dim_user"] },
      ] },
  ],
  joins: [
    { from: "fact_booking_detail", fromCol: "user_id", to: "dim_user", toCol: "user_id", rel: "N:1" },
    { from: "fact_booking_detail", fromCol: "property_id", to: "dim_property", toCol: "property_id", rel: "N:1" },
    { from: "fact_booking_daily", fromCol: "date_id", to: "dim_date", toCol: "date_id", rel: "N:1" },
    { from: "fact_booking_daily", fromCol: "property_id", to: "dim_property", toCol: "property_id", rel: "N:1" },
    { from: "dim_property", fromCol: "destination_id", to: "dim_destination", toCol: "destination_id", rel: "N:1" },
    { from: "dim_property", fromCol: "host_id", to: "dim_host", toCol: "host_id", rel: "N:1" },
  ],
  uses: { customer_analytics_metrics: ["fact_booking_detail", "dim_user", "dim_property"] },
}

// Roles deliberately omitted → neutral "TABLE" captions and connectivity-based
// headers, never a guessed FACT/DIM and never an error (§5.11).
export const blueprintUnknown: BlueprintScenarioFixture = {
  nodes: [
    { kind: "table", id: "orders", rank: 0, order: 0, coverage: 2, columnCount: 14,
      cols: ["order_id", "customer_id", "region_id", "amount"] },
    { kind: "table", id: "customers", rank: 1, order: 0, coverage: 1, columnCount: 9,
      cols: ["customer_id", "region_id", "tier"] },
    { kind: "table", id: "regions", rank: 2, order: 0, coverage: 1, columnCount: 4,
      cols: ["region_id", "name"] },
    { kind: "mv", id: "order_metrics", rank: 3, order: 0,
      mv_filter: "status = 'PAID'", materialization: "1 materialization · EVERY 6 HOURS",
      dimensions: [{ binding: "customers", name: "tier" }, { binding: "regions", name: "name" }],
      measures: [
        { name: "order_count", gov: "governed", expr: "COUNT(orders.order_id)", src: ["orders"] },
        { name: "revenue", gov: "governed", expr: "SUM(orders.amount)", src: ["orders"] },
      ] },
    { kind: "config", id: "Space config", rank: 3, order: 1,
      measures: [
        { name: "aov", gov: "curated", expr: "revenue / order_count", src: ["orders"] },
        { name: "repeat_rate", gov: "ungoverned", expr: "COUNT(DISTINCT CASE WHEN customers.tier='repeat' THEN customers.customer_id END) / COUNT(DISTINCT customers.customer_id)", src: ["customers"] },
      ] },
  ],
  joins: [
    { from: "orders", fromCol: "customer_id", to: "customers", toCol: "customer_id", rel: "N:1" },
    { from: "customers", fromCol: "region_id", to: "regions", toCol: "region_id", rel: "N:1" },
  ],
  uses: { order_metrics: ["orders", "customers"] },
}

// One denormalized wide table, no joins — a VALID model, not an error (§5.11):
// measures + metric view + config still render, and the table is never flagged
// "unrelated" (there is nothing to relate it to).
export const blueprintWide: BlueprintScenarioFixture = {
  nodes: [
    { kind: "table", id: "events_wide", rank: 0, order: 0, coverage: 3, columnCount: 62,
      cols: ["event_id", "user_id", "ts", "country", "device", "revenue"] },
    { kind: "mv", id: "engagement_metrics", rank: 1, order: 0,
      mv_filter: "event_type = 'active'", materialization: "1 materialization · EVERY 1 HOUR",
      dimensions: [{ binding: "events_wide", name: "country" }, { binding: "events_wide", name: "device" }],
      measures: [
        { name: "dau", gov: "governed", expr: "COUNT(DISTINCT events_wide.user_id)", src: ["events_wide"] },
        { name: "revenue", gov: "governed", expr: "SUM(events_wide.revenue)", src: ["events_wide"] },
      ] },
    { kind: "config", id: "Space config", rank: 1, order: 1,
      measures: [
        { name: "arpu", gov: "curated", expr: "revenue / dau", src: ["events_wide"] },
        { name: "power_users", gov: "ungoverned", expr: "COUNT(DISTINCT CASE WHEN events_wide.revenue > 100 THEN events_wide.user_id END)", src: ["events_wide"] },
      ] },
  ],
  joins: [],
  uses: { engagement_metrics: ["events_wide"] },
}

// Deterministic 30-table snowflake for the scale frame: one hub fact, 14 dims
// joined to it, 15 sub-dims joined round-robin to the dims (s14 wraps to d0,
// which guarantees at least one genuine crossing → a hop arc to gate on).
export function makeBlueprintScale(): BlueprintScenarioFixture {
  const nodes: BlueprintNodeFixture[] = []
  const joins: BlueprintJoinFixture[] = []
  nodes.push({ kind: "table", id: "f_sales", role: "FACT", rank: 0, order: 0, coverage: 4, columnCount: 24,
    cols: ["sale_id", "amount"] })
  for (let i = 0; i < 14; i++) {
    nodes.push({ kind: "table", id: `dim_${i}`, role: "DIM", rank: 1, order: i, coverage: i % 5 === 0 ? 0 : 1,
      columnCount: 8, cols: [`dim_${i}_id`, "name"] })
    joins.push({ from: "f_sales", fromCol: `dim_${i}_id`, to: `dim_${i}`, toCol: `dim_${i}_id`, rel: "N:1" })
  }
  for (let i = 0; i < 15; i++) {
    const parent = `dim_${i % 14}`
    nodes.push({ kind: "table", id: `sub_${i}`, role: "DIM", rank: 2, order: i, coverage: i % 4 === 0 ? 0 : 1,
      columnCount: 5, cols: [`sub_${i}_id`, "label"] })
    joins.push({ from: parent, fromCol: `sub_${i}_id`, to: `sub_${i}`, toCol: `sub_${i}_id`, rel: "N:1" })
  }
  nodes.push({ kind: "mv", id: "sales_metrics", rank: 3, order: 0,
    mv_filter: "status = 'COMPLETE'", materialization: "1 materialization · EVERY 1 DAY",
    dimensions: [{ binding: "dim_0", name: "name" }],
    measures: [
      { name: "sales_count", gov: "governed", expr: "COUNT(f_sales.sale_id)", src: ["f_sales"] },
      { name: "sales_total", gov: "governed", expr: "SUM(f_sales.amount)", src: ["f_sales"] },
    ] })
  nodes.push({ kind: "config", id: "Space config", rank: 3, order: 1,
    measures: [
      { name: "avg_sale", gov: "curated", expr: "sales_total / sales_count", src: ["f_sales"] },
    ] })
  return { nodes, joins, uses: { sales_metrics: ["f_sales", "dim_0", "dim_1"] } }
}

export const joinDetail = {
  from: "customer",
  to: "orders",
  on: "orders.customer_id = customer.id AND customer.is_current = true",
  cardinality: "many-to-one" as const,
  scd2: true,
  // Reachable rung only — "nested" is unreachable on every compute today
  // (MV-D14/D15), same rule the proposal fixtures follow.
  join_strategy: "subquery-source" as const,
}
