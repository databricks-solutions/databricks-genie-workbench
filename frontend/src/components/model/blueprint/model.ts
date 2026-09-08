/**
 * Semantic Blueprint (v4) — production model + adapter.
 *
 * The normalized, render-ready model the blueprint canvas consumes, plus
 * `fromSemanticGraph`, which folds the live `SemanticGraphResponse`
 * (backend `_build_semantic_graph`) into it. This is the production extraction
 * of the proven, gated reference math in
 * `components/auto-optimize/mockups/blueprintMath.ts` (see
 * docs/design/semantic-graph-v4-blueprint-note.md §5, §11): the mockup fixtures
 * (`BlueprintScenarioFixture`) and this `BlueprintModel` are the SAME shape, so
 * the layout / routing / cardinality / annotate modules operate on either.
 *
 * Grounding (§2): joins come only from declared `join`/MV-YAML edges; a
 * measure's source tables come only from `derives` edges or its metric view's
 * proven `uses` set; roles are rendered only when the backend proved them
 * (`role`), never guessed. Nothing here invents an edge or a role.
 */
import type {
  SemanticGraphEdge,
  SemanticGraphNode,
  SemanticGraphResponse,
} from "@/types"

export type BlueprintGov = "governed" | "curated" | "ungoverned"

export interface BlueprintMeasure {
  name: string
  gov: BlueprintGov
  expr: string
  /** Lineage → source tables (proven `derives` / transitive `uses`, §5.10). */
  src: string[]
  /** Name collision: the metric view that already exposes this name. */
  overlaps?: string
}

export interface BlueprintTable {
  kind: "table"
  id: string
  /** Proven by a metric view only — absent renders a neutral "TABLE" (§5.11). */
  role?: "FACT" | "DIM"
  rank: number
  order: number
  coverage: number
  columnCount?: number
  island?: boolean
  unmodeled?: boolean
  cold?: boolean
  /** Participating columns (join keys) — the Columns-LOD rows (§6). */
  cols: string[]
}

export interface BlueprintMv {
  kind: "mv" | "config"
  id: string
  rank: number
  order: number
  mv_filter?: string
  materialization?: string
  dimensions?: { binding: string; name: string }[]
  measures: BlueprintMeasure[]
}

export type BlueprintNode = BlueprintTable | BlueprintMv

export interface BlueprintJoin {
  from: string
  fromCol: string
  to: string
  toCol: string
  rel: "N:1" | "1:1"
  /**
   * Which endpoint is the MANY side of the relationship, so the crow's-foot lands
   * on the proven many end regardless of author order. Derived from the declared
   * relationship (`many-to-one` → `from`, `one-to-many` → `to`); `undefined` for
   * `1:1` or an unknown relationship (routing falls back to author order).
   */
  manyEnd?: "from" | "to"
  /**
   * Distinct column-pair predicates behind this ONE relationship line. ERD best
   * practice (Redgate/Vertabelo) draws a single line per table pair — a composite
   * key or a redundant second join_spec collapses here rather than stacking two
   * lines between the same cards. `undefined`/`1` renders as a plain edge.
   */
  keyCount?: number
}

export interface BlueprintModel {
  nodes: BlueprintNode[]
  joins: BlueprintJoin[]
  /** MV id → member tables it sources (the proven `uses` set). */
  uses: Record<string, string[]>
}

/** Short display name — last dotted segment, backticks stripped. */
export function shortName(id: string): string {
  const parts = (id || "").replace(/`/g, "").split(".")
  return parts[parts.length - 1] || id
}

// Relationship → rendered cardinality AND the proven MANY end. Both
// `many-to-one` and `one-to-many` draw one crow's-foot (N:1), but on OPPOSITE
// ends — losing that direction is what put feet on the wrong endpoint.
const REL_MAP: Record<string, { rel: BlueprintJoin["rel"]; manyEnd?: "from" | "to" }> = {
  "many-to-one": { rel: "N:1", manyEnd: "from" },
  "one-to-many": { rel: "N:1", manyEnd: "to" },
  "one-to-one": { rel: "1:1" },
}

function relOf(edge: SemanticGraphEdge): { rel: BlueprintJoin["rel"]; manyEnd?: "from" | "to" } {
  return REL_MAP[(edge.relationship || "").toLowerCase()] ?? { rel: "N:1", manyEnd: "from" }
}

/**
 * Best-effort column endpoints for a join from its decoded `ON` predicate
 * (§5.5/§6 — Phase-1 uses `ON` leaf names client-side until the Phase-2 column
 * model lands). Handles `a.col = b.col`, matching each side to `fromId`/`toId`
 * by identifier suffix; falls back to the two leaf names in author order, then
 * to `id`. Pure string parse — never throws.
 */
export function parseOnColumns(
  on: string | null | undefined,
  fromId: string,
  toId: string,
): { fromCol: string; toCol: string } {
  const fallback = { fromCol: "id", toCol: "id" }
  if (!on) return fallback
  const eq = on.split(/=(.+)/)
  if (eq.length < 2) return fallback
  const sides = [eq[0], eq[1]].map((s) => s.trim().replace(/`/g, ""))
  const leaf = (s: string) => {
    const p = s.split(".")
    return p[p.length - 1] || s
  }
  const qualifier = (s: string) => {
    const p = s.split(".")
    return p.length > 1 ? p.slice(0, -1).join(".") : ""
  }
  const matches = (qual: string, id: string) => {
    const q = qual.toLowerCase()
    const short = shortName(id).toLowerCase()
    return !!q && (id.toLowerCase().endsWith(q) || q.endsWith(short) || q === short)
  }
  const [a, b] = sides
  if (matches(qualifier(a), fromId) && matches(qualifier(b), toId)) {
    return { fromCol: leaf(a), toCol: leaf(b) }
  }
  if (matches(qualifier(a), toId) && matches(qualifier(b), fromId)) {
    return { fromCol: leaf(b), toCol: leaf(a) }
  }
  return { fromCol: leaf(a), toCol: leaf(b) }
}

const CONFIG_ID = "Space config"

/**
 * Fold a live `SemanticGraphResponse` into the render-ready `BlueprintModel`.
 * Deterministic and pure: node/join order follows the response; measures group
 * under their proven owning metric view (`membership`) or the synthetic Space
 * config card. Absent data degrades silently (§5.11) — no invented roles/edges.
 */
export function fromSemanticGraph(resp: SemanticGraphResponse): BlueprintModel {
  const nodes = resp.nodes ?? []
  const edges = resp.edges ?? []
  const byId: Record<string, SemanticGraphNode> = {}
  nodes.forEach((n) => (byId[n.id] = n))

  const tableIds = nodes.filter((n) => n.kind === "table").map((n) => n.id)
  const mvIds = nodes.filter((n) => n.kind === "metric_view").map((n) => n.id)

  // Declared joins (base truth) → column-accurate endpoints. Prefer the
  // server-parsed columns (Phase 2, §6); fall back to the client `ON` parse
  // (Phase 1) so a response without the column model renders identically.
  const joinEdges = edges.filter((e) => e.kind === "join" && byId[e.from] && byId[e.to])
  const rawJoins: BlueprintJoin[] = joinEdges.map((e) => {
    const parsed = parseOnColumns(e.on, e.from, e.to)
    const { rel, manyEnd } = relOf(e)
    return {
      from: e.from,
      fromCol: e.from_column ?? parsed.fromCol,
      to: e.to,
      toCol: e.to_column ?? parsed.toCol,
      rel,
      ...(manyEnd ? { manyEnd } : {}),
    }
  })

  // One line per relationship (§ ERD best practice): collapse every join edge
  // between the same unordered table pair into a SINGLE edge. A composite key
  // (`a.x=b.x AND a.y=b.y`) or a redundant duplicate join_spec would otherwise
  // stack two/three lines between the same two cards with overlapping ON labels
  // (the "2 lines between the same tables" clutter). We keep the first edge's
  // orientation/columns as the representative, count the distinct column pairs
  // in `keyCount`, and prefer N:1 if any predicate is many-to-one.
  const joins: BlueprintJoin[] = (() => {
    const byPair = new Map<string, BlueprintJoin>()
    for (const j of rawJoins) {
      const key = [j.from, j.to].slice().sort().join("\u0000")
      const ex = byPair.get(key)
      if (!ex) {
        byPair.set(key, { ...j, keyCount: 1 })
        continue
      }
      const samePredicate =
        (ex.fromCol === j.fromCol && ex.toCol === j.toCol) ||
        (ex.fromCol === j.toCol && ex.toCol === j.fromCol)
      if (!samePredicate) ex.keyCount = (ex.keyCount ?? 1) + 1
      if (j.rel === "N:1") {
        ex.rel = "N:1"
        // Adopt the many end if the representative had none (e.g. it was 1:1).
        // Edges between one pair share author orientation, so j's end maps.
        if (!ex.manyEnd && j.manyEnd) ex.manyEnd = j.manyEnd
      }
    }
    return [...byPair.values()]
  })()

  // Proven MV → member tables (`uses`) and the reverse membership set.
  const uses: Record<string, string[]> = {}
  edges
    .filter((e) => e.kind === "uses" && byId[e.from]?.kind === "metric_view")
    .forEach((e) => {
      ;(uses[e.from] ??= []).push(e.to)
    })

  // Participating columns per table, from parsed join keys (§6 Phase-1 source).
  const colsByTable: Record<string, string[]> = {}
  const addCol = (id: string, col: string) => {
    const list = (colsByTable[id] ??= [])
    if (!list.includes(col)) list.push(col)
  }
  // From the RAW joins so every participating key column still lists in the
  // Columns LOD, even when its relationship line collapsed into another pair's.
  rawJoins.forEach((j) => {
    addCol(j.from, j.fromCol)
    addCol(j.to, j.toCol)
  })

  // Measure → source tables. Governed measures reach tables transitively via
  // membership → MV → uses; loose measures via their `derives` edges. Both are
  // proven reads (§5.10) — nothing is inferred from expression text here.
  const membership: Record<string, string> = {} // measureId → mvId
  const derives: Record<string, string[]> = {} // measureId → tableIds
  edges.forEach((e) => {
    if (e.kind === "membership" && byId[e.from]?.kind === "measure") membership[e.from] = e.to
    if (e.kind === "derives" && byId[e.from]?.kind === "measure") (derives[e.from] ??= []).push(e.to)
  })

  const measureSrc = (mId: string): string[] => {
    const direct = derives[mId] ?? []
    const mv = membership[mId]
    const viaMv = mv ? uses[mv] ?? [] : []
    return [...new Set([...direct, ...viaMv])].filter((t) => byId[t]?.kind === "table")
  }

  const govOf = (n: SemanticGraphNode): BlueprintGov =>
    n.governance === "governed" || n.governance === "curated" ? n.governance : "ungoverned"

  const toMeasure = (n: SemanticGraphNode): BlueprintMeasure => ({
    name: n.label,
    gov: govOf(n),
    expr: n.expr ?? "",
    src: measureSrc(n.id),
    ...(n.overlaps ? { overlaps: n.overlaps } : {}),
  })

  const measureNodes = nodes.filter((n) => n.kind === "measure")
  const measuresByMv: Record<string, BlueprintMeasure[]> = {}
  const loose: BlueprintMeasure[] = []
  measureNodes.forEach((n) => {
    const mv = membership[n.id]
    if (mv && byId[mv]?.kind === "metric_view") (measuresByMv[mv] ??= []).push(toMeasure(n))
    else loose.push(toMeasure(n))
  })

  // Table nodes with derived annotations. island/unmodeled mirror the classic
  // canvas rules: island only when there is more than one table (§5.11);
  // unmodeled only when SOME `uses` set exists to contrast against.
  const joinedTables = new Set<string>()
  joins.forEach((j) => {
    joinedTables.add(j.from)
    joinedTables.add(j.to)
  })
  const memberTables = new Set<string>(Object.values(uses).flat())
  const anyUses = memberTables.size > 0
  const multiTable = tableIds.length > 1

  const blueprintTables: BlueprintTable[] = tableIds.map((id, i) => {
    const n = byId[id]
    const coverage = typeof n.coverage === "number" ? n.coverage : 0
    const role = n.role === "fact" ? "FACT" : n.role === "dim" ? "DIM" : undefined
    // Prefer the server column model (Phase 2, §6); fall back to the join keys
    // parsed client-side (Phase 1). Additive — identical render without columns.
    const cols = n.columns && n.columns.length ? [...new Set(n.columns)] : colsByTable[id] ?? []
    return {
      kind: "table",
      id,
      ...(role ? { role } : {}),
      rank: n.role === "dim" || n.col === 1 ? 1 : 0,
      order: i,
      coverage,
      island: multiTable && !joinedTables.has(id),
      unmodeled: anyUses && !memberTables.has(id),
      cold: coverage === 0,
      cols,
    }
  })

  const maxTableRank = blueprintTables.reduce((m, t) => Math.max(m, t.rank), 0)

  const mvNodes: BlueprintMv[] = mvIds.map((id, i) => {
    const n = byId[id]
    const dims = (n.dimensions ?? [])
      .filter((d) => d.binding)
      .map((d) => ({ binding: d.binding as string, name: d.name }))
    return {
      kind: "mv",
      id,
      rank: maxTableRank + 1,
      order: i,
      ...(n.mv_filter ? { mv_filter: n.mv_filter } : {}),
      ...(n.materialization ? { materialization: n.materialization } : {}),
      ...(dims.length ? { dimensions: dims } : {}),
      measures: measuresByMv[id] ?? [],
    }
  })

  const configNode: BlueprintMv | null = loose.length
    ? { kind: "config", id: CONFIG_ID, rank: maxTableRank + 1, order: mvIds.length, measures: loose }
    : null

  return {
    nodes: [...blueprintTables, ...mvNodes, ...(configNode ? [configNode] : [])],
    joins,
    uses,
  }
}
