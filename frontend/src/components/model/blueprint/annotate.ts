/**
 * Semantic Blueprint (v4) — self-annotation + selection focus (§5.6, §5.7).
 *
 * Pure functions of the model's counts and the placed boxes: the governance
 * ladder headline, the focus+context neighbourhood, the unmodeled-region hull,
 * the worst cold spot, and the palette map. Each callout is drawn iff its
 * backing data is present (§8) — nothing is invented.
 */
import { shortName, type BlueprintGov, type BlueprintModel, type BlueprintTable } from "./model"
import { measureIndex, nodeById, type Box } from "./layout"

export interface HeadlineCounts {
  governed: number
  curated: number
  ungoverned: number
  unmodeled: number
  cold: number
}

export function headlineCounts(m: BlueprintModel): HeadlineCounts {
  const all = m.nodes.flatMap((n) => (n.kind === "table" ? [] : n.measures))
  return {
    governed: all.filter((ms) => ms.gov === "governed").length,
    curated: all.filter((ms) => ms.gov === "curated").length,
    ungoverned: all.filter((ms) => ms.gov === "ungoverned").length,
    unmodeled: m.nodes.filter((n) => n.kind === "table" && n.unmodeled).length,
    cold: m.nodes.filter((n) => n.kind === "table" && n.cold).length,
  }
}

/** Selected node + its 1-hop neighbourhood (measure → parent + sources). */
export function neighbourhood(m: BlueprintModel, selected: string | null): Set<string> | null {
  if (!selected) return null
  const byId = nodeById(m)
  const measures = measureIndex(m)
  const ms = measures[selected]
  if (ms) return new Set([ms.parent, ...ms.src])
  const keep = new Set([selected])
  for (const j of m.joins) {
    if (j.from === selected) keep.add(j.to)
    if (j.to === selected) keep.add(j.from)
  }
  const node = byId[selected]
  if (node?.kind === "mv") (m.uses[selected] ?? []).forEach((t) => keep.add(t))
  // A metric view or Space-config card keeps every source table its measures
  // read, so selecting the card lights all its (per-measure) lineage instead of
  // dimming the whole canvas down to just the card (§5.10).
  if (node && node.kind !== "table") node.measures.forEach((mm) => mm.src.forEach((t) => keep.add(t)))
  for (const mv in m.uses) if (m.uses[mv].includes(selected)) keep.add(mv)
  return keep
}

export function govColor(g: BlueprintGov): string {
  return g === "governed" ? "var(--color-success)" : g === "curated" ? "var(--color-warning)" : "var(--color-danger)"
}

export function onStr(j: BlueprintModel["joins"][number]): string {
  return `${j.to}.${j.toCol} = ${j.from}.${j.fromCol}`
}

/**
 * `ON` predicate for the detail inset, using SHORT table names
 * (`dim_user.user_id = fact.user_id`). The fully-qualified form (`onStr`)
 * overflows the two-column inset grid; the short form fits and, with wrapping,
 * never bleeds into the adjacent measures column.
 */
export function onStrShort(j: BlueprintModel["joins"][number]): string {
  return `${shortName(j.to)}.${j.toCol} = ${shortName(j.from)}.${j.fromCol}`
}

/** Bounding rect around every unmodeled table (§5.6), or null when none. */
export function unmodeledRegion(m: BlueprintModel, box: Record<string, Box>): (Box & { pad: number }) | null {
  const un = m.nodes.filter((n): n is BlueprintTable => n.kind === "table" && !!n.unmodeled).map((n) => box[n.id]).filter(Boolean)
  if (!un.length) return null
  const x = Math.min(...un.map((b) => b.x)) - 8
  const y = Math.min(...un.map((b) => b.y)) - 8
  const x2 = Math.max(...un.map((b) => b.x + b.w)) + 8
  const y2 = Math.max(...un.map((b) => b.y + b.h)) + 8
  return { x, y, w: x2 - x, h: y2 - y, pad: 8 }
}

/** The worst cold spot (a `cold` table) to anchor the callout to, or null. */
export function worstColdSpot(m: BlueprintModel): BlueprintTable | null {
  return m.nodes.find((n): n is BlueprintTable => n.kind === "table" && !!n.cold) ?? null
}

// ── Insights inset — top 1-2 deal-breakers (§7.5) ────────────────────────────
export interface Insight {
  severity: "fail" | "warn"
  title: string
  detail: string
  /** Node id to focus on click (blueprint id: table / mv / config / measure). */
  focus: string | null
}

const TABLE_LIMIT = 30
const SOURCE_WARN = 9

/**
 * The top 1-2 issues most worth fixing, ranked by impact (§7.5). Derived from
 * the same graph signals the IQ Scan computes — island (fail), over the
 * 30-table limit (fail), ≥9 sources (warn), wide table (warn, needs a column
 * count), cold spot (warn), name collision (warn). Fails outrank warns; the cap
 * is the whole point — the full checklist stays in the IQ Scan. Pure function
 * of the model.
 */
export function rankInsights(m: BlueprintModel): Insight[] {
  const tables = m.nodes.filter((n): n is BlueprintTable => n.kind === "table")
  const out: Insight[] = []

  const island = tables.find((t) => t.island)
  if (island)
    out.push({
      severity: "fail",
      title: "Unrelated table",
      detail: `${shortLabel(island.id)} has no join — Genie can't combine it with anything.`,
      focus: island.id,
    })

  if (tables.length > TABLE_LIMIT)
    out.push({
      severity: "fail",
      title: "Over the 30-table limit",
      detail: `${tables.length} tables — those past 30 are dropped, so answers silently miss data.`,
      focus: null,
    })

  if (tables.length >= SOURCE_WARN)
    out.push({
      severity: "warn",
      title: "Too many data sources",
      detail: `${tables.length} tables — best practice is ≤5 focused tables for accuracy.`,
      focus: null,
    })

  const wide = tables.find((t) => !!t.columnCount && t.columnCount > TABLE_LIMIT)
  if (wide)
    out.push({
      severity: "warn",
      title: "Wide table",
      detail: `${shortLabel(wide.id)} has ${wide.columnCount} columns — lowers column-selection accuracy.`,
      focus: wide.id,
    })

  const cold = tables.find((t) => t.cold && !t.island)
  if (cold)
    out.push({
      severity: "warn",
      title: "Cold spot",
      detail: `${shortLabel(cold.id)} is joined but no curated SQL exercises it.`,
      focus: cold.id,
    })

  for (const n of m.nodes) {
    if (n.kind === "table") continue
    const collide = n.measures.find((ms) => ms.overlaps)
    if (collide) {
      out.push({
        severity: "warn",
        title: "Name collision",
        detail: `${collide.name} duplicates a governed name under a different definition — one question, two numbers.`,
        focus: `${n.id}::${collide.name}`,
      })
      break
    }
  }

  const rank = (i: Insight) => (i.severity === "fail" ? 0 : 1)
  return out.sort((a, b) => rank(a) - rank(b)).slice(0, 2)
}

function shortLabel(id: string): string {
  const parts = (id || "").replace(/`/g, "").split(".")
  return parts[parts.length - 1] || id
}
