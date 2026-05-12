import { useMemo, useRef, useState, useEffect } from 'react'
import { ChevronDown } from 'lucide-react'
import ForceGraph2D, { type LinkObject, type NodeObject } from 'react-force-graph-2d'

import { Card } from '@/components/ui/card'
import * as api from '@/watch/lib/api'
import type { ResourceGraph } from '@/watch/types/api'
import { useCachedFetch } from '@/watch/lib/cache'

interface Props {
  days: number
}

type Kind = 'space' | 'resource'

interface GraphNode extends NodeObject {
  id: string
  kind: Kind
  label: string
  title: string | null
  workspace_name: string | null
  query_count: number
}

interface GraphLink extends LinkObject {
  source: string | GraphNode
  target: string | GraphNode
  query_count: number
}

const SPACE_COLOR = '#FF3621'      // Databricks red
const RESOURCE_COLOR = '#1B3139'   // Databricks navy
const HIGHLIGHT_COLOR = '#FFAB00'  // amber

export function ResourceGraphView({ days }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fgRef = useRef<any>(null)
  // Set true when a filter changes so the next onEngineStop refits the view.
  const pendingFit = useRef(true)
  const [size, setSize] = useState({ width: 800, height: 640 })

  const { data, error: err } = useCachedFetch<ResourceGraph>(
    `graph:${days}:2000`,
    () => api.getResourceGraph(days, 2000),
    [days],
  )

  const allSpaceIds = useMemo(() => data?.spaces.map(s => s.space_id) ?? [], [data])
  const sortedSpaces = useMemo(() => {
    if (!data) return []
    return [...data.spaces].sort((a, b) => {
      // Named first (alpha), then untitled at the bottom.
      if (a.title && !b.title) return -1
      if (!a.title && b.title) return 1
      return (a.title ?? a.space_id).localeCompare(b.title ?? b.space_id)
    })
  }, [data])
  const titleBySpace = useMemo(
    () => Object.fromEntries((data?.spaces ?? []).map(s => [s.space_id, s.title])),
    [data],
  )
  const metaBySpace = useMemo(
    () => Object.fromEntries(
      (data?.spaces ?? []).map(s => [s.space_id, { workspace_name: s.workspace_name }]),
    ),
    [data],
  )
  const [selectedSpaceIds, setSelectedSpaceIds] = useState<Set<string> | null>(null)
  const [selectedWorkspaceIds, setSelectedWorkspaceIds] = useState<Set<string> | null>(null)
  const [minSharedSpaces, setMinSharedSpaces] = useState(1)
  const [hideUnnamedSpaces, setHideUnnamedSpaces] = useState(false)
  const [selectedCatalogs, setSelectedCatalogs] = useState<Set<string> | null>(null)
  const [selectedSchemas, setSelectedSchemas] = useState<Set<string> | null>(null)
  const [selectedTables, setSelectedTables] = useState<Set<string> | null>(null)

  // Reset selection when underlying space list changes (e.g. days window).
  useEffect(() => {
    setSelectedSpaceIds(null)
    setSelectedWorkspaceIds(null)
    setSelectedCatalogs(null)
    setSelectedSchemas(null)
    setSelectedTables(null)
  }, [data])

  // Parse full_name → {catalog, schema, table} (skip non-3-part names).
  const resourcePartsByName = useMemo(() => {
    const m: Record<string, { catalog: string; schema: string; table: string }> = {}
    for (const e of data?.edges ?? []) {
      if (m[e.full_name]) continue
      const parts = e.full_name.split('.')
      if (parts.length === 3) m[e.full_name] = { catalog: parts[0], schema: parts[1], table: parts[2] }
    }
    return m
  }, [data])

  const spaceToWorkspaceLookup = useMemo(() => {
    const m: Record<string, string | null> = {}
    for (const s of data?.spaces ?? []) m[s.space_id] = s.workspace_id ?? null
    return m
  }, [data])

  const workspaceNameLookup = useMemo(() => {
    const m: Record<string, string | null> = {}
    for (const s of data?.spaces ?? []) {
      if (s.workspace_id && !(s.workspace_id in m)) m[s.workspace_id] = s.workspace_name
    }
    return m
  }, [data])

  const namedSpaceIds = useMemo(
    () => new Set((data?.spaces ?? []).filter(s => s.title).map(s => s.space_id)),
    [data],
  )

  /**
   * Bidirectional filter pipeline. Applies all filters EXCEPT `skip`, so each
   * dropdown's options reflect what's still reachable given every other
   * dimension's current selection.
   */
  type SkipDim = 'workspace' | 'space' | 'catalog' | 'schema' | 'table' | null

  const filterContext = useMemo(() => {
    if (!data) {
      return {
        finalEdges: [] as ResourceGraph['edges'],
        droppedResources: 0,
        droppedSpaces: 0,
        workspaceOpts: [] as { workspace_id: string; workspace_name: string | null }[],
        spaceOpts: [] as ResourceGraph['spaces'],
        catalogOpts: [] as string[],
        schemaOpts: [] as string[],
        tableOpts: [] as string[],
      }
    }

    const apply = (skip: SkipDim, includeRedundancy = true): ResourceGraph['edges'] => {
      let result = data.edges
      if (hideUnnamedSpaces) {
        result = result.filter(e => namedSpaceIds.has(e.space_id))
      }
      if (skip !== 'workspace' && selectedWorkspaceIds) {
        result = result.filter(e => {
          const ws = spaceToWorkspaceLookup[e.space_id]
          return !ws || selectedWorkspaceIds.has(ws)
        })
      }
      if (skip !== 'space' && selectedSpaceIds) {
        result = result.filter(e => selectedSpaceIds.has(e.space_id))
      }
      if (skip !== 'catalog' && selectedCatalogs) {
        result = result.filter(e => {
          const p = resourcePartsByName[e.full_name]
          return !p || selectedCatalogs.has(p.catalog)
        })
      }
      if (skip !== 'schema' && selectedSchemas) {
        result = result.filter(e => {
          const p = resourcePartsByName[e.full_name]
          return !p || selectedSchemas.has(`${p.catalog}.${p.schema}`)
        })
      }
      if (skip !== 'table' && selectedTables) {
        result = result.filter(e => selectedTables.has(e.full_name))
      }
      if (includeRedundancy && minSharedSpaces > 1) {
        const counts: Record<string, Set<string>> = {}
        for (const e of result) (counts[e.full_name] ??= new Set()).add(e.space_id)
        result = result.filter(e => (counts[e.full_name]?.size ?? 0) >= minSharedSpaces)
      }
      return result
    }

    const finalEdges = apply(null)
    const preRedundancyEdges = apply(null, false)
    const droppedResources = new Set(preRedundancyEdges.map(e => e.full_name)).size
      - new Set(finalEdges.map(e => e.full_name)).size
    const droppedSpaces = new Set(preRedundancyEdges.map(e => e.space_id)).size
      - new Set(finalEdges.map(e => e.space_id)).size

    // Distinct workspaces in the cross-section that ignores the workspace filter.
    const wsEdges = apply('workspace')
    const wsIds = new Set<string>()
    for (const e of wsEdges) {
      const ws = spaceToWorkspaceLookup[e.space_id]
      if (ws) wsIds.add(ws)
    }
    const workspaceOpts = [...wsIds]
      .map(workspace_id => ({ workspace_id, workspace_name: workspaceNameLookup[workspace_id] ?? null }))
      .sort((a, b) => {
        if (a.workspace_name && !b.workspace_name) return -1
        if (!a.workspace_name && b.workspace_name) return 1
        return (a.workspace_name ?? a.workspace_id).localeCompare(b.workspace_name ?? b.workspace_id)
      })

    const spEdges = apply('space')
    const spIds = new Set(spEdges.map(e => e.space_id))
    const spaceOpts = sortedSpaces.filter(s => spIds.has(s.space_id))

    const catEdges = apply('catalog')
    const catalogOpts = [...new Set(
      catEdges.map(e => resourcePartsByName[e.full_name]?.catalog).filter((x): x is string => !!x),
    )].sort()

    const schEdges = apply('schema')
    const schemaOpts = [...new Set(
      schEdges
        .map(e => resourcePartsByName[e.full_name])
        .filter((p): p is { catalog: string; schema: string; table: string } => !!p)
        .map(p => `${p.catalog}.${p.schema}`),
    )].sort()

    const tblEdges = apply('table')
    const tableOpts = [...new Set(tblEdges.map(e => e.full_name).filter(n => resourcePartsByName[n]))].sort()

    return {
      finalEdges, droppedResources, droppedSpaces,
      workspaceOpts, spaceOpts, catalogOpts, schemaOpts, tableOpts,
    }
  }, [
    data, sortedSpaces, resourcePartsByName, spaceToWorkspaceLookup, workspaceNameLookup,
    namedSpaceIds, selectedWorkspaceIds, selectedSpaceIds, selectedCatalogs, selectedSchemas,
    selectedTables, minSharedSpaces, hideUnnamedSpaces,
  ])

  const workspaces = filterContext.workspaceOpts
  const spacesInActiveWorkspaces = filterContext.spaceOpts
  const catalogs = filterContext.catalogOpts
  const schemas = filterContext.schemaOpts
  const tables = filterContext.tableOpts

  const activeWorkspaces = useMemo<Set<string>>(
    () => selectedWorkspaceIds ?? new Set(workspaces.map(w => w.workspace_id)),
    [selectedWorkspaceIds, workspaces],
  )
  const activeCatalogs = useMemo<Set<string>>(
    () => selectedCatalogs ?? new Set(catalogs),
    [selectedCatalogs, catalogs],
  )
  const activeSchemas = useMemo<Set<string>>(
    () => selectedSchemas ?? new Set(schemas),
    [selectedSchemas, schemas],
  )
  const activeTables = useMemo<Set<string>>(
    () => selectedTables ?? new Set(tables),
    [selectedTables, tables],
  )
  const activeSpaces = useMemo<Set<string>>(
    () => selectedSpaceIds ?? new Set(spacesInActiveWorkspaces.map(s => s.space_id)),
    [selectedSpaceIds, spacesInActiveWorkspaces],
  )

  // Tune d3 forces for clearer spacing whenever data changes.
  useEffect(() => {
    if (!fgRef.current || !data) return
    fgRef.current.d3Force('charge')?.strength(-260).distanceMax(420)
    fgRef.current.d3Force('link')?.distance(90)
    fgRef.current.d3ReheatSimulation?.()
  }, [data, activeSpaces])

  // Resize observer so the graph fills its container.
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(([entry]) => {
      const cr = entry.contentRect
      setSize({ width: Math.max(400, cr.width), height: Math.max(400, cr.height) })
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const graph = useMemo(() => {
    if (!data) return { nodes: [] as GraphNode[], links: [] as GraphLink[] }
    const nodes: Record<string, GraphNode> = {}
    const links: GraphLink[] = []
    for (const e of filterContext.finalEdges) {
      const sId = `space:${e.space_id}`
      const rId = `resource:${e.full_name}`
      if (!nodes[sId]) {
        const title = titleBySpace[e.space_id] ?? null
        const meta = metaBySpace[e.space_id]
        nodes[sId] = {
          id: sId, kind: 'space',
          label: title ?? e.space_id,
          title,
          workspace_name: meta?.workspace_name ?? null,
          query_count: 0,
        }
      }
      if (!nodes[rId]) {
        nodes[rId] = {
          id: rId, kind: 'resource', label: e.full_name,
          title: null, workspace_name: null, query_count: 0,
        }
      }
      nodes[sId].query_count += e.query_count
      nodes[rId].query_count += e.query_count
      links.push({ source: sId, target: rId, query_count: e.query_count })
    }
    return { nodes: Object.values(nodes), links }
  }, [data, filterContext, titleBySpace, metaBySpace])

  // Mark the view dirty whenever the filtered node/edge count changes; the
  // actual zoomToFit happens in onEngineStop so it runs against settled
  // positions rather than mid-simulation coordinates.
  useEffect(() => {
    if (graph.nodes.length) pendingFit.current = true
  }, [graph.nodes.length, graph.links.length])

  const [hoverId, setHoverId] = useState<string | null>(null)
  const neighborhood = useMemo(() => {
    if (!hoverId) return null
    const adj = new Set<string>([hoverId])
    for (const l of graph.links) {
      const s = typeof l.source === 'string' ? l.source : l.source.id
      const t = typeof l.target === 'string' ? l.target : l.target.id
      if (s === hoverId) adj.add(t)
      if (t === hoverId) adj.add(s)
    }
    return adj
  }, [hoverId, graph.links])

  const filterCount = useMemo(() => {
    if (!data) return { selected: 0, total: 0 }
    return { selected: activeSpaces.size, total: allSpaceIds.length }
  }, [activeSpaces, allSpaceIds, data])

  return (
    <div className="grid gap-4 lg:grid-cols-[260px_1fr]">
      <Card className="flex flex-col gap-3 p-3">
        <div className="text-xs font-medium uppercase text-muted">Filters</div>
        <WorkspaceFilterDropdown
          workspaces={workspaces}
          activeWorkspaces={activeWorkspaces}
          loading={!data}
          onChange={setSelectedWorkspaceIds}
          onAll={() => setSelectedWorkspaceIds(null)}
          onNone={() => setSelectedWorkspaceIds(new Set())}
        />
        <SpaceFilterDropdown
          spaces={spacesInActiveWorkspaces}
          activeSpaces={activeSpaces}
          loading={!data}
          onChange={setSelectedSpaceIds}
          onAll={() => setSelectedSpaceIds(null)}
          onNone={() => setSelectedSpaceIds(new Set())}
          selected={filterCount.selected}
          total={spacesInActiveWorkspaces.length}
        />
        <StringFilterDropdown
          label="Catalog"
          items={catalogs}
          renderItem={s => s}
          active={activeCatalogs}
          loading={!data}
          onChange={setSelectedCatalogs}
          onAll={() => setSelectedCatalogs(null)}
          onNone={() => setSelectedCatalogs(new Set())}
        />
        <StringFilterDropdown
          label="Schema"
          items={schemas}
          renderItem={s => s.split('.').slice(1).join('.')}
          active={activeSchemas}
          loading={!data}
          onChange={setSelectedSchemas}
          onAll={() => setSelectedSchemas(null)}
          onNone={() => setSelectedSchemas(new Set())}
        />
        <StringFilterDropdown
          label="Table"
          items={tables}
          renderItem={s => s.split('.').slice(2).join('.')}
          active={activeTables}
          loading={!data}
          onChange={setSelectedTables}
          onAll={() => setSelectedTables(null)}
          onNone={() => setSelectedTables(new Set())}
        />
        <div className="mt-2 border-t border-default pt-3">
          <label className="block text-xs font-medium uppercase text-muted">
            Min spaces per resource
          </label>
          <p className="mt-1 text-[11px] text-muted/80">
            Hide resources used by fewer than N spaces. Set to 2+ to surface
            tables shared across spaces (potential redundancy).
          </p>
          <div className="mt-2 flex items-center gap-2">
            <input
              type="range"
              min={1}
              max={5}
              step={1}
              value={minSharedSpaces}
              onChange={e => setMinSharedSpaces(Number(e.target.value))}
              className="flex-1"
            />
            <span className="w-6 text-right tabular-nums text-xs">{minSharedSpaces}</span>
          </div>
          {minSharedSpaces > 1 && (filterContext.droppedResources > 0 || filterContext.droppedSpaces > 0) && (
            <p className="mt-1 text-[11px] text-muted/70">
              Hiding {filterContext.droppedResources} resource{filterContext.droppedResources === 1 ? '' : 's'}
              {filterContext.droppedSpaces > 0 && ` · ${filterContext.droppedSpaces} disconnected space${filterContext.droppedSpaces === 1 ? '' : 's'}`}
            </p>
          )}
        </div>
        <div className="mt-2 border-t border-default pt-3">
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <input
              type="checkbox"
              checked={hideUnnamedSpaces}
              onChange={e => setHideUnnamedSpaces(e.target.checked)}
            />
            <span>Hide spaces with no title</span>
          </label>
          <p className="mt-1 text-[11px] text-muted/80">
            Excludes trashed and cross-workspace spaces (anything not returned
            by the Genie API for this workspace).
          </p>
        </div>
        <div className="mt-2 border-t border-default pt-3 text-xs text-muted">
          <div className="mb-1 font-medium uppercase">Legend</div>
          <div className="flex items-center gap-2 py-0.5">
            <span className="inline-block h-2 w-2 rounded-full" style={{ background: SPACE_COLOR }} />
            <span>Genie Space</span>
          </div>
          <div className="flex items-center gap-2 py-0.5">
            <span className="inline-block h-2 w-2 rounded-full" style={{ background: RESOURCE_COLOR }} />
            <span>Resource (table / view)</span>
          </div>
          <p className="mt-2 text-muted/70">
            Node size scales with query volume (log-scaled). Genie Space nodes are
            ~1.4× the radius of resource nodes for emphasis. Hover over a node
            to highlight its neighborhood.
          </p>
        </div>
      </Card>

      <Card className="p-0">
        <div className="flex items-center justify-between border-b border-default px-4 py-2 text-xs uppercase text-muted">
          <span>Bipartite graph — {graph.nodes.length} nodes · {graph.links.length} edges</span>
          {data?.truncated && (
            <span className="normal-case text-amber-500">
              edges truncated — showing top 2,000
            </span>
          )}
        </div>
        {err && <div className="p-3 text-sm text-red-400">{err}</div>}
        <div ref={containerRef} className="relative h-[640px] w-full overflow-hidden">
          {data ? (
            <ForceGraph2D
              ref={fgRef}
              graphData={graph}
              width={size.width}
              height={size.height}
              backgroundColor="transparent"
              nodeRelSize={5}
              nodeVal={(n: NodeObject) => {
                const node = n as GraphNode
                const scale = Math.max(1, Math.log2((node.query_count || 1) + 1))
                // Space ~1.4x radius vs resource (radius scales with sqrt(nodeVal)).
                return node.kind === 'space' ? scale * 3 : scale * 1.5
              }}
              nodeColor={(n: NodeObject) => {
                const node = n as GraphNode
                if (neighborhood && !neighborhood.has(node.id)) return '#94a3b855'
                return node.kind === 'space' ? SPACE_COLOR : RESOURCE_COLOR
              }}
              nodeLabel={(n: NodeObject) => {
                const node = n as GraphNode
                const kindLabel = node.kind === 'space' ? 'Genie Space' : 'Resource'
                const escape = (s: string) =>
                  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                const lines = [`<b>${kindLabel}</b>`, escape(node.label)]
                if (node.kind === 'space' && node.workspace_name) {
                  lines.push(`<span style="opacity:.7">Workspace:</span> ${escape(node.workspace_name)}`)
                }
                lines.push(`<span style="opacity:.7">${node.query_count} queries</span>`)
                return `<div style="font:12px sans-serif;color:#0f172a;background:#fff;padding:6px 8px;border-radius:6px;box-shadow:0 4px 12px rgba(0,0,0,.18);max-width:340px;word-break:break-all">${lines.join('<br>')}</div>`
              }}
              linkColor={(l: LinkObject) => {
                if (!neighborhood) return '#cbd5e155'
                const link = l as GraphLink
                const s = typeof link.source === 'string' ? link.source : link.source.id
                const t = typeof link.target === 'string' ? link.target : link.target.id
                return neighborhood.has(s) && neighborhood.has(t) ? HIGHLIGHT_COLOR : '#cbd5e122'
              }}
              linkWidth={(l: LinkObject) => Math.min(4, Math.log2(((l as GraphLink).query_count || 1) + 1))}
              onNodeHover={(n: NodeObject | null) => setHoverId(n ? (n as GraphNode).id : null)}
              onEngineStop={() => {
                if (pendingFit.current && fgRef.current) {
                  fgRef.current.zoomToFit(400, 60)
                  pendingFit.current = false
                }
              }}
              cooldownTicks={120}
            />
          ) : (
            <div className="flex h-full items-center justify-center text-xs text-muted">
              Loading lineage graph…
            </div>
          )}
        </div>
      </Card>
    </div>
  )
}

interface SpaceFilterDropdownProps {
  spaces: ResourceGraph['spaces']
  activeSpaces: Set<string>
  loading: boolean
  onChange: (next: Set<string>) => void
  onAll: () => void
  onNone: () => void
  selected: number
  total: number
}

function SpaceFilterDropdown({
  spaces, activeSpaces, loading, onChange, onAll, onNone, selected, total,
}: SpaceFilterDropdownProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const wrapperRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current?.contains(e.target as Node)) setOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onClick)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onClick)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return spaces
    return spaces.filter(
      s =>
        (s.title?.toLowerCase().includes(q) ?? false) ||
        s.space_id.toLowerCase().includes(q),
    )
  }, [spaces, search])

  const summary =
    selected === total
      ? `All (${total})`
      : selected === 0
        ? 'None'
        : `${selected} of ${total}`

  return (
    <div ref={wrapperRef} className="relative inline-block">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="flex w-full items-center gap-2 rounded border border-default bg-elevated px-3 py-1.5 text-sm hover:bg-elevated/80"
      >
        <span className="text-xs uppercase text-muted">Genie Spaces</span>
        <span>{summary}</span>
        <ChevronDown className="ml-auto h-4 w-4 text-muted" />
      </button>
      {open && (
        <div className="absolute left-0 top-full z-20 mt-1 w-[340px] rounded border border-default bg-surface shadow-lg">
          <div className="border-b border-default p-2">
            <input
              autoFocus
              type="text"
              placeholder="Search spaces…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="w-full rounded border border-default bg-elevated px-2 py-1 text-sm focus:outline-none focus:ring-1 focus:ring-default"
            />
          </div>
          <div className="flex gap-2 border-b border-default px-2 py-2 text-xs">
            <button
              className="rounded border border-default px-2 py-1 hover:bg-elevated"
              onClick={onAll}
            >
              Select all
            </button>
            <button
              className="rounded border border-default px-2 py-1 hover:bg-elevated"
              onClick={onNone}
            >
              Clear
            </button>
            <span className="ml-auto self-center text-muted">
              {filtered.length} match{filtered.length === 1 ? '' : 'es'}
            </span>
          </div>
          <div className="max-h-[360px] overflow-y-auto p-1">
            {loading && <div className="p-4 text-center text-xs text-muted">Loading…</div>}
            {!loading && !filtered.length && (
              <div className="p-4 text-center text-xs text-muted">No matches.</div>
            )}
            {filtered.map(s => {
              const checked = activeSpaces.has(s.space_id)
              return (
                <label
                  key={s.space_id}
                  className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-xs hover:bg-elevated/50"
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => {
                      const next = new Set(activeSpaces)
                      if (checked) next.delete(s.space_id)
                      else next.add(s.space_id)
                      onChange(next)
                    }}
                  />
                  {s.title ? (
                    <span className="truncate" title={`${s.title}\n${s.space_id}`}>{s.title}</span>
                  ) : (
                    <span className="truncate font-mono text-muted/80" title={s.space_id}>{s.space_id}</span>
                  )}
                </label>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

interface WorkspaceFilterDropdownProps {
  workspaces: { workspace_id: string; workspace_name: string | null }[]
  activeWorkspaces: Set<string>
  loading: boolean
  onChange: (next: Set<string>) => void
  onAll: () => void
  onNone: () => void
}

function WorkspaceFilterDropdown({
  workspaces, activeWorkspaces, loading, onChange, onAll, onNone,
}: WorkspaceFilterDropdownProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const wrapperRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current?.contains(e.target as Node)) setOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onClick)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onClick)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return workspaces
    return workspaces.filter(
      w =>
        (w.workspace_name?.toLowerCase().includes(q) ?? false) ||
        w.workspace_id.toLowerCase().includes(q),
    )
  }, [workspaces, search])

  const total = workspaces.length
  const selected = activeWorkspaces.size
  const summary =
    selected === total
      ? `All (${total})`
      : selected === 0
        ? 'None'
        : `${selected} of ${total}`

  return (
    <div ref={wrapperRef} className="relative inline-block">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="flex w-full items-center gap-2 rounded border border-default bg-elevated px-3 py-1.5 text-sm hover:bg-elevated/80"
      >
        <span className="text-xs uppercase text-muted">Workspace</span>
        <span>{summary}</span>
        <ChevronDown className="ml-auto h-4 w-4 text-muted" />
      </button>
      {open && (
        <div className="absolute left-0 top-full z-20 mt-1 w-[340px] rounded border border-default bg-surface shadow-lg">
          <div className="border-b border-default p-2">
            <input
              autoFocus
              type="text"
              placeholder="Search workspaces…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="w-full rounded border border-default bg-elevated px-2 py-1 text-sm focus:outline-none focus:ring-1 focus:ring-default"
            />
          </div>
          <div className="flex gap-2 border-b border-default px-2 py-2 text-xs">
            <button
              className="rounded border border-default px-2 py-1 hover:bg-elevated"
              onClick={onAll}
            >
              Select all
            </button>
            <button
              className="rounded border border-default px-2 py-1 hover:bg-elevated"
              onClick={onNone}
            >
              Clear
            </button>
            <span className="ml-auto self-center text-muted">
              {filtered.length} match{filtered.length === 1 ? '' : 'es'}
            </span>
          </div>
          <div className="max-h-[360px] overflow-y-auto p-1">
            {loading && <div className="p-4 text-center text-xs text-muted">Loading…</div>}
            {!loading && !filtered.length && (
              <div className="p-4 text-center text-xs text-muted">No matches.</div>
            )}
            {filtered.map(w => {
              const checked = activeWorkspaces.has(w.workspace_id)
              return (
                <label
                  key={w.workspace_id}
                  className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-xs hover:bg-elevated/50"
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => {
                      const next = new Set(activeWorkspaces)
                      if (checked) next.delete(w.workspace_id)
                      else next.add(w.workspace_id)
                      onChange(next)
                    }}
                  />
                  {w.workspace_name ? (
                    <span className="truncate" title={`${w.workspace_name}\n${w.workspace_id}`}>
                      {w.workspace_name}
                    </span>
                  ) : (
                    <span className="truncate font-mono text-muted/80" title={w.workspace_id}>
                      {w.workspace_id}
                    </span>
                  )}
                </label>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

interface StringFilterDropdownProps {
  label: string
  items: string[]
  renderItem: (s: string) => string
  active: Set<string>
  loading: boolean
  onChange: (next: Set<string>) => void
  onAll: () => void
  onNone: () => void
}

function StringFilterDropdown({
  label, items, renderItem, active, loading, onChange, onAll, onNone,
}: StringFilterDropdownProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const wrapperRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current?.contains(e.target as Node)) setOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onClick)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onClick)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return items
    return items.filter(s => renderItem(s).toLowerCase().includes(q) || s.toLowerCase().includes(q))
  }, [items, search, renderItem])

  const total = items.length
  const selected = [...active].filter(s => items.includes(s)).length
  const summary =
    selected === total
      ? `All (${total})`
      : selected === 0
        ? 'None'
        : `${selected} of ${total}`

  return (
    <div ref={wrapperRef} className="relative inline-block">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="flex w-full items-center gap-2 rounded border border-default bg-elevated px-3 py-1.5 text-sm hover:bg-elevated/80"
      >
        <span className="text-xs uppercase text-muted">{label}</span>
        <span>{summary}</span>
        <ChevronDown className="ml-auto h-4 w-4 text-muted" />
      </button>
      {open && (
        <div className="absolute left-0 top-full z-20 mt-1 w-[340px] rounded border border-default bg-surface shadow-lg">
          <div className="border-b border-default p-2">
            <input
              autoFocus
              type="text"
              placeholder={`Search ${label.toLowerCase()}…`}
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="w-full rounded border border-default bg-elevated px-2 py-1 text-sm focus:outline-none focus:ring-1 focus:ring-default"
            />
          </div>
          <div className="flex gap-2 border-b border-default px-2 py-2 text-xs">
            <button
              className="rounded border border-default px-2 py-1 hover:bg-elevated"
              onClick={onAll}
            >
              Select all
            </button>
            <button
              className="rounded border border-default px-2 py-1 hover:bg-elevated"
              onClick={onNone}
            >
              Clear
            </button>
            <span className="ml-auto self-center text-muted">
              {filtered.length} match{filtered.length === 1 ? '' : 'es'}
            </span>
          </div>
          <div className="max-h-[360px] overflow-y-auto p-1">
            {loading && <div className="p-4 text-center text-xs text-muted">Loading…</div>}
            {!loading && !filtered.length && (
              <div className="p-4 text-center text-xs text-muted">No matches.</div>
            )}
            {filtered.map(s => {
              const checked = active.has(s)
              return (
                <label
                  key={s}
                  className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-xs hover:bg-elevated/50"
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => {
                      const next = new Set(active)
                      if (checked) next.delete(s)
                      else next.add(s)
                      onChange(next)
                    }}
                  />
                  <span className="truncate font-mono" title={s}>{renderItem(s)}</span>
                </label>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
