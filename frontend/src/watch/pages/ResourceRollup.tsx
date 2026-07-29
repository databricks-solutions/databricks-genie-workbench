import { useState } from 'react'

import { Card } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import * as api from '@/watch/lib/api'
import type { ResourceRollupItem } from '@/watch/types/api'
import { formatDate, formatInt } from '@/watch/lib/format'
import { useCachedFetch } from '@/watch/lib/cache'
import { ResourceGraphView } from './ResourceGraphView'

export function ResourceRollup() {
  const [days, setDays] = useState<number>(7)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Resource Rollup</h1>
          <p className="text-sm text-muted">
            Most-referenced tables across all Genie Agents in the workspace.
          </p>
        </div>
        <select
          value={days}
          onChange={e => setDays(Number(e.target.value))}
          className="rounded border border-default bg-elevated px-2 py-1 text-sm"
        >
          <option value={7}>last 7 days</option>
          <option value={30}>last 30 days</option>
          <option value={90}>last 90 days</option>
        </select>
      </div>

      <Tabs defaultValue="table">
        <TabsList>
          <TabsTrigger value="table">Table</TabsTrigger>
          <TabsTrigger value="graph">Graph</TabsTrigger>
        </TabsList>

        <TabsContent value="table">
          <RollupTable days={days} />
        </TabsContent>

        <TabsContent value="graph">
          <ResourceGraphView days={days} />
        </TabsContent>
      </Tabs>
    </div>
  )
}

function RollupTable({ days }: { days: number }) {
  const { data, error: err } = useCachedFetch<ResourceRollupItem[]>(
    `rollup:${days}:100`,
    () => api.getResourceRollup(days, 100),
    [days],
  )

  return (
    <>
      {err && <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{err}</Card>}
      <Card className="overflow-hidden p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-default bg-elevated text-left text-xs uppercase text-muted">
            <tr>
              <th className="px-4 py-2">Table</th>
              <th className="px-4 py-2 text-right">Agents using</th>
              <th className="px-4 py-2 text-right">Total queries</th>
              <th className="px-4 py-2">Last used</th>
            </tr>
          </thead>
          <tbody>
            {data?.map(r => (
              <tr key={r.full_name} className="border-t border-default/50">
                <td className="px-4 py-2 font-mono text-xs">{r.full_name}</td>
                <td className="px-4 py-2 text-right tabular-nums">{formatInt(r.space_count)}</td>
                <td className="px-4 py-2 text-right tabular-nums">{formatInt(r.query_count_total)}</td>
                <td className="px-4 py-2 text-muted">{formatDate(r.last_used)}</td>
              </tr>
            ))}
            {data && !data.length && (
              <tr><td colSpan={4} className="p-6 text-center text-muted">No lineage events found.</td></tr>
            )}
            {!data && <tr><td colSpan={4} className="p-6 text-center text-muted">Loading…</td></tr>}
          </tbody>
        </table>
      </Card>
    </>
  )
}
