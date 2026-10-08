// @vitest-environment jsdom
import { act, useState } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import type { MvProposal } from '@/types'
import type { SpaceTab } from '@/lib/navigation'

const api = vi.hoisted(() => ({
  getSpaceAccess: vi.fn(async (spaceId: string) => ({ space_id: spaceId, level: 'edit' })),
  getSpaceDetail: vi.fn(async () => ({ space: {}, is_starred: false, scan_result: {
    score: 7, total: 12, maturity: 'Ready to Optimize', optimization_accuracy: null, checks: [], findings: ['a finding'],
    next_steps: ['a step'], warnings: [], warning_next_steps: [], scanned_at: '2026-09-20T10:00:00Z' } })),
  getActiveRunForSpace: vi.fn(async () => ({ hasActiveRun: false, activeRunId: null, activeRunStatus: null })),
  getSpaceHistory: vi.fn(async () => ({ scans: [], optimization_events: [] })),
  scanSpace: vi.fn(),
  toggleStar: vi.fn(async () => undefined),
  fetchSpace: vi.fn(async () => ({})),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

type OptProps = {
  initialMvPrefill?: { mode: string; suggestionId?: string | null } | null
  onMvPrefillConsumed?: () => void
  onRunChange?: (runId?: string) => void
}
type ModelProps = { onReviewCreate?: (p: MvProposal | null) => void }
const stubs = vi.hoisted(() => ({ optProps: [] as OptProps[], optMounts: 0, modelProps: [] as ModelProps[] }))
vi.mock('@/components/auto-optimize/AutoOptimizeTab', async () => {
  const { useEffect } = await import('react')
  return {
    AutoOptimizeTab: (props: OptProps) => {
      stubs.optProps.push(props)
      useEffect(() => { stubs.optMounts += 1 }, [])
      return <div>editor-optimize</div>
    },
  }
})
vi.mock('@/components/model/SemanticModelTab', () => ({
  SemanticModelTab: (props: ModelProps) => { stubs.modelProps.push(props); return <div>editor-model</div> },
}))
vi.mock('@/components/auto-optimize/AutoOptimizeViewerTab', () => ({ AutoOptimizeViewerTab: () => <div>viewer-optimize</div> }))
vi.mock('@/components/version-control/SpaceVersionControlTab', () => ({ SpaceVersionControlTab: () => <div>vc-tab</div> }))
vi.mock('@/components/MaturityCurve', () => ({ MaturityCurve: () => <div>maturity-curve</div> }))

import { SpaceDetail } from './SpaceDetail'

function Harness() {
  const [tab, setTab] = useState<SpaceTab>('model')
  const [runId, setRunId] = useState<string | undefined>(undefined)
  return (
    <SpaceDetail
      spaceId="s1" displayName="Sales" activeTab={tab} runId={runId} autoScan={false}
      onBack={() => {}} onNavigate={(t, r) => { setTab(t); setRunId(r) }}
    />
  )
}

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.clearAllMocks()
  stubs.optProps.length = 0
  stubs.modelProps.length = 0
  stubs.optMounts = 0
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => { await act(async () => root.unmount()) })

const review = (suggestion_id: string) => stubs.modelProps.at(-1)!.onReviewCreate!({ suggestion_id } as MvProposal)
const lastOpt = () => stubs.optProps.at(-1)!

it('consuming the prefill at start keeps the tab mounted, and the run view opens without it', async () => {
  await act(async () => root.render(<Harness />))
  await act(async () => review('sug_a'))
  expect(stubs.optMounts).toBe(1)
  expect(lastOpt().initialMvPrefill).toEqual({ mode: 'create_and_attach', suggestionId: 'sug_a' })

  await act(async () => lastOpt().onMvPrefillConsumed!())
  expect(stubs.optMounts).toBe(1)
  expect(lastOpt().initialMvPrefill).toBeNull()

  await act(async () => lastOpt().onRunChange!('run-1'))
  expect(stubs.optMounts).toBe(2)
  expect(lastOpt().initialMvPrefill).toBeNull()
})

it('a second review deep link remounts the tab with the new suggestion', async () => {
  await act(async () => root.render(<Harness />))
  await act(async () => review('sug_a'))
  await act(async () => lastOpt().onMvPrefillConsumed!())
  await act(async () => review('sug_b'))
  expect(stubs.optMounts).toBe(2)
  expect(lastOpt().initialMvPrefill).toEqual({ mode: 'create_and_attach', suggestionId: 'sug_b' })
})
