// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import type { MvRerunPrefill } from '@/components/auto-optimize/OptimizationConfig'

const api = vi.hoisted(() => ({
  getAutoOptimizeHealth: vi.fn(async () => ({ configured: true, issues: [] })),
  getActiveRunForSpace: vi.fn(async () => ({ hasActiveRun: false, activeRunId: null, activeRunStatus: null })),
  getAutoOptimizePermissions: vi.fn(async () => null),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

type ConfigProps = { initialMv?: MvRerunPrefill | null; onStarted: (runId: string) => void }
type HistoryProps = { onSelectRun: (runId: string) => void }
const stubs = vi.hoisted(() => ({ configProps: [] as ConfigProps[], historyProps: [] as HistoryProps[] }))
vi.mock('@/components/auto-optimize/OptimizationConfig', () => ({
  OptimizationConfig: (props: ConfigProps) => { stubs.configProps.push(props); return <div>config</div> },
}))
vi.mock('@/components/auto-optimize/RunHistoryTable', () => ({
  RunHistoryTable: (props: HistoryProps) => { stubs.historyProps.push(props); return <div>history</div> },
}))
vi.mock('@/components/auto-optimize/OptimizationLoadingStepper', () => ({ OptimizationLoadingStepper: () => null }))
vi.mock('@/components/auto-optimize/RunDetailView', () => ({ RunDetailView: () => <div>run-detail</div> }))

import { AutoOptimizeTab } from './AutoOptimizeTab'

const prefill: MvRerunPrefill = { mode: 'create_and_attach', suggestionId: 'sug_a' }
let consumed: ReturnType<typeof vi.fn>
let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.clearAllMocks()
  stubs.configProps.length = 0
  stubs.historyProps.length = 0
  consumed = vi.fn()
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => { await act(async () => root.unmount()) })

async function mount() {
  await act(async () => root.render(
    <AutoOptimizeTab spaceId="s1" initialMvPrefill={prefill} onMvPrefillConsumed={consumed} />,
  ))
  expect(stubs.configProps.at(-1)!.initialMv).toEqual(prefill)
}

it('merely rendering does not consume the prefill', async () => {
  await mount()
  expect(stubs.historyProps.length).toBeGreaterThan(0)
  // The active-run read ran, so the zero calls below are not just an unfinished mount.
  expect(api.getActiveRunForSpace).toHaveBeenCalled()
  expect(consumed).not.toHaveBeenCalled()
})

it('starting a run consumes the prefill, and the configure view drops it', async () => {
  await mount()
  await act(async () => stubs.configProps.at(-1)!.onStarted('run-1'))
  expect(consumed).toHaveBeenCalledTimes(1)
  expect(stubs.configProps.at(-1)!.initialMv).toBeNull()
})

it('opening a run from history consumes the prefill', async () => {
  await mount()
  await act(async () => stubs.historyProps.at(-1)!.onSelectRun('run-1'))
  expect(consumed).toHaveBeenCalledTimes(1)
  expect(host.textContent).toContain('run-detail')
})
