// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { ApiError } from '@/lib/api'

const api = vi.hoisted(() => ({
  getSpaceAccess: vi.fn(),
  getSpaceDetail: vi.fn(async () => ({ space: {}, is_starred: false, scan_result: {
    score: 7, total: 12, maturity: 'Ready to Optimize', optimization_accuracy: null, checks: [], findings: ['a finding'],
    next_steps: ['a step'], warnings: [], warning_next_steps: [], scanned_at: '2026-09-20T10:00:00Z' } })),
  getActiveRunForSpace: vi.fn(async () => ({ hasActiveRun: true, activeRunId: 'r-1', activeRunStatus: 'RUNNING' })),
  getSpaceHistory: vi.fn(async () => ({ scans: [], optimization_events: [] })),
  // A full ScanResult: SpaceDetail reads findings.length on the rescan result.
  scanSpace: vi.fn(async () => ({
    space_id: 's1', score: 7, total: 12, maturity: 'Ready to Optimize', optimization_accuracy: null,
    checks: [], findings: ['a finding'], next_steps: ['a step'], warnings: [], warning_next_steps: [],
    scanned_at: '2026-09-20T10:00:00Z',
  })),
  toggleStar: vi.fn(async () => undefined),
  fetchSpace: vi.fn(async () => ({})),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

const stubs = vi.hoisted(() => ({ vcProps: [] as unknown[] }))
vi.mock('@/components/auto-optimize/AutoOptimizeTab', () => ({ AutoOptimizeTab: () => <div>editor-optimize</div> }))
vi.mock('@/components/auto-optimize/AutoOptimizeViewerTab', () => ({ AutoOptimizeViewerTab: () => <div>viewer-optimize</div> }))
vi.mock('@/components/model/SemanticModelTab', () => ({ SemanticModelTab: () => <div>editor-model</div> }))
vi.mock('@/components/version-control/SpaceVersionControlTab', () => ({
  SpaceVersionControlTab: (props: unknown) => { stubs.vcProps.push(props); return <div>vc-tab</div> },
}))
// MaturityCurve uses SVGPathElement.getTotalLength, which jsdom does not implement.
vi.mock('@/components/MaturityCurve', () => ({ MaturityCurve: () => <div>maturity-curve</div> }))

import { SpaceDetail } from './SpaceDetail'
import type { SpaceTab } from '@/lib/navigation'
import { NOT_FOUND_RETRY_MS, SPACE_NO_ACCESS_TITLE } from '@/lib/space-access'

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.clearAllMocks()
  stubs.vcProps.length = 0
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
  vi.useRealTimers()
})

async function mount(level: 'view' | 'edit' | 'manage' | Error, activeTab: SpaceTab = 'score', autoScan = true) {
  api.getSpaceAccess.mockImplementation(async (spaceId: string) => {
    if (level instanceof Error) throw level
    return { space_id: spaceId, level }
  })
  await act(async () => root.render(
    <SpaceDetail spaceId="s1" displayName="Sales" activeTab={activeTab} autoScan={autoScan} onBack={() => {}} onNavigate={() => {}} />,
  ))
}
const text = () => host.textContent ?? ''

it('a viewer sees the stored score and no request that needs Can Edit is sent', async () => {
  await mount('view')
  expect(api.getSpaceDetail).toHaveBeenCalledWith('s1')
  expect(api.fetchSpace).not.toHaveBeenCalled()
  expect(api.scanSpace).not.toHaveBeenCalled()
  expect(text()).toContain('7/12')
  expect(text()).toContain('You have Can View access')
  expect(text()).toContain('configuration needs Can Edit')
  for (const label of ['Re-scan', 'Run IQ Scan', 'Run Optimization', 'View Run', 'Reload']) expect(text()).not.toContain(label)
})

it('an editor keeps today’s Score tab, and the requested auto-scan runs once', async () => {
  await mount('edit')
  expect(api.fetchSpace).toHaveBeenCalledTimes(1)
  expect(api.scanSpace).toHaveBeenCalledTimes(1)
  expect(text()).toContain('Re-scan')
  expect(text()).toContain('View Run')
  expect(text()).not.toContain('You have Can View access')
})

it('a caller without access gets a permission state and no tabs or star', async () => {
  await mount(new ApiError('You need Can View permission on this Genie Agent.', 403, { code: 'space_access_denied' }))
  expect(text()).toContain('You can’t open this agent')
  expect(text()).not.toContain('History')
  expect(host.querySelector('svg.lucide-star')).toBeNull()
})

it('an unconfirmed answer is read-only and names its reason', async () => {
  await mount(new ApiError('Could not verify your access to this Genie Agent. Try again shortly.', 503, { code: 'space_access_unavailable' }))
  expect(text()).toContain('could not be confirmed')
  expect(text()).toContain('Could not verify your access')
  expect(api.scanSpace).not.toHaveBeenCalled()
  expect(api.fetchSpace).not.toHaveBeenCalled()
})

it('a viewer on Optimize gets the viewer tab, and on Model a locked section', async () => {
  await mount('view', 'optimize')
  expect(text()).toContain('viewer-optimize')
  expect(text()).not.toContain('editor-optimize')
  await mount('view', 'model')
  expect(text()).toContain('semantic model and metric-view suggestions need Can Edit')
  expect(text()).not.toContain('editor-model')
})

it('an editor on Optimize and Model gets today’s tabs', async () => {
  await mount('manage', 'optimize')
  expect(text()).toContain('editor-optimize')
  await mount('manage', 'model')
  expect(text()).toContain('editor-model')
})

it('the Versions tab receives the shared answer', async () => {
  await mount('manage', 'versions')
  expect(stubs.vcProps.at(-1)).toMatchObject({ spaceId: 's1', access: 'manage', accessReason: null })
})

it('before the answer, nothing write-capable renders', async () => {
  api.getSpaceAccess.mockImplementation(() => new Promise(() => {}))
  await act(async () => root.render(
    <SpaceDetail spaceId="s1" displayName="Sales" activeTab="score" autoScan onBack={() => {}} onNavigate={() => {}} />,
  ))
  expect(api.scanSpace).not.toHaveBeenCalled()
  expect(api.fetchSpace).not.toHaveBeenCalled()
  for (const label of ['Re-scan', 'Run IQ Scan', 'View Run', 'Reload']) expect(text()).not.toContain(label)
  expect(host.querySelector('svg.lucide-star')).toBeNull()
  // Agent Configuration is earned: neither the editor block nor the LockedSection.
  expect(text()).not.toContain('Agent Configuration')
  expect(text()).not.toContain('configuration needs Can Edit')
})

const notYetVisible = () => new ApiError('Genie Agent not found, or you cannot see it.', 404)

it('a just-created agent that 404s retries the access check once, then auto-scans', async () => {
  vi.useFakeTimers()
  // A default after the one 404, so no queued answer outlives the test.
  api.getSpaceAccess
    .mockImplementation(async (spaceId: string) => ({ space_id: spaceId, level: 'edit' }))
    .mockImplementationOnce(async () => { throw notYetVisible() })
  await act(async () => root.render(
    <SpaceDetail spaceId="s1" displayName="Sales" activeTab="score" autoScan onBack={() => {}} onNavigate={() => {}} />,
  ))
  expect(text()).not.toContain(SPACE_NO_ACCESS_TITLE)
  expect(api.scanSpace).not.toHaveBeenCalled()
  await act(async () => { await vi.advanceTimersByTimeAsync(NOT_FOUND_RETRY_MS) })
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(2)
  expect(api.scanSpace).toHaveBeenCalledTimes(1)
  expect(text()).toContain('Re-scan')
})

it('without autoScan, a 404 is the no-access state at once', async () => {
  vi.useFakeTimers()
  api.getSpaceAccess
    .mockImplementation(async (spaceId: string) => ({ space_id: spaceId, level: 'edit' }))
    .mockImplementationOnce(async () => { throw notYetVisible() })
  await act(async () => root.render(
    <SpaceDetail spaceId="s1" displayName="Sales" activeTab="score" autoScan={false} onBack={() => {}} onNavigate={() => {}} />,
  ))
  expect(text()).toContain(SPACE_NO_ACCESS_TITLE)
  await act(async () => { await vi.advanceTimersByTimeAsync(NOT_FOUND_RETRY_MS) })
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(1)
  expect(text()).toContain(SPACE_NO_ACCESS_TITLE)
})

it('a space switch clears the previous edit answer until the new one arrives', async () => {
  const pending = new Map<string, (level: 'view' | 'edit') => void>()
  api.getSpaceAccess.mockImplementation((spaceId: string) => new Promise(resolve => {
    pending.set(spaceId, level => resolve({ space_id: spaceId, level }))
  }))

  await act(async () => root.render(
    <SpaceDetail spaceId="a" displayName="Agent A" activeTab="score" autoScan onBack={() => {}} onNavigate={() => {}} />,
  ))
  await act(async () => pending.get('a')!('edit'))
  expect(api.fetchSpace).toHaveBeenCalledWith('a')
  expect(api.scanSpace).toHaveBeenCalledWith('a')
  const callsAfterA = {
    fetch: api.fetchSpace.mock.calls.length,
    scan: api.scanSpace.mock.calls.length,
  }

  await act(async () => root.render(
    <SpaceDetail spaceId="b" displayName="Agent B" activeTab="score" autoScan onBack={() => {}} onNavigate={() => {}} />,
  ))
  // B is still checking — previous edit must grant nothing.
  expect(api.fetchSpace.mock.calls.slice(callsAfterA.fetch)).not.toContainEqual(['b'])
  expect(api.scanSpace.mock.calls.slice(callsAfterA.scan)).not.toContainEqual(['b'])
  for (const label of ['Re-scan', 'Run IQ Scan', 'View Run', 'Reload']) expect(text()).not.toContain(label)
  expect(host.querySelector('svg.lucide-star')).toBeNull()
  expect(text()).not.toContain('Agent Configuration')

  await act(async () => pending.get('b')!('view'))
  expect(api.fetchSpace.mock.calls.slice(callsAfterA.fetch)).not.toContainEqual(['b'])
  expect(api.scanSpace.mock.calls.slice(callsAfterA.scan)).not.toContainEqual(['b'])
  expect(text()).toContain('You have Can View access')
})
