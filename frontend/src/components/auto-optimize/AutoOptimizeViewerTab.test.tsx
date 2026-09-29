// @vitest-environment jsdom
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { renderToStaticMarkup } from 'react-dom/server'
import { expect, it, vi } from 'vitest'
import type { GSORunSummary } from '@/types'

const run: GSORunSummary = {
  run_id: '6f1c2a54-5b4e-4d7e-9c1f-2a3b4c5d6e7f',
  space_id: 's',
  status: 'CONVERGED',
  started_at: '2026-09-20T10:00:00Z',
  completed_at: null,
  best_accuracy: 82,
  best_iteration: null,
  convergence_reason: null,
  llm_model: 'databricks-claude-sonnet-4-6',
  triggered_by: 'ana@example.com',
  benchmark_policy: 'review_only',
}

const api = vi.hoisted(() => ({
  getAutoOptimizeHealth: vi.fn(async () => ({ configured: true, issues: [] })),
  getActiveRunForSpace: vi.fn(async () => ({ hasActiveRun: true, activeRunId: 'r-active', activeRunStatus: 'RUNNING' })),
  getAutoOptimizeRunsForSpace: vi.fn(async () => [run]),
  getCurrentVersion: vi.fn(), getAutoOptimizePermissions: vi.fn(), probeMvEntitlement: vi.fn(),
  getAutoOptimizeRun: vi.fn(), getAutoOptimizeStatus: vi.fn(),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

import { AutoOptimizeViewerTab, AutoOptimizeViewerView } from './AutoOptimizeViewerTab'

it('a viewer sees the run list and active status, and no editor request is sent', async () => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  const host = document.createElement('div')
  const root = createRoot(host)
  try {
    await act(async () => root.render(<AutoOptimizeViewerTab spaceId="s" />))
    const text = host.textContent ?? ''
    expect(text).toContain('Optimization in progress')
    expect(text).toContain('CONVERGED')
    expect(text).toContain('need Can Edit permission')
    for (const label of ['View Active Run', 'View Details', 'Revert Options', 'Remove From History', 'Start Optimization']) {
      expect(text).not.toContain(label)
    }
    expect(host.querySelectorAll('button')).toHaveLength(0)
    expect(api.getAutoOptimizeRunsForSpace).toHaveBeenCalledWith('s')
    for (const editorOnly of [api.getCurrentVersion, api.getAutoOptimizePermissions, api.probeMvEntitlement, api.getAutoOptimizeRun, api.getAutoOptimizeStatus]) {
      expect(editorOnly).not.toHaveBeenCalled()
    }
  } finally {
    await act(async () => root.unmount())
  }
})

it('says Optimize is not configured in the pure view', () => {
  const html = renderToStaticMarkup(<AutoOptimizeViewerView configured={false} activeRunId={null} runs={[]} loading={false} />)
  expect(html).toContain('Optimize is not configured')
})

it('says when there are no runs yet', () => {
  const html = renderToStaticMarkup(<AutoOptimizeViewerView configured activeRunId={null} runs={[]} loading={false} />)
  expect(html).toContain('No optimization runs yet.')
})

it('clears the previous space active-run card while the next space loads', async () => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  const host = document.createElement('div')
  const root = createRoot(host)
  try {
    await act(async () => root.render(<AutoOptimizeViewerTab spaceId="a" />))
    expect(host.textContent ?? '').toContain('Optimization in progress')

    api.getActiveRunForSpace.mockImplementation(
      () => new Promise(() => { /* pending for space b */ }),
    )
    api.getAutoOptimizeRunsForSpace.mockImplementation(
      () => new Promise(() => { /* pending for space b */ }),
    )

    await act(async () => root.render(<AutoOptimizeViewerTab spaceId="b" />))
    expect(host.textContent ?? '').not.toContain('Optimization in progress')
  } finally {
    await act(async () => root.unmount())
  }
})
