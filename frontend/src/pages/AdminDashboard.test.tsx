// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  getAdminDashboard: vi.fn(async () => ({
    total_spaces: 3, scanned_spaces: 1, avg_score: 7, critical_count: 0, maturity_distribution: {},
  })),
  getLeaderboard: vi.fn(async () => ({ top: [], bottom: [] })),
  getAlerts: vi.fn(async () => []),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

import { AdminDashboard } from './AdminDashboard'

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
})

it('the agent count names its scope: the agents the caller can see', async () => {
  await act(async () => root.render(<AdminDashboard onSelectSpace={() => {}} />))
  const text = host.textContent ?? ''
  expect(text).toContain('Agents you can see')
  expect(text).not.toContain('Total Agents')
})

it('the page subtitle does not claim an org-wide scope', async () => {
  await act(async () => root.render(<AdminDashboard onSelectSpace={() => {}} />))
  const text = host.textContent ?? ''
  expect(text).toContain('Genie Agent health & observability')
  expect(text).not.toContain('Org-wide')
})
