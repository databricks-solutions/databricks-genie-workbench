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

import { readFileSync } from 'node:fs'

import { COPY } from '@/components/update-check/copy'
import { EMPTY_STATUS, UpdateStatusContext } from '@/components/update-check/update-status-context'
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

it('the header shows the installed app version', async () => {
  const status = { ...EMPTY_STATUS, info: { version: '0.2.1', commit: null, deployed_at: null, install_method: null, releases_repo: 'r', release_url: null, update_check: { mode: 'off' as const, source: 'none' as const, locked: false, persisted: false } } }
  await act(async () => root.render(<UpdateStatusContext.Provider value={status}><AdminDashboard onSelectSpace={() => {}} /></UpdateStatusContext.Provider>))
  expect(host.textContent ?? '').toContain('v0.2.1')
})

// Spec 12: the release check lives on the Admin pages, for any user.
it('the Admin view asks the one-time question and opens the release check', async () => {
  const enterAdminPage = vi.fn(() => () => {})
  const status = {
    ...EMPTY_STATUS, mode: 'ask' as const, enterAdminPage,
    info: { version: '0.2.1', commit: null, deployed_at: null, install_method: null, releases_repo: 'r', release_url: null, update_check: { mode: 'ask' as const, source: 'none' as const, locked: false, persisted: true } },
  }
  await act(async () => root.render(<UpdateStatusContext.Provider value={status}><AdminDashboard onSelectSpace={() => {}} /></UpdateStatusContext.Provider>))
  expect(host.textContent ?? '').toContain(COPY.consentText)
  expect(enterAdminPage).toHaveBeenCalledTimes(1)
})

it('pages outside the Admin view never show the release-check question', () => {
  const app = readFileSync('src/App.tsx', 'utf8')
  expect(app).not.toContain('UpdateConsentBanner')
})
