// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

import type { AppVersionInfo, UpdateCheckState } from '@/types'

const api = vi.hoisted(() => ({
  getCurrentUser: vi.fn(),
  getAppVersion: vi.fn(),
  setUpdateCheck: vi.fn(),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

import { resetMemoryStorageForTests } from '@/lib/update-check'
import { UpdateStatusProvider } from './UpdateStatusProvider'
import { useUpdateStatus } from './update-status-context'

const NOW = 1_800_000_000_000
const RELEASE = { tag_name: '0.2.2', name: 'v0.2.2', html_url: 'https://github.com/databricks-solutions/databricks-genie-workbench/releases/tag/0.2.2', published_at: '2026-11-12T10:00:00Z' }
const okResponse = (body: unknown) => ({ ok: true, status: 200, json: async () => body }) as unknown as Response

function info(update_check: Partial<UpdateCheckState> = {}, version = '0.2.1'): AppVersionInfo {
  return {
    version, commit: 'abc123', deployed_at: '2026-11-05T15:04:00Z', install_method: 'cli',
    releases_repo: 'databricks-solutions/databricks-genie-workbench', release_url: null,
    update_check: { mode: 'on', source: 'app', locked: false, persisted: true, ...update_check },
  }
}

function Probe() {
  const s = useUpdateStatus()
  return (
    <div>
      <pre data-testid="s">{JSON.stringify({ mode: s.mode, isAdmin: s.isAdmin, updateAvailable: s.updateAvailable, showUpdateBanner: s.showUpdateBanner, saveError: s.saveError, latest: s.result?.latest?.tag ?? null })}</pre>
      <button data-act="check" onClick={() => { void s.checkNow() }} />
      <button data-act="on" onClick={() => { void s.setConsent(true) }} />
      <button data-act="dismiss" onClick={() => s.dismissUpdate()} />
    </div>
  )
}

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  localStorage.clear(); resetMemoryStorageForTests()
  api.getCurrentUser.mockResolvedValue({ email: 'a@example.com', is_admin: true, groups: ['admins'], auth_source: 'obo_headers' })
  api.getAppVersion.mockResolvedValue(info())
  api.setUpdateCheck.mockReset()
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => { await act(async () => root.unmount()) })

async function flush() { for (let i = 0; i < 6; i++) await act(async () => {}) }
const state = () => JSON.parse(host.querySelector('[data-testid="s"]')!.textContent!)
const click = async (act_: string) => { await act(async () => { host.querySelector(`[data-act="${act_}"]`)!.dispatchEvent(new MouseEvent('click', { bubbles: true })) }) }

async function renderWith(fetchImpl: typeof fetch) {
  await act(async () => root.render(<UpdateStatusProvider fetchImpl={fetchImpl} now={() => NOW}><Probe /></UpdateStatusProvider>))
  await flush()
}

it('non-admins never call GitHub', async () => {
  api.getCurrentUser.mockResolvedValue({ email: 'u@example.com', is_admin: false, groups: [], auth_source: 'obo_headers' })
  const fetchMock = vi.fn(async () => okResponse(RELEASE))
  await renderWith(fetchMock as unknown as typeof fetch)
  expect(fetchMock).not.toHaveBeenCalled()
  expect(state()).toMatchObject({ isAdmin: false, updateAvailable: false })
})

it('an undecided setting asks and does not call GitHub', async () => {
  api.getAppVersion.mockResolvedValue(info({ mode: 'ask', source: 'none' }))
  const fetchMock = vi.fn(async () => okResponse(RELEASE))
  await renderWith(fetchMock as unknown as typeof fetch)
  expect(fetchMock).not.toHaveBeenCalled()
  expect(state().mode).toBe('ask')
})

it('admins see a newer release once and can dismiss the banner for that tag', async () => {
  const fetchMock = vi.fn(async () => okResponse(RELEASE))
  await renderWith(fetchMock as unknown as typeof fetch)
  expect(fetchMock).toHaveBeenCalledTimes(1)
  expect(state()).toMatchObject({ updateAvailable: true, showUpdateBanner: true, latest: '0.2.2' })
  await click('dismiss')
  expect(state()).toMatchObject({ updateAvailable: true, showUpdateBanner: false })
  expect(localStorage.getItem('gwb.updateCheck.dismissed')).toBe('0.2.2')
})

it('a fresh cached result is reused without a request', async () => {
  localStorage.setItem('gwb.updateCheck.result', JSON.stringify({ checked_at: NOW - 3_600_000, ok: true, latest: { tag: '0.2.2', name: 'x', html_url: RELEASE.html_url, published_at: '' }, last_success_at: NOW - 3_600_000 }))
  const fetchMock = vi.fn(async () => okResponse(RELEASE))
  await renderWith(fetchMock as unknown as typeof fetch)
  expect(fetchMock).not.toHaveBeenCalled()
  expect(state().updateAvailable).toBe(true)
})

it('a build ahead of the latest release is up to date', async () => {
  const fetchMock = vi.fn(async () => okResponse({ ...RELEASE, tag_name: '0.2', html_url: 'https://github.com/x/y/releases/tag/0.2' }))
  await renderWith(fetchMock as unknown as typeof fetch)
  expect(state()).toMatchObject({ updateAvailable: false, latest: '0.2' })
})

// Review Focus 4: overlapping triggers share one request.
it('keeps one GitHub request in flight', async () => {
  let resolve!: (r: Response) => void
  const fetchMock = vi.fn(() => new Promise<Response>((r) => { resolve = r }))
  await renderWith(fetchMock as unknown as typeof fetch)
  await click('check')
  await click('check')
  document.dispatchEvent(new Event('visibilitychange'))
  expect(fetchMock).toHaveBeenCalledTimes(1)
  await act(async () => resolve(okResponse(RELEASE)))
  await flush()
  expect(state().latest).toBe('0.2.2')
})

it('without Lakebase the answer is kept in this browser and the check starts', async () => {
  api.getAppVersion.mockResolvedValue(info({ mode: 'ask', source: 'none', persisted: false }))
  api.setUpdateCheck.mockResolvedValue({ mode: 'on', source: 'app', locked: false, persisted: false })
  const fetchMock = vi.fn(async () => okResponse(RELEASE))
  await renderWith(fetchMock as unknown as typeof fetch)
  await click('on')
  await flush()
  expect(api.setUpdateCheck).toHaveBeenCalledWith(true)
  expect(localStorage.getItem('gwb.updateCheck.consent')).toBe('on')
  expect(state().mode).toBe('on')
  expect(fetchMock).toHaveBeenCalledTimes(1)
})

it('a failed save shows the save error', async () => {
  api.getAppVersion.mockResolvedValue(info({ mode: 'ask', source: 'none' }))
  api.setUpdateCheck.mockRejectedValue(new Error('503'))
  await renderWith(vi.fn() as unknown as typeof fetch)
  await click('on')
  await flush()
  expect(state().saveError).toBe("Couldn't save, try again.")
})

it('a locked setting is never changed from the app', async () => {
  api.getAppVersion.mockResolvedValue(info({ mode: 'off', source: 'install', locked: true }))
  await renderWith(vi.fn() as unknown as typeof fetch)
  await click('on')
  expect(api.setUpdateCheck).not.toHaveBeenCalled()
})
