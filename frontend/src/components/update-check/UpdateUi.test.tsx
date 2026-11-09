// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

import type { AppVersionInfo, UpdateCheckState } from '@/types'
import { AdminNavDot } from './AdminNavDot'
import { AppVersionCard } from './AppVersionCard'
import { COPY, DOCS_UPGRADE_URL } from './copy'
import { UpdateAvailableBanner } from './UpdateAvailableBanner'
import { UpdateConsentBanner } from './UpdateConsentBanner'
import { EMPTY_STATUS, UpdateStatusContext } from './update-status-context'
import type { UpdateStatus } from './update-status-context'

const NOW = 1_800_000_000_000
const HOUR = 3_600_000
const LATEST = { tag: '0.2.2', name: 'v0.2.2', html_url: 'https://github.com/databricks-solutions/databricks-genie-workbench/releases/tag/0.2.2', published_at: '2026-11-12T10:00:00Z' }

function info(uc: Partial<UpdateCheckState> = {}, extra: Partial<AppVersionInfo> = {}): AppVersionInfo {
  return {
    version: '0.2.1', commit: '99e7783ab12c', deployed_at: '2026-11-05T15:04:00Z', install_method: 'cli',
    releases_repo: 'databricks-solutions/databricks-genie-workbench',
    release_url: 'https://github.com/databricks-solutions/databricks-genie-workbench/releases/tag/0.2.1',
    update_check: { mode: 'on', source: 'app', locked: false, persisted: true, ...uc }, ...extra,
  }
}

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => { await act(async () => root.unmount()) })

async function show(ui: React.ReactNode, status: Partial<UpdateStatus>) {
  await act(async () => root.render(<UpdateStatusContext.Provider value={{ ...EMPTY_STATUS, ...status }}>{ui}</UpdateStatusContext.Provider>))
}
const text = () => host.textContent ?? ''
const button = (label: string) => [...host.querySelectorAll('button')].find((b) => b.textContent === label)

it('consent banner asks any user once when undecided, with Turn on as the main choice', async () => {
  const setConsent = vi.fn(async () => {})
  await show(<UpdateConsentBanner />, { mode: 'ask', info: info({ mode: 'ask', source: 'none' }), setConsent })
  expect(text()).toContain(COPY.consentText)
  await act(async () => { button('Turn on')!.click() })
  expect(setConsent).toHaveBeenCalledWith(true)
  await act(async () => { button('No thanks')!.click() })
  expect(setConsent).toHaveBeenCalledWith(false)
})

it('consent banner is hidden once decided', async () => {
  await show(<UpdateConsentBanner />, { mode: 'on', info: info() })
  expect(text()).toBe('')
  await show(<UpdateConsentBanner />, { mode: 'off', info: info({ mode: 'off' }) })
  expect(text()).toBe('')
})

it('update banner names both versions, links release notes and upgrade steps, and dismisses', async () => {
  const dismissUpdate = vi.fn()
  await show(<UpdateAvailableBanner />, { info: info(), showUpdateBanner: true, updateAvailable: true, result: { checked_at: NOW, ok: true, latest: LATEST, last_success_at: NOW }, dismissUpdate })
  expect(text()).toContain("Genie Workbench 0.2.2 is available (you're on 0.2.1).")
  const hrefs = [...host.querySelectorAll('a')].map((a) => a.getAttribute('href'))
  expect(hrefs).toEqual([LATEST.html_url, DOCS_UPGRADE_URL])
  await act(async () => { button('Dismiss')!.click() })
  expect(dismissUpdate).toHaveBeenCalled()
})

it('nav dot shows only when an update is available', async () => {
  await show(<AdminNavDot />, { updateAvailable: true })
  expect(host.querySelector('[aria-label="Update available"]')).not.toBeNull()
  await show(<AdminNavDot />, { updateAvailable: false })
  expect(host.querySelector('[aria-label="Update available"]')).toBeNull()
})

it('version card shows build details, the status line, and the toggle to any user', async () => {
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', info: info({}, { commit: null }), result: { checked_at: NOW - 2 * HOUR, ok: true, latest: { ...LATEST, tag: '0.2.1' }, last_success_at: NOW - 2 * HOUR } })
  expect(text()).toContain('0.2.1')
  expect(text()).toContain(COPY.notRecorded)
  expect(text()).toContain('CLI install')
  expect(text()).toContain(COPY.upToDate)
  expect(button('Check now')).toBeDefined()
  expect(button('Turn off')).toBeDefined()
})

it('version card reports up to date, update available, and blocked networks', async () => {
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', info: info(), result: { checked_at: NOW - 2 * HOUR, ok: true, latest: { ...LATEST, tag: '0.2.1' }, last_success_at: NOW - 2 * HOUR } })
  expect(text()).toContain(`${COPY.upToDate} · checked 2h ago`)
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', updateAvailable: true, info: info(), result: { checked_at: NOW - 2 * HOUR, ok: true, latest: LATEST, last_success_at: NOW - 2 * HOUR } })
  expect(text()).toMatch(/Latest release: 0\.2\.2, published .+ · checked 2h ago/)
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', info: info(), result: { checked_at: NOW, ok: false, latest: null, last_success_at: NOW - 30 * HOUR, error: 'GitHub returned 403' } })
  expect(text()).toContain(`${COPY.cantReach} (last successful check: 30h ago). ${COPY.networksBlock}`)
})

it('version card shows the lock hint instead of a toggle when set at install', async () => {
  await show(<AppVersionCard now={() => NOW} />, { mode: 'off', locked: true, info: info({ mode: 'off', source: 'install', locked: true }) })
  expect(text()).toContain(COPY.checkOff)
  expect(text()).toContain(COPY.lockedHint)
  expect(button('Turn on')).toBeUndefined()
})

it('version card offers a toggle when not locked and reports setting errors', async () => {
  const setConsent = vi.fn(async () => {})
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', info: info(), setConsent })
  await act(async () => { button('Turn off')!.click() })
  expect(setConsent).toHaveBeenCalledWith(false)
  await show(<AppVersionCard now={() => NOW} />, { mode: 'off', info: info({ mode: 'off', source: 'error' }) })
  expect(text()).toContain(COPY.settingError)
})

it('an unknown installed version never claims an update', async () => {
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', info: info({}, { version: 'unknown', release_url: null }), result: { checked_at: NOW, ok: true, latest: LATEST, last_success_at: NOW } })
  expect(text()).toContain('Latest release: 0.2.2')
  expect(text()).not.toContain(COPY.upToDate)
})

// Final review (spec 12): anyone can answer, so say whose answer it is.
it('the question and the toggle say whether the answer applies to everyone or this browser', async () => {
  expect(COPY.appliesToEveryone).toBe('Your answer applies to everyone using this app.')
  expect(COPY.appliesToThisBrowser).toBe('Your answer applies only to this browser.')
  await show(<UpdateConsentBanner />, { mode: 'ask', info: info({ mode: 'ask', source: 'none', persisted: true }) })
  expect(text()).toContain(COPY.appliesToEveryone)
  await show(<UpdateConsentBanner />, { mode: 'ask', info: info({ mode: 'ask', source: 'none', persisted: false }) })
  expect(text()).toContain(COPY.appliesToThisBrowser)
  await show(<AppVersionCard now={() => NOW} />, { mode: 'on', info: info() })
  expect(text()).toContain(COPY.appliesToEveryone)
  await show(<AppVersionCard now={() => NOW} />, { mode: 'off', locked: true, info: info({ mode: 'off', source: 'install', locked: true }) })
  expect(text()).not.toContain(COPY.appliesToEveryone)
})
