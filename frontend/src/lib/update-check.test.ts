// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  CHECK_INTERVAL_MS,
  compareVersions,
  effectiveMode,
  fetchLatestRelease,
  isCheckDue,
  isUpdateAvailable,
  loadCheckResult,
  loadLocalConsent,
  nextResult,
  parseVersion,
  resetMemoryStorageForTests,
  saveCheckResult,
  saveLocalConsent,
} from './update-check'
import type { CheckResult } from './update-check'

const REPO = 'databricks-solutions/databricks-genie-workbench'
const RELEASE = { tag_name: '0.2.2', name: 'Genie Workbench v0.2.2', html_url: 'https://github.com/databricks-solutions/databricks-genie-workbench/releases/tag/0.2.2', published_at: '2026-11-12T10:00:00Z' }
const okResponse = (body: unknown) => ({ ok: true, status: 200, json: async () => body }) as unknown as Response

beforeEach(() => { localStorage.clear(); resetMemoryStorageForTests() })
afterEach(() => { vi.restoreAllMocks() })

describe('versions', () => {
  it('parses plain, v-prefixed, two-part and three-part versions', () => {
    expect(parseVersion('0.2.1')).toEqual([0, 2, 1])
    expect(parseVersion('v0.0.5')).toEqual([0, 0, 5])
    expect(parseVersion(' 0.2 ')).toEqual([0, 2])
    expect(parseVersion('0.3-rc1')).toBeNull()
    expect(parseVersion('unknown')).toBeNull()
    expect(parseVersion('')).toBeNull()
  })

  it('compares numerically with zero padding', () => {
    expect(compareVersions([0, 2], [0, 2, 0])).toBe(0)
    expect(compareVersions([0, 2, 1], [0, 2])).toBe(1)
    expect(compareVersions([0, 10], [0, 9])).toBe(1)
  })

  it('only claims an update when both versions parse and latest is newer', () => {
    expect(isUpdateAvailable('0.2.2', '0.2.1')).toBe(true)
    expect(isUpdateAvailable('0.2', '0.2.1')).toBe(false) // build from main ahead of the release
    expect(isUpdateAvailable('0.3-rc1', '0.2.1')).toBeNull()
    expect(isUpdateAvailable('0.2.2', 'unknown')).toBeNull()
  })
})

describe('cadence', () => {
  const at = (checked_at: number): CheckResult => ({ checked_at, ok: true, latest: null, last_success_at: checked_at })
  it('is due with no result, not due within 24h, due after 24h', () => {
    const now = 1_800_000_000_000
    expect(isCheckDue(null, now)).toBe(true)
    expect(isCheckDue(at(now - 60 * 60 * 1000), now)).toBe(false)
    expect(isCheckDue(at(now - CHECK_INTERVAL_MS), now)).toBe(true)
  })

  // Review Focus 5: a future timestamp must not suppress checks until that time.
  it('treats a checked_at in the future as due', () => {
    const now = 1_800_000_000_000
    expect(isCheckDue(at(now + 7 * CHECK_INTERVAL_MS), now)).toBe(true)
  })

  it('keeps the last good release across a failed attempt', () => {
    const ok = nextResult(null, 1000, { latest: { tag: '0.2.2', name: 'x', html_url: RELEASE.html_url, published_at: '' } })
    const failed = nextResult(ok, 2000, { error: 'GitHub returned 403' })
    expect(failed).toEqual({ checked_at: 2000, ok: false, latest: ok.latest, last_success_at: 1000, error: 'GitHub returned 403' })
  })
})

describe('fetchLatestRelease', () => {
  it('sends no referrer, no cookies, and no installed version', async () => {
    const fetchMock = vi.fn(async () => okResponse(RELEASE))
    const latest = await fetchLatestRelease(REPO, fetchMock as unknown as typeof fetch)
    expect(latest).toEqual({ tag: '0.2.2', name: 'Genie Workbench v0.2.2', html_url: RELEASE.html_url, published_at: RELEASE.published_at })
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit]
    expect(url).toBe(`https://api.github.com/repos/${REPO}/releases/latest`)
    expect(init).toMatchObject({ method: 'GET', mode: 'cors', credentials: 'omit', referrerPolicy: 'no-referrer', cache: 'no-store', headers: { Accept: 'application/vnd.github+json' } })
    expect(url).not.toContain('0.2.1')
  })

  it('rejects on a non-2xx status', async () => {
    const fetchMock = vi.fn(async () => ({ ok: false, status: 403, json: async () => ({}) }) as unknown as Response)
    await expect(fetchLatestRelease(REPO, fetchMock as unknown as typeof fetch)).rejects.toThrow('403')
  })

  // Review Focus 1: a captive portal answers 200 with HTML.
  it('rejects a 200 that is not JSON', async () => {
    const html = { ok: true, status: 200, json: async () => { throw new SyntaxError('Unexpected token <') } } as unknown as Response
    await expect(fetchLatestRelease(REPO, vi.fn(async () => html) as unknown as typeof fetch)).rejects.toThrow('non-JSON')
  })

  it('rejects a JSON body without a tag or with a non-GitHub link', async () => {
    await expect(fetchLatestRelease(REPO, vi.fn(async () => okResponse({ message: 'hi' })) as unknown as typeof fetch)).rejects.toThrow()
    await expect(fetchLatestRelease(REPO, vi.fn(async () => okResponse({ ...RELEASE, html_url: 'https://evil.example/x' })) as unknown as typeof fetch)).rejects.toThrow()
  })

  it('times out', async () => {
    const hanging = vi.fn((_url: string, init: RequestInit) => new Promise<Response>((_, reject) => {
      init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')))
    }))
    await expect(fetchLatestRelease(REPO, hanging as unknown as typeof fetch, 10)).rejects.toThrow()
  })
})

describe('storage', () => {
  it('round-trips results and consent', () => {
    const r: CheckResult = { checked_at: 5, ok: true, latest: null, last_success_at: 5 }
    saveCheckResult(r)
    expect(loadCheckResult()).toEqual(r)
    saveLocalConsent('off')
    expect(loadLocalConsent()).toBe('off')
  })

  // Review finding #5: a cache entry with an unexpected shape (tampering, or a
  // future release writing a different shape) must be ignored, not crash render.
  it('ignores stored results with an unexpected shape', () => {
    localStorage.setItem('gwb.updateCheck.result', JSON.stringify({ checked_at: 5, ok: true, latest: { tag: 7 }, last_success_at: 5 }))
    expect(loadCheckResult()).toBeNull()
    localStorage.setItem('gwb.updateCheck.result', JSON.stringify({ checked_at: 5, ok: true, latest: { tag: '0.2.2', name: 'x', html_url: 'https://evil.example/x', published_at: '' }, last_success_at: 5 }))
    expect(loadCheckResult()).toBeNull()
    localStorage.setItem('gwb.updateCheck.result', JSON.stringify({ checked_at: 5, ok: 'yes', latest: null, last_success_at: 5 }))
    expect(loadCheckResult()).toBeNull()
  })

  it('ignores corrupt stored results', () => {
    localStorage.setItem('gwb.updateCheck.result', '{not json')
    expect(loadCheckResult()).toBeNull()
  })

  // Review Focus 3: storage disabled.
  it('falls back to memory when localStorage throws', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('SecurityError') })
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('SecurityError') })
    saveLocalConsent('on')
    expect(loadLocalConsent()).toBe('on')
  })
})

describe('effectiveMode', () => {
  const base = { mode: 'ask' as const, source: 'none' as const, locked: false, persisted: true }
  it('uses the server answer when locked, stored, or errored', () => {
    expect(effectiveMode({ ...base, mode: 'off', source: 'install', locked: true }, 'on')).toBe('off')
    expect(effectiveMode({ ...base, mode: 'on', source: 'app' }, 'off')).toBe('on')
    expect(effectiveMode({ ...base, mode: 'off', source: 'error' }, 'on')).toBe('off')
  })
  it('uses this browser\'s answer only when the server cannot persist one', () => {
    expect(effectiveMode({ ...base, persisted: false }, 'on')).toBe('on')
    expect(effectiveMode({ ...base, persisted: true }, 'on')).toBe('ask')
    expect(effectiveMode({ ...base, persisted: false }, null)).toBe('ask')
  })
})
