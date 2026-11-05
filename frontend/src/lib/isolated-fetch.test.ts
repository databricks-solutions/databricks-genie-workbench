// @vitest-environment jsdom
// Review finding #1: a cross-origin fetch from the app page sends `Origin: <app URL>`,
// which on Databricks Apps includes the app name and workspace ID. The release check
// must run from an opaque-origin frame so the browser sends `Origin: null` instead.
import { afterEach, expect, it } from 'vitest'

import { isolatedFetch } from './isolated-fetch'
import { fetchLatestRelease } from './update-check'

const URL_ = 'https://api.github.com/repos/databricks-solutions/databricks-genie-workbench/releases/latest'
const RELEASE = { tag_name: '0.2.2', name: 'v0.2.2', html_url: 'https://github.com/databricks-solutions/databricks-genie-workbench/releases/tag/0.2.2', published_at: '2026-11-12T10:00:00Z' }

afterEach(() => { document.body.innerHTML = '' })

function frame(): HTMLIFrameElement {
  const f = document.querySelector('iframe')
  if (!f) throw new Error('no iframe')
  return f
}
function tokenOf(f: HTMLIFrameElement): string {
  const m = /"token":"([0-9a-f]+)"/.exec(f.srcdoc)
  if (!m) throw new Error('no token in srcdoc')
  return m[1]
}
function reply(f: HTMLIFrameElement, data: unknown, source: Window | null = f.contentWindow) {
  window.dispatchEvent(new MessageEvent('message', { data, source }))
}

it('runs the request in a hidden frame sandboxed to an opaque origin, without referrer or cookies', async () => {
  const pending = isolatedFetch(URL_, { headers: { Accept: 'application/vnd.github+json' } })
  const f = frame()
  expect(f.getAttribute('sandbox')).toBe('allow-scripts') // no allow-same-origin: opaque origin, Origin: null
  expect(f.getAttribute('referrerpolicy')).toBe('no-referrer')
  expect(f.hidden).toBe(true)
  expect(f.srcdoc).toContain(JSON.stringify(URL_))
  expect(f.srcdoc).toContain('"credentials":"omit"')
  expect(f.srcdoc).toContain('"referrerPolicy":"no-referrer"')
  expect(f.srcdoc).toContain('"mode":"cors"')
  expect(f.srcdoc).toContain('"accept":"application/vnd.github+json"')
  reply(f, { token: tokenOf(f), ok: true, status: 200, body: JSON.stringify(RELEASE) })
  const res = await pending
  expect(res.ok).toBe(true)
  expect(res.status).toBe(200)
  expect(await res.json()).toEqual(RELEASE)
  expect(document.querySelector('iframe')).toBeNull()
})

it('ignores messages from other windows or with the wrong token', async () => {
  const pending = isolatedFetch(URL_)
  const f = frame()
  reply(f, { token: tokenOf(f), ok: true, status: 200, body: '{"tag_name":"9.9.9"}' }, window)
  reply(f, { token: 'not-the-token', ok: true, status: 200, body: '{"tag_name":"9.9.9"}' })
  expect(document.querySelector('iframe')).not.toBeNull()
  reply(f, { token: tokenOf(f), ok: false, status: 403, body: '{}' })
  const res = await pending
  expect(res.status).toBe(403)
  expect(res.ok).toBe(false)
})

it('turns an error reported by the frame into a rejection and removes the frame', async () => {
  const pending = isolatedFetch(URL_)
  const f = frame()
  reply(f, { token: tokenOf(f), error: 'Failed to fetch' })
  await expect(pending).rejects.toThrow('Failed to fetch')
  expect(document.querySelector('iframe')).toBeNull()
})

it('rejects and removes the frame when aborted', async () => {
  const controller = new AbortController()
  const pending = isolatedFetch(URL_, { signal: controller.signal })
  expect(document.querySelector('iframe')).not.toBeNull()
  controller.abort()
  await expect(pending).rejects.toThrow()
  expect(document.querySelector('iframe')).toBeNull()
})

it('is the default transport for fetchLatestRelease', async () => {
  const pending = fetchLatestRelease('databricks-solutions/databricks-genie-workbench')
  const f = frame()
  reply(f, { token: tokenOf(f), ok: true, status: 200, body: JSON.stringify(RELEASE) })
  expect((await pending).tag).toBe('0.2.2')
})
