import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { SpaceVersionControlTab } from './SpaceVersionControlTab'
import { AccessNotice, NoAccessState, VersionControlHeader } from './access-chrome'
import { describeCaptureError, describeObservation } from './capture-notice'
import { originMeta } from './version-format'
import { VersionControlError } from '@/lib/version-control-api'
import type { ObservationResult, VersionSummary } from '@/types/version-control'

const summary = { version_id: 'v1' } as VersionSummary
const result = (over: Partial<ObservationResult>): ObservationResult =>
  ({ status: {} as ObservationResult['status'], captured_version: null, busy: false, ...over })
const vcError = (status: number) =>
  new VersionControlError(status, { code: 'x', message: 'server said no', retryable: false, stale: false })

describe('capture notices', () => {
  it('reports a new version when a previously-unseen version id is captured', () => {
    expect(describeObservation(result({ captured_version: summary })))
      .toEqual({ tone: 'success', message: expect.stringContaining('Saved a new version') })
  })

  it('reports no changes when nothing was captured', () => {
    expect(describeObservation(result({})))
      .toEqual({ tone: 'info', message: expect.stringContaining('No changes') })
  })

  // Regression: the observer's dedup branch returns the existing HEAD as captured_version on
  // an unchanged open. If that id was already in our loaded history, nothing new was saved —
  // it must read as "no changes", not "Saved a new version".
  it('reports no changes when the returned version id is already known (unchanged head)', () => {
    expect(describeObservation(result({ captured_version: summary }), new Set(['v1'])))
      .toEqual({ tone: 'info', message: expect.stringContaining('No changes') })
  })

  it('reports a busy capture', () => {
    expect(describeObservation(result({ busy: true })))
      .toEqual({ tone: 'info', message: expect.stringContaining('already in progress') })
  })

  it('maps 503 to a not-enabled error', () => {
    expect(describeCaptureError(vcError(503)))
      .toEqual({ tone: 'error', message: expect.stringContaining('not enabled') })
  })

  it('maps 403 to a permission error', () => {
    expect(describeCaptureError(vcError(403)))
      .toEqual({ tone: 'error', message: expect.stringContaining('access') })
  })

  it('surfaces the access refusal the server named', () => {
    const err = new VersionControlError(403, {
      code: 'space_access_denied', message: 'You need Can Edit permission on this Genie Agent.',
      retryable: false, stale: false })
    expect(describeCaptureError(err))
      .toEqual({ tone: 'error', message: 'You need Can Edit permission on this Genie Agent.' })
  })

  it('surfaces the server message for other VC errors', () => {
    expect(describeCaptureError(vcError(409)))
      .toEqual({ tone: 'error', message: 'server said no' })
  })

  it('falls back to a generic error for non-VC failures', () => {
    expect(describeCaptureError(new Error('boom')))
      .toEqual({ tone: 'error', message: expect.stringContaining('failed') })
  })
})

it('relabels the workbench origin as Auto-captured', () => {
  expect(originMeta('workbench').label).toBe('Auto-captured')
})

it('earns no write affordance before the access route answers', () => {
  const html = renderToStaticMarkup(<SpaceVersionControlTab spaceId="space-1" />)
  expect(html).toContain('Checking access')
  expect(html).not.toContain('Capture current state')
  expect(html).not.toContain('Check the live space for changes')
  expect(html).not.toContain('No versions captured yet')
})

it('shows an editor the capture controls and the auto-capture explanation', () => {
  const html = renderToStaticMarkup(<VersionControlHeader access="edit" onCapture={() => {}} />)
  expect(html).toContain('auto-captured')
  expect(html).toContain('Capture current state')
  expect(html).toContain('Check the live space for changes')
  expect(html).not.toContain('Refresh history')
})

it('hides every write affordance from a viewer and says what Can Edit unlocks', () => {
  const header = renderToStaticMarkup(<VersionControlHeader access="view" onCapture={() => {}} />)
  expect(header).not.toContain('Capture current state')
  expect(header).not.toContain('Check the live space for changes')
  expect(header).not.toContain('whenever you open this tab')
  expect(renderToStaticMarkup(<AccessNotice access="view" />)).toContain('Can Edit')
})

it('renders a permission state with the reason Genie gave instead of a blank', () => {
  const html = renderToStaticMarkup(<NoAccessState reason="You need the aclPath entitlement: /sqlanalytics" />)
  expect(html).toContain('version history')
  expect(html).toContain('/sqlanalytics')
  expect(renderToStaticMarkup(<NoAccessState reason={null} />)).toContain('Can View')
})
