import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import { GENIE_AGENTS_SETUP_DOCS_URL, genieAgentsListUrl } from '@/lib/genie-agents'
import { GenieCodeSetupBannerView } from './GenieCodeSetupBanner'

const HOST = 'https://example.cloud.databricks.com'
const AGENTS_URL = `${HOST}/genie/rooms`

describe('genieAgentsListUrl', () => {
  it('points at the workspace Genie Agents list and trims trailing slashes', () => {
    expect(genieAgentsListUrl(`${HOST}/`)).toBe(AGENTS_URL)
    expect(genieAgentsListUrl(`${HOST}///`)).toBe(AGENTS_URL)
  })

  it('returns null when the host is unknown, so no link points at the app origin', () => {
    expect(genieAgentsListUrl(null)).toBeNull()
    expect(genieAgentsListUrl('')).toBeNull()
    expect(genieAgentsListUrl('   ')).toBeNull()
  })
})

describe.each([
  { variant: 'full (Create page)', compact: false, title: 'Set up Genie Agents with Genie Code' },
  { variant: 'compact (home page)', compact: true, title: 'Set up Genie Agents with Genie Code.' },
])('GenieCodeSetupBannerView $variant', ({ compact, title }) => {
  const withHost = renderToStaticMarkup(<GenieCodeSetupBannerView workspaceHost={HOST} compact={compact} />)
  const noHost = renderToStaticMarkup(<GenieCodeSetupBannerView workspaceHost={null} compact={compact} />)

  it('points users to Genie Code without mentioning deprecation', () => {
    expect(withHost).toContain(title)
    expect(withHost).not.toMatch(/deprecat/i)
  })

  it('links to Genie Agents and the docs in new tabs, announced to screen readers', () => {
    expect(withHost).toContain('Open Genie Agents<span')
    expect(withHost).toContain(`href="${AGENTS_URL}"`)
    expect(withHost).toContain(`href="${GENIE_AGENTS_SETUP_DOCS_URL}"`)
    expect(withHost.match(/target="_blank"/g)).toHaveLength(2)
    expect(withHost.match(/rel="noopener noreferrer"/g)).toHaveLength(2)
    expect(withHost.match(/\(opens in a new tab\)/g)).toHaveLength(2)
  })

  it('hides the Genie Agents link until the workspace host is known, keeping the docs link', () => {
    expect(noHost).not.toContain('Open Genie Agents<span')
    expect(noHost).not.toContain('/genie/rooms')
    expect(noHost).toContain(`href="${GENIE_AGENTS_SETUP_DOCS_URL}"`)
  })
})
