import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import { GENIE_AGENTS_SETUP_DOCS_URL, genieAgentsListUrl } from '@/lib/genie-agents'
import { GenieCodeSetupBannerView } from './GenieCodeSetupBanner'

describe('genieAgentsListUrl', () => {
  it('points at the workspace Genie Agents list and trims trailing slashes', () => {
    expect(genieAgentsListUrl('https://example.cloud.databricks.com/')).toBe(
      'https://example.cloud.databricks.com/genie/rooms',
    )
  })

  it('falls back to a relative link when the host is unknown', () => {
    expect(genieAgentsListUrl(null)).toBe('/genie/rooms')
  })
})

describe('GenieCodeSetupBannerView', () => {
  const html = renderToStaticMarkup(
    <GenieCodeSetupBannerView workspaceHost="https://example.cloud.databricks.com" />,
  )

  it('points users to Genie Code for setting up agents', () => {
    expect(html).toContain('Set up Genie Agents with Genie Code')
    expect(html).not.toMatch(/deprecat/i)
  })

  it('links to Genie Agents in the workspace and to the Genie Code set-up docs in new tabs', () => {
    expect(html).toContain('href="https://example.cloud.databricks.com/genie/rooms"')
    expect(html).toContain(`href="${GENIE_AGENTS_SETUP_DOCS_URL}"`)
    expect(html.match(/target="_blank"/g)).toHaveLength(2)
    expect(html.match(/rel="noopener noreferrer"/g)).toHaveLength(2)
  })
})
