import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import { VersionDetailPanel } from './version-detail-panel'
import { versionFixture } from './fixtures'
import type { VersionDetail } from '@/types/version-control'

const detail: VersionDetail = {
  ...versionFixture,
  origin: 'optimizer',
  optimizer_run_id: 'run/42',
  champion_id: 'champion-7',
  restored_from_version_id: 'original-1',
  snapshot: { data_sources: [{ table: 'sales' }] },
}

it('detail_panel_shows_origin_fingerprints_lineage_and_config', () => {
  const html = renderToStaticMarkup(<VersionDetailPanel detail={detail} onClose={vi.fn()} />)
  // 'data_sources' survives in the raw-JSON disclosure; 'Configuration'/'View raw JSON'
  // are the structured config surface that replaced the raw snapshot dump.
  for (const text of ['Optimizer', 'Fingerprints', versionFixture.observed_by, 'original-1', 'champion-7', 'Configuration', 'View raw JSON', 'data_sources', 'Canonicalizer']) {
    expect(html).toContain(text)
  }
})

it('detail_panel_omits_config_section_when_snapshot_absent', () => {
  const { snapshot, ...withoutSnapshot } = detail
  void snapshot
  const html = renderToStaticMarkup(<VersionDetailPanel detail={withoutSnapshot} onClose={vi.fn()} />)
  expect(html).not.toContain('Configuration')
  expect(html).not.toContain('View raw JSON')
})

it('detail_panel_omits_restore_button_without_handler', () => {
  const html = renderToStaticMarkup(<VersionDetailPanel detail={detail} onClose={vi.fn()} />)
  expect(html).not.toContain('Restore this version')
})

it('detail_panel_restore_disabled_with_explainer_when_flag_off', () => {
  const html = renderToStaticMarkup(
    <VersionDetailPanel detail={detail} onClose={vi.fn()} onRestore={vi.fn()} restoreEnabled={false} />,
  )
  expect(html).toContain('Restore this version')
  expect(html).toContain('Restore is not enabled on this deployment')
  expect(html).toContain('disabled')
})

it('detail_panel_restore_enabled_when_flag_on', () => {
  const html = renderToStaticMarkup(
    <VersionDetailPanel detail={detail} onClose={vi.fn()} onRestore={vi.fn()} restoreEnabled />,
  )
  expect(html).toContain('Restore this version')
  expect(html).toContain('Apply this version as the live configuration')
})

describe('VersionDetailPanel tags', () => {
  it('seeds the editor input from the existing tag label and shows a Remove tag control', () => {
    const html = renderToStaticMarkup(
      <VersionDetailPanel
        detail={detail}
        onClose={vi.fn()}
        tag={{ label: 'Golden', note: null }}
        onSetTag={vi.fn()}
        onRemoveTag={vi.fn()}
      />,
    )
    // Editor input is seeded with the existing label (getByDisplayValue equivalent in markup).
    expect(html).toContain('value="Golden"')
    expect(html).toContain('Remove tag')
    expect(html).toContain('Save')
  })

  it('shows the editor with an empty input and no Remove control when no tag exists', () => {
    const html = renderToStaticMarkup(
      <VersionDetailPanel detail={detail} onClose={vi.fn()} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
    )
    expect(html).toContain('Save')
    expect(html).not.toContain('Remove tag')
  })

  it('omits the tag editor entirely when onSetTag is not provided', () => {
    const html = renderToStaticMarkup(<VersionDetailPanel detail={detail} onClose={vi.fn()} />)
    expect(html).not.toContain('Remove tag')
    expect(html).not.toContain('Tag label')
  })
})
