// @vitest-environment jsdom
import { renderToStaticMarkup } from 'react-dom/server'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { describe, expect, it, vi } from 'vitest'
import { VersionDetailPanel } from './version-detail-panel'
import { versionFixture } from './fixtures'
import type { VersionDetail } from '@/types/version-control'

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })

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
  // The editor is collapsed by default (space-saving). These helpers mount it and click the
  // collapsed affordance to reveal the full editor, mirroring the real interaction.
  const clickByAriaLabel = async (host: HTMLElement, name: string) => {
    const el = host.querySelector(`[aria-label="${name}"]`) as HTMLButtonElement | null
    expect(el, `expected control aria-label="${name}"`).toBeTruthy()
    await act(async () => { el!.click() })
  }
  const inputValue = (host: HTMLElement) =>
    (host.querySelector('#vc-tag-label') as HTMLInputElement | null)?.value
  const noteValue = (host: HTMLElement) =>
    (host.querySelector('[aria-label="Tag comment"]') as HTMLTextAreaElement | null)?.value

  it('collapses a tagged version to a compact summary (no editor chrome) by default', () => {
    const html = renderToStaticMarkup(
      <VersionDetailPanel
        detail={detail}
        onClose={vi.fn()}
        tag={{ label: 'Golden', note: 'keep this one' }}
        onSetTag={vi.fn()}
        onRemoveTag={vi.fn()}
      />,
    )
    // Summary shows the label + note preview but not the expanded editor controls.
    expect(html).toContain('Golden')
    expect(html).toContain('keep this one')
    expect(html).toContain('aria-label="Edit tag"')
    expect(html).not.toContain('Save')
    expect(html).not.toContain('Remove tag')
    expect(html).not.toContain('Tag comment')
  })

  it('collapses an untagged version to an "Add tag" affordance by default', () => {
    const html = renderToStaticMarkup(
      <VersionDetailPanel detail={detail} onClose={vi.fn()} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
    )
    expect(html).toContain('aria-label="Add tag"')
    expect(html).not.toContain('Save')
    expect(html).not.toContain('Remove tag')
  })

  it('expands to the editor seeded from the existing tag, with a Remove control', async () => {
    const host = document.createElement('div')
    const root = createRoot(host)
    try {
      await act(async () => root.render(
        <VersionDetailPanel detail={detail} onClose={vi.fn()} tag={{ label: 'Golden', note: null }} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
      ))
      await clickByAriaLabel(host, 'Edit tag')
      expect(inputValue(host)).toBe('Golden')
      expect(host.textContent).toContain('Save')
      expect(host.querySelector('[aria-label="Remove tag"]')).toBeTruthy()
    } finally {
      await act(async () => root.unmount())
    }
  })

  it('expands to an empty editor with no Remove control when no tag exists', async () => {
    const host = document.createElement('div')
    const root = createRoot(host)
    try {
      await act(async () => root.render(
        <VersionDetailPanel detail={detail} onClose={vi.fn()} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
      ))
      await clickByAriaLabel(host, 'Add tag')
      expect(inputValue(host)).toBe('')
      expect(host.querySelector('[aria-label="Remove tag"]')).toBeNull()
    } finally {
      await act(async () => root.unmount())
    }
  })

  it('omits the tag editor entirely when onSetTag is not provided', () => {
    const html = renderToStaticMarkup(<VersionDetailPanel detail={detail} onClose={vi.fn()} />)
    expect(html).not.toContain('Remove tag')
    expect(html).not.toContain('Tag comment')  // comment textarea aria-label absent
    expect(html).not.toContain('aria-label="Add tag"')
    expect(html).not.toContain('aria-label="Edit tag"')
  })

  it('renders preset chips and a comment textarea seeded from the existing note when expanded', async () => {
    const host = document.createElement('div')
    const root = createRoot(host)
    try {
      await act(async () => root.render(
        <VersionDetailPanel detail={detail} onClose={vi.fn()} tag={{ label: 'Golden', note: 'before rollout' }} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
      ))
      await clickByAriaLabel(host, 'Edit tag')
      for (const preset of ['Champion', 'Challenger', 'Baseline', 'v1']) {
        expect(host.textContent).toContain(preset)
      }
      expect(noteValue(host)).toBe('before rollout')
    } finally {
      await act(async () => root.unmount())
    }
  })

  it('shows a transient "Saved" cue after a successful save', async () => {
    const host = document.createElement('div')
    const root = createRoot(host)
    const onSetTag = vi.fn().mockResolvedValue(true)
    try {
      await act(async () => root.render(
        <VersionDetailPanel detail={detail} onClose={vi.fn()} tag={{ label: 'Golden', note: null }} onSetTag={onSetTag} onRemoveTag={vi.fn()} />,
      ))
      await clickByAriaLabel(host, 'Edit tag')
      const save = [...host.querySelectorAll('button')].find(b => b.textContent === 'Save') as HTMLButtonElement
      expect(save).toBeTruthy()
      // Click Save and let the resolved handler flush so the cue state commits.
      await act(async () => { save.click(); await new Promise(resolve => setTimeout(resolve, 0)) })
      expect(onSetTag).toHaveBeenCalledWith('Golden', null)
      expect(host.textContent).toContain('Saved')
    } finally {
      await act(async () => root.unmount())
    }
  })

  // Regression: switching versions must give the editor the CURRENT version's tag — a
  // tagged→untagged switch must clear the box, otherwise Save would write the prior
  // version's label onto the new version. The parent remounts per version_id via a React
  // `key`, so keying by version_id here reproduces that remount on switch.
  it('re-seeds the input to empty when the keyed panel switches to an untagged version', async () => {
    const tagged: VersionDetail = { ...detail, version_id: 'tagged-1' }
    const untagged: VersionDetail = { ...detail, version_id: 'untagged-2' }
    const host = document.createElement('div')
    const root = createRoot(host)
    try {
      await act(async () => root.render(
        <VersionDetailPanel key={tagged.version_id} detail={tagged} onClose={vi.fn()} tag={{ label: 'Golden', note: null }} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
      ))
      await clickByAriaLabel(host, 'Edit tag')
      expect(inputValue(host)).toBe('Golden')
      // Key changes with the version_id → React remounts → fresh instance seeds from the new
      // (absent) tag; expanding its editor shows an empty label.
      await act(async () => root.render(
        <VersionDetailPanel key={untagged.version_id} detail={untagged} onClose={vi.fn()} onSetTag={vi.fn()} onRemoveTag={vi.fn()} />,
      ))
      await clickByAriaLabel(host, 'Add tag')
      expect(inputValue(host)).toBe('')
    } finally {
      await act(async () => root.unmount())
    }
  })
})
