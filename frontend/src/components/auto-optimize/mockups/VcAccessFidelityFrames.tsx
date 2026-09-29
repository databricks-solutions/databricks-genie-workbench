/**
 * PR #332 M1b fidelity frames — the REAL Version Control access chrome (header, notices,
 * permission state, rail) in each access state the tab can reach (VC-D-authz1).
 */
import { AccessNotice, DetailPlaceholder, NoAccessState, VersionControlHeader, VersionPanes } from "@/components/version-control/access-chrome"
import { EDITOR_DETAIL_HINT, VIEWER_DETAIL_HINT, VIEWER_EMPTY_HINT } from "@/components/version-control/access-state"
import { versionFixture } from "@/components/version-control/fixtures"
import { History } from "@/components/version-control/history"

const page = { items: [versionFixture, { ...versionFixture, version_id: "external-2" }], next_cursor: null }
const emptyPage = { items: [], next_cursor: null }
const noop = () => {}

export function VcCheckingFrame() {
  return (
    <div className="space-y-3">
      <VersionControlHeader access="checking" onCapture={noop} />
      <VersionPanes rail={<History page={emptyPage} loading onNext={noop} />} detail={<DetailPlaceholder>Checking your access…</DetailPlaceholder>} />
    </div>
  )
}

export function VcEditorFrame() {
  return (
    <div className="space-y-3">
      <VersionControlHeader access="edit" onCapture={noop} />
      <VersionPanes
        rail={<History page={page} onNext={noop} onSelect={noop} compareIds={[]} onToggleCompare={noop} />}
        detail={<DetailPlaceholder>{EDITOR_DETAIL_HINT}</DetailPlaceholder>}
      />
    </div>
  )
}

export function VcViewerFrame() {
  return (
    <div className="space-y-3">
      <VersionControlHeader access="view" onCapture={noop} />
      <AccessNotice access="view" />
      <VersionPanes rail={<History page={page} onNext={noop} emptyHint={VIEWER_EMPTY_HINT} />} detail={<DetailPlaceholder>{VIEWER_DETAIL_HINT}</DetailPlaceholder>} />
    </div>
  )
}

export function VcViewerEmptyFrame() {
  return (
    <div className="space-y-3">
      <VersionControlHeader access="view" onCapture={noop} />
      <AccessNotice access="view" />
      <VersionPanes rail={<History page={emptyPage} onNext={noop} emptyHint={VIEWER_EMPTY_HINT} />} detail={<DetailPlaceholder>{VIEWER_DETAIL_HINT}</DetailPlaceholder>} />
    </div>
  )
}

export function VcNoAccessFrame() {
  return (
    <div className="space-y-3">
      <VersionControlHeader access="none" onCapture={noop} />
      <NoAccessState reason="You need Can View permission on this Genie Agent." />
    </div>
  )
}

export function VcEntitlementFrame() {
  return (
    <div className="space-y-3">
      <VersionControlHeader access="none" onCapture={noop} />
      <NoAccessState reason="Your account is missing a workspace entitlement Genie needs, such as Databricks SQL access. Ask a workspace admin to grant it." />
    </div>
  )
}
