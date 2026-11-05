import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { isUpdateAvailable } from "@/lib/update-check"

import { COPY, formatAgo, formatDay, installMethodLabel } from "./copy"
import { useUpdateStatus } from "./update-status-context"
import type { UpdateStatus } from "./update-status-context"

function statusLine(s: UpdateStatus, now: number): string {
  if (!s.info) return ""
  if (s.info.update_check.source === "error") return COPY.settingError
  if (s.mode === "off") return COPY.checkOff
  if (s.mode === "ask") return COPY.askPending
  const r = s.result
  if (!r) return s.checking ? COPY.checking : ""
  if (!r.ok) {
    const last = r.last_success_at ? ` (last successful check: ${formatAgo(r.last_success_at, now)})` : ""
    return `${COPY.cantReach}${last}. ${COPY.networksBlock}`
  }
  if (!r.latest) return ""
  if (isUpdateAvailable(r.latest.tag, s.info.version) === null) return `Latest release: ${r.latest.tag}`
  if (s.updateAvailable) {
    const published = r.latest.published_at ? `, published ${formatDay(r.latest.published_at)}` : ""
    return `Latest release: ${r.latest.tag}${published} · checked ${formatAgo(r.checked_at, now)}`
  }
  return `${COPY.upToDate} · checked ${formatAgo(r.checked_at, now)}`
}

export function AppVersionCard({ now = Date.now }: { now?: () => number }) {
  const s = useUpdateStatus()
  if (!s.info) return null
  const { info } = s
  const canToggle = s.isAdmin && !s.locked && info.update_check.source !== "error"
  return (
    <Card className="p-4">
      <h2 className="mb-3 text-sm font-medium uppercase text-muted">App version</h2>
      <div className="grid gap-2 text-sm sm:grid-cols-2">
        <Row k="Version"><span className="font-mono text-xs">{info.version}</span></Row>
        <Row k="Commit"><span className="font-mono text-xs">{info.commit ?? COPY.notRecorded}</span></Row>
        <Row k="Install method"><span>{installMethodLabel(info.install_method)}</span></Row>
        <Row k="Deployed"><span>{info.deployed_at ? formatDay(info.deployed_at) : COPY.notRecorded}</span></Row>
      </div>
      {info.release_url && (
        <a href={info.release_url} target="_blank" rel="noopener noreferrer" className="mt-3 inline-block text-sm text-accent hover:underline">
          {COPY.releaseNotes}
        </a>
      )}
      {s.isAdmin && (
        <div className="mt-4 space-y-2 border-t border-default/50 pt-3 text-sm">
          <p className="text-primary">{statusLine(s, now())}</p>
          <div className="flex flex-wrap items-center gap-2">
            {s.mode === "on" && (
              <Button size="sm" variant="secondary" disabled={s.checking} onClick={() => void s.checkNow()}>
                {s.checking ? COPY.checking : COPY.checkNow}
              </Button>
            )}
            {canToggle && (
              <Button size="sm" variant="ghost" onClick={() => void s.setConsent(s.mode !== "on")}>
                {s.mode === "on" ? COPY.turnOff : COPY.turnOn}
              </Button>
            )}
          </div>
          {s.locked && <p className="text-xs text-muted">{COPY.lockedHint}</p>}
          {s.saveError && <p className="text-xs text-red-400">{s.saveError}</p>}
        </div>
      )}
    </Card>
  )
}

function Row({ k, children }: { k: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between border-b border-default/50 py-1">
      <span className="text-muted">{k}</span>
      {children}
    </div>
  )
}
