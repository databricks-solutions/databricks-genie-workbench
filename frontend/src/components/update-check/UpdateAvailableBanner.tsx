import { Button } from "@/components/ui/button"

import { COPY, DOCS_UPGRADE_URL } from "./copy"
import { useUpdateStatus } from "./update-status-context"

export function UpdateAvailableBanner() {
  const s = useUpdateStatus()
  const latest = s.result?.latest
  if (!s.showUpdateBanner || !latest || !s.info) return null
  return (
    <div role="status" className="flex flex-wrap items-center gap-3 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm">
      <span className="flex-1 text-primary">{COPY.updateAvailable(latest.tag, s.info.version)}</span>
      <a href={latest.html_url} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
        {COPY.releaseNotes}
      </a>
      <a href={DOCS_UPGRADE_URL} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
        {COPY.howToUpgrade}
      </a>
      <Button size="sm" variant="ghost" onClick={s.dismissUpdate}>{COPY.dismiss}</Button>
    </div>
  )
}
