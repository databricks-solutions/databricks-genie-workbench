import { Button } from "@/components/ui/button"

import { COPY, DOCS_RELEASE_CHECK_URL } from "./copy"
import { useUpdateStatus } from "./update-status-context"

export function UpdateConsentBanner() {
  const s = useUpdateStatus()
  if (!s.isAdmin || s.mode !== "ask" || !s.info) return null
  return (
    <div
      role="region"
      aria-label="Release check"
      className="mb-6 flex flex-wrap items-center gap-3 rounded-lg border border-accent/30 bg-accent/10 px-4 py-3 text-sm"
    >
      <span className="min-w-[16rem] flex-1 text-primary">
        {COPY.consentText}{" "}
        <a href={DOCS_RELEASE_CHECK_URL} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
          {COPY.learnMore}
        </a>
      </span>
      <Button size="sm" onClick={() => void s.setConsent(true)}>{COPY.consentOn}</Button>
      <Button size="sm" variant="ghost" onClick={() => void s.setConsent(false)}>{COPY.consentOff}</Button>
      {s.saveError && <span className="text-red-400">{s.saveError}</span>}
    </div>
  )
}
