import { useEffect, useState } from 'react'
import { NOT_FOUND_RETRY_MS, resolveSpaceAccess, type SpaceAccessAnswer, type SpaceAccessState } from '@/lib/space-access'

export function useSpaceAccess(
  spaceId: string,
  { retryNotFound = false }: { retryNotFound?: boolean } = {},
): { access: SpaceAccessState; reason: string | null } {
  const [answer, setAnswer] = useState<SpaceAccessAnswer | null>(null)
  useEffect(() => {
    let live = true
    let timer: ReturnType<typeof setTimeout> | undefined
    void resolveSpaceAccess(spaceId).then(first => {
      if (!live) return
      if (!(retryNotFound && first.notFound)) { setAnswer(first); return }
      timer = setTimeout(() => {
        void resolveSpaceAccess(spaceId).then(next => { if (live) setAnswer(next) })
      }, NOT_FOUND_RETRY_MS)
    })
    return () => { live = false; if (timer) clearTimeout(timer) }
  }, [spaceId, retryNotFound])
  // Keyed by space: after a switch, the previous space's answer must grant nothing.
  if (answer?.spaceId !== spaceId) return { access: 'checking', reason: null }
  return { access: answer.access, reason: answer.reason }
}
