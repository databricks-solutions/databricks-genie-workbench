import { useEffect, useState } from 'react'
import { resolveSpaceAccess, type SpaceAccessAnswer, type SpaceAccessState } from '@/lib/space-access'

export function useSpaceAccess(spaceId: string): { access: SpaceAccessState; reason: string | null } {
  const [answer, setAnswer] = useState<SpaceAccessAnswer | null>(null)
  useEffect(() => {
    let live = true
    void resolveSpaceAccess(spaceId).then(next => { if (live) setAnswer(next) })
    return () => { live = false }
  }, [spaceId])
  // Keyed by space: after a switch, the previous space's answer must grant nothing.
  if (answer?.spaceId !== spaceId) return { access: 'checking', reason: null }
  return { access: answer.access, reason: answer.reason }
}
