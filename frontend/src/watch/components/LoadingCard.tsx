import { useEffect, useState } from 'react'

import { Card } from '@/components/ui/card'

export function LoadingCard() {
  const [elapsed, setElapsed] = useState(0)
  useEffect(() => {
    const id = setInterval(() => setElapsed(e => e + 1), 1000)
    return () => clearInterval(id)
  }, [])
  return (
    <Card className="p-6 text-center">
      <p className="font-medium">Loading… ({elapsed}s)</p>
      <p className="mt-2 text-xs text-muted">
        First load runs a fresh system-table query (typically 30–60s on a busy warehouse).
        Subsequent visits within 5 min are cached and load instantly.
      </p>
    </Card>
  )
}
