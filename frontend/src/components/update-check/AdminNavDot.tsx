import { COPY } from "./copy"
import { useUpdateStatus } from "./update-status-context"

export function AdminNavDot() {
  const { updateAvailable } = useUpdateStatus()
  if (!updateAvailable) return null
  return (
    <span
      role="img"
      aria-label={COPY.navDotLabel}
      title={COPY.navDotLabel}
      className="ml-0.5 inline-block h-2 w-2 rounded-full bg-emerald-500"
    />
  )
}
