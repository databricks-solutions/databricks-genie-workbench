import { VersionControlError } from '@/lib/version-control-api'
import type { ObservationResult } from '@/types/version-control'

export type CaptureNotice = { tone: 'success' | 'info' | 'error'; message: string }

// Map an observation outcome to a single user-facing notice with a tone. Kept pure and in
// its own module so the distinct terminal states are unit-tested directly (the component
// just renders them).
//
// `knownVersionIds` are the version ids the caller had ALREADY loaded before observing.
// This matters because the observer's dedup branch returns the existing observed *head* as
// `captured_version` on an unchanged open (it is not None) — so "a version object came back"
// is NOT proof that a new version was appended. A capture is genuinely new only when its id
// was not already present in the history we held before the observe.
export function describeObservation(
  result: ObservationResult,
  knownVersionIds: ReadonlySet<string> = new Set(),
): CaptureNotice {
  if (result.busy) return { tone: 'info', message: 'A capture is already in progress — showing the latest history.' }
  const captured = result.captured_version
  if (captured && !knownVersionIds.has(captured.version_id)) {
    return { tone: 'success', message: 'Saved a new version — changes were detected in the live configuration.' }
  }
  return { tone: 'info', message: 'No changes since the last captured version.' }
}

// Map a capture/observe failure to a notice. 503 = writes flag off; 403 = the caller can't
// read the live space; everything else surfaces the server message (or a generic fallback).
export function describeCaptureError(err: unknown): CaptureNotice {
  if (err instanceof VersionControlError) {
    if (err.status === 503) return { tone: 'error', message: 'Version capture is not enabled on this deployment.' }
    if (err.status === 403) return { tone: 'error', message: 'You do not have access to read this space’s configuration.' }
    return { tone: 'error', message: err.message }
  }
  return { tone: 'error', message: 'Capture failed. Please try again.' }
}
