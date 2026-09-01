/**
 * Reconnect policy for the Create Agent SSE stream.
 *
 * Background (RCA): the backend re-emits a `session` event at the start of
 * EVERY stream, including auto-reconnect continuations. The original code
 * reset the reconnect counter on every `session` event, which made the
 * MAX_AUTO_RECONNECTS cap unreachable — a create whose connection dropped
 * would reconnect forever, re-driving space creation each time and producing
 * duplicate Genie Agents. These pure helpers encode the corrected rules so
 * they can be unit-tested independently of the React component.
 */

export const MAX_AUTO_RECONNECTS = 3

/**
 * Should the reconnect budget be reset when a `session` event arrives?
 *
 * Only on a genuine new user turn — NEVER on a continuation (which is what an
 * auto-reconnect sends). Resetting on a continuation defeats the cap.
 */
export function shouldResetReconnectBudget(isContinuation: boolean): boolean {
  return !isContinuation
}

export type ReconnectDecision =
  | { action: "reconnect"; attempt: number }
  | { action: "stop"; reason: "space-already-created" | "cap-reached" | "not-a-drop" }

/**
 * Decide what to do when a stream ends.
 *
 * @param connectionLost   true if the stream ended via connection drop (vs a
 *                         clean done).
 * @param hasSession       whether we have a session id to resume.
 * @param spaceAlreadyCreated  true once a `created` event has been seen this
 *                         session — a created space must never be re-driven.
 * @param currentCount     the reconnect counter BEFORE this decision.
 */
export function decideReconnect(params: {
  connectionLost: boolean
  hasSession: boolean
  spaceAlreadyCreated: boolean
  currentCount: number
}): ReconnectDecision {
  const { connectionLost, hasSession, spaceAlreadyCreated, currentCount } = params

  if (!connectionLost || !hasSession) {
    return { action: "stop", reason: "not-a-drop" }
  }
  // A space already exists — reconnecting would re-create it. Stop.
  if (spaceAlreadyCreated) {
    return { action: "stop", reason: "space-already-created" }
  }
  const next = currentCount + 1
  if (next <= MAX_AUTO_RECONNECTS) {
    return { action: "reconnect", attempt: next }
  }
  return { action: "stop", reason: "cap-reached" }
}
