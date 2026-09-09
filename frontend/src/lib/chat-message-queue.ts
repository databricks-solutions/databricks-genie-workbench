export interface QueuedChatMessage {
  text: string
  selections?: Record<string, unknown>
}

type SendChatMessage = (text: string, selections?: Record<string, unknown>) => void

export function queueChatMessage(
  text: string,
  selections?: Record<string, unknown>,
): QueuedChatMessage {
  return {
    text: text.trim(),
    selections: selections ? { ...selections } : undefined,
  }
}

export function drainQueuedChatMessage(
  queued: QueuedChatMessage,
  send: SendChatMessage,
): void {
  send(queued.text, queued.selections)
}
