import { describe, expect, it, vi } from "vitest"
import { drainQueuedChatMessage, queueChatMessage } from "./chat-message-queue"

describe("Create Agent chat message queue", () => {
  it("preserves structured selections when a streaming send is queued and drained", () => {
    const selections = { description: "Sales by region", action: "create" }
    const queued = queueChatMessage("  Create the agent  ", selections)
    const send = vi.fn()

    drainQueuedChatMessage(queued, send)

    expect(send).toHaveBeenCalledOnce()
    expect(send).toHaveBeenCalledWith("Create the agent", selections)
  })

  it("snapshots top-level selections when queueing", () => {
    const selections: Record<string, unknown> = { description: "Original" }
    const queued = queueChatMessage("Update it", selections)
    selections.description = "Changed later"

    expect(queued.selections).toEqual({ description: "Original" })
  })
})
