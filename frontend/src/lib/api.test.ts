import { afterEach, describe, expect, it, vi } from "vitest"
import { ApiError, extractDetailMessage, getModels, streamAgentChat, triggerAutoOptimize } from "./api"

afterEach(() => {
  vi.unstubAllGlobals()
})

describe("extractDetailMessage", () => {
  it("returns the string detail directly", () => {
    expect(extractDetailMessage("space not found", "fallback")).toBe(
      "space not found",
    )
  })

  it("falls back when detail is empty string", () => {
    expect(extractDetailMessage("", "fallback")).toBe("fallback")
  })

  it("joins pydantic 422 validation error lists by msg", () => {
    const detail = [
      { msg: "field required", loc: ["body", "space_id"] },
      { msg: "must be int", loc: ["body", "attempt"] },
    ]
    expect(extractDetailMessage(detail, "fallback")).toBe(
      "field required; must be int",
    )
  })

  it("picks `error` from a structured dict", () => {
    const detail = {
      error: "Optimization prerequisites are not met.",
      reason_code: "missing_permission",
      error_code: null,
      actionable_by: "customer",
    }
    expect(extractDetailMessage(detail, "fallback")).toBe(
      "Optimization prerequisites are not met.",
    )
  })

  it("falls through to `user_message` when `error` is absent", () => {
    expect(
      extractDetailMessage({ user_message: "Grant CAN MANAGE on the space" }, "f"),
    ).toBe("Grant CAN MANAGE on the space")
  })

  it("JSON.stringifies unknown objects rather than returning '[object Object]'", () => {
    const out = extractDetailMessage({ foo: "bar" }, "fallback")
    expect(out).toBe('{"foo":"bar"}')
    expect(out).not.toBe("[object Object]")
  })

  it("returns fallback for null/undefined detail", () => {
    expect(extractDetailMessage(null, "fallback")).toBe("fallback")
    expect(extractDetailMessage(undefined, "fallback")).toBe("fallback")
  })
})

describe("ApiError", () => {
  it("carries structured detail for callers that need reason_code / actionable_by", () => {
    const detail = {
      error: "Missing optimization permission",
      reason_code: "permission_denied",
      actionable_by: "customer",
    }
    const err = new ApiError(detail.error, 412, detail)
    expect(err.message).toBe("Missing optimization permission")
    expect(err.status).toBe(412)
    expect(err.detail).toEqual(detail)
    expect(err.detail?.reason_code).toBe("permission_denied")
  })

  it("defaults detail to null when not provided", () => {
    const err = new ApiError("boom", 500)
    expect(err.detail).toBeNull()
  })
})

describe("model selection API payloads", () => {
  it("fetches /api/models", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => [
        { name: "chat", displayName: "Chat", isDefault: true },
      ],
    }))
    vi.stubGlobal("fetch", fetchMock)

    const models = await getModels()

    expect(models).toEqual([
      { name: "chat", displayName: "Chat", isDefault: true },
    ])
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/models",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    )
  })

  it("sends llm_model when triggering optimization", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        runId: "run",
        jobRunId: "job-run",
        jobUrl: null,
        status: "QUEUED",
      }),
    }))
    vi.stubGlobal("fetch", fetchMock)

    await triggerAutoOptimize({
      space_id: "space",
      apply_mode: "genie_config",
      levers: [1, 2],
      llm_model: "selected-chat",
    })

    const [, options] = fetchMock.mock.calls[0]
    expect(JSON.parse(String(options.body))).toEqual({
      space_id: "space",
      apply_mode: "genie_config",
      levers: [1, 2],
      llm_model: "selected-chat",
    })
  })

  it("sends model in Create Agent chat request", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      body: {
        getReader: () => ({
          read: async () => ({ done: true, value: undefined }),
        }),
      },
    }))
    vi.stubGlobal("fetch", fetchMock)

    streamAgentChat(
      "hello",
      "session",
      null,
      {
        onSession: () => {},
        onStep: () => {},
        onThinking: () => {},
        onToolCall: () => {},
        onToolResult: () => {},
        onMessageDelta: () => {},
        onMessage: () => {},
        onCreated: () => {},
        onUpdated: () => {},
        onError: () => {},
        onDone: () => {},
      },
      "space",
      "selected-chat",
    )

    const [, options] = fetchMock.mock.calls[0]
    expect(JSON.parse(String(options.body))).toEqual({
      message: "hello",
      session_id: "session",
      selections: null,
      space_id: "space",
      model: "selected-chat",
    })
  })
})
