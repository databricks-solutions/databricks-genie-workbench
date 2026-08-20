import { afterEach, describe, expect, it, vi } from "vitest"
import {
  ApiError,
  extractDetailMessage,
  getAutoOptimizeLoopState,
  getAutoOptimizePublishRecord,
  getAutoOptimizeRevertOptions,
  getModels,
  removeAutoOptimizeRunFromHistory,
  revertAutoOptimizeRun,
  streamAgentChat,
  triggerAutoOptimize,
} from "./api"

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
      benchmark_policy: "review_only",
    })

    const [, options] = fetchMock.mock.calls[0]
    expect(JSON.parse(String(options.body))).toEqual({
      space_id: "space",
      apply_mode: "genie_config",
      levers: [1, 2],
      llm_model: "selected-chat",
      benchmark_policy: "review_only",
    })
  })

  it("round-trips the GSO loop knobs in the trigger request", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        runId: "run",
        jobRunId: "job-run",
        jobUrl: null,
        status: "QUEUED",
        targetAccuracy: 0.85,
        maxAttempts: 5,
      }),
    }))
    vi.stubGlobal("fetch", fetchMock)

    const resp = await triggerAutoOptimize({
      space_id: "space",
      target_accuracy: 0.85,
      max_attempts: 5,
      benchmark_policy: "repair_allowed",
    })

    const [, options] = fetchMock.mock.calls[0]
    expect(JSON.parse(String(options.body))).toEqual({
      space_id: "space",
      target_accuracy: 0.85,
      max_attempts: 5,
      benchmark_policy: "repair_allowed",
    })
    expect(resp.targetAccuracy).toBe(0.85)
    expect(resp.maxAttempts).toBe(5)
  })

  it("fetches the loop-state read path", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ runId: "run", loopState: null, attempts: [] }),
    }))
    vi.stubGlobal("fetch", fetchMock)

    const out = await getAutoOptimizeLoopState("run")

    expect(out).toEqual({ runId: "run", loopState: null, attempts: [] })
    expect(fetchMock.mock.calls[0][0]).toBe("/api/auto-optimize/runs/run/loop-state")
  })

  it("fetches the publish-record read path and tolerates errors", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ runId: "run", publishRecord: null }),
    }))
    vi.stubGlobal("fetch", fetchMock)

    const out = await getAutoOptimizePublishRecord("run")

    expect(out).toEqual({ runId: "run", publishRecord: null })
    expect(fetchMock.mock.calls[0][0]).toBe("/api/auto-optimize/runs/run/publish")
  })

  it("fetches the combined revert-options preview", async () => {
    const preview = {
      runId: "run",
      spaceId: "space",
      championAvailable: true,
      baselineAvailable: true,
      benchmarkChampionAvailable: true,
      benchmarkBaselineAvailable: true,
      benchmarkDiffs: {
        champion: {
          currentCount: 12,
          targetCount: 11,
          willAdd: 2,
          willRemove: 3,
          willChange: 1,
        },
        baseline: {
          currentCount: 12,
          targetCount: 10,
          willAdd: 1,
          willRemove: 3,
          willChange: 2,
        },
      },
    }
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => preview,
    }))
    vi.stubGlobal("fetch", fetchMock)

    await expect(getAutoOptimizeRevertOptions("run")).resolves.toEqual(preview)
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/auto-optimize/runs/run/revert-options",
    )
  })

  it("sends both independent revert scopes through one action", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ status: "reverted", runId: "run", message: "ok" }),
    }))
    vi.stubGlobal("fetch", fetchMock)

    await revertAutoOptimizeRun("run", {
      configTarget: "champion",
      benchmarkTarget: "champion",
    })

    const [url, options] = fetchMock.mock.calls[0]
    expect(url).toBe(
      "/api/auto-optimize/runs/run/revert?config_target=champion&benchmark_target=champion",
    )
    expect(options).toEqual(expect.objectContaining({ method: "POST" }))
  })

  it("removes an optimization history entry with DELETE", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        status: "removed",
        runId: "run",
        spaceId: "space",
        message: "ok",
      }),
    }))
    vi.stubGlobal("fetch", fetchMock)

    await removeAutoOptimizeRunFromHistory("run")

    const [url, options] = fetchMock.mock.calls[0]
    expect(url).toBe("/api/auto-optimize/runs/run/history-entry")
    expect(options).toEqual(expect.objectContaining({ method: "DELETE" }))
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
