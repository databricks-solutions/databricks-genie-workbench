import { describe, expect, it } from "vitest"
import { ApiError, extractDetailMessage } from "./api"

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

  it("picks `error` from a structured dict (Prompt Registry probe shape)", () => {
    const detail = {
      error: "Prompt Registry is not available in this workspace.",
      reason_code: "not_enabled",
      error_code: null,
      actionable_by: "customer",
      prompt_registry_available: false,
    }
    expect(extractDetailMessage(detail, "fallback")).toBe(
      "Prompt Registry is not available in this workspace.",
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
      error: "Prompt Registry is not available",
      reason_code: "permission_denied",
      actionable_by: "customer",
      prompt_registry_available: false,
    }
    const err = new ApiError(detail.error, 412, detail)
    expect(err.message).toBe("Prompt Registry is not available")
    expect(err.status).toBe(412)
    expect(err.detail).toEqual(detail)
    expect(err.detail?.reason_code).toBe("permission_denied")
  })

  it("defaults detail to null when not provided", () => {
    const err = new ApiError("boom", 500)
    expect(err.detail).toBeNull()
  })
})
