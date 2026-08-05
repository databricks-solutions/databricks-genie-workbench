import { describe, expect, it } from "vitest"
import { buildAppRouteUrl, parseAppRoute, routesEqual } from "./navigation"

describe("app navigation routes", () => {
  it("defaults to the Agent list", () => {
    expect(parseAppRoute("")).toEqual({ view: "list" })
  })

  it("restores a directly linked optimization run", () => {
    expect(parseAppRoute("?view=detail&space=agent-1&tab=optimize&run=run-1")).toEqual({
      view: "detail",
      spaceId: "agent-1",
      tab: "optimize",
      runId: "run-1",
    })
  })

  it("infers detail view from a space and rejects invalid tabs", () => {
    expect(parseAppRoute("?space=agent-1&tab=unknown&run=run-1")).toEqual({
      view: "detail",
      spaceId: "agent-1",
      tab: "score",
    })
  })

  it("does not retain a run outside the Optimize tab", () => {
    expect(parseAppRoute("?view=detail&space=agent-1&tab=history&run=run-1")).toEqual({
      view: "detail",
      spaceId: "agent-1",
      tab: "history",
    })
  })

  it("preserves platform parameters while replacing app navigation", () => {
    expect(buildAppRouteUrl(
      { view: "detail", spaceId: "agent-1", tab: "optimize", runId: "run-1" },
      "https://example.test/apps/workbench?o=123&view=admin#section",
    )).toBe("/apps/workbench?o=123&view=detail&space=agent-1&tab=optimize&run=run-1#section")
  })

  it("compares all persisted route fields", () => {
    expect(routesEqual(
      { view: "detail", spaceId: "agent-1", tab: "optimize", runId: "run-1" },
      { view: "detail", spaceId: "agent-1", tab: "optimize", runId: "run-1" },
    )).toBe(true)
    expect(routesEqual(
      { view: "detail", spaceId: "agent-1", tab: "optimize", runId: "run-1" },
      { view: "detail", spaceId: "agent-1", tab: "optimize" },
    )).toBe(false)
  })
})
