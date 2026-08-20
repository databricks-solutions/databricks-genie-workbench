import { describe, expect, it, vi } from "vitest"
import { createScanCoordinator } from "./scan-coordinator"

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

describe("createScanCoordinator", () => {
  it("coalesces ordinary requests onto the active scan", async () => {
    const scan = deferred<boolean>()
    const runScan = vi.fn(() => scan.promise)
    const coordinator = createScanCoordinator(runScan)

    const first = coordinator.request()
    const second = coordinator.request()

    expect(second).toBe(first)
    expect(runScan).toHaveBeenCalledTimes(0)
    await Promise.resolve()
    expect(runScan).toHaveBeenCalledTimes(1)

    scan.resolve(true)
    await expect(first).resolves.toBe(true)
  })

  it("waits for an active scan before starting one forced follow-up", async () => {
    const beforeRollback = deferred<boolean>()
    const afterRollback = deferred<boolean>()
    const runScan = vi
      .fn<() => Promise<boolean>>()
      .mockReturnValueOnce(beforeRollback.promise)
      .mockReturnValueOnce(afterRollback.promise)
    const coordinator = createScanCoordinator(runScan)

    const first = coordinator.request()
    await Promise.resolve()
    const forced = coordinator.request(true)
    const duplicateForced = coordinator.request(true)

    expect(duplicateForced).toBe(forced)
    expect(runScan).toHaveBeenCalledTimes(1)

    beforeRollback.resolve(true)
    await first
    await Promise.resolve()
    await Promise.resolve()
    expect(runScan).toHaveBeenCalledTimes(2)

    afterRollback.resolve(true)
    await expect(forced).resolves.toBe(true)
  })

  it("still runs the forced follow-up when the stale scan fails", async () => {
    const beforeRollback = deferred<boolean>()
    const runScan = vi
      .fn<() => Promise<boolean>>()
      .mockReturnValueOnce(beforeRollback.promise)
      .mockResolvedValueOnce(true)
    const coordinator = createScanCoordinator(runScan)

    const first = coordinator.request()
    await Promise.resolve()
    const forced = coordinator.request(true)
    beforeRollback.reject(new Error("stale scan failed"))

    await expect(first).rejects.toThrow("stale scan failed")
    await expect(forced).resolves.toBe(true)
    expect(runScan).toHaveBeenCalledTimes(2)
  })
})
