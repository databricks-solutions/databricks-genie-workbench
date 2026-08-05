export interface ScanCoordinator {
  request: (force?: boolean) => Promise<boolean>
}

export function createScanCoordinator(runScan: () => Promise<boolean>): ScanCoordinator {
  let inFlight: Promise<boolean> | null = null
  let forcedRequest: Promise<boolean> | null = null

  function startOrJoin(): Promise<boolean> {
    if (inFlight) return inFlight

    const request = Promise.resolve().then(runScan)
    inFlight = request
    const clear = () => {
      if (inFlight === request) inFlight = null
    }
    void request.then(clear, clear)
    return request
  }

  function request(force = false): Promise<boolean> {
    if (!force) return startOrJoin()
    if (forcedRequest) return forcedRequest

    const activeBeforeForce = inFlight
    const request = (async () => {
      if (activeBeforeForce) {
        await activeBeforeForce.catch(() => false)
      }
      return startOrJoin()
    })()
    forcedRequest = request
    const clear = () => {
      if (forcedRequest === request) forcedRequest = null
    }
    void request.then(clear, clear)
    return request
  }

  return { request }
}
