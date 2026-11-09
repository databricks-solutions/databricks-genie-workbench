/**
 * Transport for the daily release check (spec 5.7).
 *
 * A cross-origin fetch made from the app page carries `Origin: <app URL>`, which
 * on Databricks Apps includes the app name and workspace ID. To keep the promise
 * that the check sends nothing about the workspace, the request runs inside a
 * hidden iframe sandboxed with only `allow-scripts`. That frame has an opaque
 * origin, so the browser sends `Origin: null`, with no referrer and no cookies.
 * The frame posts the response status and body text back to this window.
 *
 * Only GET is supported; that is all the release check needs.
 */

interface FrameReply {
  token?: unknown
  ok?: unknown
  status?: unknown
  body?: unknown
  error?: unknown
}

/** JSON that is safe to inline inside a <script> element. */
function scriptJson(value: unknown): string {
  return JSON.stringify(value).replace(/</g, "\\u003c")
}

function frameDocument(config: Record<string, unknown>): string {
  return `<!doctype html><meta name="referrer" content="no-referrer"><script>
var C = ${scriptJson(config)};
function send(m) { m.token = C.token; parent.postMessage(m, C.parentOrigin); }
try {
  fetch(C.url, C.init)
    .then(function (r) { return r.text().then(function (t) { send({ ok: r.ok, status: r.status, body: t }); }); })
    .catch(function (e) { send({ error: String((e && e.message) || e) }); });
} catch (e) { send({ error: String((e && e.message) || e) }); }
</script>`
}

function newToken(): string {
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")
}

function requestUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") return input
  if (input instanceof URL) return input.href
  return input.url
}

export const isolatedFetch: typeof fetch = (input, init) =>
  new Promise<Response>((resolve, reject) => {
    const signal = init?.signal ?? undefined
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"))
      return
    }
    const token = newToken()
    const origin = window.location.origin
    const frame = document.createElement("iframe")
    frame.setAttribute("sandbox", "allow-scripts")
    frame.setAttribute("referrerpolicy", "no-referrer")
    frame.setAttribute("aria-hidden", "true")
    frame.setAttribute("tabindex", "-1")
    frame.hidden = true
    frame.srcdoc = frameDocument({
      url: requestUrl(input),
      token,
      parentOrigin: origin && origin !== "null" ? origin : "*",
      init: {
        method: "GET",
        mode: "cors",
        credentials: "omit",
        referrerPolicy: "no-referrer",
        cache: "no-store",
        headers: Object.fromEntries(new Headers(init?.headers).entries()),
      },
    })

    const cleanup = () => {
      window.removeEventListener("message", onMessage)
      signal?.removeEventListener("abort", onAbort)
      frame.remove()
    }
    const onAbort = () => {
      cleanup()
      reject(new DOMException("Aborted", "AbortError"))
    }
    const onMessage = (event: MessageEvent) => {
      if (event.source !== frame.contentWindow) return
      const data = event.data as FrameReply | null
      if (!data || data.token !== token) return
      cleanup()
      if (typeof data.error === "string") {
        reject(new Error(data.error))
        return
      }
      const body = typeof data.body === "string" ? data.body : ""
      resolve({
        ok: data.ok === true,
        status: typeof data.status === "number" ? data.status : 0,
        json: async () => JSON.parse(body),
      } as unknown as Response)
    }

    window.addEventListener("message", onMessage)
    signal?.addEventListener("abort", onAbort)
    document.body.appendChild(frame)
  })
