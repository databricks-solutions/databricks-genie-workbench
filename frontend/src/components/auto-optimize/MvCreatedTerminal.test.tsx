import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { MvCreatedTerminal } from "./MvAcceptFlow"

type Props = Parameters<typeof MvCreatedTerminal>[0]

const GRANT = "GRANT SELECT ON VIEW finance.sales.order_revenue TO `gso-sp`"
const BASE: Props = {
  attached: true,
  alreadyExisted: false,
  provenance: "OBO_CREATED",
  owner: null,
  grantSql: GRANT,
  catalogUrl: "https://h.example.com/explore/data/finance/sales/order_revenue",
  onStartRun: () => {},
}

const render = (over: Partial<Props>) => renderToStaticMarkup(<MvCreatedTerminal {...BASE} {...over} />)
// The visible text: tags and React's text-node separators dropped, entities decoded.
const textOf = (html: string) =>
  html
    .replace(/<!-- -->/g, "")
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')

describe("MvCreatedTerminal", () => {
  it("OBO_CREATED and attached keeps today's copy and shows the GRANT", () => {
    const html = render({})
    expect(html).toContain("Created &amp; attached to your Agent")
    expect(html).toContain("One step left:")
    expect(html).toContain("so grant it<span class=\"font-mono\"> SELECT</span>")
    expect(html).toContain('title="Copy to clipboard"')
    expect(textOf(html)).toContain(GRANT)
    expect(html).not.toContain("Owned by")
    expect(html).toContain("View in Catalog Explorer")
    expect(html).toContain("Start an optimization run")
  })

  it("OBO_CREATED without a GRANT points to Show detail", () => {
    const html = render({ grantSql: null })
    expect(html).toContain("The copy-ready <span class=\"font-mono\">GRANT</span> statement is on the proposal")
    expect(html).not.toContain('title="Copy to clipboard"')
  })

  it("OBO_CREATED and not attached keeps today's grant clause", () => {
    const text = textOf(render({ attached: false }))
    expect(text).toContain("Created — not yet attached")
    expect(text).toContain("then grant the optimizer SELECT so a run can read it.")
  })

  it("USER_CREATED, attached, owner set: the owner sentence, the ask, and no GRANT", () => {
    const html = render({
      provenance: "USER_CREATED", alreadyExisted: true, owner: "other@example.com", grantSql: null,
    })
    const text = textOf(html)
    expect(text).toContain("Attached to your Agent (view already existed)")
    expect(html).toContain(
      "Owned by other@example.com, so only they can change it or grant the optimizer access to it.",
    )
    expect(text).toContain(
      "One step left: the optimizer runs as a separate service principal, so ask other@example.com to grant it SELECT to let an optimization run read and measure it.",
    )
    expect(html).not.toContain("GRANT")
    expect(html).not.toContain('title="Copy to clipboard"')
  })

  it("USER_CREATED offers no GRANT even when one is passed", () => {
    const html = render({ provenance: "USER_CREATED", owner: "other@example.com", grantSql: GRANT })
    expect(html).not.toContain("GRANT")
  })

  it("USER_CREATED with no owner says another user and its owner", () => {
    const html = render({ provenance: "USER_CREATED", alreadyExisted: true, owner: null, grantSql: null })
    expect(html).toContain("Owned by another user, so only they can change it")
    expect(textOf(html)).toContain("so ask its owner to grant it SELECT")
  })

  it("USER_CREATED and not attached asks the owner to grant", () => {
    const html = render({
      provenance: "USER_CREATED", attached: false, alreadyExisted: true, owner: "other@example.com", grantSql: null,
    })
    const text = textOf(html)
    expect(text).toContain("View exists — not yet attached")
    expect(text).toContain("Owned by other@example.com")
    expect(text).toContain("then ask the owner to grant the optimizer SELECT so a run can read it.")
    expect(text).not.toContain("then grant the optimizer")
    expect(html).not.toContain("GRANT")
  })

  it("no render carries a confidence word or a percent", () => {
    const renders = [
      render({}),
      render({ attached: false, grantSql: null }),
      render({ provenance: "USER_CREATED", owner: "other@example.com", grantSql: null }),
      render({ provenance: "USER_CREATED", owner: null, attached: false, grantSql: null }),
    ]
    for (const html of renders) {
      expect(textOf(html).toLowerCase()).not.toContain("confidence")
      expect(textOf(html)).not.toContain("%")
    }
  })
})
