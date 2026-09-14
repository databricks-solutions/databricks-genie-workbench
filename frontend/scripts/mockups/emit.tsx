/**
 * MV-advisor mockups — REVIEW SCAFFOLD emitter (see src/.../mockups/mvMockData.ts).
 *
 * NOT a presentational module — this is a build script. It renders every frame
 * to static HTML in BOTH themes (light + dark: the panels are full of dark:
 * variants, so a single-theme export reviews half the states) and links the
 * Tailwind-compiled mockups.css produced by vite.css.config.mts. No network.
 *
 * Bundled by vite.ssr.config.mts and run with node. Deleted with the mockups.
 */
import { mkdirSync, copyFileSync, writeFileSync } from "node:fs"
import path from "node:path"
import { renderToStaticMarkup } from "react-dom/server"
import { MOCKUP_FRAMES } from "@/components/auto-optimize/mockups/frames"

const FRONTEND = process.cwd() // scripts run from frontend/
const OUT = path.resolve(FRONTEND, "../docs/design/mockups")
const COMPILED_CSS = path.resolve(FRONTEND, "scripts/mockups/.dist-css/mockups.css")

const THEMES = [
  { name: "light", htmlClass: "" },
  { name: "dark", htmlClass: "dark" },
] as const

function page(title: string, bodyMarkup: string, htmlClass: string): string {
  return `<!doctype html>
<html lang="en" class="${htmlClass}">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${title}</title>
    <link rel="stylesheet" href="./mockups.css" />
  </head>
  <body class="bg-pattern">
    <div style="max-width: 56rem; margin: 0 auto; padding: 2rem;">
      <p class="text-xs text-muted" style="margin-bottom: 0.75rem;">MV-advisor mockup · review scaffold</p>
      <h1 class="text-primary font-display" style="font-size: 1.25rem; font-weight: 700; margin-bottom: 1.25rem;">${title}</h1>
      ${bodyMarkup}
    </div>
  </body>
</html>
`
}

function main() {
  mkdirSync(OUT, { recursive: true })
  copyFileSync(COMPILED_CSS, path.join(OUT, "mockups.css"))

  const links: string[] = []
  for (const frame of MOCKUP_FRAMES) {
    const markup = renderToStaticMarkup(frame.element)
    for (const theme of THEMES) {
      const file = `${frame.id}-${theme.name}.html`
      writeFileSync(path.join(OUT, file), page(frame.title, markup, theme.htmlClass))
      links.push(`<li><a href="./${file}">${frame.title} — ${theme.name}</a></li>`)
    }
  }

  const index = `<!doctype html>
<html lang="en" class="dark">
  <head>
    <meta charset="utf-8" />
    <title>MV-advisor mockups</title>
    <link rel="stylesheet" href="./mockups.css" />
  </head>
  <body class="bg-pattern">
    <div style="max-width: 56rem; margin: 0 auto; padding: 2rem;">
      <h1 class="text-primary font-display" style="font-size: 1.5rem; font-weight: 700;">MV-advisor mockups</h1>
      <p class="text-muted" style="margin: 0.5rem 0 1.5rem;">Review scaffold (Prompt 10). Each frame exported in light and dark. See README.md for disposal.</p>
      <ul class="text-accent" style="line-height: 2;">${links.join("")}</ul>
    </div>
  </body>
</html>
`
  writeFileSync(path.join(OUT, "index.html"), index)
  console.log(`Wrote ${MOCKUP_FRAMES.length * THEMES.length + 1} HTML files + mockups.css to ${OUT}`)
}

main()
