/**
 * SQL code block with syntax highlighting and collapsible behavior.
 */

import { useState } from "react"
import { Highlight, themes } from "prism-react-renderer"
import { ChevronDown, ChevronUp, Copy, Check } from "lucide-react"
import { useTheme } from "@/hooks/useTheme"

interface SqlCodeBlockProps {
  code: string
  maxLines?: number
}

export function SqlCodeBlock({ code, maxLines = 10 }: SqlCodeBlockProps) {
  const [isExpanded, setIsExpanded] = useState(false)
  const [copied, setCopied] = useState(false)
  const { resolvedTheme } = useTheme()

  const lines = code.split("\n")
  const totalLines = lines.length
  const hasMoreLines = totalLines > maxLines
  const displayCode = isExpanded ? code : lines.slice(0, maxLines).join("\n")
  const hiddenLines = totalLines - maxLines

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  // Use dark theme for dark mode, light theme for light mode
  const theme = resolvedTheme === "dark" ? themes.oneDark : themes.oneLight

  return (
    <div className="rounded-lg overflow-hidden border border-default">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 bg-elevated border-b border-default">
        <span className="text-sm font-medium text-secondary">SQL</span>
        <button
          onClick={handleCopy}
          className="p-1.5 rounded hover:bg-sunken text-muted hover:text-secondary transition-colors"
          title="Copy to clipboard"
        >
          {copied ? (
            <Check className="w-4 h-4 text-success" />
          ) : (
            <Copy className="w-4 h-4" />
          )}
        </button>
      </div>

      {/* Code */}
      <Highlight theme={theme} code={displayCode} language="sql">
        {({ style, tokens, getLineProps, getTokenProps }) => (
          <pre
            className="p-4 overflow-x-auto text-sm font-mono"
            style={{ ...style, margin: 0, background: "transparent" }}
          >
            {tokens.map((line, i) => (
              <div key={i} {...getLineProps({ line })}>
                {line.map((token, key) => (
                  <span key={key} {...getTokenProps({ token })} />
                ))}
              </div>
            ))}
          </pre>
        )}
      </Highlight>

      {/* Expand/Collapse */}
      {hasMoreLines && (
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-elevated border-t border-default text-sm text-muted hover:text-secondary hover:bg-sunken transition-colors"
        >
          {isExpanded ? (
            <>
              <ChevronUp className="w-4 h-4" />
              Show less
            </>
          ) : (
            <>
              <ChevronDown className="w-4 h-4" />
              ... {hiddenLines} more line{hiddenLines !== 1 ? "s" : ""}
            </>
          )}
        </button>
      )}
    </div>
  )
}
