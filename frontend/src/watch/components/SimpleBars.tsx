export function SimpleBars({
  data,
  formatY,
  colorClass = 'bg-blue-500',
}: {
  data: { x: string; y: number }[]
  formatY?: (v: number) => string
  colorClass?: string
}) {
  const max = Math.max(1, ...data.map(d => d.y))
  return (
    <div className="space-y-1">
      {data.map((d, i) => (
        <div key={i} className="flex items-center gap-2 text-xs">
          <span className="w-16 shrink-0 text-muted">{d.x}</span>
          <div className="h-3 flex-1 rounded bg-elevated">
            <div
              className={`h-3 rounded ${colorClass}`}
              style={{ width: `${(d.y / max) * 100}%` }}
            />
          </div>
          <span className="w-16 shrink-0 text-right tabular-nums">
            {formatY ? formatY(d.y) : d.y.toLocaleString()}
          </span>
        </div>
      ))}
      {!data.length && <p className="text-xs text-muted">No data.</p>}
    </div>
  )
}
