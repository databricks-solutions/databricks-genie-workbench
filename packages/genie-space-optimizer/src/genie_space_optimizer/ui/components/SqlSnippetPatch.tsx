interface SqlSnippetPatchProps {
  patch: Record<string, unknown>;
}

const TYPE_LABELS: Record<string, { label: string; className: string }> = {
  measures: { label: "Measure", className: "bg-blue-100 text-blue-800" },
  filters: { label: "Filter", className: "bg-amber-100 text-amber-800" },
  expressions: { label: "Dimension", className: "bg-purple-100 text-purple-800" },
};

const PATCH_TYPE_TO_SNIPPET: Record<string, { snippetType: string; op: string }> = {
  add_sql_snippet_measure: { snippetType: "measures", op: "add" },
  update_sql_snippet_measure: { snippetType: "measures", op: "update" },
  remove_sql_snippet_measure: { snippetType: "measures", op: "remove" },
  add_sql_snippet_filter: { snippetType: "filters", op: "add" },
  update_sql_snippet_filter: { snippetType: "filters", op: "update" },
  remove_sql_snippet_filter: { snippetType: "filters", op: "remove" },
  add_sql_snippet_expression: { snippetType: "expressions", op: "add" },
  update_sql_snippet_expression: { snippetType: "expressions", op: "update" },
  remove_sql_snippet_expression: { snippetType: "expressions", op: "remove" },
  proactive_sql_expression: { snippetType: "measures", op: "add" },
};

function inferFromPatchType(pt: string): { snippetType: string; op: string } {
  return PATCH_TYPE_TO_SNIPPET[pt] ?? { snippetType: "unknown", op: "add" };
}

export function SqlSnippetPatch({ patch }: SqlSnippetPatchProps) {
  let command: Record<string, unknown> = {};
  try {
    command =
      typeof patch.command === "string"
        ? JSON.parse(patch.command)
        : (patch.command as Record<string, unknown>) || {};
  } catch {
    command = {};
  }

  const patchData =
    typeof patch.patch === "object" && patch.patch
      ? (patch.patch as Record<string, unknown>)
      : {};

  const pt = String(patch.patchType || patch.type || patch.action_type || "");
  const inferred = inferFromPatchType(pt);

  const snippet =
    (command.snippet as Record<string, unknown>) ||
    (patch.sql_snippet as Record<string, unknown>) ||
    (patchData.sql_snippet as Record<string, unknown>) ||
    patchData ||
    {};

  const snippetType =
    (command.snippet_type as string) ||
    (patch.snippet_type as string) ||
    (patchData.snippet_type as string) ||
    inferred.snippetType;

  const op = (command.op as string) || inferred.op;

  const displayName =
    (snippet.display_name as string) || (snippet.alias as string) || (patchData.display_name as string) || "";
  const sql = Array.isArray(snippet.sql)
    ? (snippet.sql as string[]).join("\n")
    : (snippet.sql as string) || "";
  const synonyms = (snippet.synonyms as string[]) || [];
  const instruction = Array.isArray(snippet.instruction)
    ? (snippet.instruction as string[]).join(" ")
    : (snippet.instruction as string) || "";

  const typeInfo = TYPE_LABELS[snippetType] ?? {
    label: snippetType,
    className: "bg-gray-100 text-gray-800",
  };
  const opLabel =
    op === "add" ? "Add" : op === "update" ? "Update" : op === "remove" ? "Remove" : op;
  const target = String(patch.targetObject || "");

  return (
    <div className="space-y-2 rounded border bg-white p-3 text-xs">
      <div className="flex items-center gap-2">
        <span
          className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${typeInfo.className}`}
        >
          {typeInfo.label}
        </span>
        <span className="font-medium">
          {opLabel}{displayName ? `: ${displayName}` : ""}
        </span>
        {target && !displayName && (
          <span className="text-muted">{target}</span>
        )}
      </div>

      {sql && (
        <pre className="overflow-auto rounded bg-elevated/40 p-2 text-[10px] font-mono">
          {sql}
        </pre>
      )}

      {synonyms.length > 0 && (
        <div className="flex flex-wrap gap-1">
          <span className="text-muted">Synonyms:</span>
          {synonyms.map((s, i) => (
            <span key={i} className="rounded bg-gray-100 px-1.5 py-0.5 text-[10px]">
              {s}
            </span>
          ))}
        </div>
      )}

      {instruction && <p className="text-muted italic">{instruction}</p>}
    </div>
  );
}
