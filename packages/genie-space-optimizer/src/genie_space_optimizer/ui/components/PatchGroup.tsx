import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { SqlSnippetPatch } from "@/components/SqlSnippetPatch";
import { ChevronDown, ChevronRight } from "lucide-react";

interface PatchItem {
  patchType?: string;
  type?: string;
  action_type?: string;
  scope?: string;
  riskLevel?: string;
  targetObject?: string;
  rolledBack?: boolean;
  rollbackReason?: string | null;
  command?: unknown;
  patch?: unknown;
  appliedAt?: string | null;
}

interface PatchGroupProps {
  patches: PatchItem[];
  maxCollapsed?: number;
}

function parseCommand(raw: unknown): Record<string, unknown> {
  if (typeof raw === "object" && raw !== null && !Array.isArray(raw)) {
    return raw as Record<string, unknown>;
  }
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed === "object" && parsed !== null) return parsed;
      if (typeof parsed === "string") {
        const inner = JSON.parse(parsed);
        if (typeof inner === "object" && inner !== null) return inner;
      }
    } catch { /* not JSON */ }
  }
  return {};
}

const PATCH_TYPE_LABELS: Record<string, { label: string; className: string }> = {
  enable_example_values: { label: "Format Assistance", className: "bg-sky-100 text-sky-800" },
  enable_value_dictionary: { label: "Entity Matching", className: "bg-sky-100 text-sky-800" },
  proactive_description_enrichment: { label: "Column Description", className: "bg-violet-100 text-violet-800" },
  proactive_table_description_enrichment: { label: "Table Description", className: "bg-violet-100 text-violet-800" },
  proactive_join_discovery: { label: "Join Discovery", className: "bg-amber-100 text-amber-800" },
  proactive_example_sql: { label: "Example SQL", className: "bg-emerald-100 text-emerald-800" },
  proactive_sql_expression: { label: "SQL Expression", className: "bg-teal-100 text-teal-800" },
  proactive_instruction_seeding: { label: "Instruction Seeded", className: "bg-blue-100 text-blue-800" },
  proactive_space_description: { label: "Space Description", className: "bg-blue-100 text-blue-800" },
  proactive_sample_question: { label: "Sample Question", className: "bg-blue-100 text-blue-800" },
  instruction_restructure: { label: "Instruction Restructure", className: "bg-blue-100 text-blue-800" },
  update_column_description: { label: "Column Description", className: "bg-violet-100 text-violet-800" },
  update_description: { label: "Table Description", className: "bg-violet-100 text-violet-800" },
  update_function_description: { label: "Function Description", className: "bg-emerald-100 text-emerald-800" },
  add_column_synonym: { label: "Column Synonym", className: "bg-pink-100 text-pink-800" },
  add_join_spec: { label: "Join Specification", className: "bg-amber-100 text-amber-800" },
  update_join_spec: { label: "Join Specification", className: "bg-amber-100 text-amber-800" },
  iterative_join_mining: { label: "Join Mining", className: "bg-amber-100 text-amber-800" },
  add_instruction: { label: "Instruction", className: "bg-blue-100 text-blue-800" },
  update_instruction: { label: "Instruction", className: "bg-blue-100 text-blue-800" },
  remove_instruction: { label: "Remove Instruction", className: "bg-red-100 text-red-800" },
  rewrite_instruction: { label: "Instruction Rewrite", className: "bg-blue-100 text-blue-800" },
  add_example_sql: { label: "Example SQL", className: "bg-emerald-100 text-emerald-800" },
  update_example_sql: { label: "Example SQL", className: "bg-emerald-100 text-emerald-800" },
  remove_example_sql: { label: "Remove Example SQL", className: "bg-red-100 text-red-800" },
  add_tvf_parameter: { label: "TVF Parameter", className: "bg-orange-100 text-orange-800" },
  remove_tvf: { label: "Remove TVF", className: "bg-red-100 text-red-800" },
  add_sql_snippet_measure: { label: "SQL Measure", className: "bg-teal-100 text-teal-800" },
  add_sql_snippet_filter: { label: "SQL Filter", className: "bg-teal-100 text-teal-800" },
  add_sql_snippet_expression: { label: "SQL Dimension", className: "bg-teal-100 text-teal-800" },
};

function getPatchType(patch: PatchItem): string {
  return String(patch.patchType || patch.type || patch.action_type || "unknown");
}

function shortenTarget(target: string): string {
  const parts = target.split(".");
  if (parts.length >= 3) return parts.slice(-2).join(".");
  return target;
}

function PatchInlineRenderer({ patch }: { patch: PatchItem }) {
  const pt = getPatchType(patch);
  const target = patch.targetObject ? shortenTarget(patch.targetObject) : "";

  if (pt.includes("sql_snippet") || pt === "proactive_sql_expression") {
    return <SqlSnippetPatch patch={patch as Record<string, unknown>} />;
  }

  if (pt === "enable_example_values" || pt === "enable_value_dictionary") {
    return (
      <span className="text-xs text-muted">
        Enabled on <code className="rounded bg-elevated/60 px-1 py-0.5 text-[11px]">{target}</code>
      </span>
    );
  }

  if (pt.includes("description_enrichment") || pt === "proactive_table_description_enrichment") {
    const patchData = patch.patch as Record<string, unknown> | undefined;
    const sections = patchData?.structured_sections as Record<string, string> | undefined;
    return (
      <div className="text-xs">
        <code className="rounded bg-elevated/60 px-1 py-0.5 text-[11px]">{target}</code>
        {sections && Object.keys(sections).length > 0 && (
          <span className="ml-1.5 text-muted">
            ({Object.keys(sections).join(", ")})
          </span>
        )}
      </div>
    );
  }

  if (pt === "proactive_join_discovery" || pt === "add_join_spec" || pt === "update_join_spec" || pt === "iterative_join_mining") {
    return (
      <span className="text-xs text-muted">
        {target.includes("<->") ? target : `Join: ${target}`}
      </span>
    );
  }

  if (pt === "add_example_sql" || pt === "update_example_sql" || pt === "proactive_example_sql") {
    const cmd = parseCommand(patch.command);
    const patchData = typeof patch.patch === "object" && patch.patch
      ? (patch.patch as Record<string, unknown>)
      : {};
    const question = String(cmd.example_question || cmd.question || patchData.question || patchData.example_question || "").slice(0, 100);
    return (
      <span className="text-xs text-muted">
        {question ? `"${question}"` : target}
      </span>
    );
  }

  if (pt === "add_instruction" || pt === "update_instruction" || pt === "rewrite_instruction") {
    return <InstructionPatchInline patch={patch} target={target} />;
  }

  if (pt === "update_column_description" || pt === "add_column_synonym") {
    const patchData = patch.patch as Record<string, unknown> | undefined;
    const sections = patchData?.structured_sections as Record<string, string> | undefined;
    const synonyms = patchData?.synonyms as string[] | undefined;
    return (
      <div className="text-xs">
        <code className="rounded bg-elevated/60 px-1 py-0.5 text-[11px]">{target}</code>
        {sections && <span className="ml-1.5 text-muted">({Object.keys(sections).join(", ")})</span>}
        {synonyms && <span className="ml-1.5 text-muted">+{synonyms.length} synonyms</span>}
      </div>
    );
  }

  if (target) {
    return (
      <span className="text-xs text-muted">
        <code className="rounded bg-elevated/60 px-1 py-0.5 text-[11px]">{target}</code>
      </span>
    );
  }

  return null;
}

function InstructionPatchInline({ patch, target }: { patch: PatchItem; target: string }) {
  const [showDetail, setShowDetail] = useState(false);
  const cmd = parseCommand(patch.command);
  const patchData = typeof patch.patch === "object" && patch.patch
    ? (patch.patch as Record<string, unknown>)
    : {};
  const newText = String(cmd.new_text || patchData.new_text || cmd.instruction_text || "");
  const oldText = String(cmd.old_text || patchData.old_text || patchData.old_value || "");
  const pt = getPatchType(patch);
  const isRewrite = pt === "rewrite_instruction";
  const preview = newText.slice(0, 140);

  if (!newText && !oldText) {
    return (
      <span className="text-xs text-muted">
        {target || "instruction change"}
      </span>
    );
  }

  return (
    <div className="flex-1 text-xs">
      <div className="flex items-start gap-1">
        <button
          type="button"
          className="mt-0.5 shrink-0 text-muted/50 hover:text-muted"
          onClick={(e) => { e.stopPropagation(); setShowDetail((v) => !v); }}
        >
          {showDetail
            ? <ChevronDown className="h-3 w-3" />
            : <ChevronRight className="h-3 w-3" />}
        </button>
        <span className="text-muted">
          {preview}{preview.length >= 140 ? "…" : ""}
        </span>
      </div>
      {showDetail && (
        <div className="mt-1.5 ml-4 space-y-1.5">
          {isRewrite && oldText && (
            <div className="rounded border border-red-200 bg-red-50/50 px-2 py-1.5">
              <p className="mb-0.5 text-[10px] font-medium text-red-600">Old instruction</p>
              <p className="whitespace-pre-wrap text-[11px] leading-relaxed text-muted">{oldText}</p>
            </div>
          )}
          {newText && (
            <div className="rounded border border-green-200 bg-green-50/50 px-2 py-1.5">
              <p className="mb-0.5 text-[10px] font-medium text-green-600">
                {isRewrite ? "New instruction" : "Instruction"}
              </p>
              <p className="whitespace-pre-wrap text-[11px] leading-relaxed text-muted">{newText}</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function PatchDetailRenderer({ patch }: { patch: PatchItem }) {
  const pt = getPatchType(patch);

  if (pt.includes("sql_snippet") || pt === "proactive_sql_expression") {
    return <SqlSnippetPatch patch={patch as Record<string, unknown>} />;
  }

  const target = patch.targetObject ? shortenTarget(patch.targetObject) : "";
  return (
    <div className="rounded border bg-white px-2 py-1.5 text-xs">
      <div className="flex flex-wrap items-center gap-2">
        <TypeBadge patchType={pt} />
        {patch.scope && <span className="text-muted">scope: {patch.scope}</span>}
        {patch.riskLevel && <span className="text-muted">risk: {patch.riskLevel}</span>}
        {patch.rolledBack && (
          <Badge variant="outline" className="border-db-orange/30 text-db-orange">
            rolled back
          </Badge>
        )}
      </div>
      {target && <p className="mt-1 text-muted">target: {target}</p>}
      {patch.command != null ? (
        <pre className="mt-1 max-h-24 overflow-auto rounded bg-elevated/40 p-1 text-[11px]">
          {JSON.stringify(patch.command, null, 2)}
        </pre>
      ) : patch.patch != null ? (
        <pre className="mt-1 max-h-24 overflow-auto rounded bg-elevated/40 p-1 text-[11px]">
          {JSON.stringify(patch.patch, null, 2)}
        </pre>
      ) : null}
    </div>
  );
}

function TypeBadge({ patchType }: { patchType: string }) {
  const info = PATCH_TYPE_LABELS[patchType] ?? {
    label: patchType.replace(/_/g, " "),
    className: "bg-gray-100 text-gray-700",
  };
  return (
    <span className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-medium ${info.className}`}>
      {info.label}
    </span>
  );
}

interface GroupData {
  patchType: string;
  patches: PatchItem[];
  scope: string | undefined;
  riskLevel: string | undefined;
  hasRolledBack: boolean;
}

function groupPatches(patches: PatchItem[]): GroupData[] {
  const map = new Map<string, PatchItem[]>();
  for (const p of patches) {
    const pt = getPatchType(p);
    const list = map.get(pt);
    if (list) list.push(p);
    else map.set(pt, [p]);
  }
  return Array.from(map.entries()).map(([pt, items]) => ({
    patchType: pt,
    patches: items,
    scope: items[0]?.scope,
    riskLevel: items[0]?.riskLevel,
    hasRolledBack: items.some((p) => p.rolledBack),
  }));
}

function canUseInlineRenderer(patchType: string): boolean {
  return [
    "enable_example_values",
    "enable_value_dictionary",
    "proactive_description_enrichment",
    "proactive_table_description_enrichment",
    "proactive_join_discovery",
    "add_join_spec",
    "update_join_spec",
    "iterative_join_mining",
    "add_example_sql",
    "update_example_sql",
    "proactive_example_sql",
    "add_instruction",
    "update_instruction",
    "rewrite_instruction",
    "update_column_description",
    "add_column_synonym",
  ].includes(patchType);
}

function PatchGroupRow({ group }: { group: GroupData }) {
  const [expanded, setExpanded] = useState(false);
  const useInline = canUseInlineRenderer(group.patchType);
  const count = group.patches.length;

  return (
    <div className="rounded-lg border bg-white">
      <button
        type="button"
        className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs hover:bg-elevated/30"
        onClick={() => setExpanded((p) => !p)}
      >
        <TypeBadge patchType={group.patchType} />
        <span className="font-medium tabular-nums text-muted">
          {count} {count === 1 ? "change" : "changes"}
        </span>
        {group.scope && group.riskLevel && (
          <span className="text-[10px] text-muted/60">
            {group.scope} · {group.riskLevel}
          </span>
        )}
        {group.hasRolledBack && (
          <Badge variant="outline" className="border-db-orange/30 text-db-orange text-[10px]">
            has rollbacks
          </Badge>
        )}
        <ChevronDown
          className={`ml-auto h-3.5 w-3.5 text-muted transition-transform duration-200 ${
            expanded ? "rotate-180" : ""
          }`}
        />
      </button>

      {expanded && (
        <div className="space-y-1 border-t border-dashed px-3 py-2">
          {useInline ? (
            group.patches.map((patch, idx) => (
              <div key={idx} className="flex items-start gap-2 py-0.5">
                <span className="w-4 shrink-0 text-right text-[10px] text-muted/40">{idx + 1}</span>
                <PatchInlineRenderer patch={patch} />
                {patch.rolledBack && (
                  <Badge variant="outline" className="ml-auto border-db-orange/30 text-db-orange text-[10px]">
                    rolled back
                  </Badge>
                )}
              </div>
            ))
          ) : (
            group.patches.map((patch, idx) => (
              <PatchDetailRenderer key={idx} patch={patch} />
            ))
          )}
        </div>
      )}
    </div>
  );
}

export function PatchGroup({ patches }: PatchGroupProps) {
  if (!patches.length) return null;

  const groups = groupPatches(patches);

  return (
    <div className="space-y-1.5">
      {groups.map((group) => (
        <PatchGroupRow key={group.patchType} group={group} />
      ))}
    </div>
  );
}
