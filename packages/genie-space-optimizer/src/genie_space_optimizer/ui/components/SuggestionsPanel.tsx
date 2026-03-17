import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  useGetImprovementSuggestionsSuspense,
  useReviewSuggestion,
  useApplySuggestion,
  getImprovementSuggestionsKey,
  type SuggestionOut,
} from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  CheckCircle,
  XCircle,
  Play,
  Loader2,
  Lightbulb,
  FlaskConical,
  BarChart3,
} from "lucide-react";

interface SuggestionsPanelProps {
  runId: string;
}

type Suggestion = {
  suggestionId: string;
  runId: string;
  spaceId: string;
  iteration: number | null;
  suggestionType: string;
  title: string;
  rationale: string | null;
  definition: string | null;
  affectedQuestions: string[];
  estimatedImpact: string | null;
  status: string;
  reviewedBy: string | null;
  reviewedAt: string | null;
};

function toSuggestion(raw: SuggestionOut): Suggestion {
  const r = raw as Record<string, unknown>;
  return {
    suggestionId: (r.suggestionId as string) ?? "",
    runId: (r.runId as string) ?? "",
    spaceId: (r.spaceId as string) ?? "",
    iteration: (r.iteration as number | null) ?? null,
    suggestionType: (r.suggestionType as string) ?? "",
    title: (r.title as string) ?? "",
    rationale: (r.rationale as string | null) ?? null,
    definition: (r.definition as string | null) ?? null,
    affectedQuestions: (r.affectedQuestions as string[]) ?? [],
    estimatedImpact: (r.estimatedImpact as string | null) ?? null,
    status: (r.status as string) ?? "PROPOSED",
    reviewedBy: (r.reviewedBy as string | null) ?? null,
    reviewedAt: (r.reviewedAt as string | null) ?? null,
  };
}

function typeBadge(type: string) {
  if (type === "METRIC_VIEW") {
    return (
      <Badge variant="outline" className="border-blue-300 bg-blue-50 text-blue-700 dark:border-blue-700 dark:bg-blue-950 dark:text-blue-300">
        <BarChart3 className="mr-1 h-3 w-3" />
        Metric View
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="border-purple-300 bg-purple-50 text-purple-700 dark:border-purple-700 dark:bg-purple-950 dark:text-purple-300">
      <FlaskConical className="mr-1 h-3 w-3" />
      Function
    </Badge>
  );
}

function statusBadge(status: string) {
  switch (status) {
    case "ACCEPTED":
      return <Badge className="bg-green-600">Accepted</Badge>;
    case "REJECTED":
      return <Badge variant="secondary">Rejected</Badge>;
    case "IMPLEMENTED":
      return <Badge className="bg-emerald-600">Implemented</Badge>;
    default:
      return <Badge variant="outline">Proposed</Badge>;
  }
}

export function SuggestionsPanel({ runId }: SuggestionsPanelProps) {
  const queryClient = useQueryClient();
  const { data } = useGetImprovementSuggestionsSuspense({
    params: { run_id: runId },
  });

  const rawList = ((data as Record<string, unknown>)?.data ?? data ?? []) as SuggestionOut[];
  const suggestions = rawList.map(toSuggestion);

  const reviewMutation = useReviewSuggestion();
  const applyMutation = useApplySuggestion();

  const [pendingAction, setPendingAction] = useState<Record<string, string>>({});

  function handleReview(suggestionId: string, status: "ACCEPTED" | "REJECTED") {
    setPendingAction((p) => ({ ...p, [suggestionId]: status.toLowerCase() }));
    reviewMutation.mutate(
      { params: { suggestion_id: suggestionId }, data: { status } },
      {
        onSettled: () => {
          setPendingAction((p) => {
            const next = { ...p };
            delete next[suggestionId];
            return next;
          });
          queryClient.invalidateQueries({
            queryKey: getImprovementSuggestionsKey({ run_id: runId }),
          });
        },
      },
    );
  }

  function handleApply(suggestionId: string) {
    setPendingAction((p) => ({ ...p, [suggestionId]: "applying" }));
    applyMutation.mutate(
      { params: { suggestion_id: suggestionId } },
      {
        onSettled: () => {
          setPendingAction((p) => {
            const next = { ...p };
            delete next[suggestionId];
            return next;
          });
          queryClient.invalidateQueries({
            queryKey: getImprovementSuggestionsKey({ run_id: runId }),
          });
        },
      },
    );
  }

  if (suggestions.length === 0) {
    return (
      <Card>
        <CardContent className="flex flex-col items-center justify-center py-12 text-muted">
          <Lightbulb className="mb-3 h-10 w-10 opacity-40" />
          <p className="text-sm">No improvement suggestions for this run.</p>
          <p className="mt-1 text-xs">
            The strategist may propose metric views or functions in future iterations.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-3">
      {suggestions.map((s) => {
        const action = pendingAction[s.suggestionId];
        const isReviewed = s.status === "ACCEPTED" || s.status === "REJECTED";
        const isImplemented = s.status === "IMPLEMENTED";

        return (
          <Card key={s.suggestionId}>
            <CardHeader className="pb-3">
              <div className="flex items-start justify-between gap-2">
                <div className="flex flex-wrap items-center gap-2">
                  {typeBadge(s.suggestionType)}
                  <CardTitle className="text-base">{s.title}</CardTitle>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  {statusBadge(s.status)}
                  {s.iteration != null && (
                    <Badge variant="outline" className="text-xs">
                      Iter {s.iteration}
                    </Badge>
                  )}
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              {s.rationale && (
                <p className="text-sm text-muted">{s.rationale}</p>
              )}

              {s.definition && (
                <pre className="max-h-48 overflow-auto rounded-md border bg-elevated/50 p-3 text-xs">
                  <code>{s.definition}</code>
                </pre>
              )}

              <div className="flex flex-wrap items-center gap-3 text-xs text-muted">
                {s.estimatedImpact && (
                  <span className="flex items-center gap-1">
                    <BarChart3 className="h-3 w-3" />
                    {s.estimatedImpact}
                  </span>
                )}
                {s.affectedQuestions.length > 0 && (
                  <span>
                    {s.affectedQuestions.length} affected question{s.affectedQuestions.length !== 1 ? "s" : ""}
                  </span>
                )}
                {s.reviewedBy && (
                  <span>Reviewed by {s.reviewedBy}</span>
                )}
              </div>

              {!isReviewed && !isImplemented && (
                <div className="flex gap-2 pt-1">
                  <Button
                    size="sm"
                    variant="outline"
                    className="border-green-300 text-green-700 hover:bg-green-50 dark:border-green-700 dark:text-green-400 dark:hover:bg-green-950"
                    onClick={() => handleReview(s.suggestionId, "ACCEPTED")}
                    disabled={!!action}
                  >
                    {action === "accepted" ? (
                      <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                    ) : (
                      <CheckCircle className="mr-1 h-3 w-3" />
                    )}
                    Accept
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="border-red-300 text-red-700 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
                    onClick={() => handleReview(s.suggestionId, "REJECTED")}
                    disabled={!!action}
                  >
                    {action === "rejected" ? (
                      <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                    ) : (
                      <XCircle className="mr-1 h-3 w-3" />
                    )}
                    Reject
                  </Button>
                </div>
              )}

              {s.status === "ACCEPTED" && (
                <div className="pt-1">
                  <Button
                    size="sm"
                    onClick={() => handleApply(s.suggestionId)}
                    disabled={!!action}
                  >
                    {action === "applying" ? (
                      <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                    ) : (
                      <Play className="mr-1 h-3 w-3" />
                    )}
                    Apply SQL Definition
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
