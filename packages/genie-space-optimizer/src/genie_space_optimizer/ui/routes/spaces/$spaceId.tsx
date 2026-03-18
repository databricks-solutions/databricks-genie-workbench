import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import {
  useGetSpaceDetailSuspense,
  useTriggerOptimization,
  useGetPermissionDashboard,
  type SpacePermissions,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { type ReactNode, Suspense, useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import {
  ArrowLeft,
  ChevronDown,
  Check,
  Copy,
  Rocket,
  ShieldAlert,
} from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { CrossRunChart } from "@/components/CrossRunChart";
import { OptimizationLoadingStepper } from "@/components/OptimizationLoadingStepper";

export const Route = createFileRoute("/spaces/$spaceId")({
  component: () => (
    <ErrorBoundary fallback={<SpaceDetailError />}>
      <Suspense fallback={<SpaceDetailSkeleton />}>
        <SpaceDetail />
      </Suspense>
    </ErrorBoundary>
  ),
});

function SpaceDetailError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load space</AlertTitle>
      <AlertDescription>
        Could not load space details. The space may not exist or the server is
        unavailable.
      </AlertDescription>
    </Alert>
  );
}

function SpaceDetailSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-64" />
      <Skeleton className="h-4 w-96" />
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Skeleton className="h-48 rounded-lg" />
        <Skeleton className="h-48 rounded-lg" />
      </div>
      <Skeleton className="h-64 rounded-lg" />
    </div>
  );
}

const LEVERS = [
  { id: 1, label: "Tables & Columns", desc: "Update table descriptions, column descriptions, and synonyms" },
  { id: 2, label: "Metric Views", desc: "Update metric view column descriptions" },
  { id: 3, label: "Table-Valued Functions", desc: "Remove underperforming TVFs" },
  { id: 4, label: "Join Specifications", desc: "Add, update, or remove join relationships between tables" },
  { id: 5, label: "Genie Space Instructions", desc: "Rewrite global routing instructions and add domain-specific guidance" },
] as const;

function SpaceDetail() {
  const { spaceId } = Route.useParams();
  const navigate = useNavigate();
  const { data: space } = useGetSpaceDetailSuspense({
    params: { space_id: spaceId },
    ...selector(),
  });
  const triggerOpt = useTriggerOptimization();
  const { data: permData, isLoading: permsLoading } = useGetPermissionDashboard({
    params: { space_id: spaceId },
  });
  const [applyMode, setApplyMode] = useState<"genie_config" | "both">(
    "genie_config",
  );
  const [ucConfirmOpen, setUcConfirmOpen] = useState(false);
  const [ucAcknowledged, setUcAcknowledged] = useState(false);
  const [selectedLevers, setSelectedLevers] = useState<Set<number>>(
    new Set(LEVERS.map((l) => l.id)),
  );
  const [deployTarget, setDeployTarget] = useState("");
  const [stepperOpen, setStepperOpen] = useState(false);
  const [stepperError, setStepperError] = useState<string | null>(null);
  const [stepperComplete, setStepperComplete] = useState(false);
  const [pendingRunId, setPendingRunId] = useState<string | null>(null);

  const toggleLever = (id: number) => {
    setSelectedLevers((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const hasActiveRun = space?.hasActiveRun ?? false;

  const spacePerms: SpacePermissions | undefined = (
    (permData as any)?.data?.spaces ?? (permData as any)?.spaces ?? []
  ).find((s: SpacePermissions) => s.spaceId === spaceId);
  const hasSpaceAccess = spacePerms?.spHasManage ?? false;
  const allReadGranted =
    spacePerms?.schemas?.every((s) => s.readGranted) ?? false;
  const allWriteGranted =
    spacePerms?.schemas?.every((s) => s.writeGranted) ?? false;
  const canStartOptimization = hasSpaceAccess && allReadGranted;
  const canStartWithWrites = canStartOptimization && allWriteGranted;
  const benchmarkQuestions = (
    (space as { benchmarkQuestions?: string[] })?.benchmarkQuestions ?? []
  );
  const joins = (
    (space as {
      joins?: Array<{
        leftTable: string;
        rightTable: string;
        relationshipType?: string | null;
        joinColumns?: string[];
      }>;
    })?.joins ?? []
  );
  const optimizationHistory = (
    (space as unknown as {
      optimizationHistory?: Array<{
        runId: string;
        status: string;
        baselineScore?: number | null;
        optimizedScore?: number | null;
        timestamp: string;
      }>;
    })?.optimizationHistory ?? []
  );
  const activeRunId = optimizationHistory.find((run) =>
    ["QUEUED", "IN_PROGRESS", "RUNNING"].includes((run.status || "").toUpperCase()),
  )?.runId;

  function doStartOptimization() {
    setStepperOpen(true);
    setStepperError(null);
    setStepperComplete(false);
    setPendingRunId(null);

    triggerOpt.mutate(
      {
        params: {},
        data: {
          space_id: spaceId,
          apply_mode: applyMode,
          levers: selectedLevers.size === LEVERS.length
            ? undefined
            : Array.from(selectedLevers).sort(),
          deploy_target: deployTarget.trim() || undefined,
        },
      },
      {
        onSuccess: (res) => {
          const runId = res.data?.runId;
          const jobUrl = res.data?.jobUrl;
          if (runId) {
            setPendingRunId(runId);
            setStepperComplete(true);
            if (jobUrl) {
              toast.success("Optimization started", {
                description: "Run launched. Open Workflows for live task logs.",
                action: {
                  label: "Open in Workflows",
                  onClick: () => {
                    window.open(jobUrl, "_blank", "noopener,noreferrer");
                  },
                },
              });
            }
          }
        },
        onError: (err) => {
          const status = (err as { status?: number }).status;
          const detail =
            (err as { body?: { detail?: string } }).body?.detail ||
            (err as Error).message ||
            "Failed to start optimization";
          setStepperError(detail);
          if (status === 409) {
            toast.warning("Optimization already in progress", {
              description: detail,
            });
          } else {
            toast.error("Optimization failed", { description: detail });
          }
        },
      },
    );
  }

  function handleOptimize() {
    if (applyMode === "both") {
      setUcAcknowledged(false);
      setUcConfirmOpen(true);
    } else {
      doStartOptimization();
    }
  }

  if (!space) return null;

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <button
          className="mb-2 flex items-center gap-1 text-sm text-muted hover:text-primary"
          onClick={() => navigate({ to: "/" })}
        >
          <ArrowLeft className="h-4 w-4" />
          Back to Dashboard
        </button>
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">{space.name}</h1>
          {hasActiveRun && activeRunId && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => navigate({ to: "/runs/$runId", params: { runId: activeRunId } })}
            >
              View Active Run
            </Button>
          )}
        </div>
      </div>

      <AlertDialog open={ucConfirmOpen} onOpenChange={setUcConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Confirm UC Writes</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-3">
                <p>
                  This optimization will write changes directly to Unity Catalog
                  assets, including column descriptions, table descriptions, and
                  TVF definitions.
                </p>
                {space.tables && space.tables.length > 0 && (
                  <div className="rounded border p-2 text-xs">
                    <p className="mb-1 font-medium">Affected schemas:</p>
                    <ul className="list-inside list-disc">
                      {[
                        ...new Set(
                          space.tables.map(
                            (t: { catalog?: string; schema?: string }) =>
                              `${t.catalog ?? "?"}.${t.schema ?? "?"}`,
                          ),
                        ),
                      ].map((s) => (
                        <li key={String(s)}>{String(s)}</li>
                      ))}
                    </ul>
                  </div>
                )}
                <label className="flex items-start gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={ucAcknowledged}
                    onChange={(e) => setUcAcknowledged(e.target.checked)}
                    className="mt-0.5"
                  />
                  I understand these changes will modify Unity Catalog assets
                  directly and cannot be automatically reverted.
                </label>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={!ucAcknowledged}
              onClick={() => {
                setUcConfirmOpen(false);
                doStartOptimization();
              }}
            >
              Confirm &amp; Start
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <OptimizationLoadingStepper
        isOpen={stepperOpen}
        isComplete={stepperComplete}
        error={stepperError}
        onNavigate={() => {
          setStepperOpen(false);
          if (pendingRunId) {
            navigate({ to: "/runs/$runId", params: { runId: pendingRunId } });
          }
        }}
      />

      <Tabs defaultValue="description" className="space-y-4">
        <TabsList className="h-auto w-full justify-start overflow-x-auto">
          <TabsTrigger value="description">Description</TabsTrigger>
          <TabsTrigger value="instructions">Instructions</TabsTrigger>
          <TabsTrigger value="sample-questions">Sample Questions</TabsTrigger>
          <TabsTrigger value="benchmark-questions">Benchmark Questions</TabsTrigger>
          <TabsTrigger value="referenced-tables">Referenced Tables</TabsTrigger>
          <TabsTrigger value="referenced-joins">Referenced Joins</TabsTrigger>
          <TabsTrigger value="referenced-functions">Referenced Functions</TabsTrigger>
          <TabsTrigger value="analytics">Analytics</TabsTrigger>
          <TabsTrigger value="optimization-history">Optimization History</TabsTrigger>
        </TabsList>

        <TabsContent value="description">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Description</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted">
                {space.description || "No description available."}
              </p>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="instructions">
          <Card className="border-db-gray-border bg-white overflow-hidden">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Instructions</CardTitle>
            </CardHeader>
            <CardContent className="overflow-hidden">
              <pre className="whitespace-pre-wrap break-words text-sm text-muted overflow-x-auto max-h-96 overflow-y-auto">
                {space.instructions || "No instructions configured."}
              </pre>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sample-questions">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Sample Questions</CardTitle>
            </CardHeader>
            <CardContent>
              {space.sampleQuestions.length === 0 ? (
                <p className="text-sm italic text-muted">
                  No sample questions configured.
                </p>
              ) : (
                <ul className="list-inside list-disc space-y-1 text-sm text-muted">
                  {space.sampleQuestions.map((q: string, i: number) => (
                    <li key={`${q}-${i}`}>{q}</li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="benchmark-questions">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Benchmark Questions</CardTitle>
            </CardHeader>
            <CardContent>
              {benchmarkQuestions.length === 0 ? (
                <p className="text-sm italic text-muted">
                  No benchmark questions found for this space yet.
                </p>
              ) : (
                <ul className="list-inside list-disc space-y-1 text-sm text-muted">
                  {benchmarkQuestions.map((q: string, i: number) => (
                    <li key={`${q}-${i}`}>{q}</li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="referenced-tables">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Referenced Tables</CardTitle>
            </CardHeader>
            <CardContent>
              {space.tables.length === 0 ? (
                <p className="text-sm italic text-muted">
                  No tables referenced.
                </p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Table Name</TableHead>
                      <TableHead>Catalog</TableHead>
                      <TableHead>Schema</TableHead>
                      <TableHead className="text-right">Columns</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {space.tables.map((t) => (
                      <TableRow key={t.name}>
                        <TableCell className="font-medium">{t.name}</TableCell>
                        <TableCell>{t.catalog}</TableCell>
                        <TableCell>{t.schema_name}</TableCell>
                        <TableCell className="text-right">{t.columnCount}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="referenced-joins">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Referenced Joins</CardTitle>
            </CardHeader>
            <CardContent>
              {joins.length === 0 ? (
                <p className="text-sm italic text-muted">
                  No join specifications referenced.
                </p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Left Table</TableHead>
                      <TableHead>Right Table</TableHead>
                      <TableHead>Join Columns</TableHead>
                      <TableHead>Relationship</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {joins.map((join, idx) => (
                      <TableRow key={`${join.leftTable}-${join.rightTable}-${idx}`}>
                        <TableCell className="font-medium">{join.leftTable}</TableCell>
                        <TableCell>{join.rightTable}</TableCell>
                        <TableCell>
                          {join.joinColumns && join.joinColumns.length > 0
                            ? join.joinColumns.join(", ")
                            : "—"}
                        </TableCell>
                        <TableCell>{join.relationshipType || "—"}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="referenced-functions">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Referenced Functions</CardTitle>
            </CardHeader>
            <CardContent>
              {space.functions && space.functions.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Function Name</TableHead>
                      <TableHead>Catalog</TableHead>
                      <TableHead>Schema</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {space.functions.map((fn) => (
                      <TableRow key={fn.name}>
                        <TableCell className="font-medium">{fn.name}</TableCell>
                        <TableCell>{fn.catalog}</TableCell>
                        <TableCell>{fn.schema_name}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <p className="text-sm italic text-muted">
                  No functions referenced.
                </p>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="analytics">
          <div className="space-y-4">
            <CrossRunChart runs={optimizationHistory} />
          </div>
        </TabsContent>

        <TabsContent value="optimization-history">
          <Card className="border-db-gray-border bg-white">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Optimization History</CardTitle>
            </CardHeader>
            <CardContent>
              {optimizationHistory.length === 0 ? (
                <p className="text-sm italic text-muted">
                  No optimization history yet.
                </p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Run ID</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Baseline</TableHead>
                      <TableHead className="text-right">Optimized</TableHead>
                      <TableHead className="text-right">Date</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {optimizationHistory.map((run) => (
                      <TableRow
                        key={run.runId}
                        className="cursor-pointer hover:bg-elevated/50"
                        onClick={() =>
                          navigate({
                            to: "/runs/$runId",
                            params: { runId: run.runId },
                          })
                        }
                      >
                        <TableCell className="font-mono text-xs">
                          {run.runId.slice(0, 8)}…
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">{run.status}</Badge>
                        </TableCell>
                        <TableCell className="text-right">
                          {run.baselineScore != null
                            ? `${run.baselineScore.toFixed(1)}%`
                            : "—"}
                        </TableCell>
                        <TableCell className="text-right">
                          {run.optimizedScore != null
                            ? `${run.optimizedScore.toFixed(1)}%`
                            : "—"}
                        </TableCell>
                        <TableCell className="text-right text-xs text-muted">
                          {run.timestamp
                            ? new Date(run.timestamp).toLocaleDateString()
                            : ""}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* ── Optimization Controls ─────────────────────────────────── */}
      <Card className="border-db-gray-border bg-white">
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Start Optimization</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-start gap-6">
            {/* Apply mode toggle */}
            <div className="space-y-2">
              <p className="text-xs font-medium text-muted">Apply mode</p>
              <div className="inline-flex rounded-md border border-db-gray-border p-1">
                <Button
                  type="button"
                  size="sm"
                  variant={applyMode === "genie_config" ? "default" : "ghost"}
                  onClick={() => setApplyMode("genie_config")}
                >
                  Config Only
                </Button>
                <div className="relative">
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    disabled
                    className="opacity-50"
                  >
                    Config + UC Write Backs
                  </Button>
                  <span className="absolute -top-2 -right-2 rounded-full bg-amber-100 text-amber-700 text-[10px] font-medium px-1.5 py-0.5 border border-amber-200">
                    Coming soon
                  </span>
                </div>
              </div>
              <p className="text-xs text-muted max-w-xs">
                Changes will be applied only to the selected Genie Space configuration. Underlying Unity Catalog tables, columns, and descriptions will not be modified.
              </p>
            </div>

            {/* Lever checkboxes */}
            <div className="space-y-2">
              <Collapsible defaultOpen>
                <CollapsibleTrigger className="flex items-center gap-1 text-xs font-medium text-muted hover:text-primary">
                  <ChevronDown className="h-3 w-3" />
                  What will be optimized
                </CollapsibleTrigger>
                <CollapsibleContent className="mt-2 space-y-2">
                  {LEVERS.map((lever) => (
                    <label
                      key={lever.id}
                      className="flex items-start gap-2 cursor-pointer"
                    >
                      <Checkbox
                        checked={selectedLevers.has(lever.id)}
                        onCheckedChange={() => toggleLever(lever.id)}
                        className="mt-0.5"
                      />
                      <div>
                        <span className="text-sm font-medium">{lever.label}</span>
                        <p className="text-xs text-muted">{lever.desc}</p>
                      </div>
                    </label>
                  ))}
                </CollapsibleContent>
              </Collapsible>
            </div>

            {/* Deployment target */}
            <div className="space-y-2">
              <Collapsible>
                <CollapsibleTrigger className="flex items-center gap-1 text-xs font-medium text-muted hover:text-primary">
                  <ChevronDown className="h-3 w-3" />
                  Deployment target (optional)
                </CollapsibleTrigger>
                <CollapsibleContent className="mt-2">
                  <Input
                    placeholder="https://target-workspace.cloud.databricks.com"
                    value={deployTarget}
                    onChange={(e) => setDeployTarget(e.target.value)}
                    className="h-8 w-72 text-xs"
                  />
                  <p className="mt-1 text-[10px] text-muted">
                    Leave empty to skip cross-environment deployment.
                  </p>
                </CollapsibleContent>
              </Collapsible>
            </div>
          </div>

          {/* Permission alerts */}
          {!canStartOptimization && !permsLoading && (
            <Alert variant="destructive" className="border-amber-300 bg-amber-50 text-amber-900 [&>svg]:text-amber-600">
              <ShieldAlert className="h-4 w-4" />
              <AlertTitle>Missing permissions</AlertTitle>
              <AlertDescription className="space-y-3">
                <PermissionStep
                  step={1}
                  title="Grant Genie Space access"
                  description={
                    <>
                      Open the Genie Space sharing dialog and add{" "}
                      <CopyableInline text={spacePerms?.spDisplayName ?? ""} />{" "}
                      with <strong>CAN_EDIT</strong> permission.
                    </>
                  }
                  granted={hasSpaceAccess}
                />
                <PermissionStep
                  step={2}
                  title="Grant data access"
                  description={<>The service principal needs <strong>SELECT</strong> and <strong>EXECUTE</strong> on the underlying schemas.</>}
                  granted={allReadGranted}
                  code={
                    spacePerms?.schemas
                      ?.filter((s) => !s.readGranted)
                      .map((s) => s.readGrantCommand || `GRANT SELECT ON SCHEMA ${s.catalog}.${s.schema_name} TO '...';\nGRANT EXECUTE ON SCHEMA ${s.catalog}.${s.schema_name} TO '...';`)
                      .join("\n\n") || undefined
                  }
                />
                <p className="text-xs">
                  <Link
                    to="/settings"
                    search={{ spaceId }}
                    className="underline hover:text-amber-800"
                  >
                    Go to Settings
                  </Link>{" "}
                  for the full permission dashboard.
                </p>
              </AlertDescription>
            </Alert>
          )}

          {/* UC Write Backs alert hidden — mode is disabled (coming soon) */}

          {permsLoading && (
            <p className="text-xs text-muted">
              Checking permissions…
            </p>
          )}

          {/* Start button */}
          <div className="flex items-center gap-3">
            <Button
              onClick={handleOptimize}
              disabled={
                triggerOpt.isPending ||
                hasActiveRun ||
                !canStartOptimization ||
                selectedLevers.size === 0 ||
                (applyMode === "both" && !canStartWithWrites)
              }
              className="bg-db-red hover:bg-db-red/90"
              title={
                hasActiveRun
                  ? "An optimization run is already in progress"
                  : !canStartOptimization
                    ? "Required permissions are missing"
                    : undefined
              }
            >
              <Rocket className="mr-2 h-4 w-4" />
              {triggerOpt.isPending
                ? "Starting…"
                : hasActiveRun
                  ? "Optimization In Progress"
                  : "Start Optimization"}
            </Button>
            {hasActiveRun && activeRunId && (
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => navigate({ to: "/runs/$runId", params: { runId: activeRunId } })}
              >
                View Active Run
              </Button>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function PermissionStep({
  step,
  title,
  description,
  granted,
  code,
}: {
  step: number;
  title: string;
  description: ReactNode;
  granted: boolean;
  code?: string;
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    if (!code) return;
    navigator.clipboard.writeText(code).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <div className={`rounded-md border px-3 py-2 ${granted ? "border-green-300 bg-green-50" : "border-amber-300 bg-amber-50"}`}>
      <div className="flex items-center gap-2">
        <span className={`flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-[10px] font-bold ${granted ? "bg-green-600 text-white" : "bg-amber-500 text-white"}`}>
          {granted ? <Check className="h-3 w-3" /> : step}
        </span>
        <span className={`text-sm font-medium ${granted ? "text-green-800" : "text-amber-900"}`}>
          {title}
          {granted && <span className="ml-1.5 text-xs font-normal text-green-600">(granted)</span>}
        </span>
      </div>
      {!granted && (
        <>
          <p className="mt-1 ml-7 text-xs text-amber-800">{description}</p>
          {code && (
            <div className="relative mt-2 ml-7">
              <pre className="rounded bg-amber-100/80 px-3 py-2 pr-10 font-mono text-[11px] leading-relaxed text-amber-900 overflow-x-auto whitespace-pre-wrap break-all">
                {code}
              </pre>
              <Button
                variant="ghost"
                size="icon"
                className="absolute right-1 top-1 h-6 w-6 text-amber-700 hover:bg-amber-200 hover:text-amber-900"
                onClick={handleCopy}
                title="Copy to clipboard"
              >
                {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
              </Button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function CopyableInline({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  if (!text) return null;

  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <span className="inline-flex items-center gap-1 rounded bg-amber-100/80 px-1.5 py-0.5 font-mono text-[11px] text-amber-900">
      {text}
      <button
        type="button"
        onClick={handleCopy}
        className="inline-flex items-center text-amber-600 hover:text-amber-900"
        title="Copy to clipboard"
      >
        {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
      </button>
    </span>
  );
}
