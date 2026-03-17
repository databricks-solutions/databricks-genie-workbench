import { createFileRoute, useNavigate } from "@tanstack/react-router";
import {
  useListSpacesSuspense,
  useGetActivity,
  useGetActivitySuspense,
  useCheckSpaceAccess,
} from "@/lib/api";
import type { AccessLevelEntry } from "@/lib/api";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { SpaceCard } from "@/components/SpaceCard";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Suspense, useMemo, useState, useEffect, useCallback } from "react";
import { LayoutGrid, Activity, BarChart3, Search, ChevronLeft, ChevronRight } from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

type SpaceSummary = {
  id: string;
  name: string;
  description?: string;
  tableCount?: number;
  lastModified?: string;
  qualityScore?: number | null;
  accessLevel?: string | null;
};

type ActivityItem = {
  runId: string;
  spaceName: string;
  status: string;
  initiatedBy?: string | null;
  optimizedScore?: number | null;
  timestamp?: string | null;
};

function toSpaceSummaryArray(value: unknown): SpaceSummary[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is Record<string, unknown> => !!item && typeof item === "object")
    .map((item) => ({
      id: String(item.id ?? ""),
      name: String(item.name ?? ""),
      description: String(item.description ?? ""),
      tableCount: typeof item.tableCount === "number" ? item.tableCount : 0,
      lastModified: item.lastModified != null ? String(item.lastModified) : "",
      qualityScore: typeof item.qualityScore === "number" ? item.qualityScore : null,
      accessLevel: typeof item.accessLevel === "string" ? item.accessLevel : null,
    }));
}

function toActivityItemArray(value: unknown): ActivityItem[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is Record<string, unknown> => !!item && typeof item === "object")
    .map((item) => ({
      runId: String(item.runId ?? ""),
      spaceName: String(item.spaceName ?? ""),
      status: String(item.status ?? ""),
      initiatedBy: item.initiatedBy != null ? String(item.initiatedBy) : "",
      optimizedScore: typeof item.optimizedScore === "number" ? item.optimizedScore : null,
      timestamp: item.timestamp != null ? String(item.timestamp) : "",
    }))
    .filter((item) => item.runId.length > 0);
}

export const Route = createFileRoute("/")({
  component: () => (
    <ErrorBoundary fallback={<DashboardError />}>
      <Suspense fallback={<DashboardSkeleton />}>
        <DashboardContent />
      </Suspense>
    </ErrorBoundary>
  ),
});

function DashboardError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load dashboard</AlertTitle>
      <AlertDescription>
        Could not fetch spaces. Please check your connection and try again.
      </AlertDescription>
    </Alert>
  );
}

function DashboardSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {[1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-24 rounded-lg" />
        ))}
      </div>
      <Skeleton className="h-10 w-full rounded-md" />
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {[1, 2, 3, 4, 5, 6].map((i) => (
          <Skeleton key={i} className="h-36 rounded-lg" />
        ))}
      </div>
    </div>
  );
}

function DashboardContent() {
  const { data } = useListSpacesSuspense(selector());
  const resp = data as Record<string, unknown>;
  const spaces = toSpaceSummaryArray(resp?.spaces ?? resp);
  const totalCount =
    typeof resp?.totalCount === "number" ? resp.totalCount : spaces.length;
  const scopedToUser =
    typeof resp?.scopedToUser === "boolean" ? resp.scopedToUser : false;
  return (
    <Dashboard
      spaces={spaces}
      totalCount={totalCount}
      scopedToUser={scopedToUser}
    />
  );
}

function ActivitySection({ navigate }: { navigate: ReturnType<typeof useNavigate> }) {
  const { data } = useGetActivitySuspense(selector());
  const activity = toActivityItemArray(data);
  if (!activity || activity.length === 0) return null;

  return (
    <Card className="border-db-gray-border bg-white">
      <CardHeader>
        <CardTitle className="text-sm font-semibold">
          Recent Activity
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Space</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Initiated By</TableHead>
              <TableHead className="text-right">Score</TableHead>
              <TableHead className="text-right">Time</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {activity.map((item) => (
                <TableRow
                  key={item.runId}
                  className="cursor-pointer hover:bg-elevated/50"
                  onClick={() =>
                    navigate({
                      to: "/runs/$runId",
                      params: { runId: item.runId },
                    })
                  }
                >
                  <TableCell className="font-medium">
                    {item.spaceName}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">{item.status}</Badge>
                  </TableCell>
                  <TableCell>{item.initiatedBy}</TableCell>
                  <TableCell className="text-right">
                    {item.optimizedScore != null
                      ? `${item.optimizedScore.toFixed(1)}%`
                      : "\u2014"}
                  </TableCell>
                  <TableCell className="text-right text-xs text-muted">
                    {item.timestamp
                      ? new Date(item.timestamp).toLocaleDateString()
                      : ""}
                  </TableCell>
                </TableRow>
              ),
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

const PAGE_SIZE = 12;

function Dashboard({
  spaces,
  totalCount,
  scopedToUser,
}: {
  spaces: SpaceSummary[];
  totalCount: number;
  scopedToUser: boolean;
}) {
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [page, setPage] = useState(0);
  const navigate = useNavigate();

  const { mutateAsync: checkAccess } = useCheckSpaceAccess();
  const [accessMap, setAccessMap] = useState<Record<string, string | null>>({});

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300);
    return () => clearTimeout(t);
  }, [search]);

  useEffect(() => {
    setPage(0);
  }, [debouncedSearch]);

  const filtered = useMemo(() => {
    const q = debouncedSearch.toLowerCase();
    if (!q) return spaces;
    return spaces.filter(
      (s) =>
        s.name.toLowerCase().includes(q) ||
        (s.description ?? "").toLowerCase().includes(q) ||
        s.id.toLowerCase().includes(q),
    );
  }, [spaces, debouncedSearch]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const paged = useMemo(
    () => filtered.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE),
    [filtered, safePage],
  );

  const fetchAccessForPage = useCallback(
    async (ids: string[]) => {
      if (ids.length === 0) return;
      const unchecked = ids.filter((id) => !(id in accessMap));
      if (unchecked.length === 0) return;
      try {
        const result = await checkAccess({
          data: { spaceIds: unchecked },
          params: {},
        });
        const entries = (result?.data ?? []) as AccessLevelEntry[];
        setAccessMap((prev) => {
          const next = { ...prev };
          for (const e of entries) {
            next[e.spaceId] = e.accessLevel ?? null;
          }
          return next;
        });
      } catch {
        // permission check failed silently — cards show without badges
      }
    },
    [checkAccess, accessMap],
  );

  useEffect(() => {
    const ids = paged.map((s) => s.id);
    void fetchAccessForPage(ids);
    // Only trigger when the visible page IDs change
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paged.map((s) => s.id).join(",")]);

  const avgScore = useMemo(() => {
    if (spaces.length === 0) return 0;
    const scored = spaces.filter((s) => s.qualityScore != null);
    if (scored.length === 0) return 0;
    const sum = scored.reduce((acc, s) => acc + (s.qualityScore ?? 0), 0);
    return sum / scored.length;
  }, [spaces]);

  const { data: activityData } = useGetActivity({ query: {} });
  const recentRunsCount = toActivityItemArray(
    (activityData as Record<string, unknown> | undefined)?.data,
  ).length;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted">
              Total Spaces
            </CardTitle>
            <LayoutGrid className="h-4 w-4 text-muted" />
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">{totalCount}</span>
            {!scopedToUser && (
              <p className="mt-1 text-xs text-muted">
                All workspace spaces
              </p>
            )}
          </CardContent>
        </Card>

        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted">
              Recent Runs
            </CardTitle>
            <Activity className="h-4 w-4 text-muted" />
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {recentRunsCount > 0 ? recentRunsCount : "\u2014"}
            </span>
          </CardContent>
        </Card>

        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted">
              Avg Quality
            </CardTitle>
            <BarChart3 className="h-4 w-4 text-muted" />
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {avgScore > 0 ? `${avgScore.toFixed(1)}%` : "\u2014"}
            </span>
          </CardContent>
        </Card>
      </div>

      <div className="relative">
        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted" />
        <Input
          placeholder="Search spaces by name, description, or space ID..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-9"
        />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {paged.map((space) => (
          <SpaceCard
            key={space.id}
            {...space}
            accessLevel={accessMap[space.id] ?? undefined}
            onClick={() => {
              void navigate({
                to: "/spaces/$spaceId",
                params: { spaceId: space.id },
              });
            }}
          />
        ))}
        {filtered.length === 0 && (
          <p className="col-span-full py-12 text-center text-sm text-muted">
            {debouncedSearch
              ? "No spaces match your search."
              : "No Genie Spaces found."}
          </p>
        )}
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-4 pt-2">
          <Button
            variant="outline"
            size="sm"
            disabled={safePage === 0}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
          >
            <ChevronLeft className="mr-1 h-4 w-4" />
            Prev
          </Button>
          <span className="text-sm text-muted">
            Page {safePage + 1} of {totalPages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={safePage >= totalPages - 1}
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
          >
            Next
            <ChevronRight className="ml-1 h-4 w-4" />
          </Button>
        </div>
      )}

      <ErrorBoundary fallback={null}>
        <Suspense fallback={null}>
          <ActivitySection navigate={navigate} />
        </Suspense>
      </ErrorBoundary>
    </div>
  );
}
