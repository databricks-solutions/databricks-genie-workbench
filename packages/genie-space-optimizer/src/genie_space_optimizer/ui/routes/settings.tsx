import { createFileRoute, useSearch } from "@tanstack/react-router";
import {
  useGetPermissionDashboard,
  useListSpaces,
  useCheckSpaceAccess,
  type SpacePermissions,
  type AccessLevelEntry,
} from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AccordionItem } from "@/components/ui/accordion";
import { ProcessFlow } from "@/components/ProcessFlow";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import React, { useState, useEffect, useMemo, useCallback } from "react";
import { ErrorBoundary } from "react-error-boundary";
import {
  Shield,
  CheckCircle2,
  AlertTriangle,
  Copy,
  Check,
  BookOpen,
  Server,
  Info,
  ExternalLink,
  Search,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Filter,
} from "lucide-react";
import { toast } from "sonner";

type SpaceSummary = {
  id: string;
  name: string;
  description?: string;
};

function toSpaceSummaryArray(value: unknown): SpaceSummary[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter(
      (item): item is Record<string, unknown> =>
        !!item && typeof item === "object",
    )
    .map((item) => ({
      id: String(item.id ?? ""),
      name: String(item.name ?? ""),
      description: String(item.description ?? ""),
    }));
}

export const Route = createFileRoute("/settings")({
  validateSearch: (search: Record<string, unknown>) => ({
    spaceId: typeof search.spaceId === "string" ? search.spaceId : undefined,
  }),
  component: () => (
    <ErrorBoundary fallback={<SettingsError />}>
      <SettingsContent />
    </ErrorBoundary>
  ),
});

function SettingsError() {
  return (
    <Alert variant="destructive" className="mt-6">
      <AlertTriangle className="h-4 w-4" />
      <AlertTitle>Failed to load settings</AlertTitle>
      <AlertDescription>
        Could not load permission settings. Please check your connection and try
        again.
      </AlertDescription>
    </Alert>
  );
}

function SettingsSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-48" />
      <Skeleton className="h-[200px] w-full" />
      <Skeleton className="h-[300px] w-full" />
    </div>
  );
}

const SETTINGS_PAGE_SIZE = 10;

function SettingsContent() {
  const { spaceId: deepLinkedSpaceId } = useSearch({ from: "/settings" });

  const { data: metaData, isLoading: metaLoading } =
    useGetPermissionDashboard({
      params: { metadata_only: true },
    });
  const meta = (metaData as any)?.data ?? metaData;
  const spPrincipalId: string = meta?.spPrincipalId ?? "";
  const spPrincipalDisplayName: string = meta?.spPrincipalDisplayName ?? "";
  const frameworkCatalog: string = meta?.frameworkCatalog ?? "";
  const frameworkSchema: string = meta?.frameworkSchema ?? "";
  const experimentBasePath: string = meta?.experimentBasePath ?? "";
  const jobName: string = meta?.jobName ?? "";
  const jobUrl: string | null = meta?.jobUrl ?? null;
  const workspaceHost: string | null = meta?.workspaceHost ?? null;

  const { data: spacesData, isLoading: spacesLoading } = useListSpaces({
    query: {},
  });
  const spacesResp = spacesData as any;
  const allSpaces = toSpaceSummaryArray(
    spacesResp?.data?.spaces ?? spacesResp?.spaces ?? spacesResp?.data ?? spacesResp,
  );

  const [searchInput, setSearchInput] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [page, setPage] = useState(0);
  const [editableOnly, setEditableOnly] = useState(false);

  useEffect(() => {
    if (deepLinkedSpaceId && allSpaces.length > 0 && !searchInput) {
      const match = allSpaces.find((s) => s.id === deepLinkedSpaceId);
      if (match) {
        setSearchInput(match.name);
        setDebouncedSearch(match.name.toLowerCase());
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deepLinkedSpaceId, allSpaces.length]);

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(searchInput), 300);
    return () => clearTimeout(t);
  }, [searchInput]);

  useEffect(() => {
    setPage(0);
  }, [debouncedSearch, editableOnly]);

  const filtered = useMemo(() => {
    const q = debouncedSearch.toLowerCase();
    if (!q) return allSpaces;
    return allSpaces.filter(
      (s) =>
        s.name.toLowerCase().includes(q) ||
        (s.description ?? "").toLowerCase().includes(q) ||
        s.id.toLowerCase().includes(q),
    );
  }, [allSpaces, debouncedSearch]);

  const { mutateAsync: checkAccess } = useCheckSpaceAccess();
  const [accessMap, setAccessMap] = useState<
    Record<string, string | null>
  >({});
  const accessMapRef = React.useRef(accessMap);
  accessMapRef.current = accessMap;

  const displaySpaces = useMemo(() => {
    if (!editableOnly) return filtered;
    return filtered.filter((s) => {
      const level = accessMap[s.id];
      return level === "CAN_EDIT" || level === "CAN_MANAGE";
    });
  }, [filtered, accessMap, editableOnly]);

  const totalPages = Math.max(
    1,
    Math.ceil(displaySpaces.length / SETTINGS_PAGE_SIZE),
  );
  const safePage = Math.min(page, totalPages - 1);
  const paged = useMemo(
    () =>
      displaySpaces.slice(
        safePage * SETTINGS_PAGE_SIZE,
        (safePage + 1) * SETTINGS_PAGE_SIZE,
      ),
    [displaySpaces, safePage],
  );

  const fetchAccessForIds = useCallback(
    async (ids: string[]) => {
      if (ids.length === 0) return;
      const unchecked = ids.filter((id) => !(id in accessMapRef.current));
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
        setAccessMap((prev) => {
          const next = { ...prev };
          for (const id of unchecked) {
            if (!(id in next)) next[id] = null;
          }
          return next;
        });
      }
    },
    [checkAccess],
  );

  const pagedKey = paged.map((s) => s.id).join(",");
  useEffect(() => {
    const ids = paged.map((s) => s.id);
    void fetchAccessForIds(ids);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pagedKey]);

  const [expandedItems, setExpandedItems] = useState<string[]>(
    deepLinkedSpaceId ? [deepLinkedSpaceId] : [],
  );

  const isLoading = metaLoading || spacesLoading;
  if (isLoading) return <SettingsSkeleton />;

  const checkedCount = filtered.filter((s) => s.id in accessMap).length;
  const editableCount = filtered.filter((s) => {
    const l = accessMap[s.id];
    return l === "CAN_EDIT" || l === "CAN_MANAGE";
  }).length;

  return (
    <Tabs defaultValue="permissions" className="space-y-6">
      <TabsList>
        <TabsTrigger value="permissions" className="gap-1.5">
          <Shield className="h-3.5 w-3.5" />
          Permissions
        </TabsTrigger>
        <TabsTrigger value="how-it-works" className="gap-1.5">
          <BookOpen className="h-3.5 w-3.5" />
          How It Works
        </TabsTrigger>
      </TabsList>

      <TabsContent value="permissions">
        <div className="space-y-6">
          <div>
            <h1 className="text-2xl font-bold text-primary">
              Permission Settings
            </h1>
            <p className="mt-1 text-sm text-muted">
              Review service principal access per Genie Space. Where access is
              missing, use the provided SQL commands or sharing instructions to
              grant it.
            </p>
          </div>

          <SPIdentityCard
            spId={spPrincipalId}
            spDisplayName={spPrincipalDisplayName}
          />

          <div className="flex items-center gap-3">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted" />
              <Input
                placeholder="Search spaces by name, description, or space ID..."
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                className="pl-9"
              />
            </div>
            <Button
              variant={editableOnly ? "default" : "outline"}
              size="sm"
              className="shrink-0 gap-1.5"
              onClick={() => setEditableOnly((v) => !v)}
            >
              <Filter className="h-3.5 w-3.5" />
              Editable only
            </Button>
          </div>

          {checkedCount < filtered.length && (
            <p className="text-center text-xs text-muted">
              Checked {checkedCount} of {filtered.length} spaces
              {editableCount > 0 && ` · ${editableCount} editable`}
            </p>
          )}

          {paged.length === 0 ? (
            <Card>
              <CardContent className="py-8 text-center text-sm text-muted">
                {debouncedSearch
                  ? "No spaces match your search."
                  : editableOnly
                    ? "No editable spaces found yet. Try disabling the filter."
                    : "No Genie Spaces found."}
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="space-y-3">
                {paged.map((space) => (
                  <LazySpacePermissionCard
                    key={space.id}
                    spaceId={space.id}
                    title={space.name}
                    workspaceHost={workspaceHost}
                    isExpanded={expandedItems.includes(space.id)}
                    onToggle={(open) => {
                      setExpandedItems((prev) =>
                        open
                          ? [...prev, space.id]
                          : prev.filter((id) => id !== space.id)
                      );
                    }}
                    accessLevel={accessMap[space.id]}
                  />
                ))}
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
                    onClick={() =>
                      setPage((p) => Math.min(totalPages - 1, p + 1))
                    }
                  >
                    Next
                    <ChevronRight className="ml-1 h-4 w-4" />
                  </Button>
                </div>
              )}
            </>
          )}

          <FrameworkResourcesCard
            catalog={frameworkCatalog}
            schema={frameworkSchema}
            experimentBasePath={experimentBasePath}
            jobName={jobName}
            jobUrl={jobUrl}
          />
        </div>
      </TabsContent>

      <TabsContent value="how-it-works">
        <ProcessFlow />
      </TabsContent>
    </Tabs>
  );
}

function SPIdentityCard({
  spId,
  spDisplayName,
}: {
  spId: string;
  spDisplayName?: string;
}) {
  const [copied, setCopied] = useState(false);
  const [showClientId, setShowClientId] = useState(false);

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Shield className="h-4 w-4 text-blue-600" />
          Service Principal
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-2">
          <p className="text-base font-semibold text-primary">
            {spDisplayName || spId}
          </p>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
            onClick={() => handleCopy(spDisplayName || spId)}
          >
            {copied ? (
              <Check className="h-3.5 w-3.5 text-green-600" />
            ) : (
              <Copy className="h-3.5 w-3.5" />
            )}
          </Button>
        </div>
        <p className="mt-1 text-xs text-muted">
          This service principal runs background optimization jobs. Use the
          instructions below to grant it the required access.
        </p>
        {spDisplayName && (
          <div className="mt-2">
            <Button
              variant="link"
              size="sm"
              className="h-auto p-0 text-xs text-muted"
              onClick={() => setShowClientId(!showClientId)}
            >
              {showClientId ? "Hide" : "Show"} Client ID
            </Button>
            {showClientId && (
              <div className="mt-1 flex items-center gap-2">
                <code className="rounded-md bg-elevated px-2.5 py-1.5 text-xs font-mono">
                  {spId}
                </code>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0"
                  onClick={() => handleCopy(spId)}
                >
                  <Copy className="h-3 w-3" />
                </Button>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === "ready") {
    return (
      <Badge
        variant="outline"
        className="border-green-200 bg-green-50 text-green-700"
      >
        <CheckCircle2 className="mr-1 h-3 w-3" />
        Ready
      </Badge>
    );
  }
  if (status === "action_needed") {
    return (
      <Badge
        variant="outline"
        className="border-amber-200 bg-amber-50 text-amber-700"
      >
        <AlertTriangle className="mr-1 h-3 w-3" />
        Action Needed
      </Badge>
    );
  }
  return (
    <Badge
      variant="outline"
      className="border-red-200 bg-red-50 text-red-700"
    >
      <Shield className="mr-1 h-3 w-3" />
      Not Configured
    </Badge>
  );
}

function AccessLevelBadge({ level }: { level: string | null | undefined }) {
  if (level === undefined) {
    return (
      <Badge variant="outline" className="gap-1 text-muted">
        <Loader2 className="h-3 w-3 animate-spin" />
        Checking
      </Badge>
    );
  }
  if (level === "CAN_MANAGE") {
    return (
      <Badge variant="outline" className="border-green-200 bg-green-50 text-green-700">
        <CheckCircle2 className="mr-1 h-3 w-3" />
        Can Manage
      </Badge>
    );
  }
  if (level === "CAN_EDIT") {
    return (
      <Badge variant="outline" className="border-blue-200 bg-blue-50 text-blue-700">
        <CheckCircle2 className="mr-1 h-3 w-3" />
        Can Edit
      </Badge>
    );
  }
  if (level === "CAN_VIEW") {
    return (
      <Badge variant="outline" className="text-muted">
        View Only
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="text-muted">
      No Access
    </Badge>
  );
}

function LazySpacePermissionCard({
  spaceId,
  title,
  workspaceHost,
  isExpanded,
  onToggle,
  accessLevel,
}: {
  spaceId: string;
  title: string;
  workspaceHost?: string | null;
  isExpanded: boolean;
  onToggle: (open: boolean) => void;
  accessLevel?: string | null;
}) {
  const isEditable = accessLevel === "CAN_EDIT" || accessLevel === "CAN_MANAGE";

  const { data: permData, isLoading } = useGetPermissionDashboard({
    params: { space_id: spaceId },
    query: { enabled: isExpanded && isEditable },
  });
  const permResp = (permData as any)?.data ?? permData;
  const space: SpacePermissions | undefined = (
    permResp?.spaces ?? []
  ).find((s: SpacePermissions) => s.spaceId === spaceId);

  const spaceUrl = workspaceHost
    ? `${workspaceHost.split("?")[0]}/genie/rooms/${spaceId}${workspaceHost.includes("?") ? `?${workspaceHost.split("?")[1]}` : ""}`
    : null;

  const isMuted = accessLevel !== undefined && !isEditable;

  const titleContent = (
    <div className="flex flex-1 items-center justify-between pr-2">
      <div className="text-left">
        <div className="flex items-center gap-1.5">
          <p className="font-semibold">{title}</p>
          {spaceUrl && (
            <a
              href={spaceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:text-blue-800"
              onClick={(e) => e.stopPropagation()}
            >
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          )}
        </div>
        <p className="text-xs text-muted">{spaceId}</p>
      </div>
    </div>
  );

  return (
    <AccordionItem
      title={titleContent}
      open={isExpanded}
      onOpenChange={onToggle}
      disabled={isMuted}
      action={
        <div className="flex items-center gap-2">
          <AccessLevelBadge level={accessLevel} />
          {space && <StatusBadge status={space.status} />}
        </div>
      }
    >
      {isMuted ? (
        <p className="py-2 text-sm text-muted">
          You need Can Edit or Can Manage access to configure this space.
        </p>
      ) : isLoading || !space ? (
        <div className="space-y-3 py-2">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-24 w-full" />
        </div>
      ) : (
        <SpacePermissionDetails space={space} />
      )}
    </AccordionItem>
  );
}

function SpacePermissionDetails({ space }: { space: SpacePermissions }) {
  return (
    <div className="space-y-4">
      <div className="rounded border p-3">
        <p className="mb-2 text-sm font-medium">Space Access</p>
        <div className="flex items-center gap-2">
          <span className="text-sm">SP CAN_MANAGE</span>
          {space.spHasManage ? (
            <Badge
              variant="outline"
              className="border-green-200 bg-green-50 text-green-700"
            >
              <CheckCircle2 className="mr-1 h-3 w-3" />
              Granted
            </Badge>
          ) : (
            <Badge
              variant="outline"
              className="border-red-200 bg-red-50 text-red-700"
            >
              Not Granted
            </Badge>
          )}
        </div>
        {!space.spHasManage && space.spGrantInstructions && (
          <div className="mt-2 flex items-start gap-2 rounded bg-amber-50 border border-amber-200 px-3 py-2">
            <Info className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
            <p className="text-xs text-amber-800">
              {space.spGrantInstructions}
            </p>
          </div>
        )}
      </div>

      {(space.schemas ?? []).length > 0 && (
        <div className="rounded border p-3">
          <p className="mb-2 text-sm font-medium">Data Access</p>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Schema</TableHead>
                <TableHead className="text-center">Read (SELECT)</TableHead>
                <TableHead className="text-center">
                  <span>Write (MODIFY)</span>
                  <p className="text-[10px] font-normal text-muted">
                    Optional — only for UC write backs
                  </p>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(space.schemas ?? []).map((s) => (
                <TableRow key={`${s.catalog}.${s.schema_name}`}>
                  <TableCell className="font-mono text-xs">
                    {s.catalog}.{s.schema_name}
                  </TableCell>
                  <TableCell className="text-center">
                    <GrantCell
                      granted={s.readGranted}
                      grantCommand={s.readGrantCommand}
                    />
                  </TableCell>
                  <TableCell className="text-center">
                    <GrantCell
                      granted={s.writeGranted}
                      grantCommand={s.writeGrantCommand}
                      isOptional
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}

function SqlHint({ sql }: { sql: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
    toast.info("SQL copied to clipboard");
  };
  return (
    <div className="mt-1 flex items-start gap-1.5 rounded bg-elevated/60 px-2 py-1.5 text-left max-w-xs">
      <code className="flex-1 whitespace-pre-wrap break-all text-[10px] font-mono leading-tight text-muted">
        {sql}
      </code>
      <Button
        variant="ghost"
        size="sm"
        className="h-5 w-5 shrink-0 p-0"
        onClick={handleCopy}
      >
        {copied ? (
          <Check className="h-3 w-3 text-green-600" />
        ) : (
          <Copy className="h-3 w-3" />
        )}
      </Button>
    </div>
  );
}

function GrantCell({
  granted,
  grantCommand,
  isOptional,
}: {
  granted: boolean;
  grantCommand?: string | null;
  isOptional?: boolean;
}) {
  if (granted) {
    return (
      <Badge
        variant="outline"
        className="border-green-200 bg-green-50 text-green-700"
      >
        <CheckCircle2 className="mr-1 h-3 w-3" />
        Granted
      </Badge>
    );
  }

  if (grantCommand) {
    return (
      <div className="flex flex-col items-center gap-0.5">
        <Badge
          variant="outline"
          className="border-red-200 bg-red-50 text-red-700"
        >
          Not Granted
        </Badge>
        <SqlHint sql={grantCommand} />
      </div>
    );
  }

  return (
    <Badge variant="outline" className="text-gray-400">
      {isOptional ? "Optional" : "—"}
    </Badge>
  );
}

function FrameworkResourcesCard({
  catalog,
  schema,
  experimentBasePath,
  jobName,
  jobUrl,
}: {
  catalog: string;
  schema: string;
  experimentBasePath: string;
  jobName: string;
  jobUrl?: string | null;
}) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Server className="h-4 w-4 text-purple-600" />
          Framework Resources
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-muted">Optimization tables</span>
            <code className="font-mono text-xs">
              {catalog}.{schema}
            </code>
          </div>
          <div className="flex justify-between">
            <span className="text-muted">MLflow experiments</span>
            <code className="font-mono text-xs">{experimentBasePath}</code>
          </div>
          <div className="flex justify-between">
            <span className="text-muted">Background job</span>
            {jobUrl ? (
              <a
                href={jobUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="font-mono text-xs text-blue-600 hover:underline"
              >
                {jobName} ↗
              </a>
            ) : (
              <code className="font-mono text-xs">{jobName}</code>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
