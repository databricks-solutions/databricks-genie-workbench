import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Database, Clock } from "lucide-react";
import { useGetPendingReviews } from "@/lib/api";

interface SpaceCardProps {
  id: string;
  name: string;
  description?: string;
  tableCount?: number;
  lastModified?: string;
  qualityScore?: number | null;
  accessLevel?: string | null;
  onClick: () => void;
}

function scoreBadge(score: number | null | undefined) {
  if (score == null) return <Badge variant="outline">No score</Badge>;
  const pct = Math.round(score * 100) / 100;
  if (pct >= 80)
    return (
      <Badge className="bg-db-green text-white hover:bg-db-green/90">
        {pct.toFixed(1)}%
      </Badge>
    );
  if (pct >= 60)
    return (
      <Badge className="bg-db-orange text-white hover:bg-db-orange/90">
        {pct.toFixed(1)}%
      </Badge>
    );
  return (
    <Badge className="bg-db-red-error text-white hover:bg-db-red-error/90">
      {pct.toFixed(1)}%
    </Badge>
  );
}

function relativeTime(iso: string): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(ms / 60_000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function accessBadge(level: string | null | undefined) {
  if (!level) return null;
  if (level === "CAN_MANAGE" || level === "CAN_EDIT") {
    return (
      <Badge variant="outline" className="border-green-400 text-green-700">
        Editable
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="border-gray-300 text-gray-500">
      View Only
    </Badge>
  );
}

export function SpaceCard({
  id,
  name,
  description,
  tableCount,
  lastModified,
  qualityScore,
  accessLevel,
  onClick,
}: SpaceCardProps) {
  const { data: reviewsData } = useGetPendingReviews({
    params: { space_id: id },
    query: { enabled: !!id },
  });
  const pendingCount = reviewsData?.data?.totalPending ?? 0;

  return (
    <Card
      className="cursor-pointer border border-db-gray-border bg-white transition-shadow hover:shadow-md"
      onClick={onClick}
      tabIndex={0}
      role="button"
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick();
        }
      }}
    >
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between gap-2">
          <CardTitle className="line-clamp-1 text-base font-semibold text-primary">
            {name}
          </CardTitle>
          <div className="flex items-center gap-1.5">
            {accessBadge(accessLevel)}
            {pendingCount > 0 && (
              <Badge variant="outline" className="border-amber-300 text-amber-700">
                {pendingCount} review{pendingCount > 1 ? "s" : ""}
              </Badge>
            )}
            {scoreBadge(qualityScore)}
          </div>
        </div>
        <CardDescription className="line-clamp-2 text-sm">
          {description || "No description"}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex items-center gap-4 text-xs text-muted">
        {!!tableCount && (
          <span className="flex items-center gap-1">
            <Database className="h-3.5 w-3.5" />
            {tableCount} table{tableCount !== 1 ? "s" : ""}
          </span>
        )}
        {lastModified && (
          <span className="flex items-center gap-1">
            <Clock className="h-3.5 w-3.5" />
            {relativeTime(lastModified)}
          </span>
        )}
      </CardContent>
    </Card>
  );
}
