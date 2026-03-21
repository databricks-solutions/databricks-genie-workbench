"use client";

import * as React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

export interface DataModelField {
  name: string;
  type: string;
  example?: string;
}

export interface DataModelCardProps {
  title: string;
  fields: DataModelField[];
  compact?: boolean;
}

export function DataModelCard({
  title,
  fields,
  compact = false,
}: DataModelCardProps) {
  return (
    <Card className="overflow-hidden">
      <CardHeader className={compact ? "py-3" : undefined}>
        <CardTitle className={compact ? "text-sm" : undefined}>{title}</CardTitle>
      </CardHeader>
      <CardContent className={compact ? "pt-0 pb-3" : "pt-0"}>
        <div
          className={cn(
            "grid gap-x-4 gap-y-2",
            compact ? "gap-y-1.5 text-xs" : "gap-y-2 text-sm"
          )}
          style={{
            gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)",
          }}
        >
          <div className="font-medium text-muted">Field</div>
          <div className="font-medium text-muted">Type</div>
          <div className="font-medium text-muted">Example</div>
          {fields.map((field) => (
            <React.Fragment key={field.name}>
              <span className="font-mono font-semibold">{field.name}</span>
              <span className="text-muted">{field.type}</span>
              <span className="font-mono text-xs text-db-blue">
                {field.example ?? "-"}
              </span>
            </React.Fragment>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
