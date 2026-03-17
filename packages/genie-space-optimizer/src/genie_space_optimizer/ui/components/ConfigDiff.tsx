import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

interface TableDescription {
  tableName: string;
  description: string;
}

interface SpaceConfiguration {
  instructions: string;
  sampleQuestions: string[];
  tableDescriptions: TableDescription[];
}

interface ConfigDiffProps {
  original: SpaceConfiguration;
  optimized: SpaceConfiguration;
}

function DiffPanel({
  label,
  originalLines,
  optimizedLines,
}: {
  label: string;
  originalLines: string[];
  optimizedLines: string[];
}) {
  const origSet = new Set(originalLines);
  const optSet = new Set(optimizedLines);

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div>
        <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
          Original {label}
        </h4>
        <div className="space-y-1 rounded-md border border-db-gray-border bg-white p-3">
          {originalLines.length === 0 ? (
            <p className="text-sm italic text-muted">Empty</p>
          ) : (
            originalLines.map((line, i) => (
              <div
                key={i}
                className={`rounded px-2 py-1 text-sm ${
                  !optSet.has(line)
                    ? "bg-red-50 text-red-800"
                    : "text-primary"
                }`}
              >
                {line}
              </div>
            ))
          )}
        </div>
      </div>
      <div>
        <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
          Optimized {label}
        </h4>
        <div className="space-y-1 rounded-md border border-db-gray-border bg-white p-3">
          {optimizedLines.length === 0 ? (
            <p className="text-sm italic text-muted">Empty</p>
          ) : (
            optimizedLines.map((line, i) => (
              <div
                key={i}
                className={`rounded px-2 py-1 text-sm ${
                  !origSet.has(line)
                    ? "bg-green-50 text-green-800"
                    : "text-primary"
                }`}
              >
                {line}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

export function ConfigDiff({ original, optimized }: ConfigDiffProps) {
  const origInstrLines = original.instructions
    ? original.instructions.split("\n").filter(Boolean)
    : [];
  const optInstrLines = optimized.instructions
    ? optimized.instructions.split("\n").filter(Boolean)
    : [];

  const origTableLines = original.tableDescriptions.map(
    (t) => `${t.tableName}: ${t.description}`,
  );
  const optTableLines = optimized.tableDescriptions.map(
    (t) => `${t.tableName}: ${t.description}`,
  );

  return (
    <Card className="border-db-gray-border bg-white">
      <CardHeader>
        <CardTitle className="text-sm font-semibold">
          Configuration Changes
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="instructions">
          <TabsList className="mb-4">
            <TabsTrigger value="instructions">Instructions</TabsTrigger>
            <TabsTrigger value="questions">Sample Questions</TabsTrigger>
            <TabsTrigger value="tables">Table Descriptions</TabsTrigger>
          </TabsList>

          <TabsContent value="instructions">
            <DiffPanel
              label="Instructions"
              originalLines={origInstrLines}
              optimizedLines={optInstrLines}
            />
          </TabsContent>

          <TabsContent value="questions">
            <DiffPanel
              label="Sample Questions"
              originalLines={original.sampleQuestions}
              optimizedLines={optimized.sampleQuestions}
            />
          </TabsContent>

          <TabsContent value="tables">
            <DiffPanel
              label="Table Descriptions"
              originalLines={origTableLines}
              optimizedLines={optTableLines}
            />
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}
