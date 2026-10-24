/**
 * PR #332 M7e-2 fidelity frame (MV-D123) — the REAL deployed Model tab view:
 *   m7e2-a — one calculation, SUM(amount), over orders in two catalogs is two
 *            Space-config measures with one label, each drawn from its own table
 *            (reference: 9a, ModelTabPopulatedFrame). The fixture is what
 *            `_build_semantic_graph` returns for that space; the merge key itself
 *            is guarded by backend/tests/test_semantic_graph.py::
 *            test_one_calculation_over_two_tables_is_two_nodes.
 *
 * Disposed with the rest of the scaffold (see docs/design/mockups/README.md).
 */
import { BlueprintCanvas } from "@/components/model/SemanticBlueprint"
import { SemanticModelView } from "@/components/model/SemanticModelTab"
import { fromSemanticGraph } from "@/components/model/blueprint/model"
import type { SemanticGraphResponse } from "@/types"

const TWO_TABLES_ONE_CALCULATION: SemanticGraphResponse = {
  space_id: "01ef9a2b3c4d5e6f",
  nodes: [
    { id: "east.sales.orders", kind: "table", label: "orders", col: 0, row: 0, coverage: 1 },
    { id: "west.sales.orders", kind: "table", label: "orders", col: 0, row: 1, coverage: 1 },
    { id: "measure:sum · amount", kind: "measure", label: "sum · amount", col: 3, row: 0, governance: "curated", origin: "curated SQL", coverage: 2, expr: "SUM(`amount`)" },
    { id: "measure:sum · amount#sum(amount) @ west.sales.orders", kind: "measure", label: "sum · amount", col: 3, row: 1, governance: "curated", origin: "curated SQL", coverage: 2, expr: "SUM(`amount`)" },
  ],
  edges: [
    { from: "measure:sum · amount", to: "east.sales.orders", kind: "derives" },
    { from: "measure:sum · amount#sum(amount) @ west.sales.orders", to: "west.sales.orders", kind: "derives" },
  ],
  proposals: [],
  coverage_status: "COMPUTED",
  coverage_reason: null,
}

export function ModelTabOneCalculationTwoTablesFrame() {
  return <SemanticModelView graph={TWO_TABLES_ONE_CALCULATION} isLoading={false} error={null} onRefresh={() => {}} />
}

// Not registered: the same graph on the REAL canvas with one chip selected, for the
// selection pin (static markup cannot fire a click).
export function OneCalculationTwoTablesCanvas({ selected }: { selected: string }) {
  return (
    <BlueprintCanvas model={fromSemanticGraph(TWO_TABLES_ONE_CALCULATION)} zoom="mid" selected={selected} layoutMode="fact" onSelect={() => {}} />
  )
}
