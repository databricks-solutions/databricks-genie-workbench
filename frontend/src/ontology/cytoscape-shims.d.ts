/* eslint-disable @typescript-eslint/no-explicit-any */
// Ambient module shims for the estate-graph render libraries (Phase 3e Step B,
// MV-D48). cytoscape / cytoscape-fcose / react-cytoscapejs ship no types and we
// add no @types dependency (MV-D45). These loose declarations keep `tsc` happy;
// the component below constrains the shapes it actually uses.
declare module "cytoscape" {
  const cytoscape: any
  export default cytoscape
}

declare module "cytoscape-fcose" {
  const fcose: any
  export default fcose
}

declare module "react-cytoscapejs" {
  import type { ComponentType } from "react"
  const CytoscapeComponent: ComponentType<any>
  export default CytoscapeComponent
}
