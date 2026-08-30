"""Ontology batch-side package (Phase 2).

Wheel-importable by the backend (the Phase-1 services import the pure transforms
back), and runnable on a job cluster (must NOT import ``backend.*``). Holds:

  - ``transforms``   PURE transforms shared with the Phase-1 routes (parity source)
  - ``ddl``          Delta DDL for all genie_ont_* tables (snapshots + empty Phase-3)
  - ``graph``        L2 fused signal-graph (nodes/edges; the clustering input)
  - ``er``           L3 entity resolution / dedupe -> the canonical identity map
  - ``cluster``      L4 domain / sub-domain clustering (Leiden-CPM, multiplex, seeded)
  - ``materialize``  SP reads → transforms → idempotent Delta MERGE of the snapshots
                     + identity map + Domain/Member proposals

Read-only w.r.t. Unity Catalog governance: the ONLY UC writes are the genie_ont_*
Delta snapshot MERGEs. No SET/UNSET/CREATE/ALTER/DROP governed-tag DDL anywhere.
See ``docs/design/ontology-phase2-build.md``.
"""
