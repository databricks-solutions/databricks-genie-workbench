"""Ontology services (Phase 1, read-only).

  - ``inventory``    OBO ``information_schema`` counts (MV-D43 fast-path)
  - ``tag_graph``    SP governed-tag + assignment reads (MV-D37), TTL-cached
  - ``taxonomy``     deterministic Domain → Sub-Domain tree from the tag graph
  - ``dedupe``       exact + fuzzy collisions + cleanup flags (no embeddings)
  - ``ont_settings`` company name + catalog allowlist persistence (MV-D42)
"""
