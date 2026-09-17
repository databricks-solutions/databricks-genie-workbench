# Ontology Map — captured API fixtures (MV-D77)

Real responses pulled once from the live app (`fevm-serverless`,
`serverless_stable_6t92c3_catalog`) so the dev-only harness tunes visuals against
**prod density**, not toy graphs. Do NOT re-hit the live API — use these.

| File | Endpoint | Shape |
|---|---|---|
| `graph.applied.json` | `GET /api/ontology/graph?origin=applied` | `OntologyGraph` — 17 rollups (3 domains + 13 sub-domains + Ungrouped), 89 assets, `state="fresh"` |
| `graph.proposed.json` | `GET /api/ontology/graph?origin=proposed` | Same shape; **intentionally sparse** (Ungrouped-only) — this estate is fully governed-tag-backed, so there is no `proposed` cluster to render. Use it to prove the Applied↔Proposed toggle + empty-state honesty (MV-D43), not for density. |
| `expand.mv.json` | `GET /api/ontology/graph/expand?node=mv:…cost_attribution` | `OntologyGraphExpand` — 43 `measure` children + `mv_measure` edges |
| `expand.subdomain.json` | `GET /api/ontology/graph/expand?node=sug_89092c5e71ddaf65` | `OntologyGraphExpand` — 50 `page` children + `page_source` edges |

Notes for the harness:
- **MV-D97 authority-encoding demo:** `graph.applied.json` asset (and domain) nodes carry a
  **deterministic synthetic `size`** spread over `[0.5, 2.0]` (hashed from the node id), and a
  deterministic subset of asset nodes carry an additive `meta.certified` / `meta.deprecated`
  flag, so the MV-D80 contact sheet actually exercises popularity-as-radius + certification-as-
  ring + deprecation-muted (the live capture was uniform `size=1.0`, no authority meta). Purely
  a harness demo signal (dev-only fixture); it does not touch prod. Regenerate with the FNV
  hash → `0.5 + frac*1.5` size and `id_hash % 7 → certified` / `% 11 → deprecated` recipe.
- **Focused view** is not a separate fixture — derive it in-memory from `graph.applied.json`
  by passing `focusTop` to the pure model (`estateGraphModel.ts`); the app never fetches a
  focused graph.
- **Slow / failing expand** variants are synthesized in the mock seam (delay / reject), not
  captured — the success shape is `expand.mv.json` / `expand.subdomain.json`.
- Re-capture recipe (only if the contract changes): `databricks auth token -p fevm-serverless`
  → `Authorization: Bearer` against the app URL; expand keys are `mv:<fqn>` (measures) and the
  sub-domain rollup `domain_id` (pages).
