# Ontology — Curation Redesign · Stage 2 Goal-Mode driver

Copy-paste launcher for **Stage 2 of the curation redesign** (sub-domains from
explicit boundaries) with a long-running agent (Claude Code / Cursor Goal Mode). Run
it on the **`ontology`** branch **after Stage 1 has landed and been deploy-verified**
(the Alaska Airlines domains surfacing, facets filtered). The prompt is bounded to
the **offline-verifiable** wheel slice; it stops before deploy.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-build.md` (§6;
  also §5 grouping it builds on, §11–§14)
- **Live evidence:** `docs/design/ontology-signal-inventory-findings.md` (§1 slash
  sub-tags, §3 `mvm_subdomain` value tag — the proven explicit boundaries)
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (MV-D54; honor
  MV-D43 / D45 / D49 / D50 / D53)
- **Design context:** `docs/design/ontology-engine-architecture.md` §5–§6
- **Baseline (do NOT regress):** Stage 1 + 17g + 17f + re-grain + 3b + 3a
- **Project rules:** `AGENTS.md`

**Post-Stage-1 reality (verified against the code — reuse, don't re-add):** the slash
parsers already exist (`transforms.domain_part` / `subdomain_part` /
`is_subdomain_key` / `acts_as_subdomain` / `build_taxonomy_dict`), as do
`cluster._bind_level` and `cluster.qualify_subdomain_keys`. The sub-domain split lives
in `cluster.cluster()` (~L702–724): it **unconditionally** re-clusters each Domain's
subgraph at `gamma_fine` via `_run_multiplex`, then binds slash tags onto those
structural sub-communities — the docstring already marks it *"Stage-2 refines the
boundary source."* The one genuinely-new read is threading a per-assignment
`tag_value` through `transforms.assemble_tag_graph` members (today `{fqn, asset_type}`
only) so a value-carrying tag (`mvm_subdomain`, 14 values) can name a sub-domain.

Acceptance is **offline for the code, deploy-gated for verification**: the agent
builds + tests green, then STOPS; a human runs `./scripts/deploy.sh --update`, clicks
**Refresh ontology**, and confirms sub-domains match the governed slash sub-tags /
`mvm_subdomain` values (e.g. Commercial → Reservation, Loyalty, Fare and Pricing…),
each with a plain boundary reason, and Leiden ran only where no explicit boundary
existed.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build the OFFLINE Stage 2 of the ontology curation redesign — SUB-DOMAINS from
EXPLICIT boundaries first, Leiden only as fallback. Branch: ontology, atop LANDED
Stage 1 + 17g/17f/re-grain/3b/3a. Wheel ONLY; NO backend/route/TS/settings/dependency
change.

SPEC (§6 + §11-§14): docs/design/ontology-curation-redesign-build.md
EVIDENCE: ontology-signal-inventory-findings.md (§1 slash sub-tags; §3 mvm_subdomain)
DECISIONS: mv-advisor-playbook.md (MV-D54; honor MV-D43/45/49/50/53)
BASELINE (no regress): Stage 1 + 17g + 17f + re-grain + 3b + 3a. RULES: AGENTS.md.

CONTEXT: Stage 1 grouped assets into DOMAINS and left the SUB-DOMAIN split UNCHANGED —
cluster.cluster() ~L702-724 UNCONDITIONALLY re-clusters each Domain's subgraph at
gamma_fine, then binds slash tags via _bind_level. The docstring marks it "Stage-2
refines the boundary source." INVERT it: an explicit boundary drives the split;
Leiden is fallback.

REUSE, DON'T FORK (all already exist post-Stage-1 — do NOT re-add):
  - transforms.py parsers: domain_part / subdomain_part / is_subdomain_key /
    acts_as_subdomain / build_taxonomy_dict; cluster._bind_level; qualify_subdomain_keys.
  - transforms.assemble_tag_graph — thread a per-assignment tag_value onto each member
    (ADDITIVE; default None keeps tag-only callers BYTE-IDENTICAL) so a value-carrying
    tag (mvm_subdomain) can name a sub-domain. member_fqn_of unchanged.
  - jobs/run_ontology_materialize.py — the tag reader already returns assignment rows;
    include tag_value (information_schema *_tags carry it). Scope by MV-D42 allowlist;
    OBO/run_as (MV-D50); degrade to empty (MV-D43). NO new job task.

BUILD — cluster.py sub-domain derivation, per Domain, in PRECEDENCE:
  (1) SLASH sub-tags: a governed 'Domain/Sub' whose parent is this Domain's tag ->
      one sub-domain per Sub (bind via _bind_level).
  (2) VALUE sub-tag: distinct tag_value of a value-carrying tag on this Domain's
      assets (mvm_subdomain=fare_pricing ...) -> one sub-domain per value.
  (3) SCHEMA-within-domain: a schema wholly inside the Domain's asset set.
  (4) MV / FK component within the Domain.
  Only if NONE apply -> the finer Leiden split (leidenalg RETAINED, gamma_fine, fixed
  seed). Each sub-domain stamps a plain boundary reason into evidence ('sub-tag:
  .../Reservation' | 'mvm_subdomain=fare_pricing' | 'schema: ...' | 'split by
  structure'). Sub-domains stay the EXISTING genie_ont_domains rows with parent_id +
  Domain/Sub tag_value (MV-D49). No new table.

HARD GUARDRAILS:
  - NO new API model/route/frontend/TS; NO new settings column; contracts
    byte-identical; sub-domains ride the existing parent_id/tag_value shape.
  - NO SET/UNSET/CREATE GOVERNED TAG, NO manage_uc_tags, NO web_search.
  - Metastore grain (MV-D49); OBO reads (MV-D50); degrade-not-hang (MV-D43); NO new
    dependency (MV-D45) — uv.lock UNTOUCHED (verify via git status; uv lock --check
    fails structurally on this repo per AGENTS.md).
  - Deterministic + offline; fixed Leiden seed. Leiden runs ONLY when no explicit
    boundary exists — NOT unconditionally. Do NOT change Stage 1 Domain grouping.
  - Do NOT pull forward Stage 3/4, §9 alignment, §10 harness. NO DEPLOY.

ACCEPTANCE (./scripts/test.sh green): slash sub-tag -> sub-domain (parent 'X', child
'X/Y' -> Y under X); mvm_subdomain value -> sub-domain; schema-within-domain ->
sub-domain; a Domain with NO explicit boundary falls back to Leiden (asserted: Leiden
invoked ONLY there); each sub-domain carries a boundary reason; tag_value threading
keeps tag-only graphs byte-identical; contract-frozen (taxonomy/tag-lens/routes);
firewall unchanged.
npm run lint + tsc clean; uv.lock untouched (git); §6 true.

WORKFLOW: transforms.assemble_tag_graph (tag_value) -> job reader (include tag_value)
  -> cluster.py (explicit-boundary precedence + Leiden fallback + reasons) -> tests.
  ./scripts/test.sh per slice. STOP and ask if ambiguous or a guardrail would break.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{cluster,transforms}.py,
                           #   packages/.../jobs/run_ontology_materialize.py, packages/.../tests/unit/**
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..
git status --porcelain uv.lock   # UNCHANGED (MV-D45; uv lock --check fails structurally here)

git add packages/genie-space-optimizer
git commit -m "feat(ontology): Stage 2 sub-domains from explicit boundaries (slash sub-tags + mvm_subdomain value), Leiden fallback (MV-D54)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds the wheel + redeploys the job (no dep change)
# In the live app: Ontology → Refresh ontology. Confirm sub-domains match the
#   governed slash sub-tags / mvm_subdomain values, e.g.:
#     Alaska Airlines Commercial → Reservation, Loyalty, Fare and Pricing, Yield
#       Management, Ticketing and Settlement, Passenger
#     Alaska Airlines Operations → Route, Flight, Airport, Fleet
#   each with a boundary reason; Leiden appears only where no explicit boundary exists.
# Query to eyeball:
#   SELECT p.name domain, d.name subdomain, d.tag_value, get_json_object(d.evidence,'$.reason') reason
#   FROM <gso_catalog>.genie_space_optimizer.genie_ont_domains d
#   JOIN <gso_catalog>.genie_space_optimizer.genie_ont_domains p
#     ON d.parent_id=p.domain_id AND d.metastore_id=p.metastore_id
#   ORDER BY domain, subdomain;
```

Stage 2 is offline and deterministic, but the tag/value reads, the Delta write, and
the synced mirror can only be validated in a deployed app — which is why the offline
slice stops here. Stage 3 (gates + config + honest confidence) builds next.
