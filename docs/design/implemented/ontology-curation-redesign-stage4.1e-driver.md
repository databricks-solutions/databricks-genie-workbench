# Ontology — Stage 4.1e Goal-Mode driver (the real Step-1 timeout fix: bound LLM naming to gate-survivors + concurrency + timeout headroom)

Copy-paste launcher for **Stage 4.1e** with a long-running agent. Run on the **`ontology`**
branch **after 4.1d Step 1 landed** (commit `4dbc9d95`). Step 1 bounded page drafting but the
job **still TIMEDOUT at 60 min** (run `737477992104319`) — because the real cost is **LLM
domain naming**, which fires per non-tag-bound cluster **before** the legitimacy gate, on slow
opus. This stage names **only gate-survivors**, adds bounded concurrency, and bumps the task
timeout. Additive/behavior-preserving; offline code + green tests; **stops before deploy**.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-stage4.1e-build.md`
  §3, §5, §6. **Decisions:** `mv-advisor-playbook.md` — **MV-D67**; honor
  MV-D43/D49/D50/D65/D66.
- **Live evidence:** build §1 (runs `775414490043851` and `737477992104319` both hit the
  exact 3600s task cap ⇒ the naming/ER tail is the constant, not page drafts).
- **Seam:** `ontology/materialize.py run_materialize` (cluster→gate→re-MERGE order, lines
  462/487/517/526/529), `ontology/cluster.py` (`cluster`, `_cluster_name`, `default_namer`,
  `name_leaks`), `ontology/pages.py mine_pages` Pass C, `databricks.yml` (job timeout).

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1e — fix the 4.1d Step-1 timeout by bounding LLM domain NAMING to gate-
survivors (name only surfaced, non-tag-bound domains — hundreds→dozens), add bounded
concurrency to the LLM loops, and bump the task timeout. Behavior-preserving. Offline code +
green tests; STOP before deploy.

SPEC: docs/design/ontology-curation-redesign-stage4.1e-build.md §3/§5/§6. DECISIONS:
mv-advisor-playbook.md MV-D67; honor MV-D43/D49/D50/D65/D66. Read first.

CONTEXT: materialize.run_materialize calls cluster.cluster(namer=namer) which LLM-names EVERY
non-tag-bound cluster (cluster._cluster_name) — and it runs BEFORE rank.mark_surfaced, so all
the noise domains the gate later prunes still cost one slow opus call each. Tag-bound
(reuse/reassign) clusters already skip the LLM. Both prior runs hit the exact 3600s cap even
with pages bounded to 50 ⇒ naming is the hog, not drafting.

BUILD A — defer + gate naming (materialize.py + cluster.py): call cluster.cluster(...,
namer=None) so clustering yields deterministic anchor names (tag-bound still take governed
vocab; no LLM). Add cluster.rename_surfaced(proposals, domain_rows, *, namer, company, oracle,
max_workers): AFTER rank.mark_surfaced and BEFORE the final domain re-MERGE, for each
domain_row with surfaced is True whose proposal is NON-tag-bound, call the namer on the
proposal's sorted member identifiers + anchor + company, validate with the existing name_leaks
/ LeakageOracle, and update domain_row["name"] (and the proposal) on success; empty/raise ⇒
keep the deterministic name (MV-D43). Never LLM-name tag-bound or non-surfaced rows. Keep the
namer param on run_materialize (injected, MV-D65) — it's now consumed by rename_surfaced, not
cluster.cluster. namer=None ⇒ fully deterministic names.

BUILD B — bounded concurrency (cluster.rename_surfaced + pages.mine_pages Pass C): run the
surfaced renames and the ≤page_autodraft_max_pages super-sure drafts under a
ThreadPoolExecutor(max_workers=k), k default 4 (a config knob, MV-D57 pattern; job param +
widget + OntologySettings, round-trip old rows). Compute the eligible/selected set FIRST
(deterministic order), fan out only the pure LLM calls, then apply results deterministically
so the written snapshot is identical for any k. Per-item errors degrade to the deterministic
name/stub. Stdlib only — NO new dependency. k=1 == sequential.

BUILD C — timeout headroom (databricks.yml): raise the ontology_materialize task
timeout_seconds 3600 → 5400. Insurance only.

HARD GUARDRAILS: additive/behavior-preserving — NO change to WHICH domains surface (gate
logic untouched), NO new API/route/frontend, NO new DDL/column (name exists; knobs are
settings/params), NO governed-tag write, NO new dependency. Namer invoked at most once per
surfaced non-tag-bound domain — never per raw cluster. Deterministic-first: namer/drafter=None
⇒ deterministic run; snapshot worker-count-invariant. Identity injected (MV-D65).
Degrade-not-hang (MV-D43).

ACCEPTANCE (offline): test_ontology_cluster — cluster(namer=None) ⇒ deterministic names;
rename_surfaced renames ONLY surfaced non-tag-bound (tag-bound + non-surfaced unchanged),
raising namer keeps deterministic name, namer called exactly #surfaced-non-tag-bound times,
k=1 vs k>1 identical. test_ontology_pages — Pass-C concurrency drafts the same ≤N set, never
more than max_workers in flight (instrumented stub), snapshot == k=1. test_ontology_materialize
— namer called O(#surfaced), not O(#raw clusters); deterministic names written first, upgraded
on re-MERGE. ./scripts/test.sh + wheel suite green.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs deploy-verify (§7).

---

## After the run (human-gated)
Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger with
`catalog_allowlist=["serverless_stable_6t92c3_catalog"]`, then verify per build §7: job
completes well under 3600s; snapshot written (`succeeded`); `certify=true` > 0;
`body_source="llm_auto"` in (0, cap]; surfaced domains carry LLM names; no regression
(Taxonomy > 0, 0 surfaced-but-unattached).
