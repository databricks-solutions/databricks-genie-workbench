# Ontology — Stage 4.1i Goal-Mode driver (close the batch page-drafting yield gap)

## ⚙️ Lane header (READ FIRST)

Standalone **wheel-only** follow-up to 4.1h — independent of the Wave-1/Wave-2 lanes, no
merge-order contention (touches one module + its test).

- **OWNS (edit freely):**
  `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/pages.py`
  (`_autodraft`, `default_page_drafter`, one new helper `_canonical_body`);
  `packages/genie-space-optimizer/tests/unit/test_ontology_pages.py`.
- **OFF-LIMITS:** everything else — all of `backend/`, all of `frontend/`, every other
  `ontology/*.py` module, `databricks.yml`, `docs/design/mv-advisor-playbook.md`.
- Offline only. No deploy, no job run. Commit on `ontology`, report diff + test summary, STOP.

---

## Why (evidence)

4.1h landed the drafter↔gate format contract (plain-text prompt + markdown-tolerant
parsers) and lifted live `llm_auto` **0→2/26** eligible Pages. A post-deploy probe ran the
**deployed prompt** against all **24 eligible-but-stub** Pages and bucketed each by the exact
failing gate. The gap is ONE precise, deterministic-to-fix mode:

- **23/24 fail `specificity` solely because the rewritten Definition line has NO backticked
  identifier** (`def_has_backtick=False`). The **Rules already comply** (`rules_all_bt=True`),
  and `identifier` / `chunk_safe` / leakage / empty all pass (0 each).
- The **stub** Definition *does* cite the identifier (that is why it passes); opus paraphrases
  it away — and it is a coin-flip: 1/24 passed on reprobe, matching the nondeterministic 2 live
  successes.

So the fix is to make the identifier-bearing Definition **deterministic**, not to nag the LLM.

- **Spec / decisions:** `mv-advisor-playbook.md` — **MV-D70** (this stage); honor **MV-D66**
  (bounded two-pass auto-draft), **MV-D43** (degrade-not-hang), **MV-D49** (no new DDL — markers
  ride the `evidence` JSON).
- **Seam:** `ontology/pages.py` — `_autodraft` (draft → `identifier_gate` → `chunk_safe ∧
  specificity` → leakage; returns `None`/keeps stub with NO reason today), `default_page_drafter`
  (the 4.1h plain-text prompt), the markdown-tolerant `_definition_lines`/`_rule_lines`, and the
  identifier-bearing `_stub_body` built from `spec.definition`/`spec.rules`.

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1i — close the batch page-drafting yield gap. Live 4.1h left llm_auto=2/26
eligible; a probe proved 23/24 stubbed for ONE reason — opus's rewritten Definition line has no
backticked identifier (Rules comply; identifier/chunk-safe/leakage/empty all pass). Make the
identifier-bearing Definition DETERMINISTIC + record why any draft is rejected. Additive,
wheel-only. Offline green; STOP before deploy.

SPEC: mv-advisor-playbook.md MV-D70 (this stage); honor MV-D66 (bounded two-pass auto-draft),
MV-D43 (degrade-not-hang), MV-D49 (no new DDL — markers ride evidence JSON). Read first.

CONTEXT: .../ontology/pages.py. _autodraft(cand,spec,universe,drafter,oracle) drafts via
drafter(spec.facts()), then identifier_gate -> (chunk_safe AND specificity) -> leakage,
returning None (keep stub) on ANY failure with NO reason recorded. specificity needs >=1
backticked id in the Definition AND each rule. The stub (_stub_body from
spec.definition/spec.rules) already passes every gate; spec.definition cites an in-universe
identifier. _definition_lines/_rule_lines are markdown-tolerant (4.1h).

BUILD A — deterministic Definition-identifier guarantee (_autodraft). After a non-empty draft,
add helper _canonical_body(drafted, spec) that reassembles the draft into the canonical
plain-text skeleton: "Description: <drafted description, else spec.description>"; then
"Definition:\n  <drafted definition IF _definition_lines(drafted) has a backtick ELSE
spec.definition>"; then, if any drafted rule bullets, "Rules:" + "  - <each drafted rule>".
Set evidence["definition_source"]="llm" when the drafted Definition is kept, "deterministic"
when it falls back to spec.definition. Run identifier/chunk_safe/specificity on the REASSEMBLED
body. This guarantees specificity's Definition axis WITHOUT loosening it (the substituted id is
real + in-universe) and preserves the LLM Description + Rules.

BUILD B — reject observability (_autodraft). When the reassembled body still fails a gate, or
the drafter returned empty / raised, set cand.evidence["autodraft_reject"] to the first failing
reason in {"empty","identifier","chunk_safe","specificity"} and return None (keep stub). On
success set evidence["body_source"]="llm_auto" and DELETE any autodraft_reject. Only attempted
(selected super-sure) Pages carry the marker.

BUILD C — prompt one-shot (default_page_drafter). Keep the 4.1h plain-text contract; add ONE
compact worked example whose Definition names its primary Source in backticks, e.g.
'Definition: Total cost is the governed roll-up computed from `catalog.schema.mv`.' Restate that
the Definition MUST contain >=1 backticked Allowed identifier. Do not change what is offered
(Sources only).

HARD GUARDRAILS: additive, wheel-only. NO new table/column/DDL (markers ride evidence, MV-D49);
NO backend/frontend/API/route; NO governed-tag write; NO new dependency. Do NOT loosen any gate
— the fallback only substitutes a real, in-universe, evidence-derived Definition. Degrade-not-hang
(MV-D43): empty/raising drafter => keep stub + record reject. certify/confidence untouched (Pass A).

ACCEPTANCE (offline, test_ontology_pages.py): (1) drafter returns a no-backtick Definition but
valid Rules => body_source="llm_auto", definition_source="deterministic", the Definition now
carries spec's backticked id, Rules are the drafted ones. (2) drafter whose Definition HAS a
backtick => definition_source="llm", Definition preserved. (3) drafter backticks an
out-of-universe id in a Rule => stays stub, autodraft_reject="identifier". (4) empty/raising
drafter on a selected page => stays stub, autodraft_reject="empty". (5) default_page_drafter
prompt contains the worked-example 'computed from `' snippet. ./scripts/test.sh + wheel suite green.

WORKFLOW: branch ontology. Offline only — do NOT deploy or run the job. When green, STOP and
report diff + test summary; a human runs deploy-verify.

---

## After the run (human-gated)

Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger scoped to
`["serverless_stable_6t92c3_catalog"]`, then on `genie_ont_pages`:

- `llm_auto` should jump from **2** toward **~26/26** eligible (the 23 Definition-only failures
  are rescued; the already-passing ones stay).
- `evidence.autodraft_reject` gives a one-SQL yield audit for any residual stub — no reprobe:
  `SELECT get_json_object(evidence,'$.autodraft_reject') r, COUNT(*) FROM …genie_ont_pages
   WHERE get_json_object(evidence,'$.body_source')='stub' AND certify GROUP BY r;`
- Spot-check a `definition_source='deterministic'` Page: LLM Description + Rules, evidence-derived
  identifier-bearing Definition.
