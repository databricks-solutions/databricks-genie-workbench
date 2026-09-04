# Ontology — Stage 4.1d Goal-Mode driver (Step 1: deterministic certify + capped super-sure auto-draft)

Copy-paste launcher for **Stage 4.1d Step 1** with a long-running agent (Claude Code /
Cursor Goal Mode). Run on the **`ontology`** branch **after Stage 4.1c landed** (commit
`da561361`). This is the fix for the 4.1c timeout: the batch drafted **all 641 Pages** and
blew the 60-min task cap. Step 1 makes `certify` deterministic and bounds batch drafting to
a hard-capped "super sure" set. Additive/behavior-preserving; offline code + green tests;
**stops before deploy**.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-stage4.1d-build.md`
  §3.1, §5, §6 (umbrella `ontology-curation-redesign-build.md` §8, §11–§14). Steps 2–4
  (body preservation, app single/bulk drafting) are **later** — do NOT build them here.
- **Decisions:** `docs/design/mv-advisor-playbook.md` — **MV-D66** (bounds MV-D65); honor
  MV-D43/D49/D50/D57/D65.
- **Live evidence:** build §1 (run `775414490043851`: 641 sequential drafts, ~68 min, task
  TIMEDOUT at 3600s, no snapshot).

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1d Step 1 — make `certify` a DETERMINISTIC recommendation and bound batch LLM
page-drafting to a hard-capped "super sure" set. Fixes the 4.1c timeout (drafting all 641
pages). Behavior-preserving. Offline code + green tests; STOP before deploy.

SPEC: docs/design/ontology-curation-redesign-stage4.1d-build.md §3.1/§5/§6 (umbrella
…-build.md §8, §11–§14). DECISIONS: mv-advisor-playbook.md MV-D66; honor MV-D43/D49/D50/
D57/D65. Read first. Steps 2–4 (body_source preservation, app single/bulk draft) are LATER —
DO NOT build them.

CONTEXT: pages._finalize drafts EVERY candidate inline (mine_pages loops all concepts →
_finalize → _draft_body) → 641 sequential max_tokens=400 calls → 68 min → task timeout.
And certify (pages.py:991) requires llm_ok, so the only way to make a Page "Ready to
certify" was to LLM-draft it. certify is a curator RECOMMENDATION (mirror "Ready to
certify"); the stub already passes identifier/chunk-safe/specificity gates; llm_ok is prose
polish, not correctness.

BUILD A — decouple certify (pages.py): drop llm_ok from the certify product (line 991) →
`certify = bool(spec.certify_shape and corroborated and syn_ok and not conflict)`. KEEP
llm_ok everywhere else: it still sets evidence.body_source ("llm"/"stub") and still scales
confidence (the `not llm_ok ⇒ ×0.8` line stays). Change NO other gate.

BUILD B — two-pass bounded drafting (pages.mine_pages): stop drafting inline. (1) Pass A:
finalize EVERY candidate with drafter=None (stub body, llm_ok=False) — certify/confidence/
score/evidence now all correct without any LLM. (2) Pass B: from Pass-A candidates keep
certify is True AND evidence.corroboration ≥ page_autodraft_min_corroboration; sort by score
desc, tie-break page_id; take the first page_autodraft_max_pages. (3) Pass C: for each
selected page call the injected drafter on spec.facts(); if non-empty AND it passes the SAME
identifier/chunk-safe/specificity/leakage gates, replace body + set evidence.body_source=
"llm_auto"; else keep the stub (degrade, MV-D43). Re-run flag_duplicates after. Keep the
injected drafter param (MV-D65) — it's just called ≤N times. drafter=None ⇒ fully
deterministic all-stub run (certify still lights up).

BUILD C — config (MV-D57): add page_autodraft_min_corroboration=3 and
page_autodraft_max_pages=50 to OntologySettings + job params + widgets, in-code defaults;
round-trip old rows to defaults (Stage-3/3.2 pattern). max_pages=0 ⇒ zero page LLM calls.
Thread both into mine_pages from run_ontology_materialize.py; KEEP page_drafter wiring +
mlflow.openai.autolog (now bounded). Namer/ER unchanged.

HARD GUARDRAILS: behavior-preserving — NO new API model/route/frame (those are Steps 3–4),
NO new DDL/column (body_source rides evidence JSON, MV-D49), NO governed-tag write, NO new
dependency. Bounded LLM: never more than page_autodraft_max_pages drafter calls. Identity
INJECTED (MV-D65). Degrade-not-hang (MV-D43): missing/raising drafter ⇒ stub, run still
succeeds. No auto-certify (certify is a recommendation).

ACCEPTANCE (offline): test_ontology_pages — corroborated+shape+syn+no-conflict concept ⇒
certify=true WITH drafter=None (no LLM); single-artifact/synonym-short/conflict ⇒ false;
body_source="stub" when undrafted. Cap: a stub drafter that marks bodies ⇒ ≤
page_autodraft_max_pages get body_source="llm_auto" (top-score certify+corroboration≥min,
deterministic order); max_pages=0 ⇒ zero; a raising drafter ⇒ those keep the stub, run
succeeds. test_ontology_materialize — drafter invoked ≤N times regardless of page count;
config round-trips old rows. ./scripts/test.sh + wheel suite green.

WORKFLOW: branch `ontology`. Do NOT deploy, run the job, or touch UC governance. When
offline-green, STOP and report the diff + test summary; a human runs deploy-verify (§7).

---

## After the run (human-gated)
Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger with
`catalog_allowlist=["serverless_stable_6t92c3_catalog"]`, then verify per build §7: job
completes well under the 3600s task timeout; `certify=true` > 0 (deterministic, ~591);
`body_source="llm_auto"` > 0 and ≤ `page_autodraft_max_pages`; no regression (Taxonomy > 0,
0 surfaced-but-unattached).
