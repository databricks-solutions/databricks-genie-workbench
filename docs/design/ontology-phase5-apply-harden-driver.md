# Ontology — Phase 5 (17i) Stage 2: harden + go-live · Goal-Mode driver

> **One stage per run, on the `ontology` branch.** Spec of record:
> `ontology-phase5-apply-build.md` (§1–§12). The offline slice (dry-run preview + OBO execute +
> `genie_ont_applied` audit + consent-flip + `ApplyPreview.tsx`/`DomainDraftCard` + single-writer
> carve) is **already BUILT + offline-green** (`9e1a82c4`). This stage closes the gaps that block a
> trustworthy LIVE apply, all additive, then hands off to a human deploy-verify. **STOP before the
> live write** — the governed-tag write on a real metastore is the human gate (17.0d).

## Why now
Signal Authority (P6) is complete; P2 (the one write path) is the value-unlock. The apply flow is
wired end-to-end but has **never touched live UC**, and four gaps make a real apply unsafe/dishonest:
the grant probe is stubbed, statements are f-string-interpolated (injection), the diff can't show
adds-vs-moves, and the governed-tag SQL syntax is unverified.

## Grounded facts (code, 2026-09-17)
- `services/apply.py` marks every item `executable=True` with `# TODO: probe …` — the
  **blocked → copy-ready-with-exact-grant** degrade path (a core guardrail + the "no-grant account
  never hangs" UX promise) never fires. `preflight.py`'s `membership_write` tier is still the Phase-1
  `not_exercised` placeholder.
- Statements + the audit `INSERT` + the consent `UPDATE` interpolate values via f-strings;
  `StatementParameterListItem` is imported but UNUSED. A tag value with `'` breaks the SQL.
- `current_value` is `None` everywhere (`# TODO: probe current value`) → the "diff" is "what we'll
  set", not "adds vs moves".
- `_write_audit_row` + `_flip_consents` run as the **SP** (`get_service_principal_client`); the
  `SET TAG` runs under **OBO** (`require_obo_workspace_client`) — correct, but the MV-D50 guardrail
  text says audit/flip should be OBO too. **DECISION (owner, 2026-09-17): keep SP for the table
  bookkeeping (the SP owns `genie_ont_applied`/`genie_ont_consents`); the *governed-tag write* is
  OBO.** Amend the guardrail + firewall test to match — do NOT move audit/flip to OBO.
- `grants.py` already has `app_service_principal()` + `browse_grant_line()` — the probe home. The
  firewall carve `_APPLY_ALLOWED=["set tag","unset tag","create governed tag"]` keeps those tokens in
  `services/apply.py` ONLY (`test_no_write_path_in_ontology_module`); parameterizing keeps the tokens
  in `apply.py`, so the carve still holds.

## Testability seam
All changes are additive to `services/apply.py` + `services/grants.py` + `routers/preflight.py`.
No new routes (`test_backend_router_verbs…` allowlist stays `apply/drafts/refresh`). Degrade-clean:
a probe failure ⇒ item stays optimistically executable (execute's per-statement fail-soft still
catches `PERMISSION_DENIED` → copy-ready), never a hang (MV-D43).

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Phase 5 Stage 2 (harden + go-live)** of `ontology-phase5-apply-build.md` on the
`ontology` branch. Additive, offline-green, no new dep, no new route. STOP before the live write.

STEP 0 — READ (AGENTS.md References rule): WebFetch the Databricks governed-tag + domains docs
(`/aws/en/data-governance/unity-catalog/tags`, `/aws/en/uc-semantics/domains`) and CORRECT the
exact `CREATE GOVERNED TAG` / `SET TAG` / `UNSET TAG` syntax + `{parent}/{child}` sub-domain
convention in `services/apply.py`'s builder if it diverges (e.g. `ALTER ASSET … SET TAG`,
`WITH ALLOWED_VALUES`). Record the verified syntax in a code comment.

BUILD A — SQL safety (`services/apply.py`): parameterize every write. Bind VALUES via
`StatementParameterListItem` (tag values, and all audit-INSERT / consent-UPDATE columns —
metastore_id, apply_id, statement text, error, applied_by, timestamps). IDENTIFIERS that can't be
bound (asset FQN, tag_key) → escape by doubling backticks inside a backtick-quoted identifier. Thread
`parameters=[…]` into every `execute_statement`. `plan_hash` fingerprints the statement TEMPLATE +
bound values so preview/execute still match.

BUILD B — grant probe (`services/grants.py` + `services/apply.py` + `routers/preflight.py`): add
`membership_write_probe(client, target_fqn, tag_key) -> (ok: bool, missing: list[str])` that reads
the OBO user's effective privileges (APPLY TAG / USE SCHEMA / USE CATALOG on the asset; MANAGE/ASSIGN
on the governed tag) WITHOUT writing. In `build_apply_plan`, call it per item: set
`executable`/`blocked_reason`/`required_grants` (copy-ready GRANT lines) from the result; a probe
FAILURE ⇒ leave `executable=True` (execute fail-soft still degrades to copy-ready — MV-D43). Make the
`preflight.py` `membership_write` tier REAL (mirror the enrichment-tier shape: ok / blocked + grant
lines), replacing the `not_exercised` placeholder.

BUILD C — honest diff (`services/apply.py`): probe each target's `current_value` for the tag (read-
only) so a set on a tagless asset reads "add" and a set over an existing value reads "move". Absent ⇒
None (byte-identical to today).

BUILD D — guardrail reconcile: amend the MV-D50 note in `apply.py` + `ontology-phase5-apply-build.md`
and `backend/tests/test_ontology_firewall.py` to state: the governed-tag WRITE is OBO; audit +
consent-flip bookkeeping is SP (the SP owns those tables). Do NOT move audit/flip to OBO.

GUARDRAILS (hard): additive only; single-writer carve intact (`set tag`/`unset tag`/`create governed
tag` in `services/apply.py` ONLY — `test_no_write_path_in_ontology_module` stays green); no new route
(`test_backend_router_verbs…` unchanged); dry-run writes NOTHING; execute still gated on
confirm=true + plan_hash (409); per-statement fail-soft, never 500; idempotent flip; DEFAULT-OFF
posture (no live write without confirm); no new dep (`uv.lock`/`package-lock.json` untouched).

TESTS (`packages/.../tests/unit/` + `backend/tests/` + `frontend/`): parameterized statements carry
bound params + escaped identifiers (a tag value with `'` is safe); grant probe → executable/blocked +
required_grants; blocked item ⇒ copy-ready, not executed; `current_value` add-vs-move; preflight
`membership_write` returns ok/blocked; firewall carve + POST allowlist still green; frontend renders
blocked/copy-ready rows.

ACCEPTANCE (offline): `./scripts/test.sh` green; `cd frontend && npx tsc -b && npm run lint && npm run
test`; `uv lock --check`; lockfiles untouched. Then STOP for the live deploy-verify gate.

---

## Deploy-verify gate (human, after offline-green — the STOP checkpoint)
`DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`, then in the LIVE app
on a real estate:
1. Approve ONE small domain draft (17g) → "Apply for me → Preview changes". Confirm the diff shows
   adds/moves + any blocked items with the EXACT grant (copy-ready).
2. "Apply these changes" → confirm the governed tag / membership lands in Unity Catalog **attributed
   to YOU via OBO**, a `genie_ont_applied` audit row exists, the consent flips `approved→applied`.
3. Re-preview: the applied proposal is GONE (idempotent). Verify NOTHING else was written (no
   Page/Agent/card write).
4. An account WITHOUT the write grant sees copy-ready, never a hang.
Review apply-safety (dry-run purity, consent gate, OBO attribution, single-writer carve, degrade) with
a human before 17j (hardening + E2E + undo).
