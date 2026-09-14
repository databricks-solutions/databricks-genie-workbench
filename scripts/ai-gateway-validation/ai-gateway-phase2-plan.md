# AI Gateway Migration — Phase 2 Implementation Plan (Model Catalog + BYOK / Site 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `validate_chat_model` accept a customer's own model — a full 3-level Unity Catalog id (`catalog.schema.name`) — when running on the gateway route with BYOK enabled, while the classic route and the default-off state keep rejecting anything off the curated list. Strictly additive; no behavior change until both flags flip.

**Architecture:** Today `validate_chat_model` (`model_catalog.py:90`) accepts only names in `_CURATED_COMPATIBLE_CHAT_MODEL_NAMES` and raises `ModelValidationError` otherwise. Phase 2 adds one additive branch: when `get_llm_route() is LLMRoute.GATEWAY` **and** a new `GENIE_BYOK_ENABLED` flag is on **and** the selected name is a well-formed 3-level UC id, return it unchanged (no strip, no curated-list gate — spec §1.4/§2, Decision 2). The curated→`system.ai.*` mapping already lives in the seam (`gateway_model_name`, wired at the call sites in Phase 1); BYOK 3-level ids pass through that mapper unchanged (Phase 0 test coverage). So Phase 2 is a *validation-relaxation* diff, not a re-mapping diff. Both production callers — `create.py:234/242` (create-agent override) and `auto_optimize.py:1706` (optimize-run model) — inherit the new acceptance for free.

**Tech Stack:** Python 3.12, FastAPI, Databricks SDK; pytest via `./scripts/test.sh` (`uv run --frozen --extra dev pytest`).

**Spec:** `scripts/ai-gateway-validation/ai-gateway-migration-spec.md` — §6 Phase 2, §1.4 (model mapping + entity_name trap), §2 (BYOK rules), §9 Decision 2 (config-only BYOK for v1), Appendix A.2 site 5. Where this plan and the spec differ, the recorded Rulings below win; the spec is binding otherwise.

## Global Constraints

- **Default-off / additive.** With the shipped defaults (`GENIE_LLM_ROUTE=classic`, `GENIE_BYOK_ENABLED=false`), `validate_chat_model` behaves EXACTLY as today: curated names accepted, everything else raises `ModelValidationError`. The BYOK branch is reachable ONLY when route=gateway AND BYOK on.
- **No signature change.** `validate_chat_model(model_name, *, client=None)` keeps its signature; route/BYOK state is read from the environment internally (`get_llm_route()` + `GENIE_BYOK_ENABLED`), exactly as the seam reads its own flag. Callers are untouched.
- **The seam is NOT modified.** `backend/services/llm_route.py` (and its GSO twin) are consumed, never edited — `gateway_model_name`'s 3-level passthrough is already tested (Phase 0). `backend/tests/test_llm_route_parity.py` stays green untouched.
- **No mapping in `model_catalog`.** Do NOT re-implement curated→`system.ai.*` here — that is the seam's job at call time. `validate_chat_model` returns the selected id verbatim (curated name for classic; the 3-level UC id for BYOK).
- **GSO / job config untouched.** GSO has no `validate_chat_model` and uses the passed model directly through the seam; a BYOK model reaches the job via the existing `llm_model` param. **No new job parameter, no 4-mirror change.** Do NOT touch `databricks.yml`, `gso_job.py`, `job_launcher.py`, or `trigger.py`.
- **Backend-only.** Appendix A.2 site 5 is a backend change. Frontend free-text BYOK entry UX is out of scope for this phase (the create/auto-optimize APIs already accept an arbitrary `llm_model` string, which now validates when the flags are on).
- **Test invocation.** Always `./scripts/test.sh <paths>`. Never bare `pytest`.
- **Baseline.** Post-Phase-1b the full suite is **2772 passed**. Phase 2 only adds tests + one additive branch + one env line; the count rises — below 2772 is a regression. Do NOT edit the mv-advisor rule/playbook baseline line (its parity test guards the two copies match each other, not the suite count).

### Recorded Rulings

- **R5 — BYOK well-formedness = conservative 3-level UC identifier.** A "well-formed 3-level UC id" is `^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){2}$` — three non-empty segments of UC-identifier characters separated by exactly two dots. This rejects the entity_name trap (`gte_large_en_v1_5`, no dots), 2-level (`a.b`), 4-level (`a.b.c.d`), and empty-segment (`a..c`) ids. If a real customer UC name legitimately needs other characters, loosen later; conservative-now avoids accepting garbage that would only fail downstream as a 404. *If wrong:* a legitimately-named model with exotic characters is rejected at validation — a loud, safe failure the customer can report, not a silent misroute.
- **R6 — `list_chat_models` is NOT changed.** BYOK is a user-entered free-text id validated by `validate_chat_model` (Decision 2: "the customer enters the full 3-level UC model id"); auto-enumeration of Model Services is explicitly deferred (Decision 2 — no list API surfaces them). So the curated `/api/models` dropdown stays curated-only; the BYOK id arrives via the same `llm_model` override field the pickers already post. *If wrong:* the dropdown simply won't *suggest* BYOK models (by design for v1); acceptance still works.
- **R7 — new flag `GENIE_BYOK_ENABLED` (default `false`), app-env only.** Declared in `app.yaml` beside `GENIE_LLM_ROUTE`. It is a Workbench-side acceptance gate; it is NOT a job parameter (the job validates nothing and receives the chosen model via `llm_model`). Truthy set: `{"1","true","yes","on"}` (case-insensitive), mirroring the tolerant parse style; anything else (incl. unset) = off. *If wrong:* worst case BYOK stays off — fail-safe.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `backend/services/model_catalog.py` | Curated list + `validate_chat_model` (site 5) | Add `GENIE_BYOK_ENABLED` gate + 3-level-UC-id acceptance on gateway; import `get_llm_route`/`LLMRoute` |
| `app.yaml` | App env | Declare `GENIE_BYOK_ENABLED: "false"` beside `GENIE_LLM_ROUTE` |
| `backend/tests/test_model_catalog.py` | Site 5 tests | Add BYOK route/flag/well-formedness matrix + curated-unchanged regression + seam-passthrough contract |
| `backend/tests/test_app_yaml_byok.py` (new) | Env pin | Assert `app.yaml` ships `GENIE_BYOK_ENABLED` default `false` (mirror `test_app_yaml_llm_route.py`) |

**Consumed seam surface (unchanged):** `get_llm_route() -> LLMRoute`, `LLMRoute.GATEWAY`, `gateway_model_name(id)` (3-level passthrough — used only in a test to pin the contract). Import from `backend.services.llm_route`.

---

## Task 1: Accept BYOK 3-level UC ids in `validate_chat_model` (gateway + flag)

**Files:**
- Modify: `backend/services/model_catalog.py` — imports (`:1-12`); add `_byok_enabled` + `_is_wellformed_uc_model_id` helpers; extend `validate_chat_model` body (`:95-104`)
- Modify: `app.yaml` — add `GENIE_BYOK_ENABLED` after the `GENIE_LLM_ROUTE` block (`:98-99`)
- Test: `backend/tests/test_model_catalog.py` (extend); `backend/tests/test_app_yaml_byok.py` (new)

**Interfaces:**
- Consumes: `get_llm_route`, `LLMRoute` (seam); `GENIE_BYOK_ENABLED` env.
- Produces: no signature change — `validate_chat_model` gains an additive acceptance branch; returns the BYOK id verbatim.

- [ ] **Step 1: Write the failing tests**

Extend `backend/tests/test_model_catalog.py` (add these; keep every existing test as-is):

```python
import pytest

from backend.services.llm_route import gateway_model_name


@pytest.fixture
def _gateway_byok_on(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setenv("GENIE_BYOK_ENABLED", "true")


def test_validate_chat_model_accepts_wellformed_byok_id_on_gateway(_gateway_byok_on):
    # BYOK id returned verbatim — no strip, no curated gate.
    assert model_catalog.validate_chat_model("main.byok.my_model_svc") == "main.byok.my_model_svc"


def test_validate_chat_model_curated_still_accepted_on_gateway_byok(_gateway_byok_on):
    assert model_catalog.validate_chat_model("databricks-claude-sonnet-4-6") == "databricks-claude-sonnet-4-6"


def test_byok_id_passes_through_seam_unmangled(_gateway_byok_on):
    # Contract: a validated BYOK id maps to itself through the seam (no strip rule).
    validated = model_catalog.validate_chat_model("main.byok.my_model_svc")
    assert gateway_model_name(validated) == "main.byok.my_model_svc"


def test_validate_chat_model_rejects_byok_id_on_classic_route(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "classic")
    monkeypatch.setenv("GENIE_BYOK_ENABLED", "true")
    with pytest.raises(model_catalog.ModelValidationError):
        model_catalog.validate_chat_model("main.byok.my_model_svc")


def test_validate_chat_model_rejects_byok_id_when_flag_off(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.delenv("GENIE_BYOK_ENABLED", raising=False)
    with pytest.raises(model_catalog.ModelValidationError):
        model_catalog.validate_chat_model("main.byok.my_model_svc")


@pytest.mark.parametrize("bad", ["gte_large_en_v1_5", "a.b", "a.b.c.d", "a..c", "foo", "main.byok."])
def test_validate_chat_model_rejects_malformed_byok_ids(_gateway_byok_on, bad):
    # entity_name trap (no dots), 2-level, 4-level, empty segment, bare name.
    with pytest.raises(model_catalog.ModelValidationError):
        model_catalog.validate_chat_model(bad)
```

Create `backend/tests/test_app_yaml_byok.py`:

```python
"""Pin the BYOK flag default-off in app.yaml (Phase 2).

validate_chat_model reads GENIE_BYOK_ENABLED; app.yaml MUST ship it as
"false" so BYOK acceptance stays off until deliberately enabled
(spec §2, §9 Decision 2).
"""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


def test_genie_byok_enabled_declared_default_off():
    env = _app_env()
    assert "GENIE_BYOK_ENABLED" in env, "app.yaml must declare GENIE_BYOK_ENABLED"
    assert env["GENIE_BYOK_ENABLED"] == "false"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./scripts/test.sh backend/tests/test_model_catalog.py backend/tests/test_app_yaml_byok.py -v`
Expected: FAIL — BYOK ids raise `ModelValidationError` (no branch yet); `app.yaml` has no `GENIE_BYOK_ENABLED`. The existing curated/rejection tests still PASS.

- [ ] **Step 3: Add imports + helpers to `model_catalog.py`**

Add to the imports (`:1-12`):

```python
import re

from backend.services.llm_route import LLMRoute, get_llm_route
```

Add near the other module-level helpers (after `_optimizer_prompt_budget_chars`, before the error classes):

```python
_UC_MODEL_ID_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){2}$")
_BYOK_TRUTHY = {"1", "true", "yes", "on"}


def _byok_enabled() -> bool:
    """GENIE_BYOK_ENABLED gate (spec §2, Decision 2). Off unless explicitly enabled."""
    return (os.environ.get("GENIE_BYOK_ENABLED") or "").strip().lower() in _BYOK_TRUTHY


def _is_wellformed_uc_model_id(name: str) -> bool:
    """A BYOK model is a full 3-level UC id (catalog.schema.name) — never a bare
    endpoint/registered-model name (the entity_name trap, §1.4). Requires exactly
    two dots and three non-empty UC-identifier segments."""
    return bool(_UC_MODEL_ID_RE.match(name))
```

- [ ] **Step 4: Add the additive BYOK branch to `validate_chat_model`**

Replace the tail of `validate_chat_model` (`:99-104`) so BYOK acceptance sits between the curated hit and the raise:

```python
    if selected in _CURATED_COMPATIBLE_CHAT_MODEL_NAMES:
        return selected

    # BYOK (spec §1.4/§2, Decision 2): on the gateway route with BYOK enabled,
    # additionally accept a customer's full 3-level UC model id, returned
    # verbatim (no strip — the seam passes 3-level ids through unchanged).
    # Strictly additive; the classic route and BYOK-off keep rejecting.
    if (
        get_llm_route() is LLMRoute.GATEWAY
        and _byok_enabled()
        and _is_wellformed_uc_model_id(selected)
    ):
        return selected

    raise ModelValidationError(
        f"Model '{selected}' is not in the curated list of supported chat models."
    )
```

- [ ] **Step 5: Declare the flag in `app.yaml`**

After the `GENIE_LLM_ROUTE` block (`app.yaml:98-99`), add:

```yaml
  # ---------------------------------------------------------------------------
  # BYOK (bring-your-own-model) acceptance (spec §2, §9 Decision 2). Default-off:
  # only when "true" AND GENIE_LLM_ROUTE=gateway does validate_chat_model also
  # accept a full 3-level UC model id (catalog.schema.name). Consumed by
  # backend/services/model_catalog.py.
  # ---------------------------------------------------------------------------
  - name: GENIE_BYOK_ENABLED
    value: "false"
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./scripts/test.sh backend/tests/test_model_catalog.py backend/tests/test_app_yaml_byok.py -v`
Expected: PASS (all new + all pre-existing). Confirm no import cycle: `model_catalog` → `llm_route` is one-way (`llm_route` imports only stdlib).

- [ ] **Step 7: Commit**

```bash
git add backend/services/model_catalog.py app.yaml backend/tests/test_model_catalog.py backend/tests/test_app_yaml_byok.py
git commit -m "feat(ai-gateway): Phase 2 site 5 — accept BYOK 3-level UC ids in validate_chat_model (gateway+flag, default-off)"
```

---

## Task 2: Contract grep + full-suite verification

**Files:** none (verification only — no commit unless a gap is found).

**Interfaces:** Consumes Task 1.

- [ ] **Step 1: Full suite green and grew**

Run: `./scripts/test.sh`
Expected: PASS, count ≥ 2772 + new tests. Zero failures. `backend/tests/test_llm_route_parity.py` green (seam untouched).

- [ ] **Step 2: Contract grep (a) — BYOK acceptance is gated on gateway + flag, not standalone**

Run:
```bash
rg -n "get_llm_route|_byok_enabled|_is_wellformed_uc_model_id|GENIE_BYOK_ENABLED" backend/services/model_catalog.py
```
Expected: the acceptance branch requires `get_llm_route() is LLMRoute.GATEWAY` **and** `_byok_enabled()` **and** `_is_wellformed_uc_model_id(...)` together — no branch returns a non-curated id on classic or with the flag off. Paste into the PR.

- [ ] **Step 3: Contract grep (b) — no re-mapping / no `system.ai.` literal added here**

Run:
```bash
rg -n "system\.ai\.|removeprefix|serving-endpoints" backend/services/model_catalog.py
```
Expected: **empty** — `model_catalog` does not map or hard-code gateway URLs/model ids; mapping stays in the seam.

- [ ] **Step 4: Seam + GSO/job config + deps untouched**

Run:
```bash
git diff --name-only <phase2-base>..HEAD -- \
  backend/services/llm_route.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py \
  databricks.yml scripts/deploy_lib/gso_job.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/integration/trigger.py \
  pyproject.toml uv.lock requirements.txt
```
Expected: **empty** — seam twins, all four job-config mirrors, and dependency manifests unchanged (BYOK reaches the job via the existing `llm_model` param).

- [ ] **Step 5: Record results** in the PR / run report (suite delta, both greps, empty untouched-surface diff). No code change; nothing to commit.

---

## Self-Review (run against the spec)

**1. Spec coverage (§6 Phase 2 + Appendix A.2 site 5):**
- `validate_chat_model` accepts arbitrary 3-level UC ids on gateway+BYOK → Task 1 ✅.
- Curated behavior on classic route unchanged; BYOK strictly additive and gated on route=gateway (+flag) → Task 1 tests + Task 2 grep (a) ✅.
- "model_catalog maps curated → `system.ai.*`" → the mapping is the seam's (`gateway_model_name`, wired Phase 1, tested Phase 0); Phase 2 relaxes validation only and pins the passthrough contract (`test_byok_id_passes_through_seam_unmangled`) ✅. Grep (b) proves no mapping leaked into `model_catalog`.
- entity_name-trap / strip-rule / passthrough unit coverage → strip-rule + entity_name idempotence live in the seam's Phase-0 tests; Phase 2 adds the entity_name-trap *rejection* at validation (`gte_large_en_v1_5` rejected) + BYOK passthrough ✅.
- `reasoning_effort:"none"` for BYOK reasoning models → already delivered by the Phase 1 site-2 retry; no new code (noted, not re-implemented).
- Deliberately deferred: auto-enumeration of Model Services (Decision 2), 404→downgrade governance UX (Phase 3), frontend BYOK entry UX.

**2. Placeholder scan:** every step carries real code; anchors fresh this session (`validate_chat_model` at `model_catalog.py:90`, curated set `:14-28`, callers `create.py:234/242` + `auto_optimize.py:1706`, `GENIE_LLM_ROUTE` block `app.yaml:98-99`). No "TBD". ✅

**3. Type consistency:** `validate_chat_model(model_name, *, client=None) -> str | None` unchanged; `get_llm_route()`/`LLMRoute.GATEWAY`/`gateway_model_name` match the seam. ✅

---

## Execution Handoff

Plan complete and saved to `scripts/ai-gateway-validation/ai-gateway-phase2-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, task review between tasks, broad review at the end. Task 2 depends on Task 1; both small.
2. **Inline Execution** — execute in this session via executing-plans.

Which approach?
