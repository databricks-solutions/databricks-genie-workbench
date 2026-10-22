# AI Gateway Migration — Phase 0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the AI Gateway routing decision point (`GENIE_LLM_ROUTE`, default `classic`) as one stateless seam module — mirrored byte-identically into the backend app and the GSO wheel — plus its config plumbing, with zero behavior change and no call site touched.

**Architecture:** A pure/stateless factory (`resolve_chat` / `resolve_embeddings` returning a frozen `ResolvedCall`, plus `gateway_model_name` / `request_tags` / `tag_header` / `is_reasoning_effort_400` primitives) reads `GENIE_LLM_ROUTE` from the environment and, on the default `classic`, returns today's exact classic URL/headers. Because the Workbench app (`backend/`) and the GSO engine (`packages/genie-space-optimizer/`) do **not** share a Python package, the module ships as **two byte-identical files** pinned by a parity test. Nothing imports the factory yet; the `GENIE_LLM_ROUTE` job parameter is plumbed through the same mirrors as `llm_model` but consumed by no one until Phase 1.

**Tech Stack:** Python 3.11+ (stdlib only: `json`, `os`, `dataclasses`, `enum`), pytest + `pytest-asyncio` (dev extra), Databricks Asset Bundles (`databricks.yml`), Databricks Apps (`app.yaml`).

**Spec:** `scripts/ai-gateway-validation/ai-gateway-migration-spec.md` (this plan implements §6 "Phase 0 — Seam scaffold", the module in Appendix A.1, the tag rules in §1.3–1.4, and the config lockstep in §4). Evidence for every claim: `scripts/ai-gateway-validation/report-20260913-175958.md`.

## Global Constraints

- **Default-off, fail-safe:** `GENIE_LLM_ROUTE` defaults to `classic`. Anything other than the exact string `gateway` (unset, typo, wrong case is normalized) resolves to `CLASSIC`. (Spec §0, Appendix A.1 `get_llm_route`.)
- **Byte-identical classic branch:** on `classic`, `resolve_chat` MUST return `f"{host}/serving-endpoints/{endpoint}/invocations"` with `model=None` and `extra_headers={}` — the exact string built today at `backend/services/llm_utils.py:86`. (Spec §6 Phase 0 regression guard.)
- **No call site touched:** do NOT modify `llm_utils.py`, `create_agent.py`, `llm_client.py`, `leakage.py`, or `model_catalog.py` in Phase 0. Those are Phase 1/1b/2/3. (Spec §6.)
- **Two byte-identical modules:** `backend/services/llm_route.py` and `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` must be identical bytes, pinned by a parity test (mechanism: file-content comparison, like the described `test_rules_parity.py`). Do not run a formatter on one without the other. (Spec Appendix A.)
- **Config lockstep follows the `llm_model` precedent, NOT the generic four-mirror rule:** `genie_llm_route` is a Workbench-only extra. It is added to root `databricks.yml`, `scripts/deploy_lib/gso_job.py`, and `job_launcher.py`'s run-now map — and is **intentionally absent** from `packages/genie-space-optimizer/databricks.yml`, exactly like `llm_model`. (Spec §4 and its "verified" note; `scripts/deploy_lib/gso_job.py:32`.)
- **Tag vocabulary (proposed, §1.3):** the tag header key is `Databricks-Ai-Gateway-Request-Tags`; the base tag is `{"application":"genie-workbench","component":<feature>}`; scoped extras `run_id`/`space_id` are dropped when empty (Decision 1). Phase 0 only ships the builders; no call site emits a tag yet.
- **Test baseline:** do not regress `./scripts/test.sh`. Measured floor is **681 backend + 1512 GSO** (per the mv-advisor rule ledger). New tests are additive growth; record the new totals in the final VERIFY. Run via `./scripts/test.sh` (which uses `uv run --frozen --extra dev`); a bare `pytest` omits `pytest-asyncio` and fails 12 async tests — that is an invocation defect, not a code defect.
- **No new dependencies:** the module is stdlib-only. Do not touch `pyproject.toml`, `uv.lock`, or `requirements.txt`.

---

## File Structure

**New files (created by this plan):**

| Path | Responsibility |
|---|---|
| `backend/services/llm_route.py` | The seam factory (backend copy). Stateless route resolution + tag builders. |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` | Byte-identical GSO copy of the same module. |
| `backend/tests/test_llm_route.py` | Unit tests for the backend module (classic byte-identical, gateway shape, model mapping, tags, env resolution). |
| `packages/genie-space-optimizer/tests/unit/test_llm_route.py` | Unit tests for the GSO copy (same assertions, GSO import path). |
| `backend/tests/test_llm_route_parity.py` | Pins the two module files byte-identical. |
| `backend/tests/test_app_yaml_llm_route.py` | Asserts `app.yaml` declares `GENIE_LLM_ROUTE` default `classic`. |

**Modified files:**

| Path | Change |
|---|---|
| `app.yaml` | Add `GENIE_LLM_ROUTE` env, value `"classic"`. |
| `databricks.yml` | Declare `genie_llm_route` job param (default `"classic"`) + pass through all 4 tasks' `base_parameters`. |
| `scripts/deploy_lib/gso_job.py` | Add `genie_llm_route` to each task's `base_param_keys` + to `JOB_PARAMETERS`. |
| `packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py` | Add `genie_llm_route` kwarg + run-now `job_parameters` entry. |
| `backend/tests/test_deploy_lib.py` | Update `test_four_way_declared_param_sets_stay_in_lockstep` to sanction `genie_llm_route` as a second Workbench-only extra + add a positive default-off assertion. |

---

## Task 1: Backend seam module + unit tests

**Files:**
- Create: `backend/services/llm_route.py`
- Test: `backend/tests/test_llm_route.py`

**Interfaces:**
- Consumes: nothing (stdlib only).
- Produces (relied on by Task 2's parity test and by Phase 1 adoption):
  - `class LLMRoute(str, Enum)` with `CLASSIC="classic"`, `GATEWAY="gateway"`
  - `get_llm_route() -> LLMRoute`
  - `gateway_model_name(endpoint: str) -> str`
  - `request_tags(component: str, *, run_id: str | None = None, space_id: str | None = None) -> dict[str, str]`
  - `tag_header(component: str, **scope) -> dict[str, str]`
  - `@dataclass(frozen=True) class ResolvedCall(url: str, model: str | None, extra_headers: dict[str, str], use_legacy_sdk: bool = False)`
  - `resolve_chat(host, endpoint, component, *, route=None, run_id=None, space_id=None) -> ResolvedCall`
  - `resolve_embeddings(host, endpoint, component, *, route=None, run_id=None, space_id=None) -> ResolvedCall`
  - `is_reasoning_effort_400(status: int, body_text: str) -> bool`

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_llm_route.py`:

```python
"""Phase 0 unit tests for the AI Gateway seam factory (backend copy).

The classic branch MUST be byte-identical to today's call sites; the gateway
branch is validated shape-only (no network). See
scripts/ai-gateway-validation/ai-gateway-migration-spec.md Appendix A.1.
"""

import json

import pytest

from backend.services import llm_route as lr


class TestGetLlmRoute:
    def test_unset_defaults_classic(self, monkeypatch):
        monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
        assert lr.get_llm_route() is lr.LLMRoute.CLASSIC

    def test_typo_defaults_classic(self, monkeypatch):
        monkeypatch.setenv("GENIE_LLM_ROUTE", "gatway")
        assert lr.get_llm_route() is lr.LLMRoute.CLASSIC

    def test_gateway_exact(self, monkeypatch):
        monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
        assert lr.get_llm_route() is lr.LLMRoute.GATEWAY

    def test_gateway_case_and_whitespace_normalized(self, monkeypatch):
        monkeypatch.setenv("GENIE_LLM_ROUTE", "  GATEWAY ")
        assert lr.get_llm_route() is lr.LLMRoute.GATEWAY


class TestGatewayModelName:
    def test_databricks_prefix_stripped(self):
        assert lr.gateway_model_name("databricks-claude-sonnet-4-6") == "system.ai.claude-sonnet-4-6"
        assert lr.gateway_model_name("databricks-gte-large-en") == "system.ai.gte-large-en"

    def test_system_ai_idempotent(self):
        assert lr.gateway_model_name("system.ai.gte-large-en") == "system.ai.gte-large-en"

    def test_byok_three_level_passthrough(self):
        # A 3-level UC id (contains a dot) must pass through unchanged.
        assert lr.gateway_model_name("cat.sch.my_model") == "cat.sch.my_model"


class TestRequestTags:
    def test_base_only(self):
        assert lr.request_tags("iq-scan") == {
            "application": "genie-workbench",
            "component": "iq-scan",
        }

    def test_scoped_extras_dropped_when_empty(self):
        assert lr.request_tags("gso-optimize", run_id=None, space_id="") == {
            "application": "genie-workbench",
            "component": "gso-optimize",
        }

    def test_scoped_extras_present(self):
        assert lr.request_tags("create-agent", space_id="sp1", run_id="r1") == {
            "application": "genie-workbench",
            "component": "create-agent",
            "run_id": "r1",
            "space_id": "sp1",
        }

    def test_tag_header_is_json_under_the_gateway_key(self):
        header = lr.tag_header("mv-suggest", space_id="sp9")
        assert set(header) == {"Databricks-Ai-Gateway-Request-Tags"}
        assert json.loads(header["Databricks-Ai-Gateway-Request-Tags"]) == {
            "application": "genie-workbench",
            "component": "mv-suggest",
            "space_id": "sp9",
        }


class TestResolveChat:
    def test_classic_is_byte_identical_to_today(self):
        # Mirrors backend/services/llm_utils.py:86 exactly:
        #   host = client.config.host.rstrip("/")
        #   url  = f"{host}/serving-endpoints/{model}/invocations"
        rc = lr.resolve_chat(
            "https://example.databricks.com/",
            "databricks-claude-sonnet-4-6",
            "workbench",
            route=lr.LLMRoute.CLASSIC,
        )
        assert rc.url == "https://example.databricks.com/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
        assert rc.model is None
        assert rc.extra_headers == {}
        assert rc.use_legacy_sdk is False

    def test_gateway_shape(self):
        rc = lr.resolve_chat(
            "https://example.databricks.com",
            "databricks-claude-sonnet-4-6",
            "create-agent",
            route=lr.LLMRoute.GATEWAY,
            space_id="sp1",
        )
        assert rc.url == "https://example.databricks.com/ai-gateway/mlflow/v1/chat/completions"
        assert rc.model == "system.ai.claude-sonnet-4-6"
        assert json.loads(rc.extra_headers["Databricks-Ai-Gateway-Request-Tags"]) == {
            "application": "genie-workbench",
            "component": "create-agent",
            "space_id": "sp1",
        }


class TestResolveEmbeddings:
    def test_classic_keeps_legacy_sdk(self):
        rc = lr.resolve_embeddings(
            "https://x", "databricks-gte-large-en", "leakage-embed", route=lr.LLMRoute.CLASSIC
        )
        assert rc.use_legacy_sdk is True
        assert rc.url == ""
        assert rc.model is None
        assert rc.extra_headers == {}

    def test_gateway_shape(self):
        rc = lr.resolve_embeddings(
            "https://x/", "databricks-gte-large-en", "leakage-embed", route=lr.LLMRoute.GATEWAY
        )
        assert rc.url == "https://x/ai-gateway/mlflow/v1/embeddings"
        assert rc.model == "system.ai.gte-large-en"
        assert rc.use_legacy_sdk is False


class TestIsReasoningEffort400:
    def test_true_only_on_400_mentioning_flag(self):
        assert lr.is_reasoning_effort_400(400, 'Extra inputs: reasoning_effort') is True

    def test_false_on_other_400(self):
        assert lr.is_reasoning_effort_400(400, "response_format not permitted") is False

    def test_false_on_non_400(self):
        assert lr.is_reasoning_effort_400(200, "reasoning_effort") is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./scripts/test.sh backend/tests/test_llm_route.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'backend.services.llm_route'`.

- [ ] **Step 3: Write minimal implementation**

Create `backend/services/llm_route.py` (verbatim from spec Appendix A.1 — this exact text becomes the byte-identical source of truth for Task 2):

```python
from __future__ import annotations
import json, os
from dataclasses import dataclass
from enum import Enum


class LLMRoute(str, Enum):
    CLASSIC = "classic"
    GATEWAY = "gateway"


_GATEWAY_CHAT  = "/ai-gateway/mlflow/v1/chat/completions"
_GATEWAY_EMBED = "/ai-gateway/mlflow/v1/embeddings"
_TAG_HEADER    = "Databricks-Ai-Gateway-Request-Tags"
_APPLICATION   = "genie-workbench"


def get_llm_route() -> LLMRoute:
    """GENIE_LLM_ROUTE env; anything but 'gateway' (incl. unset/typo) -> CLASSIC (fail-safe)."""
    return LLMRoute.GATEWAY if (os.environ.get("GENIE_LLM_ROUTE") or "").strip().lower() == "gateway" \
        else LLMRoute.CLASSIC


def gateway_model_name(endpoint: str) -> str:
    """Classic endpoint name -> gateway model id.
       '<cat>.<sch>.<name>' (BYOK 3-level UC id) -> unchanged;
       already 'system.ai.*' -> unchanged;
       'databricks-<x>' -> 'system.ai.<x>' (validated for chat AND embeddings)."""
    if "." in endpoint or endpoint.startswith("system.ai."):
        return endpoint
    return "system.ai." + endpoint.removeprefix("databricks-")


def request_tags(component: str, *, run_id: str | None = None,
                 space_id: str | None = None) -> dict[str, str]:
    tags = {"application": _APPLICATION, "component": component}
    if run_id:   tags["run_id"] = run_id       # scoped extras (Decision 1); empties dropped
    if space_id: tags["space_id"] = space_id
    return tags


def tag_header(component: str, **scope) -> dict[str, str]:
    return {_TAG_HEADER: json.dumps(request_tags(component, **scope))}


@dataclass(frozen=True)
class ResolvedCall:
    url: str                        # full endpoint URL ("" == "keep the legacy SDK path", embeddings)
    model: str | None               # model id for the BODY (gateway); None when it's in the URL (classic)
    extra_headers: dict[str, str]   # merge into the request (tag header on gateway; {} on classic)
    use_legacy_sdk: bool = False    # embeddings classic branch -> keep w.serving_endpoints.query()


def resolve_chat(host: str, endpoint: str, component: str, *, route: LLMRoute | None = None,
                 run_id: str | None = None, space_id: str | None = None) -> ResolvedCall:
    host = host.rstrip("/"); r = route or get_llm_route()
    if r is LLMRoute.CLASSIC:
        return ResolvedCall(f"{host}/serving-endpoints/{endpoint}/invocations", None, {})
    return ResolvedCall(f"{host}{_GATEWAY_CHAT}", gateway_model_name(endpoint),
                        tag_header(component, run_id=run_id, space_id=space_id))


def resolve_embeddings(host: str, endpoint: str, component: str, *, route: LLMRoute | None = None,
                       run_id: str | None = None, space_id: str | None = None) -> ResolvedCall:
    host = host.rstrip("/"); r = route or get_llm_route()
    if r is LLMRoute.CLASSIC:
        return ResolvedCall("", None, {}, use_legacy_sdk=True)   # keep SDK query()
    return ResolvedCall(f"{host}{_GATEWAY_EMBED}", gateway_model_name(endpoint),
                        tag_header(component, run_id=run_id, space_id=space_id))


def is_reasoning_effort_400(status: int, body_text: str) -> bool:
    """Retry trigger for the tool path (§2): Claude 400s on the flag, reasoning models require it."""
    return status == 400 and "reasoning_effort" in body_text
```

> Note: `route or get_llm_route()` means passing `route=LLMRoute.CLASSIC` explicitly is honored (a truthy enum member), so the env-independent unit tests above are deterministic.

- [ ] **Step 4: Run test to verify it passes**

Run: `./scripts/test.sh backend/tests/test_llm_route.py -v`
Expected: PASS (all classes green).

- [ ] **Step 5: Commit**

```bash
git add backend/services/llm_route.py backend/tests/test_llm_route.py
git commit -m "feat(ai-gateway): Phase 0 seam factory (backend) — default-off, classic byte-identical"
```

---

## Task 2: GSO twin module + GSO unit tests + parity pin

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_llm_route.py`
- Test: `backend/tests/test_llm_route_parity.py`

**Interfaces:**
- Consumes: the exact module text produced in Task 1.
- Produces: `genie_space_optimizer.optimization.llm_route` with the identical public surface, importable inside the GSO wheel; a parity test guaranteeing the two files never drift.

- [ ] **Step 1: Write the failing tests**

Create `packages/genie-space-optimizer/tests/unit/test_llm_route.py` (same assertions as Task 1, GSO import path):

```python
"""Phase 0 unit tests for the AI Gateway seam factory (GSO copy).

Byte-identical to backend/services/llm_route.py (pinned by
backend/tests/test_llm_route_parity.py); the assertions mirror
backend/tests/test_llm_route.py against the GSO import path.
"""

import json

from genie_space_optimizer.optimization import llm_route as lr


def test_get_llm_route_defaults_classic(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    assert lr.get_llm_route() is lr.LLMRoute.CLASSIC


def test_get_llm_route_gateway_exact(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    assert lr.get_llm_route() is lr.LLMRoute.GATEWAY


def test_gateway_model_name_rules():
    assert lr.gateway_model_name("databricks-claude-sonnet-4-6") == "system.ai.claude-sonnet-4-6"
    assert lr.gateway_model_name("system.ai.gte-large-en") == "system.ai.gte-large-en"
    assert lr.gateway_model_name("cat.sch.my_model") == "cat.sch.my_model"


def test_resolve_chat_classic_byte_identical():
    rc = lr.resolve_chat(
        "https://x/", "databricks-claude-sonnet-4-6", "gso-optimize", route=lr.LLMRoute.CLASSIC
    )
    assert rc.url == "https://x/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
    assert rc.model is None
    assert rc.extra_headers == {}


def test_resolve_chat_gateway_tags_run_id():
    rc = lr.resolve_chat(
        "https://x", "databricks-claude-sonnet-4-6", "gso-optimize",
        route=lr.LLMRoute.GATEWAY, run_id="r7",
    )
    assert rc.url == "https://x/ai-gateway/mlflow/v1/chat/completions"
    assert rc.model == "system.ai.claude-sonnet-4-6"
    assert json.loads(rc.extra_headers["Databricks-Ai-Gateway-Request-Tags"]) == {
        "application": "genie-workbench",
        "component": "gso-optimize",
        "run_id": "r7",
    }


def test_resolve_embeddings_classic_keeps_sdk():
    rc = lr.resolve_embeddings("https://x", "databricks-gte-large-en", "leakage-embed",
                               route=lr.LLMRoute.CLASSIC)
    assert rc.use_legacy_sdk is True
    assert rc.url == ""


def test_is_reasoning_effort_400():
    assert lr.is_reasoning_effort_400(400, "reasoning_effort not permitted") is True
    assert lr.is_reasoning_effort_400(400, "something else") is False
    assert lr.is_reasoning_effort_400(200, "reasoning_effort") is False
```

Create `backend/tests/test_llm_route_parity.py`:

```python
"""Byte-identical parity pin for the two AI Gateway seam modules.

The Workbench app (backend/) and the GSO wheel
(packages/genie-space-optimizer/) do not share a Python package, so the seam
ships as two copies. This test fails the moment they drift — the same
mechanism as the rules-parity pin. Keep both files identical; never run a
formatter on one without the other.
"""

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND = _REPO_ROOT / "backend" / "services" / "llm_route.py"
_GSO = (
    _REPO_ROOT
    / "packages" / "genie-space-optimizer" / "src" / "genie_space_optimizer"
    / "optimization" / "llm_route.py"
)


def test_both_modules_exist():
    assert _BACKEND.is_file(), _BACKEND
    assert _GSO.is_file(), _GSO


def test_seam_modules_are_byte_identical():
    assert _BACKEND.read_bytes() == _GSO.read_bytes(), (
        "backend/services/llm_route.py and the GSO optimization/llm_route.py have "
        "drifted. They MUST stay byte-identical (spec Appendix A)."
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test.sh backend/tests/test_llm_route_parity.py packages/genie-space-optimizer/tests/unit/test_llm_route.py -v`
Expected: FAIL — GSO import raises `ModuleNotFoundError: genie_space_optimizer.optimization.llm_route`; parity `test_both_modules_exist` fails on the missing GSO file.

- [ ] **Step 3: Create the byte-identical twin**

Copy the backend module to the GSO path verbatim (no edits — identical bytes):

```bash
cp backend/services/llm_route.py \
   packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test.sh backend/tests/test_llm_route_parity.py packages/genie-space-optimizer/tests/unit/test_llm_route.py -v`
Expected: PASS (GSO unit tests green; parity `test_seam_modules_are_byte_identical` green).

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py \
        packages/genie-space-optimizer/tests/unit/test_llm_route.py \
        backend/tests/test_llm_route_parity.py
git commit -m "feat(ai-gateway): Phase 0 GSO seam twin + byte-identical parity pin"
```

---

## Task 3: `app.yaml` env wiring + assertion test

**Files:**
- Modify: `app.yaml` (LLM Model section, after `LLM_MODEL` at `app.yaml:90-91`)
- Test: `backend/tests/test_app_yaml_llm_route.py`

**Interfaces:**
- Consumes: `get_llm_route()` from Task 1 (reads `GENIE_LLM_ROUTE` from the app environment).
- Produces: the deployed backend app has `GENIE_LLM_ROUTE=classic` in its environment, so `get_llm_route()` resolves `CLASSIC` in production (the one Phase 0 consumer of the env var).

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_app_yaml_llm_route.py`:

```python
"""Pin the AI Gateway route flag default-off in app.yaml (Phase 0).

The backend reads GENIE_LLM_ROUTE from its environment (Appendix A.1
get_llm_route). app.yaml MUST ship it as "classic" so the deployed app stays
on the classic path until the flag is deliberately flipped (spec §0, §9
Decision 4).
"""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


def test_genie_llm_route_declared_default_off():
    env = _app_env()
    assert "GENIE_LLM_ROUTE" in env, "app.yaml must declare GENIE_LLM_ROUTE"
    assert env["GENIE_LLM_ROUTE"] == "classic"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./scripts/test.sh backend/tests/test_app_yaml_llm_route.py -v`
Expected: FAIL — `AssertionError: app.yaml must declare GENIE_LLM_ROUTE`.

- [ ] **Step 3: Add the env var to `app.yaml`**

In `app.yaml`, immediately after the `LLM_MODEL` block (`app.yaml:90-91`):

```yaml
  - name: LLM_MODEL
    value: "__LLM_MODEL__"

  # ---------------------------------------------------------------------------
  # AI Gateway route (spec §0). Default-off: "classic" keeps the app on the
  # /serving-endpoints/{name}/invocations path. Flip to "gateway" only after the
  # soak criteria in spec §9 Decision 4. Consumed by backend/services/llm_route.py.
  # ---------------------------------------------------------------------------
  - name: GENIE_LLM_ROUTE
    value: "classic"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./scripts/test.sh backend/tests/test_app_yaml_llm_route.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app.yaml backend/tests/test_app_yaml_llm_route.py
git commit -m "feat(ai-gateway): Phase 0 app.yaml GENIE_LLM_ROUTE default-off"
```

---

## Task 4: Job-parameter config mirrors (`llm_model` precedent) + lockstep guard

**Files:**
- Modify: `databricks.yml` (job `parameters` block + all 4 tasks' `base_parameters`)
- Modify: `scripts/deploy_lib/gso_job.py` (`TASKS` base_param_keys ×4 + `JOB_PARAMETERS`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py` (`submit_optimization` kwarg + run-now map)
- Modify: `backend/tests/test_deploy_lib.py` (`test_four_way_declared_param_sets_stay_in_lockstep` + new positive test)
- Do **NOT** modify: `packages/genie-space-optimizer/databricks.yml` (the package bundle intentionally omits `llm_model`; `genie_llm_route` follows suit — spec §4).

**Interfaces:**
- Consumes: nothing from prior tasks (this is pure job-definition plumbing).
- Produces: `genie_llm_route` declared with default `"classic"` in the three Workbench mirrors, reaching every task's `base_parameters`, and present in the launcher's run-now `job_parameters` — consumed by no code until Phase 1 (site 3, GSO adoption).

> **Why this task changes an existing test:** `backend/tests/test_deploy_lib.py:582` `test_four_way_declared_param_sets_stay_in_lockstep` asserts `root_params - pkg_params == {"llm_model"}` — i.e. `llm_model` is the *only* Workbench-only extra absent from the package bundle. Adding `genie_llm_route` as a second such extra (per spec §4) makes that set `{"llm_model", "genie_llm_route"}`. This is a deliberate, spec-sanctioned update, not a regression. The same test also asserts `root_params - launcher_params == {"benchmark_repair_max_tries"}`, which is why `genie_llm_route` MUST be added to the launcher run-now map (like `llm_model`).

- [ ] **Step 1: Update the lockstep test to expect the new mirror set (failing)**

In `backend/tests/test_deploy_lib.py`, edit `test_four_way_declared_param_sets_stay_in_lockstep` (the `Package bundle omits only llm_model` assertion, currently `test_deploy_lib.py:605`):

```python
    # Package bundle omits the two Workbench-only extras (llm_model, genie_llm_route).
    assert root_params - pkg_params == {"llm_model", "genie_llm_route"}
    assert pkg_params - root_params == set()
    # The launcher overrides every declared param except benchmark_repair_max_tries.
    assert root_params - launcher_params == {"benchmark_repair_max_tries"}
    assert launcher_params - root_params == set()
```

Also update the docstring's "Sanctioned exceptions" sentence to name both extras. Then add a new positive test at the end of the file:

```python
def test_genie_llm_route_declared_default_off_across_workbench_mirrors():
    """AI Gateway Phase 0 (spec §4): genie_llm_route is a second Workbench-only
    extra, declared "classic" in root databricks.yml, gso_job.JOB_PARAMETERS,
    and the launcher run_now map; intentionally absent from the package bundle
    (mirrors llm_model)."""
    from scripts.deploy_lib.gso_job import JOB_PARAMETERS

    repo_root = _repo_root()
    root_params = {
        p["name"]: p["default"] for p in _load_bundle_job(repo_root / "databricks.yml")["parameters"]
    }
    pkg_params = {
        p["name"]
        for p in _load_bundle_job(
            repo_root / "packages" / "genie-space-optimizer" / "databricks.yml"
        )["parameters"]
    }
    launcher_params = _launcher_run_now_params()

    assert root_params.get("genie_llm_route") == "classic"
    assert JOB_PARAMETERS.get("genie_llm_route") == "classic"
    assert launcher_params.get("genie_llm_route") == "classic"
    assert "genie_llm_route" not in pkg_params  # package bundle omits it, like llm_model


def test_genie_llm_route_reaches_every_task_base_parameters():
    """Like llm_model, the route param rides into every task's base_parameters in
    both Workbench job definitions (root databricks.yml + gso_job.TASKS)."""
    from scripts.deploy_lib.gso_job import TASKS

    repo_root = _repo_root()
    root_tasks = _load_bundle_job(repo_root / "databricks.yml")["tasks"]
    for t in root_tasks:
        bp = t["notebook_task"]["base_parameters"]
        assert bp.get("genie_llm_route") == "{{job.parameters.genie_llm_route}}", t["task_key"]
    for _key, _stem, _dep, base_param_keys in TASKS:
        assert "genie_llm_route" in base_param_keys, _key
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test.sh backend/tests/test_deploy_lib.py -v -k "lockstep or genie_llm_route"`
Expected: FAIL — the updated lockstep assertion fails (`genie_llm_route` not yet declared, so `root_params - pkg_params == {"llm_model"}` ≠ expected), and the two new tests fail on missing declarations.

- [ ] **Step 3a: Declare + thread the param in `databricks.yml`**

Add the parameter right after the `llm_model` job-parameter block (`databricks.yml:110-111`):

```yaml
        - name: llm_model
          default: ${var.llm_model}
        # AI Gateway route (spec §4). Workbench-only extra, like llm_model:
        # declared here + gso_job.py + job_launcher, absent from the package bundle.
        # Default-off ("classic"); consumed only from Phase 1.
        - name: genie_llm_route
          default: "classic"
```

Then add this line to **each** of the 4 tasks' `base_parameters`, immediately after that task's existing `llm_model: "{{job.parameters.llm_model}}"` line (`databricks.yml:151`, `:170`, `:191`, `:220`):

```yaml
              genie_llm_route: "{{job.parameters.genie_llm_route}}"
```

- [ ] **Step 3b: Mirror in `scripts/deploy_lib/gso_job.py`**

Add `"genie_llm_route"` to the `base_param_keys` list of **all four** `TASKS` entries (next to each `"llm_model"`), e.g. the `intake_and_snapshot` list becomes:

```python
        [
            "run_id", "space_id", "domain", "catalog", "schema", "apply_mode",
            "levers", "max_attempts", "target_accuracy",
            "benchmark_repair_max_tries", "triggered_by",
            "benchmark_policy", "warehouse_id", "workload_warehouse_ids", "llm_model",
            "genie_llm_route",
        ],
```

Apply the same addition to `benchmark_qc_and_repair`, `optimize`, and `publish_and_audit`. Then add the default to `JOB_PARAMETERS`, right after the `"llm_model": "",` line (`gso_job.py:107`):

```python
    "llm_model": "",
    "genie_llm_route": "classic",  # AI Gateway route (spec §4); default-off, consumed from Phase 1
```

- [ ] **Step 3c: Mirror in the launcher run-now map**

In `packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py`, add the kwarg after `llm_model: str = ""` (`job_launcher.py:77`):

```python
    llm_model: str = "",
    genie_llm_route: str = "classic",
```

And add the entry to the `job_parameters` dict passed to `run_now`, right after the `"llm_model": ...` line (`job_launcher.py:130`):

```python
                "llm_model": llm_model or os.getenv("LLM_MODEL", ""),
                "genie_llm_route": genie_llm_route or os.getenv("GENIE_LLM_ROUTE", "classic"),
```

> No change to `integration/trigger.py`: like the MV-D5 params, the sole caller omits `genie_llm_route`, so it rides at the `"classic"` default until Phase 1 threads a real value (matches the documented pattern at `test_phase7_job_dag.py:140`). The launcher stays a subset-consistent mirror of the declared params.

- [ ] **Step 4: Run the impacted suites to verify green**

Run: `./scripts/test.sh backend/tests/test_deploy_lib.py packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py -v`
Expected: PASS. The `test_phase7_job_dag.py` suite loads the **package bundle** (`packages/genie-space-optimizer/databricks.yml`, unchanged), so it is unaffected; `test_deploy_lib.py` reflects the new mirror set.

- [ ] **Step 5: Commit**

```bash
git add databricks.yml scripts/deploy_lib/gso_job.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py \
        backend/tests/test_deploy_lib.py
git commit -m "feat(ai-gateway): Phase 0 plumb genie_llm_route (default-off) via the llm_model mirror set"
```

---

## Task 5: Full-suite verification + contract grep

**Files:** none (verification only).

**Interfaces:** confirms Tasks 1–4 compose without regression and the seam is inert (nothing imports it yet).

- [ ] **Step 1: Run both suites in full**

Run: `./scripts/test.sh`
Expected: PASS. Record the new totals; they must be **≥ 681 backend + 1512 GSO** (baseline) plus the Phase 0 additions. A count below baseline is a regression — investigate before proceeding.

- [ ] **Step 2: Confirm import resolution is this checkout (backend)**

Run: `uv run --frozen --extra dev python -c "import genie_space_optimizer as g; print(g.__file__)"`
Expected: a path inside this repository. (A foreign checkout voids the run.)

- [ ] **Step 3: Contract grep — the seam is inert (no call site touched)**

Run: `rg -n "llm_route" backend packages --glob '!**/llm_route.py' --glob '!**/test_*'`
Expected: **no matches.** Phase 0 must not wire the factory into any call site (spec §6 — that is Phase 1+). If anything matches outside the module files and their tests, a call site was touched — revert it.

- [ ] **Step 4: Confirm the working tree is clean apart from intended files**

Run: `git status -sb && git status -- uv.lock pyproject.toml requirements.txt`
Expected: only the Task 1–4 files are changed; `uv.lock` / `pyproject.toml` / `requirements.txt` are **untouched** (Phase 0 adds no dependency). A dirtied `uv.lock` is a finding to report, never to commit.

- [ ] **Step 5: (Optional) refresh the spec's baseline note**

If you want the spec to reflect the new floor, bump the "681 backend + 1512 GSO" figure in `scripts/ai-gateway-validation/ai-gateway-migration-spec.md` §8 to the totals recorded in Step 1, in the same PR. (The mv-advisor rule's ledger governs mv params, not this feature; do not edit `.cursor/rules/mv-advisor.mdc` for Phase 0.)

---

## Self-Review

**1. Spec coverage (§6 Phase 0 + Appendix A.1 + §4):**
- "Add `GENIE_LLM_ROUTE` (default classic) and the `resolve_chat`/`resolve_embeddings` factory returning a `ResolvedCall`" → Task 1 (backend) + Task 2 (GSO twin). ✓
- "Nothing calls it yet" → Task 5 Step 3 contract grep. ✓
- "New module (backend) + a GSO twin … pinned by a parity test" → Task 2 parity pin. ✓
- "`app.yaml` env" → Task 3. ✓
- "The 4 config mirrors (§4)" → Task 4 (root `databricks.yml`, `gso_job.py`, `job_launcher.py`; package bundle intentionally omitted per §4). ✓
- "Unit tests assert `classic` output byte-for-byte matches the current URL/headers" → `TestResolveChat.test_classic_is_byte_identical_to_today` (Task 1). ✓
- "`get_llm_route` default/typo → CLASSIC; `gateway_model_name` strip-rule / idempotence / BYOK passthrough; `request_tags` drops empty scope" → Task 1 test classes. ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N". Every code and test step is full text; Task 2 reuses Task 1's exact module via `cp` (kept identical on purpose). ✓

**3. Type consistency:** `LLMRoute`, `ResolvedCall(url, model, extra_headers, use_legacy_sdk)`, `resolve_chat`/`resolve_embeddings`/`gateway_model_name`/`request_tags`/`tag_header`/`is_reasoning_effort_400` are used identically across Tasks 1–2 and match Appendix A.1 signatures. Config param name `genie_llm_route` (job param) vs env `GENIE_LLM_ROUTE` (read by `get_llm_route`) are used consistently in Tasks 3–4. ✓

**Open item carried from the spec (not a blocker for default-off Phase 0):** spec §4 flags that the notebook-install path may deploy the job from the package bundle; confirm whether `packages/genie-space-optimizer/databricks.yml` also needs `genie_llm_route` **before flipping the flag on that path** (Phase 1+). Phase 0 mirrors `llm_model` exactly (package-bundle-omitted), which is correct while default-off.

---

## Execution Handoff

Plan complete and saved to `scripts/ai-gateway-validation/ai-gateway-phase0-plan.md` (co-located with the spec and validation harness, per the spec's "Placement note" — a deliberate deviation from the skill's default `docs/superpowers/plans/` location so the Phase 0 plan travels with its spec). Two execution options:

1. **Subagent-Driven (recommended)** — a fresh subagent per task, two-stage review between tasks, fast iteration.
2. **Inline Execution** — execute the tasks in this session with checkpoints for review.

Which approach?
