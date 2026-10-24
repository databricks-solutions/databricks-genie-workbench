"""Every LLM call goes through the AI Gateway routing module.

Scans the app backend and the GSO package source for the raw shapes an LLM call
takes (a serving-endpoints URL, the gateway path, the SDK query, an OpenAI
client or completion call). Each shape may appear only in the files listed in
``_ALLOWED``, and each of those files must build its request from the routing
module. A new call site that hardcodes a URL or skips the route fails here, so
it cannot silently stay on classic or lose its cost-attribution tag.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCAN_ROOTS = (
    _REPO_ROOT / "backend",
    _REPO_ROOT / "packages" / "genie-space-optimizer" / "src",
)
_GSO_OPT = "packages/genie-space-optimizer/src/genie_space_optimizer/optimization"

_SHAPES = (
    "serving-endpoints",
    "/invocations",
    "/ai-gateway/",
    "serving_endpoints.query(",
    "chat.completions.create(",
    "OpenAI(",
)

_ALLOWED: dict[str, tuple[set[str], str | None]] = {
    # The routing module itself (both twins): the classic URL and gateway paths.
    "backend/services/llm_route.py": (
        {"serving-endpoints", "/invocations", "/ai-gateway/", "serving_endpoints.query("}, None),
    f"{_GSO_OPT}/llm_route.py": (
        {"serving-endpoints", "/invocations", "/ai-gateway/", "serving_endpoints.query("}, None),
    # GSO OpenAI client: route-aware base_url, and call_llm built from resolve_gso_chat.
    f"{_GSO_OPT}/llm_client.py": (
        {"serving-endpoints", "/ai-gateway/", "OpenAI(", "chat.completions.create("},
        "resolve_gso_chat()"),
    f"{_GSO_OPT}/optimizer_utils.py": ({"chat.completions.create("}, "resolve_gso_chat()"),
    # Embeddings: the classic branch keeps the SDK query behind resolve_embeddings.
    f"{_GSO_OPT}/leakage.py": ({"serving_endpoints.query("}, "resolve_embeddings("),
}


def _sources():
    for root in _SCAN_ROOTS:
        for path in root.rglob("*.py"):
            rel = path.relative_to(_REPO_ROOT).as_posix()
            if "/tests/" in rel or "/node_modules/" in rel:
                continue
            yield rel, path.read_text(encoding="utf-8")


def test_scan_roots_resolve():
    assert (_REPO_ROOT / "databricks.yml").is_file()
    for root in _SCAN_ROOTS:
        assert root.is_dir(), root
    for rel in _ALLOWED:
        assert (_REPO_ROOT / rel).is_file(), rel


def test_raw_llm_call_shapes_appear_only_in_routed_files():
    violations = []
    for rel, src in _sources():
        allowed = _ALLOWED.get(rel, (set(), None))[0]
        for shape in _SHAPES:
            if shape in src and shape not in allowed:
                violations.append(f"{rel}: {shape!r}")
    assert not violations, (
        "LLM call shapes outside the routing module. Build the request from "
        "llm_route.resolve_chat / resolve_embeddings or llm_client.resolve_gso_chat:\n"
        + "\n".join(violations)
    )


def test_allowed_call_sites_build_requests_from_the_route():
    scanned = dict(_sources())
    for rel, (shapes, required) in _ALLOWED.items():
        assert any(s in scanned[rel] for s in shapes), f"scanner sees no call shape in {rel}"
        if required:
            assert required in scanned[rel], (
                f"{rel} makes an LLM call but no longer uses {required}")


def test_backend_chat_sites_resolve_through_the_route():
    for rel in ("backend/services/llm_utils.py", "backend/services/create_agent.py"):
        src = (_REPO_ROOT / rel).read_text(encoding="utf-8")
        assert "resolve_chat(" in src and "rc.extra_headers" in src, rel
