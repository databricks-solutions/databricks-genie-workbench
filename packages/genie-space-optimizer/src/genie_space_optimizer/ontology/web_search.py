"""Web search via the Unity AI Gateway MCP (Phase 4 Stage B, MV-D46) — wheel-native.

The ONE egress path of the external-context tier. It calls a **governed AI-Gateway MCP
Service** over JSON-RPC — never the raw internet, never a per-provider adapter, never a
stored token. The Gateway proxies managed credentials (auth rides the injected
``WorkspaceClient``'s ``api_client``, MV-D65: the caller owns identity), enforces the
built-in write-block policy, and usage-tracks every call (``system.ai_gateway.usage``).

**Fallback ladder** (probe in order, degrade-not-hang, MV-D43/D46):

1. ``system.ai.web_search`` — the managed AI-Gateway web-search MCP (primary).
2. ``myyoumcp`` (You.com) — where the managed service is absent.
3. A Model-Serving-native web tool — a further degrade rung (best-effort).
4. **estate-only** — all rungs absent / erroring ⇒ an EMPTY result list, never a raise.

Nothing here assembles a Context Pack or steers naming — it returns provider results
(title / url / snippet) that the resolver (``context_pack.py``) wraps as ``Provenanced``
leaves and self-validates before any of it becomes a naming prior. HIPAA/BAA workspaces
stay hard-off (the ``hipaa_baa`` gate returns empty without any egress).
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# The provider ladder, in probe order (MV-D46). Each id maps to a Gateway MCP securable;
# ``model_serving`` is the Model-Serving-native rung (no MCP securable — best-effort).
LADDER: tuple[str, ...] = ("web_search", "youcom", "model_serving")

# Provider id → AI-Gateway MCP Service securable (fqn). ``model_serving`` has no MCP
# securable (it is a serving-endpoint-native web tool); it is tried last and, absent a
# wired endpoint, simply yields nothing and the ladder degrades to estate-only.
_PROVIDER_FQN: dict[str, str] = {
    "web_search": "system.ai.web_search",
    "youcom": "myyoumcp",
}

# The MCP tool name to invoke on each service (JSON-RPC ``tools/call``). Both the managed
# web-search MCP and the You.com MCP expose a ``web_search`` tool.
_PROVIDER_TOOL: dict[str, str] = {
    "web_search": "web_search",
    "youcom": "web_search",
}

# A transport is ``(path, body) -> dict``: POST a JSON-RPC body to a Gateway MCP path and
# return the decoded JSON. Injectable so the resolver/tests can drive it without a live
# Gateway; the default builds one from an injected WorkspaceClient (MV-D65).
Transport = Callable[[str, dict[str, Any]], Any]


@dataclass(frozen=True)
class WebResult:
    """One provider result. ``url`` is REQUIRED downstream — the resolver drops any
    leaf without a ``source_url`` (MV-D38 no-unsourced-numbers / labeled+dated rail),
    so a result with an empty url is filtered before it can seed a Provenanced leaf."""
    title: str
    url: str
    snippet: str
    provider: str = ""


def _gateway_path(fqn: str) -> str:
    """The AI-Gateway MCP Service path for a securable fqn (JSON-RPC over the Gateway)."""
    return f"/ai-gateway/mcp-services/{fqn}"


def _jsonrpc_tools_call(tool: str, query: str) -> dict[str, Any]:
    """A JSON-RPC ``tools/call`` body for one web-search query (MV-D46)."""
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": {"query": query}},
    }


def _default_transport(w: Any) -> Transport | None:
    """Build the default transport from an injected ``WorkspaceClient`` — a POST through
    the SDK ``api_client`` so the Gateway proxies managed credentials (no token in app
    code, MV-D46). Returns ``None`` when no client / api_client is available (degrade)."""
    api = getattr(w, "api_client", None) if w is not None else None
    if api is None or not hasattr(api, "do"):
        return None

    def _do(path: str, body: dict[str, Any]) -> Any:
        return api.do("POST", path, body=body)

    return _do


def _coerce_results(payload: Any, provider: str) -> list[WebResult]:
    """Parse a JSON-RPC ``tools/call`` response into ``WebResult``s, tolerant of the
    shapes an MCP web-search tool returns (MCP ``result.content[].text`` carrying a JSON
    array/object of hits, or a direct ``results`` list). Never raises — an unparseable
    payload yields ``[]`` (degrade-not-hang, MV-D43)."""
    if not payload:
        return []
    try:
        result = payload.get("result") if isinstance(payload, dict) else None
        hits = _extract_hits(result if result is not None else payload)
    except Exception as e:  # noqa: BLE001 — an odd shape is not an error, just no results
        logger.info("web_search response parse degraded for %s: %s", provider, e)
        return []
    out: list[WebResult] = []
    for h in hits:
        if not isinstance(h, dict):
            continue
        url = str(h.get("url") or h.get("link") or h.get("source_url") or "").strip()
        if not url:
            continue  # no citable URL ⇒ dropped before it can seed a leaf (MV-D38)
        out.append(
            WebResult(
                title=str(h.get("title") or h.get("name") or "").strip(),
                url=url,
                snippet=str(h.get("snippet") or h.get("text") or h.get("description") or "").strip(),
                provider=provider,
            )
        )
    return out


def _extract_hits(node: Any) -> list[Any]:
    """Pull a list of hit dicts out of the several shapes MCP tools return."""
    if isinstance(node, list):
        return node
    if not isinstance(node, dict):
        return []
    # MCP tools/call: {"content": [{"type":"text","text":"<json>"}]}.
    content = node.get("content")
    if isinstance(content, list):
        collected: list[Any] = []
        for block in content:
            text = block.get("text") if isinstance(block, dict) else None
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except (ValueError, TypeError):
                # The managed ``system.ai.web_search`` MCP returns a synthesized markdown
                # ANSWER with inline ``[title](url)`` citations + a "Sources" list, NOT a
                # JSON hit array (MV-D46 live-probe finding). Parse the citations as hits so
                # the positive path works against the real Gateway; a citation-free answer
                # yields [] and the ladder degrades to the next rung / estate-only (MV-D43).
                collected.extend(_hits_from_text_answer(text))
                continue
            collected.extend(_extract_hits(parsed))
        if collected:
            return collected
    for key in ("results", "hits", "web", "items", "data"):
        v = node.get(key)
        if isinstance(v, list):
            return v
    return []


# A markdown link ``[label](url)`` restricted to citable http(s) targets; ``label`` may be
# empty (a bare ``[](url)`` still yields a citable hit).
_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\((https?://[^)\s]+)\)")
# Cap the answer prose reused as a per-citation snippet so a long synthesized answer never
# bloats the resolver's synthesis prompt / the egress row (MV-D46). The LLM only needs the
# substance, not the whole essay.
_ANSWER_SNIPPET_MAX = 1500


def _hits_from_text_answer(text: str) -> list[dict[str, Any]]:
    """Parse a synthesized markdown ANSWER (the managed ``system.ai.web_search`` shape) into
    hit dicts. Extracts inline ``[title](url)`` citations, de-duped by url in first-seen
    order; each hit carries the flattened answer prose as its snippet so the resolver's LLM
    synthesis sees the substance. Returns [] when no citable http(s) url is present — the
    resolver drops any leaf without a ``source_url`` (MV-D38), so an uncited answer never
    seeds a naming prior. Never raises (MV-D43)."""
    if not isinstance(text, str) or not text.strip():
        return []
    snippet = _plain_text(text)[:_ANSWER_SNIPPET_MAX]
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for label, url in _MD_LINK_RE.findall(text):
        u = url.strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append({"title": label.strip(), "url": u, "snippet": snippet})
    return out


def _plain_text(md: str) -> str:
    """A light markdown→text flatten for the snippet: collapse ``[label](url)`` to its label
    (or the url when unlabeled) and drop emphasis/heading/bullet markers, then collapse
    whitespace. Deterministic; never raises."""
    text = _MD_LINK_RE.sub(lambda m: m.group(1) or m.group(2), md)
    text = re.sub(r"[*_`#>]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def web_search(
    query: str,
    *,
    w: Any = None,
    enabled_providers: Iterable[str] = (),
    transport: Transport | None = None,
    hipaa_baa: bool = False,
    max_results: int = 5,
) -> list[WebResult]:
    """Run one web-search query through the AI-Gateway MCP ladder (MV-D46).

    Tries the enabled providers in :data:`LADDER` order and returns the FIRST rung that
    yields results; degrades to ``[]`` (estate-only) when the query is empty, when
    ``hipaa_baa`` is set (hard-off), when no provider is enabled, or when every rung is
    absent / errors. **Never raises** — every rung is wrapped so one broken provider can
    never sink the batch (MV-D43). ``transport`` (``(path, body) -> dict``) is injectable;
    the default is built from ``w``'s ``api_client`` so the Gateway proxies managed creds
    (no token in app code)."""
    if hipaa_baa or not query or not str(query).strip():
        return []
    enabled = {str(p) for p in enabled_providers}
    if not enabled:
        return []
    send = transport if transport is not None else _default_transport(w)
    if send is None:
        return []
    for provider in LADDER:
        if provider not in enabled:
            continue
        fqn = _PROVIDER_FQN.get(provider)
        if not fqn:
            # Model-Serving-native rung: no MCP securable wired here — degrade past it.
            continue
        tool = _PROVIDER_TOOL.get(provider, "web_search")
        try:
            payload = send(_gateway_path(fqn), _jsonrpc_tools_call(tool, str(query)))
        except Exception as e:  # noqa: BLE001 — a broken rung degrades to the next
            logger.info("web_search rung %s degraded: %s", provider, e)
            continue
        results = _coerce_results(payload, provider)
        if results:
            return results[: max(0, int(max_results))]
    return []


__all__ = ["LADDER", "Transport", "WebResult", "web_search"]
