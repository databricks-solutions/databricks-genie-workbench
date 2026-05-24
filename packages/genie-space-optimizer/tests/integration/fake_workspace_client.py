"""``FakeWorkspaceClient`` — recorder/responder for offline applier tests.

The Genie Space optimizer's applier (`genie_space_optimizer.optimization.
applier.apply_patch_set`) touches exactly **one** mutation surface on the
:class:`databricks.sdk.WorkspaceClient`:

* ``w.api_client.do("PATCH", "/api/2.0/genie/spaces/{space_id}",
  body={"serialized_space": json.dumps(config)})``

issued from
:func:`genie_space_optimizer.common.genie_client.patch_space_config`.
Patches that update Unity Catalog metadata (table / column
descriptions, synonyms) additionally call
``w.statement_execution.execute_statement(...)`` per dispatched DDL.

This module exposes a minimal fake that captures both surfaces:

* :class:`FakeApiClient` — records every ``do`` call as a dict, then
  either returns a canned ``{}`` (the production response shape for a
  successful PATCH) or raises a caller-supplied exception. Supports
  per-call overrides via an ``on_request`` callable so tests can model
  the four production apply-error shapes (size limit, validation,
  duplicate ID, transient 5xx).
* :class:`FakeStatementExecution` — records UC DDL invocations.
  Returns a stub success result so callers that read
  ``result.status.state`` see ``"SUCCEEDED"``.
* :class:`FakeWorkspaceClient` — composes both. Drop-in replacement for
  :class:`databricks.sdk.WorkspaceClient` for the applier's purposes.

Why a fake instead of a mock:

* Recording the structured PATCH payload as JSON lets tests assert on
  the exact mutation the optimizer would have sent — the canonical
  "did the applier build the right wire payload" invariant. A bare
  ``unittest.mock.MagicMock`` would also record calls but its
  attribute-access magic obscures the surface contract we want to pin.
* The fake's ``on_request`` callable models real failure shapes
  (``DatabricksError``-like exceptions) so tests can drive
  ``applier_gate`` into ``applyability_rejected`` without touching a
  workspace.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# A request handler may either return a JSON-shaped dict to be the
# response body, or return an Exception instance to be raised. Tests
# raise via ``return exc`` rather than ``raise exc`` so the handler
# composes cleanly with the ``on_request`` field's default-None path.
RequestHandler = Callable[[str, str, Optional[dict]], "dict | Exception | None"]


@dataclass
class RecordedApiCall:
    """A single recorded ``api_client.do`` invocation.

    ``body_json`` is the parsed ``serialized_space`` payload when the
    body matches the Genie PATCH shape, so assertions can match
    structurally rather than against the raw JSON string. ``None``
    when the body is not a Genie serialized_space PATCH.
    """
    method: str
    path: str
    body: dict | None
    headers: dict | None
    body_json: dict | None


@dataclass
class FakeApiClient:
    """Recorder for ``WorkspaceClient.api_client``.

    The applier's PATCH path goes through
    ``api_client.do("PATCH", "/api/2.0/genie/spaces/{space_id}", body=...)``;
    ``patch_space_config`` is the only caller. Recording at this seam
    captures the full transformed payload — instruction limits applied,
    snippets trimmed, sort-genie-config run — so tests can assert on the
    wire-final shape, not a partially canonicalised intermediate.
    """
    calls: list[RecordedApiCall] = field(default_factory=list)
    on_request: RequestHandler | None = None
    # Optional canned response body. Production returns ``{}``; tests
    # can pin a richer shape if a future caller starts reading fields
    # off the response.
    default_response: dict = field(default_factory=dict)

    def do(  # noqa: PLR0913 — kwargs mirror the SDK surface
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        headers: dict | None = None,
        query: dict | None = None,
        raw: bool = False,
        files: Any = None,
        data: Any = None,
    ) -> dict:
        """Record the call, optionally route through ``on_request``.

        The SDK accepts ``query``/``raw``/``files``/``data`` keyword
        arguments at the same surface; the fake accepts them but does
        not record them by default (the applier never sets them). If
        a future patch path starts using ``query`` etc., extend
        :class:`RecordedApiCall` to capture it explicitly rather than
        silently swallowing the value.
        """
        _ = (query, raw, files, data)  # documented contract; unused
        body_json = _maybe_decode_genie_serialized_space(body)
        self.calls.append(
            RecordedApiCall(
                method=method,
                path=path,
                body=dict(body) if isinstance(body, dict) else None,
                headers=dict(headers) if isinstance(headers, dict) else None,
                body_json=body_json,
            )
        )
        if self.on_request is not None:
            result = self.on_request(method, path, body)
            if isinstance(result, BaseException):
                raise result
            if isinstance(result, dict):
                return result
        return dict(self.default_response)

    # ── Convenience accessors ─────────────────────────────────────────

    def calls_matching(
        self,
        *,
        method: str | None = None,
        path_prefix: str | None = None,
    ) -> list[RecordedApiCall]:
        """Return recorded calls whose ``method`` / ``path`` match.

        ``path_prefix`` does a prefix match because the Genie PATCH
        path includes the ``space_id``.
        """
        out: list[RecordedApiCall] = []
        for c in self.calls:
            if method is not None and c.method != method:
                continue
            if path_prefix is not None and not c.path.startswith(path_prefix):
                continue
            out.append(c)
        return out

    def genie_patch_calls(self) -> list[RecordedApiCall]:
        """Return only the PATCH calls into the Genie Space surface."""
        return self.calls_matching(
            method="PATCH", path_prefix="/api/2.0/genie/spaces/",
        )


def _maybe_decode_genie_serialized_space(body: dict | None) -> dict | None:
    """Best-effort decode of ``body["serialized_space"]`` into a dict.

    ``patch_space_config`` sends ``{"serialized_space": json.dumps(...)}``;
    decoding the JSON string here lets tests assert against typed
    fields without re-doing the JSON round-trip in every assertion.
    Returns ``None`` for non-Genie payloads so tests that match other
    PATCH shapes still get the raw ``body`` dict.
    """
    if not isinstance(body, dict):
        return None
    raw = body.get("serialized_space")
    if not isinstance(raw, str):
        return None
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


@dataclass
class _FakeStatementStatus:
    state: str = "SUCCEEDED"


@dataclass
class _FakeStatementResult:
    """Minimal stand-in for ``databricks.sdk.service.sql.StatementResponse``.

    The applier's UC path checks ``result.status.state`` against
    ``"SUCCEEDED"`` and treats any other value as a failure. The
    statement-id / result-data fields are unused by the applier and
    therefore intentionally absent — surfacing as ``AttributeError``
    if any future caller starts depending on them so the fake's
    contract stays tight.
    """
    statement_id: str = "fake-statement-0001"
    status: _FakeStatementStatus = field(default_factory=_FakeStatementStatus)


@dataclass
class FakeStatementExecution:
    """Recorder for ``WorkspaceClient.statement_execution``.

    The forward-pipeline tape produces ``add_example_sql`` patches,
    which do not touch UC; this recorder exists so the fake is
    feature-complete for future patches that *do* fire UC DDL
    (``update_description``, ``update_column_description``,
    ``add_column_synonym``, ...).
    """
    statements: list[dict] = field(default_factory=list)
    default_state: str = "SUCCEEDED"

    def execute_statement(self, **kwargs: Any) -> _FakeStatementResult:
        self.statements.append(dict(kwargs))
        return _FakeStatementResult(
            status=_FakeStatementStatus(state=self.default_state),
        )


@dataclass
class FakeWorkspaceClient:
    """Drop-in WorkspaceClient stand-in for offline applier tests.

    Compose with custom ``on_request`` to model failures:

      .. code-block:: python

         from databricks.sdk.errors import DatabricksError
         client = FakeWorkspaceClient(
             on_request=lambda method, path, body: DatabricksError(
                 "PATCH failed: serialized_space exceeds 1MB",
             ),
         )

    A handler can also return a dict to override the canned response
    when the test cares about a specific response shape.
    """
    api_client: FakeApiClient = field(default_factory=FakeApiClient)
    statement_execution: FakeStatementExecution = field(
        default_factory=FakeStatementExecution,
    )

    @classmethod
    def with_handler(
        cls, handler: RequestHandler,
    ) -> "FakeWorkspaceClient":
        """Build a client whose api_client routes every call through ``handler``."""
        return cls(api_client=FakeApiClient(on_request=handler))


# ── Minimal valid metadata_snapshot fixture ───────────────────────────

# The applier validates the post-patch config against the
# :class:`SerializedSpace` Pydantic model with ``strict=True``. The
# minimum that passes is ``version`` ∈ {1, 2} and a non-None
# ``data_sources`` block — the ``_check_has_data_sources`` model
# validator raises ``serialized_space must contain 'data_sources'``
# otherwise.
#
# Tests use this fixture as the ``metadata_snapshot`` argument so
# the applier can run end-to-end and the FakeApiClient sees the
# resulting PATCH payload.
def minimal_valid_metadata_snapshot() -> dict:
    """Return a fresh minimal ``serialized_space`` dict.

    Fresh dict per call so each test holds its own copy — the applier
    deep-copies internally, but returning the same shared dict would
    couple tests via mutation if a future code path stops copying.
    """
    return {
        "version": 1,
        "data_sources": {
            # An empty tables list satisfies the
            # ``serialized_space must contain 'data_sources'`` model
            # validator without forcing tests to invent fake UC
            # references that the strict-ID validator would then
            # reject.
            "tables": [],
            "metric_views": [],
        },
        "instructions": {
            "example_question_sqls": [],
            "text_instructions": [],
        },
        "config": {
            "sample_questions": [],
        },
    }


__all__ = [
    "FakeApiClient",
    "FakeStatementExecution",
    "FakeWorkspaceClient",
    "RecordedApiCall",
    "RequestHandler",
    "minimal_valid_metadata_snapshot",
]
