"""Similarity interface for ER/dedupe (MV-D40 / MV-D45) — ONE seam, TWO backends.

This is the only module in the ontology package that references Lakebase Search
(``lakebase_vector`` / ``lakebase_text``). Keeping the tokens scoped here is what
lets the firewall (§11) relax them for this file alone.

  - ``InProcessCosineBackend`` — the DEFAULT (and the offline test target, MV-D45):
    cosine over L2-normalized GTE vectors + in-process edit-distance/token keyword
    score. Fully functional without Lakebase Search (just slower on large estates).
  - ``LakebaseSearchBackend`` — authored, but ACTIVE only after the §12 human enable
    (enabling Lakebase Search is beta + IRREVERSIBLE; the agent never enables it).
    It delegates the ANN + BM25 to an injected executor so it is unit-testable
    against a FAKE that mimics the SQL contract, and is never run against a real
    Lakebase in the offline slice.

Both backends share the SAME scoring math (cosine / keyword), so a workspace on
either backend gets identical verdicts on the same fixture (the parity guarantee).
Embeddings always arrive precomputed (via ``leakage.get_embedding``); no backend
re-embeds.
"""

from __future__ import annotations

import math
import re
from typing import Protocol, Sequence

# Reuse the package's single cosine + L2-normalize (GTE is not self-normalizing —
# the mv_scoring caveat) so ER scores match the MV advisor's semantics exactly.
from genie_space_optimizer.optimization.mv_scoring import _cosine

# candidate shapes:
#   embedding: (ref, vector)        vector is a list[float] (already GTE, L2 applied in _cosine)
#   keyword:   (ref, text)          text is name + comment
EmbCandidate = tuple[str, Sequence[float]]
KwCandidate = tuple[str, str]


class SimilarityBackend(Protocol):
    def topk_embedding(self, query_vec: Sequence[float], candidates: Sequence[EmbCandidate], k: int) -> list[tuple[str, float]]: ...
    def topk_keyword(self, query_text: str, candidates: Sequence[KwCandidate], k: int) -> list[tuple[str, float]]: ...


# ── Shared scoring math (used by both backends → parity) ────────────────────

_TOKEN_RE = re.compile(r"[^0-9a-zA-Z]+")


def _tokens(text: str) -> set[str]:
    return {t for t in _TOKEN_RE.split((text or "").casefold()) if t}


def _edit_ratio(a: str, b: str) -> float:
    """Normalized Levenshtein similarity in [0, 1] (1.0 == identical)."""
    a, b = (a or "").casefold(), (b or "").casefold()
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    dist = prev[-1]
    return 1.0 - dist / max(len(a), len(b))


def keyword_score(query_text: str, cand_text: str) -> float:
    """String/keyword similarity: max of char edit-ratio and token-set Jaccard.

    Edit-ratio catches typos/plurals (``order_revenue`` ~ ``orders_revenue``);
    token Jaccard catches reordering. Neither collapses a genuine paraphrase
    (``net revenue`` ~ ``revenue after discount``) — that is the embedding signal's
    job — nor two distinct concepts (``headcount`` vs ``revenue``).
    """
    edit = _edit_ratio(query_text, cand_text)
    ta, tb = _tokens(query_text), _tokens(cand_text)
    jacc = (len(ta & tb) / len(ta | tb)) if (ta or tb) else 0.0
    return max(edit, jacc)


def _topk(scored: list[tuple[str, float]], k: int) -> list[tuple[str, float]]:
    scored.sort(key=lambda t: (-t[1], t[0]))
    return scored[: k if k and k > 0 else len(scored)]


# ── Default backend: in-process (MV-D45 degrade path + offline target) ──────


class InProcessCosineBackend:
    """Cosine over GTE vectors + in-process keyword score. No external service."""

    def topk_embedding(self, query_vec, candidates, k):
        scored = [(ref, _cosine(query_vec, vec)) for ref, vec in candidates]
        return _topk(scored, k)

    def topk_keyword(self, query_text, candidates, k):
        scored = [(ref, keyword_score(query_text, text)) for ref, text in candidates]
        return _topk(scored, k)


# ── Lakebase Search backend: authored, enabled only via the §12 human gate ──


class LakebaseSearchExecutor(Protocol):
    """Runs the ANN (lakebase_vector) + BM25 (lakebase_text) searches.

    The real executor issues Lakebase Search SQL over a synced candidate table;
    the offline fake computes the SAME cosine/keyword math in-process so the two
    backends are provably at parity on a fixture. Injected so LakebaseSearchBackend
    is never coupled to a live Lakebase.
    """

    def vector_topk(self, query_vec: Sequence[float], candidates: Sequence[EmbCandidate], k: int) -> list[tuple[str, float]]: ...
    def text_topk(self, query_text: str, candidates: Sequence[KwCandidate], k: int) -> list[tuple[str, float]]: ...


class LakebaseSearchBackend:
    """ANN + BM25 via Lakebase Search — ACTIVE only after the §12 human enable.

    Enabling Lakebase Search (``lakebase_vector`` + ``lakebase_text``) on the app's
    Autoscaling Lakebase is beta and IRREVERSIBLE; this class is authored so the
    seam exists, but the offline slice never runs it against a real Lakebase — it
    runs against a fake executor (parity test). The production executor's SQL is
    the only place the ``lakebase_vector`` / ``lakebase_text`` index names appear.
    """

    def __init__(self, executor: LakebaseSearchExecutor):
        self._executor = executor

    def topk_embedding(self, query_vec, candidates, k):
        return self._executor.vector_topk(query_vec, candidates, k)

    def topk_keyword(self, query_text, candidates, k):
        return self._executor.text_topk(query_text, candidates, k)


class InProcessLakebaseExecutor:
    """A fake ``LakebaseSearchExecutor`` that computes the same math in-process.

    Stands in for the Lakebase Search SQL in offline tests so
    ``LakebaseSearchBackend`` yields byte-identical top-k to
    ``InProcessCosineBackend`` on the same fixture (backend-selection parity, §11).
    """

    _delegate = InProcessCosineBackend()

    def vector_topk(self, query_vec, candidates, k):
        return self._delegate.topk_embedding(query_vec, candidates, k)

    def text_topk(self, query_text, candidates, k):
        return self._delegate.topk_keyword(query_text, candidates, k)


def get_similarity_backend(settings: object | None = None, *, executor: LakebaseSearchExecutor | None = None) -> SimilarityBackend:
    """Return the configured backend.

    ``LakebaseSearchBackend`` iff ``settings.lakebase_search_enabled`` is truthy AND
    an executor is available; otherwise the in-process default (MV-D45). Enablement
    is the §12 human gate — the default keeps the engine fully functional and the
    offline slice on the in-process path.
    """
    enabled = bool(getattr(settings, "lakebase_search_enabled", False))
    if enabled and executor is not None:
        return LakebaseSearchBackend(executor)
    return InProcessCosineBackend()


def unit_norm(vector: Sequence[float]) -> float:
    """L2 magnitude helper (diagnostics/tests); GTE vectors are normalized in _cosine."""
    return math.sqrt(sum(float(x) * float(x) for x in vector)) if vector else 0.0
