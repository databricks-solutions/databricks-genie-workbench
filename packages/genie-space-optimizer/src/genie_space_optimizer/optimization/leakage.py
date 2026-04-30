"""Benchmark answer-shape leakage firewall (Bug #4).

This module prevents benchmark question+answer examples from being copied
verbatim or near-verbatim into inference-visible Example SQL artifacts.
It deliberately does not block structural primitives such as SQL snippets,
join specs, metadata descriptions, synonyms, dictionaries, or instructions.

Design:
* ``canonicalize_sql`` — lexically-normalized fingerprint for answer-shaped
  SQL fields.
* ``BenchmarkCorpus`` — precomputes n-gram shingles and SQL fingerprints for
  benchmark questions and expected SQL.
* ``is_benchmark_leak`` — shape-aware dispatch on patch type. Today only
  ``add_example_sql`` and ``update_example_sql`` are firewalled because they
  persist retrievable question+SQL examples. Structural SQL snippets are
  guarded by source gating, identifier allowlists, SQL validation, proposal
  grounding, and post-apply arbiter evaluation.
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# ── Thresholds ────────────────────────────────────────────────────────

NGRAM_SIMILARITY_THRESHOLD = 0.60
"""n-gram Jaccard similarity at which a string counts as a near-verbatim
leak. Chosen empirically: 0.60 reliably catches "copy with minor
rewording" without flagging unrelated strings that happen to share a
handful of short words."""

SQL_FINGERPRINT_MATCH_EXACT = True
"""If True, any exact lexical-fingerprint match is a leak even when n-gram
similarity on raw text would pass. Exact-match on canonicalized SQL is an
unambiguous signal."""

EMBEDDING_SIMILARITY_THRESHOLD = 0.85
"""Cosine similarity above which an embedding-based check flags a leak.
Chosen empirically for paraphrase detection: 0.85 flags clear rephrasings
without triggering on "same domain, different question"."""

EMBEDDING_ENDPOINT = os.environ.get(
    "GSO_FIREWALL_EMBEDDING_ENDPOINT",
    "databricks-bge-large-en",
)
"""Databricks Foundation Model endpoint used by the firewall for paraphrase
detection. Override via ``GSO_FIREWALL_EMBEDDING_ENDPOINT`` to point at
alternate models during evaluation."""


# ── Canonicalization helpers ───────────────────────────────────────────

_WHITESPACE_RE = re.compile(r"\s+")
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_TRAILING_SEMI_RE = re.compile(r";+\s*$")
_QUOTED_ALIAS_RE = re.compile(r"\bAS\s+`[^`]+`", re.IGNORECASE)


def _strip_sql(sql: str) -> str:
    """Remove comments, normalize whitespace, lower-case. Preserves
    identifiers (they're compared case-insensitively against the canonical
    form of the benchmark) but collapses any cosmetic variation."""
    if not isinstance(sql, str):
        return ""
    s = _BLOCK_COMMENT_RE.sub(" ", sql)
    s = _LINE_COMMENT_RE.sub(" ", s)
    s = _TRAILING_SEMI_RE.sub("", s)
    s = _QUOTED_ALIAS_RE.sub("AS _alias", s)
    s = _WHITESPACE_RE.sub(" ", s).strip().lower()
    return s


def canonicalize_sql(sql: str) -> str:
    """Return a stable SHA-256 fingerprint for ``sql``.

    Two SQL strings that differ only in whitespace, comments, case, or
    quoted-alias syntax produce the same fingerprint. Identifier-level
    differences (different column names, different tables) produce
    different fingerprints — those are legitimately different queries.
    """
    stripped = _strip_sql(sql)
    if not stripped:
        return ""
    return hashlib.sha256(stripped.encode("utf-8")).hexdigest()


def _tokenize(text: str, n: int = 3) -> set[str]:
    """Character n-gram shingles over a normalized form of ``text``."""
    if not isinstance(text, str):
        return set()
    t = _WHITESPACE_RE.sub(" ", text.strip().lower())
    if len(t) < n:
        return set()
    return {t[i : i + n] for i in range(len(t) - n + 1)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


# ── Benchmark corpus pre-computation ───────────────────────────────────


@dataclass
class BenchmarkCorpus:
    """Pre-computed view of the benchmark corpus for fast leakage checks.

    Both train and held-out benchmarks must be passed in — held-out
    leakage is equally disqualifying even though held-out questions are
    never fed into any LLM prompt.
    """
    questions: list[str] = field(default_factory=list)
    expected_sqls: list[str] = field(default_factory=list)
    question_shingles: list[set[str]] = field(default_factory=list)
    sql_shingles: list[set[str]] = field(default_factory=list)
    sql_fingerprints: set[str] = field(default_factory=set)
    question_ids: list[str] = field(default_factory=list)
    # Populated on-demand by precompute_benchmark_embeddings. None means
    # the embedding layer is disabled (either never called or endpoint
    # preflight failed). Test code can populate directly for unit tests.
    question_embeddings: list[list[float]] | None = None
    sql_embeddings: list[list[float]] | None = None
    embedding_endpoint: str | None = None

    @classmethod
    def from_benchmarks(cls, benchmarks: Iterable[dict]) -> "BenchmarkCorpus":
        corpus = cls()
        for b in benchmarks or []:
            if not isinstance(b, dict):
                continue
            q = str(b.get("question", "")).strip()
            sql = str(b.get("expected_sql", "")).strip()
            qid = str(b.get("id", b.get("benchmark_id", "")))
            if not q and not sql:
                continue
            corpus.questions.append(q)
            corpus.expected_sqls.append(sql)
            corpus.question_ids.append(qid)
            corpus.question_shingles.append(_tokenize(q))
            corpus.sql_shingles.append(_tokenize(sql))
            fp = canonicalize_sql(sql)
            if fp:
                corpus.sql_fingerprints.add(fp)
        return corpus

    def __len__(self) -> int:
        return len(self.questions)


# ── Embedding layer (paraphrase detection) ─────────────────────────────


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def get_embedding(text: str, w: Any, endpoint: str | None = None) -> list[float] | None:
    """Return an embedding for ``text`` via Databricks Foundation Model API.

    Returns ``None`` on any failure so callers fall back to the n-gram +
    SQL-fingerprint layer. This keeps the firewall usable in workspaces
    where the embedding endpoint is unavailable.
    """
    if not text or not text.strip() or w is None:
        return None
    ep = endpoint or EMBEDDING_ENDPOINT
    try:
        resp = w.serving_endpoints.query(name=ep, input=[text])
    except Exception as exc:
        logger.debug("get_embedding failed for endpoint=%s: %s", ep, exc)
        return None
    # Databricks serving endpoints return either OpenAI-compatible shape
    # (data[0].embedding) or a raw list. Handle both defensively.
    try:
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        if data and isinstance(data, list):
            first = data[0]
            emb = getattr(first, "embedding", None) or (
                first.get("embedding") if isinstance(first, dict) else None
            )
            if isinstance(emb, list):
                return [float(x) for x in emb]
    except Exception:
        logger.debug("get_embedding response parse failed", exc_info=True)
    return None


def precompute_benchmark_embeddings(
    corpus: BenchmarkCorpus, w: Any, endpoint: str | None = None,
) -> bool:
    """Compute and attach embeddings for every benchmark question + SQL.

    Returns True on success, False if the endpoint is unavailable. On
    failure the corpus is left without embeddings and the firewall
    degrades to n-gram + fingerprint (logged via ``firewall_embedding_disabled``
    counter when wired into the harness).
    """
    if w is None or len(corpus) == 0:
        return False
    ep = endpoint or EMBEDDING_ENDPOINT
    q_emb: list[list[float]] = []
    s_emb: list[list[float]] = []
    for q, s in zip(corpus.questions, corpus.expected_sqls):
        qe = get_embedding(q, w, endpoint=ep) if q else None
        se = get_embedding(s, w, endpoint=ep) if s else None
        if qe is None and q:
            return False
        q_emb.append(qe or [])
        s_emb.append(se or [])
    corpus.question_embeddings = q_emb
    corpus.sql_embeddings = s_emb
    corpus.embedding_endpoint = ep
    return True


def preflight_embedding_endpoint(w: Any, endpoint: str | None = None) -> bool:
    """Cheap availability check — one tiny embedding call. If this fails
    the caller should set ``firewall_embedding_disabled=True`` and log a
    prominent warning; the firewall still runs on n-gram + fingerprint."""
    emb = get_embedding("firewall preflight ping", w, endpoint=endpoint)
    return bool(emb)


# ── Patch-type -> text field mapping ────────────────────────────────────
#
# Firewall scoping (see optimizer design invariant):
#
# Structural SQL paths (``add_sql_snippet_measure`` / ``_filter`` /
# ``_expression``, ``add_join_spec``, ``update_join_spec``) are
# INTENTIONALLY ABSENT from this dict. A measure / filter / expression /
# join-condition is a reusable PRIMITIVE, not an answer — Genie still
# has to pick tables, group-bys, filters to assemble a full query. The
# persistence gates for these are:
#
#   - Proactive seeding — pre-mining arbiter-approved source filter
#     (baseline verdict == ``both_correct``) + ``validate_sql_snippet``
#     EXPLAIN+execute / ``EXPLAIN SELECT 1 FROM l JOIN r ON ... LIMIT 1``.
#   - Lever 6 / join-lever — LLM proposal + exec-validation at propose
#     time + post-iteration full-eval arbiter gate with rollback on
#     regression (equivalent invariant, different mechanism).
#   - Prose miner — user-asserted source + exec-validation.
#
# Adding the firewall here would double-gate and reject legitimate
# structural learning (a benchmark's ``SUM(revenue)`` pattern is
# STRUCTURE, not an ANSWER).
#
# Firewall scoping:
#
# Benchmark leakage is an answer-shape risk. It applies to persisted example
# SQL artifacts that carry question+SQL pairs Genie can later retrieve as
# examples. It does not apply to structural primitives or metadata updates:
# sql snippets, join specs, table/column descriptions, synonyms, dictionaries,
# and space instructions still pass through their own validators plus the
# post-apply full-eval arbiter acceptance gate.
_PATCH_TEXT_FIELDS: dict[str, tuple[str, ...]] = {
    "add_example_sql": ("example_question", "example_sql"),
    "update_example_sql": ("example_question", "example_sql"),
}

# SQL-bearing fields — when a value comes from one of these, we also check
# the canonicalized fingerprint (not just n-gram).
_SQL_FIELDS: frozenset[str] = frozenset({"example_sql", "sql"})


def _flatten_field(value: Any) -> list[str]:
    """Proposals sometimes store a text field as list[str] (e.g.
    ``synonyms: ["a", "b"]``). Return a flat list of string snippets."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list):
        out: list[str] = []
        for v in value:
            out.extend(_flatten_field(v))
        return out
    if isinstance(value, dict):
        out = []
        for v in value.values():
            out.extend(_flatten_field(v))
        return out
    return [str(value)]


def _check_string_against_corpus(
    text: str,
    corpus: BenchmarkCorpus,
    is_sql: bool,
    *,
    w: Any = None,
) -> tuple[bool, str, float]:
    """Check a single candidate string. Returns (is_leak, reason, score).

    Layered checks in order of cost:
    1. SQL fingerprint match (instant).
    2. n-gram Jaccard against question + SQL shingles.
    3. Embedding cosine (only if ``corpus.question_embeddings`` is set AND
       ``w`` is provided — otherwise skipped). Catches paraphrases that
       evade n-gram.
    """
    if not text or not text.strip():
        return False, "", 0.0

    if is_sql and corpus.sql_fingerprints:
        fp = canonicalize_sql(text)
        if fp and fp in corpus.sql_fingerprints:
            return True, "sql_fingerprint_match", 1.0

    shingles = _tokenize(text)
    if not shingles:
        return False, "", 0.0

    comparison_sets = (
        corpus.sql_shingles if is_sql else corpus.question_shingles
    )
    other_sets = (
        corpus.question_shingles if is_sql else corpus.sql_shingles
    )

    best_score = 0.0
    best_idx = -1
    best_src = ""
    for idx, sh in enumerate(comparison_sets):
        score = _jaccard(shingles, sh)
        if score > best_score:
            best_score = score
            best_idx = idx
            best_src = "sql" if is_sql else "question"

    for idx, sh in enumerate(other_sets):
        score = _jaccard(shingles, sh)
        if score > best_score:
            best_score = score
            best_idx = idx
            best_src = "question" if is_sql else "sql"

    if best_score >= NGRAM_SIMILARITY_THRESHOLD:
        return (
            True,
            f"ngram_similarity_{best_src}_qid={corpus.question_ids[best_idx] if 0 <= best_idx < len(corpus.question_ids) else '?'}",
            best_score,
        )

    # Embedding-cosine paraphrase detection — only when precomputed
    # embeddings exist AND a workspace client is provided for encoding
    # the candidate. Otherwise silently degrade (n-gram is the floor).
    if w is not None and corpus.question_embeddings:
        candidate_emb = get_embedding(text, w, endpoint=corpus.embedding_endpoint)
        if candidate_emb:
            targets: list[tuple[list[float], str, int]] = []
            if corpus.question_embeddings:
                for idx, emb in enumerate(corpus.question_embeddings):
                    if emb:
                        targets.append((emb, "question", idx))
            if corpus.sql_embeddings:
                for idx, emb in enumerate(corpus.sql_embeddings):
                    if emb:
                        targets.append((emb, "sql", idx))
            best_cos = 0.0
            best_cos_idx = -1
            best_cos_src = ""
            for emb, src, idx in targets:
                cos = _cosine_similarity(candidate_emb, emb)
                if cos > best_cos:
                    best_cos = cos
                    best_cos_idx = idx
                    best_cos_src = src
            if best_cos >= EMBEDDING_SIMILARITY_THRESHOLD:
                qid = (
                    corpus.question_ids[best_cos_idx]
                    if 0 <= best_cos_idx < len(corpus.question_ids)
                    else "?"
                )
                return (
                    True,
                    f"embedding_cosine_{best_cos_src}_qid={qid}_sim={best_cos:.3f}",
                    best_cos,
                )

    return False, "", best_score


def is_benchmark_leak(
    proposal: dict,
    patch_type: str | None,
    benchmark_corpus: BenchmarkCorpus,
    *,
    w: Any = None,
) -> tuple[bool, str]:
    """Return ``(is_leak, reason)`` for ``proposal``.

    ``patch_type`` determines which text fields are inspected:

    * ``add_example_sql`` — ``example_question``, ``example_sql``
    * ``add_instruction`` / ``update_instruction`` — ``new_text``
    * ``add_column_description`` / ``update_column_description`` — ``description`` (or ``new_text``)
    * ``add_column_dictionary`` — ``values``, ``synonyms``
    * ``add_column_synonym`` — ``synonyms``
    * ``update_join_spec`` / ``add_join_spec`` — ``description``, ``comment``
    * ``add_sql_snippet_{measure,filter,expression}`` — ``sql``, ``display_name``,
      ``synonyms``, ``instruction``

    For SQL-bearing fields the canonicalized SHA-256 fingerprint is also
    compared against the corpus fingerprints. An exact fingerprint match
    is always a leak; text fields are flagged when n-gram Jaccard >=
    ``NGRAM_SIMILARITY_THRESHOLD`` against any benchmark question OR
    expected SQL.

    Returns early (False, "") for empty corpora or unknown patch types —
    that is, the firewall is opt-in per patch type. New patch types that
    do not persist inference-visible text should be added here; see the
    test in ``tests/unit/test_leakage_firewall.py`` which enumerates all
    PATCH_TYPES in ``config.py`` and asserts each is either in
    ``_PATCH_TEXT_FIELDS`` or explicitly excluded.
    """
    if not isinstance(proposal, dict) or not benchmark_corpus or len(benchmark_corpus) == 0:
        return False, ""

    pt = patch_type or proposal.get("patch_type")
    if not pt:
        return False, ""

    fields = _PATCH_TEXT_FIELDS.get(pt)
    if not fields:
        return False, ""

    for field_name in fields:
        value = proposal.get(field_name)
        for snippet in _flatten_field(value):
            is_sql = field_name in _SQL_FIELDS
            is_leak, reason, _score = _check_string_against_corpus(
                snippet, benchmark_corpus, is_sql=is_sql, w=w,
            )
            if is_leak:
                return True, f"{pt}.{field_name}:{reason}"

    return False, ""


# ── Whole-space audit ──────────────────────────────────────────────────


def count_example_sql_leaks(
    space_config: dict, benchmark_corpus: BenchmarkCorpus,
) -> dict[str, int]:
    """Audit a serialized space config for persisted leaks.

    Iterates every inference-visible persisted artifact and applies the
    firewall. Returns ``{patch_type: count}`` — intended for post-apply
    auditing and observability, not for runtime gating (the per-proposal
    firewall does the gating).
    """
    counts: dict[str, int] = {}
    if not isinstance(space_config, dict) or len(benchmark_corpus) == 0:
        return counts

    def _tally(proposal: dict, patch_type: str) -> None:
        is_leak, _reason = is_benchmark_leak(proposal, patch_type, benchmark_corpus)
        if is_leak:
            counts[patch_type] = counts.get(patch_type, 0) + 1

    for eqs in space_config.get("example_question_sqls", []) or []:
        if isinstance(eqs, dict):
            q = eqs.get("question")
            if isinstance(q, list) and q:
                q = q[0]
            sql = eqs.get("sql")
            if isinstance(sql, list) and sql:
                sql = sql[0]
            _tally(
                {"example_question": str(q or ""), "example_sql": str(sql or "")},
                "add_example_sql",
            )

    for table in space_config.get("tables", []) or []:
        if not isinstance(table, dict):
            continue
        for eqs in table.get("example_question_sqls", []) or []:
            if isinstance(eqs, dict):
                q = eqs.get("question")
                if isinstance(q, list) and q:
                    q = q[0]
                sql = eqs.get("sql")
                if isinstance(sql, list) and sql:
                    sql = sql[0]
                _tally(
                    {"example_question": str(q or ""), "example_sql": str(sql or "")},
                    "add_example_sql",
                )
        for col in table.get("column_configs", []) or []:
            if isinstance(col, dict):
                _tally({"description": col.get("description", "")}, "add_column_description")
                _tally({"synonyms": col.get("synonyms", [])}, "add_column_synonym")
                _tally({"values": col.get("dictionary_values", [])}, "add_column_dictionary")

    snippets = space_config.get("sql_snippets", {}) or {}
    if isinstance(snippets, dict):
        for kind, patch_type in (
            ("measures", "add_sql_snippet_measure"),
            ("filters", "add_sql_snippet_filter"),
            ("expressions", "add_sql_snippet_expression"),
        ):
            for item in snippets.get(kind, []) or []:
                if isinstance(item, dict):
                    sql = item.get("sql")
                    if isinstance(sql, list) and sql:
                        sql = sql[0]
                    _tally(
                        {
                            "sql": str(sql or ""),
                            "display_name": item.get("display_name", ""),
                            "synonyms": item.get("synonyms", []),
                            "instruction": item.get("instruction", ""),
                        },
                        patch_type,
                    )

    for js in space_config.get("join_specs", []) or []:
        if isinstance(js, dict):
            _tally(
                {"description": js.get("description", ""), "comment": js.get("comment", "")},
                "add_join_spec",
            )

    return counts


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1b + R1c — LeakageOracle (opaque match API)
# ═══════════════════════════════════════════════════════════════════════
#
# The oracle is the ONLY firewall surface exposed to non-benchmark
# generators (example-SQL synthesis, instruction mining). It wraps one
# or more BenchmarkCorpus instances but never reveals their text
# content — callers can ask "does this leak?" but cannot read
# benchmark questions or SQLs. This is the machine-checkable form of
# isolation invariant #4 documented in ``docs/example-sql-isolation.md``.
#
# Deliberately absent: __iter__, __repr__, direct sqls/questions
# getters, __len__ of individual corpora. Adding any of these would
# re-open the side channel the wrapper exists to close.


# Stopwords dropped from question token-set comparisons. Intentionally
# conservative — we want the set-Jaccard to be meaningful on short
# business questions, not dominated by filler words.
_QUESTION_ECHO_STOPWORDS: frozenset[str] = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "by", "can", "did",
    "do", "does", "for", "from", "give", "has", "have", "how",
    "i", "in", "is", "it", "me", "my", "of", "on", "or", "our",
    "over", "per", "please", "show", "tell", "that", "the", "their",
    "there", "these", "this", "those", "to", "us", "was", "we",
    "were", "what", "whats", "when", "where", "which", "who",
    "will", "with", "would", "you", "your",
})


_QUESTION_PUNCT_RE = re.compile(r"[^\w\s]+")


def _normalize_question_text(q: str) -> set[str]:
    """Return the canonical token set for a question.

    Normalization: lower + strip + collapse whitespace + drop
    punctuation + drop stopwords. The resulting set is what the
    Jaccard match operates on. Empty input returns an empty set.
    """
    if not q or not isinstance(q, str):
        return set()
    text = _QUESTION_PUNCT_RE.sub(" ", q.lower())
    tokens = [t for t in text.split() if t]
    return {t for t in tokens if t not in _QUESTION_ECHO_STOPWORDS}


def _question_token_set_jaccard(a: str, b: str) -> float:
    """Jaccard similarity over normalized token sets."""
    sa = _normalize_question_text(a)
    sb = _normalize_question_text(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


@dataclass(frozen=True)
class ExampleSqlLeakageDecision:
    """Tiered leakage policy outcome for example-SQL generation.

    ``block`` means the candidate must be rejected. ``warning`` means the
    candidate is allowed but operators should know SQL pattern overlaps a
    benchmark (typical case: same SELECT shape, different question intent).
    Both flags can be False — that's the no-overlap path.
    """

    block: bool
    warning: bool
    reason: str
    question_score: float = 0.0
    sql_score: float = 0.0


EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD = float(
    os.environ.get("GSO_EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD", "0.85")
)
"""Jaccard threshold above which the example-SQL question firewall
flags a benchmark-question echo. 0.85 catches "show me total revenue"
≈ "what is total revenue"; independent of the SQL-side firewall
(fingerprint + n-gram). Tune via
``GSO_EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD`` env var."""


class LeakageOracle:
    """Read-only match oracle over one or more benchmark corpora.

    Exposes ONLY boolean match methods. No ``__iter__``, no text
    getters, no ``__repr__`` that leaks content. Non-benchmark
    generators (example-SQL synthesis, instruction mining) receive an
    ``LeakageOracle`` instead of a raw :class:`BenchmarkCorpus` so the
    generator's code and prompts cannot see benchmark questions or
    SQLs even transitively.

    Union semantics: pass multiple corpora and the oracle returns
    ``True`` when ANY of them matches. This is how the example-SQL
    generator firewalls against both the current run's benchmarks
    AND the space's already-installed example_question_sqls without
    exposing either list to the caller.

    See ``docs/example-sql-isolation.md`` for the full contract.
    """

    __slots__ = ("_corpora", "_question_threshold")

    def __init__(
        self,
        *corpora: BenchmarkCorpus,
        question_threshold: float | None = None,
    ) -> None:
        self._corpora: tuple[BenchmarkCorpus, ...] = tuple(
            c for c in corpora if isinstance(c, BenchmarkCorpus) and (
                c.questions or c.expected_sqls
            )
        )
        self._question_threshold = (
            question_threshold
            if question_threshold is not None
            else EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD
        )

    def contains_sql(self, sql: str, *, w: Any = None) -> bool:
        """Return True when ``sql`` matches any corpus via fingerprint
        or n-gram Jaccard (or embedding, when available). Delegates to
        the existing ``_check_string_against_corpus`` so the detection
        logic stays in one place."""
        if not sql or not isinstance(sql, str) or not sql.strip():
            return False
        for corpus in self._corpora:
            is_leak, _reason, _score = _check_string_against_corpus(
                sql, corpus, is_sql=True, w=w,
            )
            if is_leak:
                return True
        return False

    def contains_question(
        self,
        question: str,
        *,
        threshold: float | None = None,
    ) -> bool:
        """Return True when ``question`` echoes any benchmark question
        above ``threshold`` (default
        ``EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD``). Uses token-set
        Jaccard on the canonical form — independent of the SQL-side
        firewall so paraphrase-with-different-SQL is still caught.
        """
        if not question or not isinstance(question, str) or not question.strip():
            return False
        thresh = threshold if threshold is not None else self._question_threshold
        for corpus in self._corpora:
            for benchmark_q in corpus.questions:
                score = _question_token_set_jaccard(question, benchmark_q)
                if score >= thresh:
                    return True
        return False

    def evaluate_example_sql(
        self,
        *,
        question: str,
        sql: str,
        w: Any = None,
    ) -> ExampleSqlLeakageDecision:
        """Tiered leakage policy for example-SQL candidates.

        SQL fingerprint or n-gram overlap alone is a *warning*, because
        teaching examples often share aggregation/join patterns with
        benchmarks. An exact-or-near question echo, joint high
        question+SQL similarity, or exact question + exact SQL is a
        block.
        """
        if not question or not isinstance(question, str):
            question = ""
        if not sql or not isinstance(sql, str):
            sql = ""

        best_question_score = 0.0
        best_sql_score = 0.0
        exact_sql = False

        for corpus in self._corpora:
            sql_fp = canonicalize_sql(sql)
            exact_sql = exact_sql or bool(
                sql_fp and sql_fp in corpus.sql_fingerprints
            )

            for benchmark_q in corpus.questions:
                best_question_score = max(
                    best_question_score,
                    _question_token_set_jaccard(question, benchmark_q),
                )

            sql_shingles = _tokenize(sql)
            if sql_shingles:
                for shingles in corpus.sql_shingles:
                    best_sql_score = max(
                        best_sql_score, _jaccard(sql_shingles, shingles),
                    )

        exact_or_near_question = best_question_score >= self._question_threshold
        high_joint_question = best_question_score >= 0.75
        high_joint_sql = exact_sql or best_sql_score >= NGRAM_SIMILARITY_THRESHOLD

        if exact_or_near_question and exact_sql:
            return ExampleSqlLeakageDecision(
                block=True,
                warning=False,
                reason="exact_question_and_sql",
                question_score=best_question_score,
                sql_score=1.0,
            )
        if high_joint_question and high_joint_sql:
            return ExampleSqlLeakageDecision(
                block=True,
                warning=False,
                reason="high_question_and_sql_similarity",
                question_score=best_question_score,
                sql_score=1.0 if exact_sql else best_sql_score,
            )
        if exact_or_near_question:
            return ExampleSqlLeakageDecision(
                block=True,
                warning=False,
                reason="benchmark_question_echo",
                question_score=best_question_score,
                sql_score=best_sql_score,
            )
        if exact_sql or best_sql_score >= NGRAM_SIMILARITY_THRESHOLD:
            return ExampleSqlLeakageDecision(
                block=False,
                warning=True,
                reason="sql_pattern_overlap_warning",
                question_score=best_question_score,
                sql_score=1.0 if exact_sql else best_sql_score,
            )
        return ExampleSqlLeakageDecision(
            block=False,
            warning=False,
            reason="",
            question_score=best_question_score,
            sql_score=best_sql_score,
        )

    # Deliberately absent — see class docstring for rationale. Any of
    # the following would re-open the side channel the wrapper exists
    # to close. Do not add them.
    # def __iter__(self): ...
    # def __repr__(self): ...
    # @property
    # def questions(self): ...
    # @property
    # def expected_sqls(self): ...


def is_example_sql_benchmark_leak(
    proposal: dict,
    benchmark_corpus: BenchmarkCorpus,
    *,
    w: Any = None,
) -> tuple[bool, str]:
    """Last-mile applier-side relaxed firewall for example-SQL proposals.

    Mirrors :meth:`LeakageOracle.evaluate_example_sql` but accepts a raw
    corpus (the applier has it directly) and returns ``(block, reason)``
    so the existing applier flow can keep its tuple-shaped contract.
    """
    oracle = LeakageOracle(benchmark_corpus)
    decision = oracle.evaluate_example_sql(
        question=str(
            proposal.get("example_question") or proposal.get("question") or ""
        ),
        sql=str(proposal.get("example_sql") or proposal.get("sql") or ""),
        w=w,
    )
    return decision.block, decision.reason
