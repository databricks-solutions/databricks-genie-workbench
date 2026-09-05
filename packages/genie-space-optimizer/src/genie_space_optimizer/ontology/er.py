"""L3 entity resolution / dedupe engine (Phase 3a).

The standard three-step pipeline over {governed tags, measures, MV bodies, Agent
scopes, page-name candidates}, run INSIDE the Phase-2 materializer (no new job):

  1. PII firewall  — a tag name that echoes a PII token is rejected up-front and
     never enters the identity map (tag names replicate globally in plaintext).
  2. Block         — bucket candidates cheaply by kind + singularized token, so we
     only compare within a bucket (sub-quadratic; recall 1.0 on true dups that
     share a token, including paraphrases sharing a head noun).
  3. Score         — two signals per pair via the ONE ``similarity`` backend:
     string/keyword (edit + token) and embedding (cosine over GTE vectors).
  4. Adjudicate    — auto-merge high, auto-reject/distinct low, escalate ONLY the
     near-tie band to the LLM (``common.llm.call_llm_core``) for a yes/no + reason.
  5. Confidence gate — dedup_gate-pattern thresholds decide the band boundaries.

Degrade, never block (MV-D43): if the similarity or LLM path is unavailable the
engine falls back / skips the escalation and still emits exact/string verdicts;
the run never fails on adjudication unavailability. Pure/offline: no external call
at module scope — the default LLM adjudicator lazy-imports the wheel-native client
and degrades if the endpoint is unreachable (identity is injected by the caller).
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Literal, Sequence

from genie_space_optimizer.ontology import transforms
from genie_space_optimizer.ontology.similarity import SimilarityBackend

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

Verdict = Literal["merge", "reject", "escalate", "distinct"]
Method = Literal["exact", "string", "embedding", "llm"]

# dedup_gate-pattern thresholds (§4/§6). A pair whose best signal is:
#   >= MERGE_THRESHOLD         -> auto-merge
#   in [ESCALATE_LOW, MERGE)   -> near-tie band -> escalate to the LLM
#   <  ESCALATE_LOW            -> distinct (never compared to the LLM)
MERGE_THRESHOLD = 0.90
ESCALATE_LOW = 0.72

# Bounded batch adjudication (MV-D68 — the pages pattern, MV-D66, generalized to ER).
# The near-tie band is the ONLY LLM path in ER; over a large tag inventory it was the
# last UNBOUNDED + SEQUENTIAL enricher (the batch-timeout sink after naming/pages were
# bounded). In batch it is now capped + fanned out so ER's LLM cost is a small CONSTANT,
# not O(estate): the strongest near-ties (score desc) that clear ``MIN_SCORE`` are
# adjudicated up to ``MAX_PAIRS`` via a bounded pool; every other near-tie degrades to
# ``escalate`` — left DISTINCT and surfaced as a curator dedupe proposal (never a silent
# LLM auto-merge). These are the shipped OPERATIONAL bounds (the job threads them);
# ``run_er``'s own defaults stay unbounded/sequential so the pure function and every
# existing verdict remain byte-identical.
ER_ADJUDICATE_MAX_PAIRS = 30
ER_ADJUDICATE_MIN_SCORE = 0.85
ER_ADJUDICATE_MAX_WORKERS = 4

# An adjudicator answers a near-tie: (decision, reason). decision True=merge,
# False=reject, None=could-not-adjudicate (degrade -> escalate, unmerged).
Adjudicator = Callable[["DedupeCandidate", "DedupeCandidate"], "tuple[bool | None, str | None]"]


@dataclass(frozen=True)
class DedupeCandidate:
    ref: str            # canonical member ref (tag_key / measure fqn / mv fqn / agent id)
    kind: str           # 'tag' | 'measure' | 'metric_view' | 'agent' | 'page_name'
    name: str
    text: str           # name + comment used for embedding/BM25
    context: str | None = None  # bounded context (domain/schema) — map-not-merge (MV-D60)


# Cross-context correspondence (MV-D60): the same real-world noun legitimately differs
# by context (customer vs party vs subscriber), so instead of MERGING two contexts we
# MAP them — recording a typed relation for the Stage-4 [Disambiguation] Page.
CorrespondenceRelation = Literal["same-as", "role-of", "related"]


@dataclass(frozen=True)
class Correspondence:
    a_ref: str
    b_ref: str
    context_a: str
    context_b: str
    relation: CorrespondenceRelation
    method: Method
    score: float


@dataclass(frozen=True)
class DedupeVerdict:
    canonical_id: str   # derived id (dedupe_<fingerprint>)
    members: tuple[str, ...]
    verdict: Verdict
    method: Method
    score: float
    reason: str | None  # populated only for LLM-adjudicated near-ties


# ── PII firewall (LeakageOracle reject-on-match discipline, tag names) ──────

_PII_TOKENS = {
    "ssn", "social", "socialsecurity", "email", "e_mail", "phone", "mobile",
    "dob", "birthdate", "birthday", "passport", "creditcard", "credit_card",
    "cardnumber", "taxid", "nationalid", "national_id", "sin", "iban",
}
_EMAIL_RE = re.compile(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", re.IGNORECASE)
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_LONGNUM_RE = re.compile(r"\b\d{13,19}\b")  # card / account-number shaped


def pii_reject(name: str) -> bool:
    """True when a (tag) name echoes a PII token — reject before it enters the map.

    Tag names replicate globally in plaintext, so a name that carries a PII token
    (``customer_ssn``, an email, a card-shaped number) is a leak vector. Conservative
    by design: matches PII token words in the name's tokens, plus email/SSN/long-number
    shapes. This is the extended-LeakageOracle firewall step for tag names.
    """
    if not name:
        return False
    lowered = name.casefold()
    if _EMAIL_RE.search(lowered) or _SSN_RE.search(name) or _LONGNUM_RE.search(name):
        return True
    tokens = {t for t in re.split(r"[^0-9a-zA-Z]+", lowered) if t}
    collapsed = lowered.replace("_", "").replace("-", "")
    if tokens & _PII_TOKENS:
        return True
    # Catch glued forms like "customerssn" / "emailaddress".
    return any(tok in collapsed for tok in ("ssn", "socialsecurity", "creditcard", "passport"))


# ── Blocking ────────────────────────────────────────────────────────────────


def block(candidates: Sequence[DedupeCandidate]) -> dict[tuple[str, str], list[DedupeCandidate]]:
    """Bucket candidates by (kind, singularized token). A candidate lands in one
    bucket per token, so true duplicates that share ≥1 token co-occur (recall 1.0
    on such pairs) while bucket sizes stay bounded by token frequency."""
    buckets: dict[tuple[str, str], list[DedupeCandidate]] = {}
    for c in candidates:
        toks = set(transforms._tokens(c.name or c.ref))
        for tok in toks:
            buckets.setdefault((c.kind, tok), []).append(c)
    return buckets


def candidate_pairs(buckets: dict[tuple[str, str], list[DedupeCandidate]]) -> set[tuple[str, str]]:
    """Unique unordered candidate ref-pairs to compare (deduped across buckets)."""
    pairs: set[tuple[str, str]] = set()
    for members in buckets.values():
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                a, b = members[i].ref, members[j].ref
                pairs.add((a, b) if a < b else (b, a))
    return pairs


# ── Scoring via the similarity backend ──────────────────────────────────────


def _pair_scores(
    a: DedupeCandidate,
    b: DedupeCandidate,
    backend: SimilarityBackend,
    vectors: dict[str, Sequence[float]],
) -> tuple[float, float]:
    """(embedding cosine, keyword score) for a pair, routed through the backend."""
    va, vb = vectors.get(a.ref), vectors.get(b.ref)
    cosine = 0.0
    if va and vb:
        top = backend.topk_embedding(va, [(b.ref, vb)], 1)
        cosine = top[0][1] if top else 0.0
    kw_top = backend.topk_keyword(a.text, [(b.ref, b.text)], 1)
    keyword = kw_top[0][1] if kw_top else 0.0
    return cosine, keyword


# ── Union-find ───────────────────────────────────────────────────────────────


class _UF:
    def __init__(self, refs: Sequence[str]):
        self.parent = {r: r for r in refs}

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def canonical_id_of(members: Sequence[str]) -> str:
    """Derived, stable id — a fingerprint of the sorted member set (idempotent)."""
    digest = hashlib.sha256("".join(sorted(members)).encode("utf-8")).hexdigest()
    return f"dedupe_{digest[:16]}"


def candidates_from_graph(graph: dict) -> list[DedupeCandidate]:
    """Build ``tag`` DedupeCandidates from the tag-graph structure (the primary ER
    inventory offline). ``ref`` == ``name`` == ``tag_key``; ``text`` folds in the
    allowed values so the embedding/keyword signals have the tag's vocabulary."""
    out: list[DedupeCandidate] = []
    for t in graph.get("tags", []):
        key = t.get("tag_key")
        if not key:
            continue
        allowed = " ".join(str(v) for v in (t.get("allowed_values") or []))
        text = (key + " " + allowed).strip()
        out.append(DedupeCandidate(ref=key, kind="tag", name=key, text=text))
    return out


# ── The engine ────────────────────────────────────────────────────────────────


def _cross_context(a: DedupeCandidate, b: DedupeCandidate) -> bool:
    """True iff both candidates declare a context and the contexts differ — a
    would-be merge across bounded contexts becomes a MAP, not a collapse (MV-D60)."""
    return a.context is not None and b.context is not None and a.context != b.context


def _relation(a: DedupeCandidate, b: DedupeCandidate) -> CorrespondenceRelation:
    """Type a cross-context correspondence: identical name → ``same-as``; one name's
    tokens contain the other's → ``role-of``; otherwise a near-duplicate → ``related``."""
    if a.name.casefold() == b.name.casefold():
        return "same-as"
    ta, tb = set(transforms._tokens(a.name)), set(transforms._tokens(b.name))
    if ta and tb and (ta <= tb or tb <= ta):
        return "role-of"
    return "related"


def run_er(
    candidates: Sequence[DedupeCandidate],
    *,
    backend: SimilarityBackend,
    vectors: dict[str, Sequence[float]] | None = None,
    adjudicator: Adjudicator | None = None,
    correspondences: list[Correspondence] | None = None,
    adjudicate_max_pairs: int | None = None,
    adjudicate_min_score: float = ESCALATE_LOW,
    adjudicate_max_workers: int = 1,
) -> list[DedupeVerdict]:
    """Resolve candidates into canonical entities. Returns one DedupeVerdict per
    canonical group (singletons included), most-merged first.

    ``vectors`` maps ref -> precomputed (GTE) embedding; missing vectors simply
    drop the embedding signal for that candidate (string signal still applies).
    ``adjudicator`` is called ONLY for near-tie-band pairs; if it is None or raises,
    those pairs degrade to unmerged/escalate (never a hard failure, MV-D43).

    Bounded batch adjudication (MV-D68): the near-tie band is collected first, then only
    the strongest pairs (score desc) that clear ``adjudicate_min_score`` are LLM-
    adjudicated, up to ``adjudicate_max_pairs``, fanned out over a pool of
    ``adjudicate_max_workers`` (a PURE fan-out — the union/merge writes stay sequential).
    Every near-tie NOT selected (below the floor, beyond the cap, or when no adjudicator
    is supplied) degrades to ``escalate``: left DISTINCT and surfaced as a curator dedupe
    proposal rather than silently merged. The defaults (``max_pairs=None`` /
    ``min_score=ESCALATE_LOW`` / ``max_workers=1``) reproduce the pre-MV-D68 behavior
    exactly — the job threads the shipped operational caps.

    Map-not-merge (MV-D60): a would-be merge whose two candidates sit in DIFFERENT
    bounded contexts is NOT collapsed — the two stay distinct entities, and (when the
    ``correspondences`` list is supplied) a typed :class:`Correspondence` is recorded
    for the Stage-4 [Disambiguation] Page. Candidates with no context (the default)
    behave exactly as before, so the canonical-id scheme and every existing verdict are
    byte-identical."""
    vectors = vectors or {}

    def _map_not_merge(a: DedupeCandidate, b: DedupeCandidate, method: Method, score: float) -> None:
        if correspondences is not None:
            correspondences.append(Correspondence(
                a_ref=a.ref, b_ref=b.ref, context_a=str(a.context), context_b=str(b.context),
                relation=_relation(a, b), method=method, score=score,
            ))
    # 1) PII firewall — drop PII-echoing names entirely (never enter the map).
    survivors = [c for c in candidates if not pii_reject(c.name)]
    by_ref = {c.ref: c for c in survivors}

    # 2) Block → 3) score → 4/5) adjudicate+gate. Track the strongest edge and the
    # strongest non-merge (band) outcome per ref, for group + singleton verdicts.
    uf = _UF([c.ref for c in survivors])
    merge_edge: dict[frozenset[str], tuple[Method, float, str | None]] = {}
    band_outcome: dict[str, tuple[Verdict, Method, float, str | None]] = {}

    def _note_band(ref: str, verdict: Verdict, method: Method, score: float, reason: str | None) -> None:
        rank = {"reject": 3, "escalate": 2, "distinct": 1}
        cur = band_outcome.get(ref)
        if cur is None or rank[verdict] > rank[cur[0]] or (rank[verdict] == rank[cur[0]] and score > cur[2]):
            band_outcome[ref] = (verdict, method, score, reason)

    # Phase 1 — deterministic pass (no LLM): auto-merge (exact / >= MERGE_THRESHOLD) and
    # auto-distinct (< ESCALATE_LOW) inline; COLLECT the near-tie band for Phase 2 so the
    # LLM calls can be capped + fanned out rather than issued one-by-one in-loop.
    band_pairs: list[tuple[str, str, float, Method]] = []
    for a_ref, b_ref in candidate_pairs(block(survivors)):
        a, b = by_ref[a_ref], by_ref[b_ref]
        # Exact name match is a merge with no scoring needed.
        if a.name.casefold() == b.name.casefold():
            if _cross_context(a, b):
                _map_not_merge(a, b, "exact", 1.0)  # distinct entities, mapped (MV-D60)
            else:
                uf.union(a_ref, b_ref)
                merge_edge[frozenset((a_ref, b_ref))] = ("exact", 1.0, None)
            continue

        cosine, keyword = _pair_scores(a, b, backend, vectors)
        best = max(cosine, keyword)
        method: Method = "embedding" if cosine >= keyword else "string"

        if best >= MERGE_THRESHOLD:
            if _cross_context(a, b):
                _map_not_merge(a, b, method, best)  # distinct entities, mapped (MV-D60)
            else:
                uf.union(a_ref, b_ref)
                merge_edge[frozenset((a_ref, b_ref))] = (method, best, None)
        elif best >= ESCALATE_LOW:
            band_pairs.append((a_ref, b_ref, best, method))  # near-tie — defer to Phase 2
        else:
            _note_band(a_ref, "distinct", method, best, None)
            _note_band(b_ref, "distinct", method, best, None)

    # Phase 2 — bounded batch adjudication (MV-D68). Rank the near-ties (score desc, then
    # refs for a deterministic tiebreak) and split into the LLM-adjudicated selection
    # (has an adjudicator, clears ``adjudicate_min_score``, within ``adjudicate_max_pairs``)
    # and the deferred remainder. The selection's LLM calls are a PURE fan-out via
    # ``run_bounded`` (union/merge writes stay sequential below); the deferred pairs
    # degrade to ``escalate`` — DISTINCT + surfaced as a curator dedupe proposal.
    band_pairs.sort(key=lambda t: (-t[2], t[0], t[1]))
    selected: list[tuple[str, str, float, Method]] = []
    deferred: list[tuple[str, str, float, Method]] = []
    for bp in band_pairs:
        if (
            adjudicator is not None
            and bp[2] >= adjudicate_min_score
            and (adjudicate_max_pairs is None or len(selected) < adjudicate_max_pairs)
        ):
            selected.append(bp)
        else:
            deferred.append(bp)

    def _adjudicate_pair(bp: tuple[str, str, float, Method]) -> tuple[bool | None, str | None]:
        a_ref, b_ref, _best, _method = bp
        try:
            return adjudicator(by_ref[a_ref], by_ref[b_ref])  # type: ignore[misc]
        except Exception as e:  # noqa: BLE001 — degrade, never block the run
            logger.info("ontology ER adjudication failed (%s); leaving pair unmerged", e)
            return None, None

    decisions = (
        transforms.run_bounded(selected, _adjudicate_pair, max_workers=adjudicate_max_workers)
        if selected else []
    )
    for (a_ref, b_ref, best, method), (decision, reason) in zip(selected, decisions):
        a, b = by_ref[a_ref], by_ref[b_ref]
        if decision is True:
            if _cross_context(a, b):
                _map_not_merge(a, b, "llm", best)  # distinct entities, mapped (MV-D60)
            else:
                uf.union(a_ref, b_ref)
                merge_edge[frozenset((a_ref, b_ref))] = ("llm", best, reason)
        elif decision is False:
            _note_band(a_ref, "reject", "llm", best, reason)
            _note_band(b_ref, "reject", "llm", best, reason)
        else:  # None -> could not adjudicate (LLM down): escalate, unmerged
            _note_band(a_ref, "escalate", method, best, None)
            _note_band(b_ref, "escalate", method, best, None)

    # Deferred near-ties (below floor / beyond cap / no adjudicator): escalate — left
    # distinct, surfaced for curator review rather than silently merged.
    for a_ref, b_ref, best, method in deferred:
        _note_band(a_ref, "escalate", method, best, None)
        _note_band(b_ref, "escalate", method, best, None)

    # 6) Assemble canonical groups.
    groups: dict[str, list[str]] = {}
    for ref in by_ref:
        groups.setdefault(uf.find(ref), []).append(ref)

    verdicts: list[DedupeVerdict] = []
    for members in groups.values():
        members_sorted = tuple(sorted(members))
        cid = canonical_id_of(members_sorted)
        if len(members_sorted) > 1:
            # Strongest merge edge in the group sets the group's method/score/reason:
            # highest score, ties broken by evidence precedence (exact > llm > embedding > string).
            prec = {"exact": 4, "llm": 3, "embedding": 2, "string": 1}
            member_set = set(members_sorted)
            best: tuple[Method, float, str | None] | None = None
            for edge_refs, (m, s, r) in merge_edge.items():
                if edge_refs <= member_set:
                    if best is None or s > best[1] or (s == best[1] and prec[m] > prec[best[0]]):
                        best = (m, s, r)
            bm, bs, br = best if best is not None else ("string", 0.0, None)
            verdicts.append(DedupeVerdict(cid, members_sorted, "merge", bm, bs, br))
        else:
            ref = members_sorted[0]
            outcome = band_outcome.get(ref)
            if outcome is None:
                verdicts.append(DedupeVerdict(cid, members_sorted, "distinct", "string", 0.0, None))
            else:
                v, m, s, r = outcome
                verdicts.append(DedupeVerdict(cid, members_sorted, v, m, s, r))

    verdicts.sort(key=lambda v: (-len(v.members), v.canonical_id))
    return verdicts


# ── Default LLM adjudicator (lazy backend import; degrades if unavailable) ──


def default_adjudicator(
    model: str | None = None, w: "WorkspaceClient | None" = None,
) -> Adjudicator:
    """Return a near-tie adjudicator backed by the wheel-native LLM client
    (:func:`genie_space_optimizer.common.llm.call_llm_core`).

    The identity ``w`` is INJECTED (the job passes its ``run_as`` client). If the
    import or call fails the adjudicator returns ``(None, None)`` (degrade → the
    near-tie stays unmerged). The LLM is the ONLY external call, and it is reached
    ONLY for near-tie pairs.
    """

    def _adjudicate(a: DedupeCandidate, b: DedupeCandidate) -> tuple[bool | None, str | None]:
        try:
            from genie_space_optimizer.common.llm import call_llm_core
        except Exception:  # noqa: BLE001 — client unavailable here → degrade
            return None, None
        prompt = (
            "You decide whether two data-catalog entities refer to the SAME concept "
            "(a reuse/duplicate) or are genuinely DISTINCT. Answer strictly as "
            "'YES: <short reason>' or 'NO: <short reason>'.\n\n"
            f"Entity A ({a.kind}): {a.name}\n  context: {a.text}\n"
            f"Entity B ({b.kind}): {b.name}\n  context: {b.text}\n"
        )
        try:
            resp_text, _ = call_llm_core(
                w, messages=[{"role": "user", "content": prompt}], model=model, max_tokens=120,
            )
        except Exception as e:  # noqa: BLE001 — degrade, never block the run
            logger.info("ontology ER LLM adjudication call failed: %s", e)
            return None, None
        text = (resp_text or "").strip()
        head = text.split(":", 1)
        reason = head[1].strip() if len(head) > 1 else text
        if text.upper().startswith("YES"):
            return True, reason
        if text.upper().startswith("NO"):
            return False, reason
        return None, None  # unparseable → treat as unresolved

    return _adjudicate
