"""Permutation and synthetic generators for the v1.7 fuzzer.

Five permutation generators wrap an existing ``WorkbenchInputBundle``
and return a deterministic-seeded variant that exercises a structurally
different code path:

* :func:`permute_dispatch_order` — shuffle ``hard_cases``. Tests
  order-independence of the state machine.
* :func:`permute_blame_set_mask` — drop a random subset of
  ``typed_evidence['blame_set']`` per case. Tests Stage 1's graceful
  degradation contract.
* :func:`permute_question_id_carriers` — for every entry in
  ``post_apply_eval_tape``, randomly choose which of the carrier slots
  recognised by :func:`extract_question_id` carries the QID. Exercises
  the Trial-16 RC2 canonical-lookup robustness.
* :func:`permute_qid_namespacing` — randomly swap each
  ``post_apply_eval_tape`` QID between its canonical form
  (``domain_a_gs_009``) and its short form (``gs_009``). Exercises the
  evaluation-row-vs-state-QID identity boundary that postmortem
  ``98ec8950`` flagged.
* :func:`permute_tape_coverage` — randomly drop a fraction of tape
  entries. Surfaces "no row for QID" edge cases the workbench's stub
  must fall back through.

Each generator is a pure function ``(bundle, seed) -> bundle``. The
seed is the only source of randomness, so two calls with the same seed
produce the same output — the fuzzer relies on this for shrinking and
replay.

The synthetic generator (chunk 3) lives in this module too —
:func:`synthesize_bundle` builds a bundle from scratch from a
deterministic-seeded schema.
"""
from __future__ import annotations

import dataclasses
import random
import re
from typing import Iterable, Mapping

from local_lever_workbench.models import (
    WorkbenchHardCase,
    WorkbenchInputBundle,
)


# ─── Carrier slots known to extract_question_id ─────────────────────


# Mirrors the priority chain in
# ``genie_space_optimizer.optimization._qid_extraction._from_canonical_keys``.
# Each entry is a function ``(row, qid) -> row`` that injects the QID
# into one carrier slot, stripping it from every other recognised slot
# first so the chosen carrier wins unambiguously.
_QID_CARRIER_KEYS_FLAT: tuple[str, ...] = (
    "question_id",
    "id",
    "inputs/question_id",
    "inputs.question_id",
)


def _strip_canonical_qid_keys(row: dict) -> dict:
    """Return a shallow copy with every recognised QID carrier removed."""
    cleaned: dict = {k: v for k, v in row.items() if k not in _QID_CARRIER_KEYS_FLAT}
    # Strip the QID out of nested ``inputs`` / ``request`` / ``metadata``
    # carriers too — every carrier choice must be unambiguous.
    if isinstance(cleaned.get("inputs"), dict):
        cleaned["inputs"] = {
            k: v
            for k, v in cleaned["inputs"].items()
            if k not in ("question_id", "id")
        } or None
        if cleaned["inputs"] is None:
            cleaned.pop("inputs", None)
    if isinstance(cleaned.get("request"), dict):
        req_copy = dict(cleaned["request"])
        if isinstance(req_copy.get("kwargs"), dict):
            req_copy["kwargs"] = {
                k: v for k, v in req_copy["kwargs"].items()
                if k != "question_id"
            }
        req_copy.pop("question_id", None)
        cleaned["request"] = req_copy
    if isinstance(cleaned.get("metadata"), dict):
        cleaned["metadata"] = {
            k: v
            for k, v in cleaned["metadata"].items()
            if k not in ("question_id", "id")
        }
    return cleaned


def _set_qid_carrier(row: dict, qid: str, carrier: str) -> dict:
    """Set ``qid`` into the named carrier slot, returning a new row."""
    cleaned = _strip_canonical_qid_keys(row)
    if carrier in _QID_CARRIER_KEYS_FLAT:
        cleaned[carrier] = qid
    elif carrier == "inputs.dict.question_id":
        inputs = dict(cleaned.get("inputs") or {})
        inputs["question_id"] = qid
        cleaned["inputs"] = inputs
    elif carrier == "inputs.dict.id":
        inputs = dict(cleaned.get("inputs") or {})
        inputs["id"] = qid
        cleaned["inputs"] = inputs
    elif carrier == "request.kwargs.question_id":
        request = dict(cleaned.get("request") or {})
        kwargs = dict(request.get("kwargs") or {})
        kwargs["question_id"] = qid
        request["kwargs"] = kwargs
        cleaned["request"] = request
    elif carrier == "metadata.question_id":
        meta = dict(cleaned.get("metadata") or {})
        meta["question_id"] = qid
        cleaned["metadata"] = meta
    else:
        raise ValueError(f"unknown carrier slot: {carrier!r}")
    return cleaned


_ALL_CARRIERS: tuple[str, ...] = (
    "question_id",
    "id",
    "inputs/question_id",
    "inputs.question_id",
    "inputs.dict.question_id",
    "inputs.dict.id",
    "request.kwargs.question_id",
    "metadata.question_id",
)


# ─── Permutation generators ─────────────────────────────────────────


def permute_dispatch_order(
    bundle: WorkbenchInputBundle, seed: int,
) -> WorkbenchInputBundle:
    """Shuffle ``hard_cases``. Exercises order-independence of the SM."""
    rng = random.Random(seed)
    cases = list(bundle.hard_cases)
    rng.shuffle(cases)
    return dataclasses.replace(bundle, hard_cases=tuple(cases))


def permute_blame_set_mask(
    bundle: WorkbenchInputBundle,
    seed: int,
    *,
    min_keep_ratio: float = 0.0,
) -> WorkbenchInputBundle:
    """Drop a random subset of ``typed_evidence['blame_set']`` per case.

    ``min_keep_ratio`` is the minimum fraction of blame-set entries to
    retain per case (rounded up). At 0.0 the fuzzer may drop every
    entry — Stage 1 should still produce a typed verdict (abstain with
    a typed reason) rather than crashing.
    """
    rng = random.Random(seed)
    new_cases: list[WorkbenchHardCase] = []
    for case in bundle.hard_cases:
        te = case.typed_evidence
        if not isinstance(te, dict) or not isinstance(te.get("blame_set"), list):
            new_cases.append(case)
            continue
        original = list(te["blame_set"])
        if not original:
            new_cases.append(case)
            continue
        keep_n = max(
            int(len(original) * min_keep_ratio + 0.5),
            0,
        )
        keep_set = set(rng.sample(original, k=rng.randint(keep_n, len(original))))
        new_te = dict(te)
        new_te["blame_set"] = [b for b in original if b in keep_set]
        new_cases.append(dataclasses.replace(case, typed_evidence=new_te))
    return dataclasses.replace(bundle, hard_cases=tuple(new_cases))


def permute_question_id_carriers(
    bundle: WorkbenchInputBundle, seed: int,
) -> WorkbenchInputBundle:
    """Randomly relocate each tape entry's QID into one of the carrier slots.

    Asserts the Trial-16 RC2 canonical lookup honours every carrier
    recognised by :func:`extract_question_id`.
    """
    rng = random.Random(seed)
    tape = list(bundle.post_apply_eval_tape)
    new_tape: list[Mapping[str, object]] = []
    for entry in tape:
        as_dict = dict(entry)
        qid = ""
        # Pull the QID out of any existing carrier so the carrier
        # choice below is unambiguous.
        for key in _QID_CARRIER_KEYS_FLAT:
            if not qid and as_dict.get(key):
                qid = str(as_dict[key])
        if not qid and isinstance(as_dict.get("inputs"), dict):
            qid = str(as_dict["inputs"].get("question_id") or "")
        if not qid:
            new_tape.append(as_dict)
            continue
        carrier = rng.choice(_ALL_CARRIERS)
        new_tape.append(_set_qid_carrier(as_dict, qid, carrier))
    return dataclasses.replace(bundle, post_apply_eval_tape=tuple(new_tape))


_NAMESPACE_PREFIX_RE = re.compile(r"^(domain_[a-z]+_)(gs_\d+)$")


def permute_qid_namespacing(
    bundle: WorkbenchInputBundle,
    seed: int,
    *,
    flip_probability: float = 0.5,
) -> WorkbenchInputBundle:
    """Randomly toggle each tape QID between its canonical and short form.

    Production rows sometimes carry the short form (``gs_009``) while
    the SM state's canonical QID is the namespaced form
    (``domain_a_gs_009``). The acceptance boundary must join them via
    :func:`extract_question_id` + downstream canonicalisation; this
    generator exercises that join.
    """
    rng = random.Random(seed)
    new_tape: list[Mapping[str, object]] = []
    for entry in bundle.post_apply_eval_tape:
        if rng.random() >= flip_probability:
            new_tape.append(entry)
            continue
        as_dict = dict(entry)
        for key in _QID_CARRIER_KEYS_FLAT:
            val = as_dict.get(key)
            if not isinstance(val, str):
                continue
            match = _NAMESPACE_PREFIX_RE.match(val)
            if match:
                # canonical → short
                as_dict[key] = match.group(2)
            elif val.startswith("gs_"):
                # short → canonical (pick first available domain
                # prefix from any sibling carrier). If none observed,
                # use ``domain_a_`` as the default canonical.
                prefix = "domain_a_"
                for other in _QID_CARRIER_KEYS_FLAT:
                    other_val = as_dict.get(other)
                    if isinstance(other_val, str):
                        m2 = _NAMESPACE_PREFIX_RE.match(other_val)
                        if m2:
                            prefix = m2.group(1)
                            break
                as_dict[key] = f"{prefix}{val}"
        new_tape.append(as_dict)
    return dataclasses.replace(bundle, post_apply_eval_tape=tuple(new_tape))


def permute_tape_coverage(
    bundle: WorkbenchInputBundle,
    seed: int,
    *,
    drop_probability: float = 0.5,
) -> WorkbenchInputBundle:
    """Drop a random subset of tape entries. Exercises 'no row for QID'.

    The workbench stub falls back to the baseline score when no tape
    entry matches the patched QID — the acceptance gate then rolls
    back with ``target_unchanged``. The invariants must hold equally
    on this fall-back path.
    """
    rng = random.Random(seed)
    new_tape = tuple(
        entry
        for entry in bundle.post_apply_eval_tape
        if rng.random() >= drop_probability
    )
    return dataclasses.replace(bundle, post_apply_eval_tape=new_tape)


# ─── Public registry ────────────────────────────────────────────────


# ─── Synthetic generator ────────────────────────────────────────────


_SYNTH_BLAME_TABLE_POOL: tuple[str, ...] = (
    "main.public.orders.revenue",
    "main.public.orders.order_date",
    "main.public.customers.customer_id",
    "main.public.customers.region",
    "main.public.shipments.delivered_at",
    "main.public.products.category",
    "main.public.products.list_price",
    "main.public.events.event_ts",
)


def _synth_eval_row(qid: str, *, rng: random.Random) -> dict:
    """Return a synthetic eval row that passes ``row_is_hard_failure``.

    The row carries enough fields for Stage 1's evidence-card contract
    (question_text, judge rationales, blame set surfaced via typed
    evidence). The Stage 1 LLM tape replay will admit it; the SM can
    then drive it through the funnel.
    """
    rationale_words = rng.choice([
        "missing JOIN to orders",
        "uses LIMIT instead of RANK",
        "WHERE clause filters wrong column",
        "GROUP BY misaligned with aggregation",
        "date filter omits time-zone normalization",
    ])
    return {
        "question_id": qid,
        "inputs/question_id": qid,
        "inputs/question_text": (
            f"What is the total for {qid.replace('_', ' ')}?"
        ),
        "generated_sql": (
            f"SELECT 1 -- synthetic {qid} v{rng.randint(0, 9999)}"
        ),
        "feedback/result_correctness/value": "no",
        "feedback/result_correctness/rationale": rationale_words,
        "feedback/arbiter/value": "genie_incorrect",
        "feedback/arbiter/rationale": rationale_words,
    }


def _synth_typed_evidence(
    qid: str, *, rng: random.Random,
) -> dict:
    blame_n = rng.randint(1, 5)
    blame = list(rng.sample(_SYNTH_BLAME_TABLE_POOL, k=blame_n))
    return {
        "qid": qid,
        "blame_set": blame,
        "confidence": rng.choice(["low", "medium", "high"]),
        "expected_sql_shape": rng.choice([
            "join+aggregate", "window_rank", "filter+aggregate",
        ]),
        "generated_sql_issue": rng.choice([
            "wrong_join_columns", "missing_filter", "incorrect_aggregation",
            "wrong_window_function", "schema_drift",
        ]),
        "observed_failure": rng.choice([
            "logical_accuracy", "result_correctness", "completeness",
        ]),
        "quoted_evidence": rng.sample(
            [
                "LIMIT 10 in query",
                "missing WHERE clause",
                "JOIN type=LEFT but should be INNER",
                "aggregate SUM but column is non-numeric",
            ],
            k=rng.randint(1, 3),
        ),
        "repair_hint_patch_type": rng.choice([
            "add_column_description",
            "add_table_description",
            "edit_general_instruction",
            "add_sql_snippet",
        ]),
        "suggested_repair_family": rng.choice([
            "schema_metadata", "general_instructions",
            "sql_snippet", "table_selection",
        ]),
    }


def synthesize_bundle(
    *,
    base: WorkbenchInputBundle,
    seed: int,
    n_qids: int | None = None,
) -> WorkbenchInputBundle:
    """Synthesize a deterministic-seeded bundle from an observed-shape schema.

    ``base`` supplies the metadata snapshot + the QID pool — synthetic
    cases reuse base QIDs so the Stage-1/2/3 LLM tape harnesses keep
    routing correctly. The synthesizer replaces ``row`` and
    ``typed_evidence`` per case with seeded random variants, and
    builds a procedural ``post_apply_eval_tape`` with random correctness
    outcomes.

    ``n_qids`` defaults to ``len(base.hard_cases)``; when smaller, a
    random subset is chosen.
    """
    rng = random.Random(seed)
    pool = list(base.hard_cases)
    if not pool:
        raise ValueError("synthesize_bundle requires a non-empty base bundle")
    target_n = n_qids if n_qids is not None else len(pool)
    target_n = max(1, min(target_n, len(pool)))
    chosen = rng.sample(pool, k=target_n)

    synth_cases: list[WorkbenchHardCase] = []
    synth_tape: list[Mapping[str, object]] = []
    for case in chosen:
        qid = case.qid
        synth_row = _synth_eval_row(qid, rng=rng)
        synth_te = _synth_typed_evidence(qid, rng=rng)
        synth_cases.append(
            WorkbenchHardCase(
                qid=qid,
                row=synth_row,
                typed_evidence=synth_te,
                expected_card_violations=case.expected_card_violations,
            )
        )
        # Bias toward "fixed" so most synthetic runs reach ACCEPTED —
        # the rolled-back path is already covered by the always-on
        # suite. A small fraction rolls back to surface the gate
        # path under synthetic evidence too.
        correctness = 1.0 if rng.random() < 0.75 else 0.0
        synth_tape.append({
            "question_id": qid,
            "inputs/question_id": qid,
            "generated_sql": (
                f"SELECT POST -- synthetic {qid} seed{seed}"
            ),
            "feedback/result_correctness/value": correctness,
            "eval_row_id": f"workbench-synth-{seed}-{qid}",
        })

    return dataclasses.replace(
        base,
        hard_cases=tuple(synth_cases),
        post_apply_eval_tape=tuple(synth_tape),
    )


# ─── Public registry ────────────────────────────────────────────────


_GENERATORS = {
    "dispatch_order": permute_dispatch_order,
    "blame_set_mask": permute_blame_set_mask,
    "question_id_carriers": permute_question_id_carriers,
    "qid_namespacing": permute_qid_namespacing,
    "tape_coverage": permute_tape_coverage,
}


def list_permutation_names() -> tuple[str, ...]:
    return tuple(_GENERATORS.keys())


def apply_permutation(
    bundle: WorkbenchInputBundle, name: str, seed: int,
) -> WorkbenchInputBundle:
    """Apply the named permutation. Raises KeyError on unknown name."""
    return _GENERATORS[name](bundle, seed)


def apply_permutation_chain(
    bundle: WorkbenchInputBundle,
    names: Iterable[str],
    seed: int,
) -> WorkbenchInputBundle:
    """Apply several permutations in sequence with seeds derived from ``seed``.

    Each permutation gets ``seed + i`` so the chain is reproducible
    from a single seed.
    """
    out = bundle
    for i, name in enumerate(names):
        out = apply_permutation(out, name, seed + i)
    return out


__all__ = [
    "apply_permutation",
    "apply_permutation_chain",
    "list_permutation_names",
    "permute_blame_set_mask",
    "permute_dispatch_order",
    "permute_qid_namespacing",
    "permute_question_id_carriers",
    "permute_tape_coverage",
    "synthesize_bundle",
]
