from dataclasses import dataclass

from genie_space_optimizer.optimization.enforced_mechanism_switch import (
    EnforcedSwitchOutcome,
    enforced_switch_survivors,
)
from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism


@dataclass
class _Prop:
    intent_id: str
    qid: str
    rca_kind: str
    mechanism: PatchMechanism


def _hist(qid, rca, rejected):
    return InertMechanismHistory(
        qid=qid, rca_kind=rca, rejected_mechanisms=tuple(rejected)
    )


def test_drops_reemit_when_fallback_exists_in_slate():
    # gs_026 / top_n: lever-5 (EXAMPLE_SQL) rejected; slate has both an
    # EXAMPLE_SQL re-emit AND a SQL_SNIPPET fallback -> drop the re-emit.
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
        _Prop("p2", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.SQL_SNIPPET),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert isinstance(outcome, EnforcedSwitchOutcome)
    survivors = {p.intent_id for p in outcome.survivors}
    assert survivors == {"p2"}
    assert outcome.dropped[0].intent_id == "p1"
    assert outcome.dropped_reasons["p1"].startswith(
        "GSO_TRIAL30_ENFORCED_SWITCH"
    )


def test_keeps_reemit_when_no_fallback_in_slate():
    # Only the re-emitted EXAMPLE_SQL is present; no structural fallback
    # survived -> keep it, emit NO_FALLBACK_AVAILABLE (never zero out).
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []
    assert outcome.no_fallback_qids == ["gs_026"]


def test_novel_mechanism_untouched():
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.SQL_SNIPPET),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []
    assert outcome.no_fallback_qids == []


def test_lever_alias_caught_via_enum_normalization():
    # rejected stored as a patch_type token (add_example_sql -> EXAMPLE_SQL);
    # a re-emit with that mechanism must be caught even though the history
    # token isn't a lever-id.
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
        _Prop("p2", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.METADATA_DESCRIPTION),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse",
                     ("add_example_sql",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p2"}


def test_no_history_is_identity():
    props = [
        _Prop("p1", "gs_001", "wrong_column",
              PatchMechanism.METADATA_DESCRIPTION),
    ]
    outcome = enforced_switch_survivors(props, ())
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []


def test_different_qid_history_does_not_affect_other_qid():
    # History for gs_026 must not influence a proposal for gs_999.
    props = [
        _Prop("p1", "gs_999", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []
