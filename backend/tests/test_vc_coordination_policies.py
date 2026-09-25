"""Offline contract tests for the production coordination-authorization policies.

These are the callbacks M08 wires into the live CoordinationService (M03 owns the
mechanism; M06/M02 own the policy). They are exercised here against faithful
service fakes so the *bindings* are pinned without a live platform.
"""

from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

from backend.services.version_control import contracts as vc
from backend.services.version_control.coordination import policies


def uid(n):
    return str(uuid5(NAMESPACE_URL, f"policy-{n}"))


_BINDING = vc.BindingRef(uid(1), 1, "sales", "target", "space-1", "prod")
_EXEC = vc.ExecutorContext("target", "https://t", "exec-sp", "service", object(), "job/1")


# -- resolve_binding ---------------------------------------------------------

def test_resolve_binding_returns_binding():
    registry = SimpleNamespace(resolve=lambda bid: _BINDING if bid == _BINDING.binding_id else None)
    assert policies.resolve_binding(registry)(_BINDING.binding_id) == _BINDING


def test_resolve_binding_missing_returns_none():
    def resolve(_bid):
        raise KeyError("missing")

    assert policies.resolve_binding(SimpleNamespace(resolve=resolve))("x") is None


# -- verify_enrollment -------------------------------------------------------

def test_verify_enrollment_true_when_binding_is_current():
    registry = SimpleNamespace(resolve=lambda bid: _BINDING)
    proof = vc.EnrollmentProof(uid(2), _BINDING.binding_id, 1, uid(2))
    assert policies.verify_enrollment(registry)(_BINDING, proof) is True


def test_verify_enrollment_false_when_binding_absent_or_forked():
    def resolve(_bid):
        raise KeyError("no chain")

    proof = vc.EnrollmentProof(uid(2), _BINDING.binding_id, 1, uid(2))
    assert policies.verify_enrollment(SimpleNamespace(resolve=resolve))(_BINDING, proof) is False


# -- insert_enrolled ---------------------------------------------------------

def test_insert_enrolled_delegates_to_store_initialize():
    seen = []
    store = SimpleNamespace(initialize=lambda binding, enrollment: seen.append((binding, enrollment)))
    row = SimpleNamespace(binding=_BINDING)
    policies.insert_enrolled(store)(row)
    assert seen == [(_BINDING, None)]


# -- validate_authorization --------------------------------------------------

def _grant():
    return vc.AuthorizationGrant(
        _BINDING, vc.RequestIdentity(uid(3), "key", "d" * 64), uid(4),
        vc.Fingerprints("c" * 64, "b" * 64, "d" * 64, "vc-c14n/1"),
        "e" * 64, __import__("datetime").datetime(2099, 1, 1, tzinfo=__import__("datetime").UTC),
        uid(4), "f" * 64)


def test_validate_authorization_reauthorizes_the_durable_request_not_the_grant_identity():
    # The grant only carries a RequestIdentity; the policy must re-derive the
    # durable MutationRequest from facts (keyed on operation_id) and re-run the
    # full ApprovalService policy against it.
    grant = _grant()
    mutation_request = object()
    seen = {}

    def get_request(operation_id):
        seen["op"] = operation_id
        return SimpleNamespace(request=mutation_request)

    def authorize(request, executor):
        seen["authorized"] = request
        return grant

    facts = SimpleNamespace(get_request=get_request)
    approvals = SimpleNamespace(authorize=authorize)
    assert policies.validate_authorization(approvals, facts)(grant, _EXEC) is True
    assert seen["op"] == grant.request.operation_id
    assert seen["authorized"] is mutation_request


def test_validate_authorization_false_when_grant_differs():
    grant = _grant()
    other = vc.AuthorizationGrant(grant.binding, grant.request, "different", grant.expected_base_fingerprints,
                                  grant.rendered_target_digest, grant.expires_at, grant.approval_id,
                                  grant.approval_digest)
    facts = SimpleNamespace(get_request=lambda op: SimpleNamespace(request=object()))
    approvals = SimpleNamespace(authorize=lambda request, executor: other)
    assert policies.validate_authorization(approvals, facts)(grant, _EXEC) is False


def test_validate_authorization_false_when_revoked_raises():
    def authorize(request, executor):
        raise PermissionError("Approver policy membership revoked")

    facts = SimpleNamespace(get_request=lambda op: SimpleNamespace(request=object()))
    assert policies.validate_authorization(SimpleNamespace(authorize=authorize), facts)(_grant(), _EXEC) is False


def test_validate_authorization_false_when_request_absent():
    def get_request(operation_id):
        raise KeyError(operation_id)

    approvals = SimpleNamespace(authorize=lambda request, executor: _grant())
    facts = SimpleNamespace(get_request=get_request)
    assert policies.validate_authorization(approvals, facts)(_grant(), _EXEC) is False


# -- verify_create_intent ----------------------------------------------------

def test_verify_create_intent_true_when_request_digest_matches():
    preimage = vc.CreateIntentRef(uid(5), uid(3), _BINDING.binding_id, 1, "d" * 64)
    request = SimpleNamespace(identity=SimpleNamespace(request_digest="d" * 64))
    facts = SimpleNamespace(get_request=lambda op: SimpleNamespace(request=request))
    assert policies.verify_create_intent(facts)(preimage) is True


def test_verify_create_intent_false_on_digest_mismatch():
    preimage = vc.CreateIntentRef(uid(5), uid(3), _BINDING.binding_id, 1, "d" * 64)
    request = SimpleNamespace(identity=SimpleNamespace(request_digest="e" * 64))
    facts = SimpleNamespace(get_request=lambda op: SimpleNamespace(request=request))
    assert policies.verify_create_intent(facts)(preimage) is False


# -- validate_stage_evidence -------------------------------------------------

def _observation(digest="a" * 64):
    return vc.ObservationRef(uid(6), _BINDING.binding_id, 1, digest, "e" * 64)


def test_validate_stage_evidence_true_for_committed_matching_digest():
    obs = _observation()
    evidence = vc.StageEvidence("VC/1.0", vc.PatchStage.CONFIG_OBSERVED,
                                __import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").UTC),
                                obs.state_digest, obs)
    row = SimpleNamespace(binding=_BINDING)
    assert policies.validate_stage_evidence(object(), row, evidence) is True


def test_validate_stage_evidence_false_when_digest_or_binding_mismatch():
    obs = _observation()
    tampered = vc.StageEvidence("VC/1.0", vc.PatchStage.CONFIG_OBSERVED,
                                __import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").UTC),
                                "0" * 64, obs)
    row = SimpleNamespace(binding=_BINDING)
    assert policies.validate_stage_evidence(object(), row, tampered) is False


def test_validate_stage_evidence_false_without_observation():
    evidence = vc.StageEvidence("VC/1.0", vc.PatchStage.CONFIG_IN_FLIGHT,
                                __import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").UTC),
                                "d" * 64, None)
    assert policies.validate_stage_evidence(object(), SimpleNamespace(binding=_BINDING), evidence) is False


# -- off-path fail-closed policies ------------------------------------------

def test_authorize_heads_fails_closed():
    assert policies.authorize_heads(_BINDING, object(), object()) is False


def test_authorize_human_recovery_fails_closed():
    assert policies.authorize_human_recovery(_BINDING, object()) is False
