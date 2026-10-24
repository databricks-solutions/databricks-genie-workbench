"""Validate effective privileges, including inheritance, without widening grants."""

FACT_TABLES = frozenset({"genie_space_versions", "genie_space_registry", "genie_space_operations"})
ARTIFACT_VOLUMES = frozenset({"vc_snapshots", "vc_approval_evidence", "vc_outbound_packages", "vc_target_receipts"})


def verify_artifact_permissions(volumes, expected_writers, operation_writers, source_principals) -> bool:
    if set(volumes) != ARTIFACT_VOLUMES or set(expected_writers) != ARTIFACT_VOLUMES:
        return False
    if not operation_writers or set(operation_writers) & set(source_principals):
        return False
    if len(set(expected_writers.values())) != len(ARTIFACT_VOLUMES) or not all(expected_writers.values()):
        return False
    securables = set()
    runtime_principals = set(expected_writers.values()) | set(source_principals) | set(operation_writers)
    for name, volume in volumes.items():
        securable = volume.get("securable", "")
        if (len(securable.split(".")) != 3 or not securable.endswith(f".{name}")
                or "/" in securable or volume.get("privileges_verified") is not True
                or not volume.get("owner") or volume["owner"] in runtime_principals
                or set(volume.get("writers", ())) != {expected_writers[name]}):
            return False
        if name != "vc_outbound_packages" and expected_writers[name] in source_principals:
            return False
        securables.add(securable)
    return len(securables) == len(ARTIFACT_VOLUMES)


# Unity Catalog tables expose only SELECT and MODIFY (no INSERT/UPDATE). The
# effective-permission proofs therefore assert SELECT+MODIFY exactly; append-only
# tables additionally require delta.appendOnly and a non-owner principal, and the
# coordination write separation is enforced by the protocol (separate SPs, single
# concurrent run, Serializable, CAS), not by DML-verb grants.
_TABLE_WRITE = frozenset({"SELECT", "MODIFY"})


def verify_coordination_permissions(proof) -> bool:
    principals = [proof.get(role) for role in ("owner", "executor", "enrollment")]
    return (
        all(principals) and len(set(principals)) == 3
        and frozenset(proof.get("executor_privileges", ())) == _TABLE_WRITE
        and frozenset(proof.get("enrollment_privileges", ())) == _TABLE_WRITE
        and type(proof.get("max_concurrent_runs")) is int
        and proof["max_concurrent_runs"] == 1
        and proof.get("queue_enabled") is True
        and proof.get("isolation") == "Serializable"
    )


def verify_fact_permissions(facts) -> bool:
    if set(facts) != FACT_TABLES:
        return False
    return all(
        fact.get("append_only") is True
        and bool(fact.get("owner")) and bool(fact.get("principal"))
        and fact["owner"] != fact["principal"]
        and frozenset(fact.get("privileges", ())) == _TABLE_WRITE
        for fact in facts.values()
    )
