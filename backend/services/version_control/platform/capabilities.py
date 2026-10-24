"""Live capability checks; outages never reuse an earlier positive result."""

STORAGE_CAPABILITIES = frozenset({
    "fine_grained_dml", "serializable", "nonowner_runtime", "append_only", "enrollment_isolated",
})
FIRST_WRITE_CAPABILITIES = STORAGE_CAPABILITIES | frozenset({
    "coordination_available", "evidence_available", "authorization_available", "identity_verified",
    "owner_handlers_integrated", "canonicalization_verified", "recovery_policy_verified",
    "live_permissions_verified", "live_concurrency_verified", "failure_matrix_verified",
    "bundle_authority_verified", "routed_write_coverage", "artifact_permissions_verified",
})
REQUIRED_WRITER_PATHS = frozenset({
    "create", "edit", "restore", "reconcile", "optimizer_apply", "promotion", "compensation",
})


def capabilities_ready(probe, required) -> bool:
    if probe is None:
        return False
    try:
        evidence = probe()
        return all(evidence.get(capability) is True for capability in required)
    except Exception:
        return False


def storage_write_ready(probe) -> bool:
    return capabilities_ready(probe, STORAGE_CAPABILITIES)


def topology_read_ready(proof) -> bool:
    if (not all(proof.get(key) for key in ("source_workspace", "target_workspace", "source_metastore", "target_metastore"))
            or proof["source_workspace"] == proof["target_workspace"]
            or proof.get("remote_write_credentials") is not False
            or any(proof.get(key) is not True for key in
                   ("packages_readable", "receipts_readable", "source_target_write_denied"))):
        return False
    if proof["source_metastore"] == proof["target_metastore"]:
        return proof.get("shared_uc_grants_verified") is True
    if proof.get("delta_sharing_verified") is not True:
        return False
    representation = proof.get("artifact_representation")
    return ((representation == "volumes" and proof.get("volume_sharing_verified") is True)
            or (representation == "reviewed_readonly" and proof.get("representation_reviewed") is True))
