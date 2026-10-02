#!/usr/bin/env python3
"""Idempotent VC sandbox identity + namespace provisioner (M08-owned).

Creates the least-privilege service principals the VC integration layer needs,
mints workspace-level OAuth M2M secrets for each, writes per-role profiles into
``~/.databrickscfg``, and (optionally) creates the disposable catalog + control
schema and grants the provisioner CREATE so the ``vc-provision`` job's owner
migrations can run.

This authors NO Genie content and NO table/Volume DDL — the owner migrations in
``backend/version_control_ddl/`` remain the single source of table/Volume shape.
It only stands up identities and the empty namespace.

Secrets are never printed. The client_secret returned by the secrets proxy is
written straight into the profile file and dropped. Non-secret identity facts
(applicationId, SCIM id, profile name) are recorded to a state file OUTSIDE the
repo (``~/.vc-sandbox/<workspace_key>.json``) so a later run is repeatable and so
``VC_INTEGRATION_CONFIG`` can be assembled without re-minting secrets.

Usage:
    python scripts/version_control/provision_sandbox.py \
        --profile fevm-serverless \
        --workspace-key primary \
        --roles provisioner,executor,enrollment \
        [--catalog vc_sandbox --control-schema vc_ctl --warehouse-id <id>]

Re-running is safe: existing SPs are reused (matched by display name), secrets
are only minted for roles missing a live profile, and catalog/schema creation is
IF NOT EXISTS.
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import subprocess
import time
from pathlib import Path

# Display-name prefix keeps every sandbox identity greppable and disposable.
SP_PREFIX = "gwb-vc"
CONFIG_FILE = Path(os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg"))
STATE_ROOT = Path.home() / ".vc-sandbox"


def _run(args: list[str], *, profile: str, capture: bool = True) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_")}
    if CONFIG_FILE:
        env["DATABRICKS_CONFIG_FILE"] = str(CONFIG_FILE)
    return subprocess.run(
        [*args, "--profile", profile, "-o", "json"],
        env=env, capture_output=capture, text=True, timeout=180, check=False,
    )


def _api(method: str, path: str, *, profile: str, body: dict | None = None) -> dict:
    args = ["databricks", "api", method, path]
    if body is not None:
        args += ["--json", json.dumps(body)]
    result = _run(args, profile=profile)
    if result.returncode:
        raise SystemExit(f"API {method} {path} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _host(profile: str) -> str:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    if profile not in cfg:
        raise SystemExit(f"Profile {profile!r} not found in {CONFIG_FILE}")
    return cfg[profile]["host"].rstrip("/")


def _list_service_principals(profile: str) -> dict[str, dict]:
    result = _run(["databricks", "service-principals", "list"], profile=profile)
    if result.returncode:
        raise SystemExit(f"Listing service principals failed: {result.stderr.strip()}")
    out: dict[str, dict] = {}
    for sp in json.loads(result.stdout):
        name = sp.get("displayName")
        if name:
            out[name] = sp
    return out


def _ensure_sp(display_name: str, existing: dict[str, dict], profile: str) -> dict:
    if display_name in existing:
        return existing[display_name]
    result = _run(["databricks", "service-principals", "create",
                   "--display-name", display_name], profile=profile)
    if result.returncode:
        raise SystemExit(f"Creating SP {display_name} failed: {result.stderr.strip()}")
    created = json.loads(result.stdout)
    print(f"  created service principal {display_name} (appId {created.get('applicationId')})")
    time.sleep(1)
    return created


def _mint_secret(scim_id: str, profile: str) -> str:
    result = _run(["databricks", "service-principal-secrets-proxy", "create", str(scim_id)],
                  profile=profile)
    if result.returncode:
        raise SystemExit(f"Minting secret for SP {scim_id} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)["secret"]


def _profile_has_live_oauth(profile_name: str, host: str) -> bool:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    if profile_name not in cfg:
        return False
    section = cfg[profile_name]
    return (section.get("auth_type") == "oauth-m2m"
            and section.get("host", "").rstrip("/") == host
            and bool(section.get("client_id")) and bool(section.get("client_secret")))


def _write_profile(profile_name: str, host: str, client_id: str, client_secret: str) -> None:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    cfg[profile_name] = {
        "host": host,
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_type": "oauth-m2m",
    }
    with open(CONFIG_FILE, "w") as handle:
        cfg.write(handle)
    os.chmod(CONFIG_FILE, 0o600)


def _catalog_exists(catalog: str, profile: str) -> bool:
    result = _run(["databricks", "catalogs", "get", catalog], profile=profile)
    return result.returncode == 0


def _grant_run_as(account_id: str, app_id: str, deployer: str, profile: str) -> str:
    """Grant ``deployer`` roles/servicePrincipal.user on one SP (idempotent).

    Required so the bundle deployer can bind the SP into a job's ``run_as``
    field. Preserves any existing grant rules (e.g. the creator's manager role).
    """
    role = "roles/servicePrincipal.user"
    name = f"accounts/{account_id}/servicePrincipals/{app_id}/ruleSets/default"
    current = _api("get", f"/api/2.0/preview/accounts/access-control/rule-sets?name={name}&etag=",
                   profile=profile)
    rules = current.get("grant_rules", [])
    if any(rule["role"] == role and deployer in rule.get("principals", []) for rule in rules):
        return "already granted"
    rules.append({"principals": [deployer], "role": role})
    _api("put", "/api/2.0/preview/accounts/access-control/rule-sets", profile=profile, body={
        "name": name,
        "rule_set": {"name": name, "etag": current["etag"], "grant_rules": rules},
    })
    return "granted"


def _account_id(profile: str) -> str:
    result = _run(["databricks", "auth", "describe"], profile=profile)
    if result.returncode == 0:
        try:
            described = json.loads(result.stdout)
            account = described.get("details", {}).get("account_id") or described.get("account_id")
            if account:
                return account
        except json.JSONDecodeError:
            pass
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    account = cfg[profile].get("account_id") if profile in cfg else None
    if not account:
        raise SystemExit(f"Could not resolve account_id for profile {profile!r}")
    return account


def _ensure_namespace(catalog: str, control_schema: str, warehouse_id: str,
                      provisioner_app_id: str, consumer_app_ids: list[str],
                      profile: str) -> None:
    statements = []
    if not _catalog_exists(catalog, profile):
        statements.append(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    else:
        print(f"  catalog {catalog} already exists; hosting control schema inside it")
    statements += [
        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{control_schema}`",
        f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{provisioner_app_id}`",
        (f"GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME "
         f"ON SCHEMA `{catalog}`.`{control_schema}` TO `{provisioner_app_id}`"),
    ]
    # Namespace access for every consumer principal. This is admin/catalog-owner
    # authority, so it is issued here (admin profile) rather than by the
    # provision job (which runs as the provisioner and owns only the objects it
    # creates). Object-level privileges are granted by the provision job's
    # least-privilege matrix. Same-account SPs from a paired same-metastore
    # workspace (e.g. the cross-workspace `source`) are grantable here too.
    for consumer in dict.fromkeys(consumer_app_ids):
        if consumer and consumer != provisioner_app_id:
            statements.append(f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{consumer}`")
            statements.append(
                f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{control_schema}` TO `{consumer}`")
    for statement in statements:
        response = _api("post", "/api/2.0/sql/statements", profile=profile, body={
            "warehouse_id": warehouse_id, "statement": statement,
            "wait_timeout": "30s", "on_wait_timeout": "CONTINUE",
        })
        state = response["status"]["state"]
        while state in {"PENDING", "RUNNING"}:
            time.sleep(2)
            response = _api("get", f"/api/2.0/sql/statements/{response['statement_id']}", profile=profile)
            state = response["status"]["state"]
        if state != "SUCCEEDED":
            raise SystemExit(f"Namespace statement failed ({state}): {statement}\n{response['status']}")
        print(f"  ok: {statement}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--workspace-key", required=True,
                        help="Short key for the state file, e.g. primary/secondary")
    parser.add_argument("--roles", required=True,
                        help="Comma-separated role names, e.g. provisioner,executor,enrollment")
    parser.add_argument("--catalog")
    parser.add_argument("--control-schema")
    parser.add_argument("--warehouse-id")
    parser.add_argument("--grant-run-as-to",
                        help="User (e.g. me@databricks.com) to grant servicePrincipal.user "
                             "on each SP so the bundle deployer can bind run_as")
    parser.add_argument("--extra-consumers", default="",
                        help="Comma-separated application ids (e.g. a same-metastore "
                             "cross-workspace source SP) to also grant USE CATALOG/USE SCHEMA")
    args = parser.parse_args()

    roles = [role.strip() for role in args.roles.split(",") if role.strip()]
    host = _host(args.profile)
    STATE_ROOT.mkdir(mode=0o700, exist_ok=True)
    state_path = STATE_ROOT / f"{args.workspace_key}.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"host": host, "roles": {}}

    print(f"Provisioning roles {roles} on {host} (profile {args.profile})")
    existing = _list_service_principals(args.profile)

    for role in roles:
        display_name = f"{SP_PREFIX}-{role}"
        profile_name = f"vc-{args.workspace_key}-{role}"
        sp = _ensure_sp(display_name, existing, args.profile)
        app_id = sp["applicationId"]
        scim_id = sp["id"]
        if not _profile_has_live_oauth(profile_name, host):
            secret = _mint_secret(scim_id, args.profile)
            _write_profile(profile_name, host, app_id, secret)
            del secret
            print(f"  wrote profile [{profile_name}] (oauth-m2m)")
        else:
            print(f"  profile [{profile_name}] already live; leaving secret intact")
        state["roles"][role] = {
            "display_name": display_name, "profile": profile_name,
            "application_id": app_id, "scim_id": scim_id, "host": host,
        }

    if args.grant_run_as_to:
        account_id = _account_id(args.profile)
        deployer = f"users/{args.grant_run_as_to}"
        print(f"Granting {deployer} roles/servicePrincipal.user on each SP")
        for role in roles:
            status = _grant_run_as(account_id, state["roles"][role]["application_id"],
                                   deployer, args.profile)
            print(f"  {role}: {status}")

    if args.catalog and args.control_schema and args.warehouse_id:
        provisioner = state["roles"].get("provisioner")
        if not provisioner:
            raise SystemExit("Namespace creation requires the provisioner role in --roles")
        print(f"Ensuring namespace {args.catalog}.{args.control_schema}")
        extra = [appid.strip() for appid in args.extra_consumers.split(",") if appid.strip()]
        consumers = [meta["application_id"] for meta in state["roles"].values()] + extra
        _ensure_namespace(args.catalog, args.control_schema, args.warehouse_id,
                          provisioner["application_id"], consumers, args.profile)
        state["catalog"] = args.catalog
        state["control_schema"] = args.control_schema
        state["warehouse_id"] = args.warehouse_id

    state_path.write_text(json.dumps(state, indent=2, sort_keys=True))
    os.chmod(state_path, 0o600)
    print(f"State written to {state_path}")
    print("Non-secret identity summary:")
    for role, meta in sorted(state["roles"].items()):
        print(f"  {role:12s} appId={meta['application_id']} profile={meta['profile']}")


if __name__ == "__main__":
    main()
