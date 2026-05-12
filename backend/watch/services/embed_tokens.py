"""Embed-token mint for AI/BI Lakeview dashboard embedding.

Implements the three-step OIDC flow that lets a Databricks App embed a
dashboard via the @databricks/aibi-client SDK without relying on the
viewer's workspace session cookie.

Reference: https://docs.databricks.com/aws/en/dashboards/embedding/external-embed
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from dataclasses import dataclass

import httpx

from backend.services.auth import get_databricks_host, get_service_principal_client

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(15.0)


@dataclass
class EmbedToken:
    access_token: str
    expires_in: int
    issued_at: int


def _sp_credentials() -> tuple[str, str]:
    cid = os.environ.get("DATABRICKS_CLIENT_ID")
    csec = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if cid and csec:
        return cid, csec

    cfg = get_service_principal_client().config
    cid = getattr(cfg, "client_id", None)
    csec = getattr(cfg, "client_secret", None)
    if not cid or not csec:
        raise RuntimeError(
            "Embed-token mint requires SP credentials. Set DATABRICKS_CLIENT_ID "
            "and DATABRICKS_CLIENT_SECRET (auto-injected on Databricks Apps) or "
            "configure a CLI profile that resolves to a service principal."
        )
    return cid, csec


async def mint_embed_token(
    dashboard_id: str,
    external_viewer_id: str | None = None,
) -> EmbedToken:
    host = get_databricks_host()
    if not host:
        raise RuntimeError("DATABRICKS_HOST is not configured")

    client_id, client_secret = _sp_credentials()
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    auth_header = {"Authorization": f"Basic {basic}"}

    async with httpx.AsyncClient(timeout=_TIMEOUT) as http:
        step1 = await http.post(
            f"{host}/oidc/v1/token",
            headers={**auth_header, "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "client_credentials", "scope": "all-apis"},
        )
        if step1.status_code != 200:
            raise RuntimeError(
                f"OIDC all-apis token request failed: {step1.status_code} {step1.text}"
            )
        all_apis_token = step1.json()["access_token"]

        tokeninfo_url = (
            f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published/tokeninfo"
        )
        params: dict[str, str] = {}
        if external_viewer_id:
            params["external_viewer_id"] = external_viewer_id
        step2 = await http.get(
            tokeninfo_url,
            headers={"Authorization": f"Bearer {all_apis_token}"},
            params=params,
        )
        if step2.status_code != 200:
            raise RuntimeError(
                f"tokeninfo request failed: {step2.status_code} {step2.text}"
            )
        token_info = step2.json()

        body = dict(token_info)
        auth_details = body.pop("authorization_details", None)
        body.update({"grant_type": "client_credentials"})
        if auth_details is not None:
            body["authorization_details"] = json.dumps(auth_details)

        step3 = await http.post(
            f"{host}/oidc/v1/token",
            headers={**auth_header, "Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )
        if step3.status_code != 200:
            raise RuntimeError(
                f"scoped-token request failed: {step3.status_code} {step3.text}"
            )
        scoped = step3.json()

    return EmbedToken(
        access_token=scoped["access_token"],
        expires_in=int(scoped.get("expires_in", 3600)),
        issued_at=int(time.time()),
    )
