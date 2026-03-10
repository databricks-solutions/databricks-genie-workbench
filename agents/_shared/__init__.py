"""Shared utilities for Genie Workbench agents.

Provides cross-cutting concerns that multiple agents need:
- auth_bridge: Bridge @app_agent UserContext into monolith + AI Dev Kit auth
- sp_fallback: Service principal fallback for Genie API scope errors
- lakebase_client: Shared PostgreSQL connection pool management
"""
