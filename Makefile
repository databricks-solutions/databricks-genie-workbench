# Genie Space Optimizer — deployment helpers
# Usage:
#   make help             Show usage and current variable values
#   make setup            First-time install: deploy + start app + verify
#   make deploy           Subsequent deploys: clean + bundle + app deploy + verify
#   make verify           Check that workspace has the expected wheel
#   make clean-wheels     Remove stale wheels from the workspace

PROFILE       ?= DEFAULT
APP_NAME      ?= genie-space-optimizer
CATALOG       ?= main
SCHEMA        ?= genie_optimization
WAREHOUSE_ID  ?=

DEPLOYER_EMAIL := $(shell databricks current-user me --profile $(PROFILE) -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))" 2>/dev/null)
WS_BUILD_PATH   = /Workspace/Users/$(DEPLOYER_EMAIL)/.bundle/genie-space-optimizer/dev/files/.build

.PHONY: help setup deploy verify clean-wheels

# ── Help ─────────────────────────────────────────────────────────────

help:
	@echo "Genie Space Optimizer Deployment"
	@echo ""
	@echo "First-time setup (single command):"
	@echo "  make setup APP_NAME=my-app CATALOG=my_catalog WAREHOUSE_ID=abc123"
	@echo ""
	@echo "Subsequent deploys:"
	@echo "  make deploy"
	@echo ""
	@echo "Current variable values (override with VAR=value):"
	@echo "  PROFILE      = $(PROFILE)"
	@echo "  APP_NAME     = $(APP_NAME)"
	@echo "  CATALOG      = $(CATALOG)"
	@echo "  SCHEMA       = $(SCHEMA)"
	@echo "  WAREHOUSE_ID = $(WAREHOUSE_ID)"
	@echo "  DEPLOYER     = $(DEPLOYER_EMAIL)"
	@echo "  WS_BUILD     = $(WS_BUILD_PATH)"

# ── First-time setup (handles app-not-running) ──────────────────────

setup: _check_vars clean-wheels _bundle_deploy _ensure_app_running _apply_grants _app_deploy verify
	@echo ""
	@echo "Setup complete! App URL:"
	@databricks apps get $(APP_NAME) --profile $(PROFILE) -o json 2>/dev/null \
		| python3 -c "import sys,json; print(json.load(sys.stdin).get('url','(unknown)'))" 2>/dev/null \
		|| echo "(could not retrieve URL)"

# ── Default deploy target ────────────────────────────────────────────

deploy: _check_vars clean-wheels _bundle_deploy _apply_grants _app_deploy verify

# ── Internal targets ─────────────────────────────────────────────────

_check_vars:
	@if [ -z "$(WAREHOUSE_ID)" ]; then \
		echo "ERROR: WAREHOUSE_ID is required. Example:"; \
		echo "  make deploy WAREHOUSE_ID=abc123def"; \
		exit 1; \
	fi
	@if [ -z "$(DEPLOYER_EMAIL)" ]; then \
		echo "ERROR: Could not detect deployer email. Check your Databricks CLI profile ($(PROFILE))."; \
		exit 1; \
	fi

_bundle_deploy:
	databricks bundle deploy --profile $(PROFILE) \
		--var "app_name=$(APP_NAME)" \
		--var "catalog=$(CATALOG)" \
		--var "gold_schema=$(SCHEMA)" \
		--var "warehouse_id=$(WAREHOUSE_ID)" \
		--var "deploy_profile=$(PROFILE)"

_apply_grants:
	@echo "Applying UC grants to app service principal..."
	@uv run python resources/grant_app_uc_permissions.py \
		--profile $(PROFILE) \
		--app-name $(APP_NAME) \
		--catalog $(CATALOG) \
		--schema $(SCHEMA) \
		--warehouse-id $(WAREHOUSE_ID) \
		|| echo "[grant-app-sp] Grants deferred — will retry at app startup."

_app_deploy:
	databricks apps deploy $(APP_NAME) \
		--source-code-path "$(WS_BUILD_PATH)" \
		--profile $(PROFILE)

_ensure_app_running:
	@echo "Ensuring app compute is running..."
	@databricks apps start $(APP_NAME) --profile $(PROFILE) 2>/dev/null || true
	@echo "Waiting for app to become ready..."
	@for i in 1 2 3 4 5 6; do \
		state=$$(databricks apps get $(APP_NAME) --profile $(PROFILE) -o json 2>/dev/null \
			| python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state',''))" 2>/dev/null); \
		if [ "$$state" = "ACTIVE" ]; then echo "  App compute is active."; break; fi; \
		echo "  Compute state: $$state — waiting 10s..."; \
		sleep 10; \
	done

# ── Verify ───────────────────────────────────────────────────────────

verify:
	@echo "Workspace wheels:"
	@databricks workspace list "$(WS_BUILD_PATH)" --profile $(PROFILE) 2>&1 | grep -i ".whl" \
		|| (echo "ERROR: No wheel found on workspace at $(WS_BUILD_PATH)" && exit 1)
	@echo "Verification passed."

# ── Cleanup stale wheels ─────────────────────────────────────────────

clean-wheels:
	@echo "Removing stale wheels from workspace..."
	@for whl in $$(databricks workspace list "$(WS_BUILD_PATH)" --profile $(PROFILE) 2>/dev/null \
		| grep "\.whl" | awk '{print $$NF}'); do \
		echo "  Deleting: $$(basename $$whl)"; \
		databricks workspace delete "$$whl" --profile $(PROFILE) 2>/dev/null || true; \
	done
	@echo "Cleanup done."
