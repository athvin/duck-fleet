.PHONY: up ui test check test-cli test-dev test-shell test-catalog test-history test-import test-export test-sdk test-snapshots test-advise test-advise-dbt test-local-io test-pgwire-clients test-adbc down
up:
	docker compose build
	docker compose up -d --wait server
# Builds the admin console bundle and the CLI that embeds it (duckfleet ui).
ui:
	cd web/admin && npm ci && npm run build
	cd server && cargo build --locked -p duckfleet-cli
test:
	bash scripts/test-local.sh
check:
	docker build --target check -f docker/server.Dockerfile .
	docker build --target aws-credentials-check -f docker/server.Dockerfile .
test-cli:
	bash scripts/test-native-cli.sh
test-dev:
	bash scripts/test-dev-lifecycle.sh
test-shell:
	bash scripts/test-shell.sh
test-catalog:
	bash scripts/test-catalog.sh
test-history:
	bash scripts/test-history.sh

test-import:
	bash scripts/test-import.sh

test-export:
	bash scripts/test-export.sh

test-snapshots:
	bash scripts/test-snapshots.sh

test-advise:
	bash scripts/test-advise.sh

test-advise-dbt:
	bash scripts/test-advise-dbt.sh

test-local-io:
	bash scripts/test-local-io.sh

test-pgwire-clients:
	bash scripts/test-pgwire-clients.sh

test-adbc:
	bash scripts/test-adbc.sh

test-sdk:
	cd server && cargo +$${DUCKFLEET_SDK_TOOLCHAIN:-1.99.0} test -p duckfleet --locked --test compose -- --ignored --test-threads=1

.PHONY: check-infra
check-infra:
	bash scripts/check-infra.sh

# Needs kind, kubectl and helm. Creates and destroys its own cluster; no AWS access.
.PHONY: test-k8s
test-k8s:
	bash scripts/test-k8s.sh
down:
	docker compose down
