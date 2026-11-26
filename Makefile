.PHONY: up test check test-cli test-dev test-shell test-catalog test-history test-import test-export test-sdk down
up:
	docker compose build
	docker compose up -d --wait server
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
