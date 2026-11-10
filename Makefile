.PHONY: up test check down
up:
	docker compose build
	docker compose up -d --wait server
test:
	bash scripts/test-local.sh
check:
	docker build --target check -f docker/server.Dockerfile .
	docker build --target aws-credentials-check -f docker/server.Dockerfile .

.PHONY: check-infra
check-infra:
	bash scripts/check-infra.sh
down:
	docker compose down
