.PHONY: up test check down
up:
	docker compose up --build -d --wait server
test:
	bash scripts/test-local.sh
check:
	docker build --target check -f docker/server.Dockerfile .
down:
	docker compose down
