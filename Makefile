COMPOSE := docker compose

.PHONY: up up-ollama up-oracle-db down logs ps rebuild migrate shell test-postgres-integration test-oracle-integration

up:
	$(COMPOSE) up --build

up-ollama:
	$(COMPOSE) --profile ollama up --build

up-oracle-db:
	$(COMPOSE) --profile oracle up --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f app postgres

ps:
	$(COMPOSE) ps

rebuild:
	$(COMPOSE) build --no-cache app

migrate:
	$(COMPOSE) run --rm app migrate

shell:
	$(COMPOSE) run --rm app /bin/sh

test-postgres-integration:
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_import_write -- --ignored
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_enrichment_job -- --ignored
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_derived_metadata -- --ignored

test-oracle-integration:
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_import_write -- --ignored
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_enrichment_job -- --ignored
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_derived_metadata -- --ignored
