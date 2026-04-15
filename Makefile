COMPOSE := docker compose
ORACLE_RUNTIME_BASE_IMAGE := open_archive-oracle-runtime:23.26.1
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_M),arm64)
ORACLE_DB_IMAGE ?= container-registry.oracle.com/database/free:23.26.0.0-lite-arm64
else ifeq ($(UNAME_M),aarch64)
ORACLE_DB_IMAGE ?= container-registry.oracle.com/database/free:23.26.0.0-lite-arm64
else
ORACLE_DB_IMAGE ?= container-registry.oracle.com/database/free:latest-lite
endif

ifeq ($(OA_RELATIONAL_STORE),oracle)
DB_PROFILE ?= oracle
else
DB_PROFILE ?= postgres
endif

COMPOSE_PROFILES ?= $(DB_PROFILE)
COMPOSE_RUN = $(if $(COMPOSE_PROFILES),COMPOSE_PROFILES=$(COMPOSE_PROFILES) )OA_ORACLE_IMAGE=$(ORACLE_DB_IMAGE) $(COMPOSE)

.PHONY: ensure-runtime-base build-runtime-base reset-oracle-db up up-ollama up-oracle-db up-qdrant down logs ps rebuild migrate shell status verify test-postgres-integration test-oracle-integration

ensure-runtime-base:
	@docker image inspect $(ORACLE_RUNTIME_BASE_IMAGE) >/dev/null 2>&1 || $(MAKE) build-runtime-base

build-runtime-base:
	docker build -f docker/oracle-runtime-base.Dockerfile -t $(ORACLE_RUNTIME_BASE_IMAGE) .

up: ensure-runtime-base
	$(COMPOSE_RUN) up --build

up-ollama: ensure-runtime-base
	COMPOSE_PROFILES=$(DB_PROFILE),ollama OA_ORACLE_IMAGE=$(ORACLE_DB_IMAGE) $(COMPOSE) up --build

up-qdrant:
	COMPOSE_PROFILES=qdrant $(COMPOSE) up -d qdrant

up-oracle-db: ensure-runtime-base
	COMPOSE_PROFILES=oracle OA_ORACLE_IMAGE=$(ORACLE_DB_IMAGE) $(COMPOSE) up --build

reset-oracle-db:
	-$(COMPOSE_RUN) rm -sf oracle app
	-docker volume rm -f open_archive_oracle_data

down:
	$(COMPOSE_RUN) down

logs:
	$(COMPOSE_RUN) logs -f app postgres oracle

ps:
	$(COMPOSE_RUN) ps

rebuild: build-runtime-base
	$(COMPOSE_RUN) build --no-cache app

migrate:
	$(COMPOSE_RUN) run --rm app migrate

shell:
	$(COMPOSE_RUN) run --rm app /bin/sh

status:
	./scripts/pipeline_status.sh

verify:
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	cargo test --lib

test-postgres-integration:
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_import_write -- --ignored
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_enrichment_job -- --ignored
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_enrichment_execution -- --ignored
	OA_POSTGRES_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test postgres_derived_metadata -- --ignored

test-oracle-integration:
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_import_write -- --ignored
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_enrichment_job -- --ignored
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_enrichment_execution -- --ignored
	OA_ORACLE_INTEGRATION_TESTS=1 OA_ALLOW_SCHEMA_RESET=1 cargo test --test oracle_derived_metadata -- --ignored
