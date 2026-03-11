COMPOSE := docker compose

.PHONY: up up-ollama up-oracle-db down logs ps rebuild migrate shell

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
