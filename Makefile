# arrow-buoys — top-level compose Makefile
.DEFAULT_GOAL := help

.PHONY: help env network build up down restart ps logs tail once clean prune

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

ifneq (,$(wildcard ./.env))
  include .env
  export
endif

PROJECT         ?= arrow-buoys
NET_NAME        ?= arrow-buoys-net
DATA_DIR        ?= ./data
OUT_DIR         ?= ./out
STATIONS        ?= SANF1,SMKF1,LONF1,VAKF1,KYWF1
REFRESH_MINUTES ?= 60
ARROW_PORT      ?= 8080

DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
COMPOSE        := $(DOCKER_COMPOSE) -p $(PROJECT)

env: ## Show current env config
	@echo "PROJECT=$(PROJECT)"; \
	 echo "NET_NAME=$(NET_NAME)"; \
	 echo "DATA_DIR=$(DATA_DIR)"; \
	 echo "OUT_DIR=$(OUT_DIR)"; \
	 echo "STATIONS=$(STATIONS)"; \
	 echo "REFRESH_MINUTES=$(REFRESH_MINUTES)"; \
	 echo "ARROW_PORT=$(ARROW_PORT)"

network: ## Ensure external Docker network exists
	@docker network inspect $(NET_NAME) >/dev/null 2>&1 \
		|| docker network create $(NET_NAME)
	@echo "$(GREEN)Network ready: $(NET_NAME)$(RESET)"

build: network ## Build all service images (multi-stage)
	@mkdir -p $(DATA_DIR) $(OUT_DIR)
	@$(COMPOSE) build
	@echo "$(GREEN)Build complete$(RESET)"

up: network ## Start all services in detached mode
	@mkdir -p $(DATA_DIR) $(OUT_DIR)
	@$(COMPOSE) up -d
	@echo "$(GREEN)Stack is up$(RESET)"

down: ## Stop and remove containers
	@$(COMPOSE) down

restart: ## Restart all services
	@$(COMPOSE) restart

ps: ## Show service status
	@$(COMPOSE) ps

logs: ## Show all logs (no follow)
	@$(COMPOSE) logs --no-color

tail: ## Tail logs (follow)
	@$(COMPOSE) logs -f --tail=200

once: network ## Run ingester one-shot (REFRESH_MINUTES=0), then exit
	@mkdir -p $(DATA_DIR)
	@$(COMPOSE) run --rm -e REFRESH_MINUTES=0 go-ingest

clean: ## Remove containers + locally built images (keeps data/out)
	@$(COMPOSE) down --rmi local --remove-orphans || true
	@echo "$(GREEN)Clean complete$(RESET)"

prune: ## Full removal: images, volumes, orphans (keeps external network)
	@$(COMPOSE) down --rmi all --volumes --remove-orphans || true
	@echo "$(YELLOW)Prune complete$(RESET)"

help: ## Show this help
	@echo ''
	@echo 'Usage:'
	@echo '  $(YELLOW)make$(RESET) $(GREEN)<target>$(RESET)'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z0-9_\-]+:.*?## / { \
		printf "  $(YELLOW)%-18s$(GREEN)%s$(RESET)\n", $$1, $$2 }' $(MAKEFILE_LIST)
