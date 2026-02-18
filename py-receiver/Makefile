.DEFAULT_GOAL := help
.PHONY: help env install install-dev sync update freeze run run-args shell \
        fmt fmt-check lint type test clean clean-build

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RESET  := $(shell tput -Txterm sgr0)

UV     ?= uv
UVX    ?= uvx
VENV   ?= .venv

ifneq (,$(wildcard ../.env))
  include ../.env
  export
endif
ifneq (,$(wildcard ./.env))
  include ./.env
  export
endif

ARROW_PORT  ?= 8080
SOURCE_URL  ?= http://localhost:$(ARROW_PORT)/stream
OUT_DIR     ?= ./out
PYPROJECT   := $(wildcard pyproject.toml)
REQ_FILE    ?= requirements.txt
REQ_DEV     ?= requirements-dev.txt

env: ## Show environment config
	@echo "UV=$(UV)"; \
	 echo "VENV=$(VENV)"; \
	 echo "SOURCE_URL=$(SOURCE_URL)"; \
	 echo "OUT_DIR=$(OUT_DIR)"; \
	 echo "PYPROJECT=$(PYPROJECT)"; \
	 echo "REQ_FILE=$(REQ_FILE)"

install: ## Create venv and install runtime deps
	@if [ -f "$(PYPROJECT)" ]; then \
		$(UV) venv $(VENV) && $(UV) sync; \
	elif [ -f "$(REQ_FILE)" ]; then \
		$(UV) venv $(VENV) && $(UV) pip install -r $(REQ_FILE); \
	else \
		$(UV) venv $(VENV); \
	fi
	@echo "$(GREEN)Runtime deps installed$(RESET)"

install-dev: ## Install all deps including dev tools
	@if [ -f "$(PYPROJECT)" ]; then \
		$(UV) venv $(VENV) && $(UV) sync --all-extras; \
	else \
		$(UV) venv $(VENV); \
		[ -f "$(REQ_FILE)" ] && $(UV) pip install -r $(REQ_FILE) || true; \
		[ -f "$(REQ_DEV)" ] && $(UV) pip install -r $(REQ_DEV) || true; \
	fi
	@echo "$(GREEN)Dev deps installed$(RESET)"

sync: install ## Re-sync deps from pyproject.toml
update: ## Upgrade all deps to latest compatible versions
	@[ -f "$(PYPROJECT)" ] && $(UV) sync --upgrade || echo "Bump versions in $(REQ_FILE) then run install."

freeze: ## Export pinned requirements.txt
	@[ -f "$(PYPROJECT)" ] && $(UV) export --no-hashes > requirements.txt && echo "Wrote requirements.txt" \
	|| echo "Using $(REQ_FILE) directly."

run: install ## Run the receiver script
	@mkdir -p $(OUT_DIR)
	SOURCE_URL="$(SOURCE_URL)" OUT_DIR="$(OUT_DIR)" $(UV) run python main.py

run-args: install ## Run with extra args: make run-args ARGS="--foo bar"
	@mkdir -p $(OUT_DIR)
	SOURCE_URL="$(SOURCE_URL)" OUT_DIR="$(OUT_DIR)" $(UV) run python main.py $(ARGS)

shell: install ## Open a shell in the venv
	@$(UV) run bash -l

fmt: install-dev ## Format code with black + isort
	@$(UVX) isort .
	@$(UVX) black .

fmt-check: install-dev ## Check formatting without modifying files
	@$(UVX) isort --check-only .
	@$(UVX) black --check .

lint: install-dev ## Lint with ruff
	@$(UVX) ruff check .

type: install-dev ## Type-check with mypy
	@$(UVX) mypy .

test: install-dev ## Run pytest
	@$(UVX) pytest -q $(TESTS)

clean: ## Remove caches (preserves .venv)
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache .mypy_cache .ruff_cache 2>/dev/null || true
	@echo "$(GREEN)Caches removed$(RESET)"

clean-build: ## Remove build artifacts; NUKE=1 also removes .venv
	@rm -rf build dist *.egg-info 2>/dev/null || true
	@[ "$(NUKE)" = "1" ] && rm -rf $(VENV) && echo "$(YELLOW)Removed $(VENV)$(RESET)" || true
	@echo "$(GREEN)Build artifacts removed$(RESET)"

help: ## Show this help
	@echo ''
	@echo 'Usage:'
	@echo '  $(YELLOW)make$(RESET) $(GREEN)<target>$(RESET)'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z0-9_\-]+:.*?## / { \
		printf "  $(YELLOW)%-18s$(GREEN)%s$(RESET)\n", $$1, $$2 }' $(MAKEFILE_LIST)
