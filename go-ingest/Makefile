.DEFAULT_GOAL := help
.PHONY: help clean clean-build lint lint-go lint-vet test build-binary

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RESET  := $(shell tput -Txterm sgr0)

BINARY_NAME := ingest
BIN_DIR     := ./bin
OUT         := $(BIN_DIR)/$(BINARY_NAME)

help: ## Show help
	@echo ''
	@echo 'Usage:'
	@echo '  $(YELLOW)make$(RESET) $(GREEN)<target>$(RESET)'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z0-9_\-]+:.*?## / { \
		printf "  $(YELLOW)%-18s$(GREEN)%s$(RESET)\n", $$1, $$2 }' $(MAKEFILE_LIST)

clean: clean-build ## Clean everything
clean-build: ## Remove local binaries
	@rm -rf $(BIN_DIR)
	@echo "$(GREEN)Cleaned$(RESET)"

lint: lint-go lint-vet ## Run all linters
lint-go: ## Run go fmt
	@go fmt ./...
lint-vet: ## Run go vet
	@go vet ./...

test: ## Run tests
	@echo "No tests yet — add *_test.go files and rerun."

build-binary: ## Build local binary (for dev; Docker uses multi-stage)
	@mkdir -p $(BIN_DIR)
	@CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags "-s -w" -o $(OUT) .
	@echo "$(GREEN)Binary: $(OUT)$(RESET)"
