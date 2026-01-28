TOP_DIR := .
SRC_DIR := $(TOP_DIR)/genie_simulation
LIB_NAME := genie-simulation
LIB_VERSION := $(shell grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)

UV := uv
SYNC := $(UV) sync
PYTHON := $(UV) run python
RUFF_CHECK := $(UV) run ruff check --fix --ignore E501
RUFF_FORMAT := $(UV) run ruff format
FIND := $(shell which find)
RM := rm -rf

# Default conversations file
CONVERSATIONS_FILE ?= conversations.yaml
# Default Genie space ID (must be set for export)
GENIE_SPACE_ID ?= 

.PHONY: all install depends check format clean distclean export loadtest help

all: install

install: depends
	$(SYNC)

depends:
	@$(SYNC)

check:
	$(RUFF_CHECK) $(SRC_DIR) $(TOP_DIR)/locustfile.py

format: check
	$(RUFF_FORMAT) $(SRC_DIR) $(TOP_DIR)/locustfile.py

clean:
	$(FIND) $(SRC_DIR) -name \*.pyc -exec rm -f {} \; 2>/dev/null || true
	$(FIND) $(SRC_DIR) -name \*.pyo -exec rm -f {} \; 2>/dev/null || true

distclean: clean
	$(RM) $(TOP_DIR)/.mypy_cache
	$(RM) $(TOP_DIR)/.ruff_cache
	$(FIND) $(SRC_DIR) \( -name __pycache__ -a -type d \) -prune -exec rm -rf {} \; 2>/dev/null || true

# Export Genie conversations to YAML
# Usage: make export GENIE_SPACE_ID=<space-id> [CONVERSATIONS_FILE=output.yaml]
export:
ifndef GENIE_SPACE_ID
	$(error GENIE_SPACE_ID is required. Usage: make export GENIE_SPACE_ID=<space-id>)
endif
	$(UV) run export-conversations $(GENIE_SPACE_ID) -o $(CONVERSATIONS_FILE) --include-all

# Run load test
# Usage: make loadtest [CONVERSATIONS_FILE=conversations.yaml] [USERS=5] [RUNTIME=10m]
USERS ?= 5
RUNTIME ?= 10m
SAMPLE_SIZE ?= 
SAMPLE_SEED ?= 

loadtest:
	$(UV) run genie-loadtest \
		--conversations $(CONVERSATIONS_FILE) \
		-u $(USERS) \
		-t $(RUNTIME) \
		$(if $(SAMPLE_SIZE),--sample-size $(SAMPLE_SIZE),) \
		$(if $(SAMPLE_SEED),--sample-seed $(SAMPLE_SEED),)

# Quick test with fewer users and shorter runtime
loadtest-quick:
	$(UV) run genie-loadtest \
		--conversations $(CONVERSATIONS_FILE) \
		-u 2 \
		-t 2m \
		--sample-size 5

help:
	$(info $(LIB_NAME) v$(LIB_VERSION))
	$(info )
	$(info $$> make [all|install|depends|check|format|clean|distclean|export|loadtest|loadtest-quick|help])
	$(info )
	$(info   Setup:)
	$(info       all          - install dependencies (default))
	$(info       install      - install dependencies)
	$(info       depends      - sync dependencies)
	$(info )
	$(info   Code Quality:)
	$(info       check        - run ruff linter with auto-fix)
	$(info       format       - format source code with ruff)
	$(info )
	$(info   Cleanup:)
	$(info       clean        - remove .pyc/.pyo files)
	$(info       distclean    - remove all build artifacts and caches)
	$(info )
	$(info   Genie Tools:)
	$(info       export       - export Genie conversations to YAML)
	$(info                      Required: GENIE_SPACE_ID=<space-id>)
	$(info                      Optional: CONVERSATIONS_FILE=output.yaml (default: conversations.yaml))
	$(info )
	$(info       loadtest     - run Genie load test)
	$(info                      Optional: CONVERSATIONS_FILE=input.yaml (default: conversations.yaml))
	$(info                      Optional: USERS=5 (default: 5))
	$(info                      Optional: RUNTIME=10m (default: 10m))
	$(info                      Optional: SAMPLE_SIZE=10)
	$(info                      Optional: SAMPLE_SEED=42)
	$(info )
	$(info       loadtest-quick - quick test with 2 users for 2 minutes)
	$(info )
	$(info   Examples:)
	$(info       make export GENIE_SPACE_ID=01f0c482e842191587af6a40ad4044d8)
	$(info       make loadtest USERS=10 RUNTIME=5m)
	$(info       make loadtest SAMPLE_SIZE=20 SAMPLE_SEED=42)
	$(info )
	$(info       help         - show this help message)
	@true
