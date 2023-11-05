include config/colors.txt

GOCMD := go
TRACKER_FOLDER = cmd/tracker
NODE_FOLDER = cmd/node

# If true, builds for linux/amd64 (in order to run on coreemu)
REMOTE ?= 0
ifeq ($(REMOTE), 1)
	GOCMD = GOARCH=amd64 GOOS=linux go
endif

.PHONY: all build tracker node format test clean help

all: help

build: tracker node

tracker:
	@mkdir -p out/bin
	@$(GOCMD) build -o out/bin/tracker ./$(TRACKER_FOLDER)
	@echo "${GREEN}Successfully built ${RESET}${RED}tracker${RESET}"

node:
	@mkdir -p out/bin
	@$(GOCMD) build -o out/bin/node ./$(NODE_FOLDER)
	@echo "${GREEN}Successfully built ${RESET}${RED}node${RESET}"

test:
	@$(GOCMD) test ./...
	@echo "${GREEN}Successfully ran tests${RESET}"

format:
	@$(GOCMD) fmt ./...
	@echo "${GREEN}Successfully formatted project${RESET}"

clean:
	@rm -rf out
	@echo "${GREEN}Successfully cleaned project${RESET}"

help:
	@echo "${CYAN}PessiTorrent-CC${RESET}"
	@echo ""
	@echo "${YELLOW}Usage:${RESET}"
	@echo "  make <command>"
	@echo ""
	@echo "${YELLOW}Available Commands:${RESET}"
	@echo "  build       Builds the project"
	@echo "  tracker     Builds the tracker"
	@echo "  node        Builds the node"
	@echo "  format      Formats the project"
	@echo "  test        Runs the tests"
	@echo "  clean       Cleans the project"
	@echo "  help        Help about any command"