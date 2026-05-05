# crawlrs Makefile.
#
# `make help` lists every target. Each target is small enough that
# the underlying command is the docs.

.DEFAULT_GOAL := help

# ---- Cargo workspace ------------------------------------------------

.PHONY: build
build: ## cargo build --workspace
	cargo build --workspace

.PHONY: test
test: ## cargo test --workspace (137 fast unit + 11 testcontainer integration)
	cargo test --workspace

.PHONY: nextest
nextest: ## cargo nextest (faster CI test runner; same suite)
	cargo nextest run --workspace

.PHONY: fmt
fmt: ## cargo fmt --all
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## cargo fmt --all --check (CI-friendly; no rewrites)
	cargo fmt --all -- --check

.PHONY: clippy
clippy: ## cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo clippy --workspace --all-targets --all-features -- -D warnings

.PHONY: lint
lint: fmt-check clippy ## fmt-check + clippy

.PHONY: clean
clean: ## cargo clean + remove helm-resolved tarballs
	cargo clean
	rm -rf charts/crawlrs-demo/charts/

# ---- Binary helpers -------------------------------------------------

.PHONY: run
run: ## Run the crawler against ./crawl.toml + ./seeds.txt
	cargo run -p crawlrs-bin -- crawl --config ./crawl.toml --seeds ./seeds.txt

.PHONY: validate
validate: ## Validate the example config
	cargo run -p crawlrs-bin -- validate --config crates/crawlrs-bin/examples/crawl.toml

.PHONY: version
version: ## Print the binary version
	cargo run -p crawlrs-bin -- version

# ---- Helm charts ----------------------------------------------------

HELM ?= helm

.PHONY: chart-lint
chart-lint: ## helm lint both charts
	$(HELM) lint charts/crawlrs
	$(HELM) lint charts/crawlrs-demo

.PHONY: chart-template
chart-template: ## helm template both charts; useful for diffing renders
	$(HELM) template my-crawlrs charts/crawlrs > /tmp/crawlrs.rendered.yaml
	$(HELM) template crawlrs-demo charts/crawlrs-demo > /tmp/crawlrs-demo.rendered.yaml
	@echo "rendered:"
	@echo "  /tmp/crawlrs.rendered.yaml"
	@echo "  /tmp/crawlrs-demo.rendered.yaml"

.PHONY: chart-deps
chart-deps: ## helm dep build crawlrs-demo + unpack subchart tarballs (helm 3.16 quirk)
	$(HELM) repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
	$(HELM) dep build charts/crawlrs-demo
	cd charts/crawlrs-demo/charts && for f in *.tgz; do tar -xzf "$$f"; done

# ---- Help -----------------------------------------------------------

.PHONY: help
help: ## Print this help.
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
