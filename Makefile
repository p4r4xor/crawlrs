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
chart-deps: ## helm dep build crawlrs-demo + unpack the file:// crawlrs subchart (helm 3.16 quirk)
	$(HELM) dep build charts/crawlrs-demo
	@cd charts/crawlrs-demo/charts && for f in *.tgz; do tar -xzf "$$f"; done

# ---- Local container deployment -------------------------------------
#
# End-to-end local stack: kind cluster + locally-built image + the
# crawlrs-demo Helm chart (Redis + Postgres via official upstream
# images, bundled observability, kind=local store backend). See
# local/README.md for the full walkthrough.

KIND               ?= kind
KUBECTL            ?= kubectl
DOCKER             ?= docker
LOCAL_CLUSTER_NAME ?= crawlrs-local
LOCAL_NAMESPACE    ?= crawlrs
LOCAL_RELEASE      ?= crawlrs-demo
LOCAL_IMAGE        ?= crawlrs:local

.PHONY: local-deps-check
local-deps-check: ## Verify docker / kind / helm / kubectl are on PATH
	@command -v $(DOCKER)  >/dev/null 2>&1 || { echo "missing: docker"; exit 1; }
	@command -v $(KIND)    >/dev/null 2>&1 || { echo "missing: kind (install: https://kind.sigs.k8s.io/docs/user/quick-start/#installation)"; exit 1; }
	@command -v $(HELM)    >/dev/null 2>&1 || { echo "missing: helm"; exit 1; }
	@command -v $(KUBECTL) >/dev/null 2>&1 || { echo "missing: kubectl"; exit 1; }
	@echo "ok: docker, kind, helm, kubectl all present"

.PHONY: image
image: ## docker build -t crawlrs:local .  (cargo-chef multi-stage; first build ~10min)
	$(DOCKER) build -t $(LOCAL_IMAGE) .

.PHONY: local-cluster-up
local-cluster-up: local-deps-check ## kind create cluster (idempotent)
	@if $(KIND) get clusters | grep -qx "$(LOCAL_CLUSTER_NAME)"; then \
		echo "kind cluster $(LOCAL_CLUSTER_NAME) already exists"; \
	else \
		$(KIND) create cluster --config local/kind.yaml; \
	fi

.PHONY: local-cluster-down
local-cluster-down: ## kind delete cluster (loses all PVC data)
	$(KIND) delete cluster --name $(LOCAL_CLUSTER_NAME)

.PHONY: local-up
local-up: local-cluster-up image chart-deps ## Full pipeline: cluster + image + load + helm install
	$(KIND) load docker-image $(LOCAL_IMAGE) --name $(LOCAL_CLUSTER_NAME)
	$(KUBECTL) create namespace $(LOCAL_NAMESPACE) --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(HELM) upgrade --install $(LOCAL_RELEASE) charts/crawlrs-demo \
		--values local/values.local.yaml \
		--set-file crawlrs.seeds.content=local/seeds.txt \
		--wait --timeout 5m \
		--namespace $(LOCAL_NAMESPACE)
	@echo ""
	@echo "Stack is up. Endpoints exposed via kind extraPortMappings:"
	@echo "  http://localhost:3000   Grafana (admin / admin)"
	@echo "  http://localhost:9090   crawlrs /metrics, /healthz, /readyz"
	@echo ""
	@echo "Useful commands:"
	@echo "  make local-logs       # tail crawlrs logs"
	@echo "  make local-status     # pod state + helm status"
	@echo "  make local-down       # uninstall helm release (keeps cluster)"

.PHONY: local-down
local-down: ## helm uninstall (keeps the cluster + PVCs)
	$(HELM) uninstall $(LOCAL_RELEASE) --namespace $(LOCAL_NAMESPACE) || true

.PHONY: local-logs
local-logs: ## Tail crawler pod logs (crawlrs-demo-0, ordinal 0)
	$(KUBECTL) logs -n $(LOCAL_NAMESPACE) -f $(LOCAL_RELEASE)-0 --all-containers=true

.PHONY: local-pf
local-pf: ## Fallback port-forward; not normally needed (NodePort + kind extraPortMappings give host:3000 + host:9090)
	@echo "NOTE: NodePort services + kind extraPortMappings already publish"
	@echo "      http://localhost:3000 (Grafana) and http://localhost:9090 (metrics)."
	@echo "      Use this target only if those host ports are blocked. Forwarding"
	@echo "      to alternate ports 3001 / 9091 to avoid colliding with docker-proxy:"
	@echo ""
	@echo "  http://localhost:3001   Grafana"
	@echo "  http://localhost:9091   crawlrs /metrics"
	@echo ""
	@echo "Ctrl-C to stop both."
	@( $(KUBECTL) port-forward -n $(LOCAL_NAMESPACE) svc/$(LOCAL_RELEASE)-grafana 3001:3000 & \
	   $(KUBECTL) port-forward -n $(LOCAL_NAMESPACE) sts/$(LOCAL_RELEASE) 9091:9090 & \
	   wait )

.PHONY: local-status
local-status: ## kubectl get pods + helm status
	@$(KUBECTL) get pods -n $(LOCAL_NAMESPACE) -o wide
	@echo ""
	@$(HELM) status $(LOCAL_RELEASE) -n $(LOCAL_NAMESPACE) | head -20

# ---- Help -----------------------------------------------------------

.PHONY: help
help: ## Print this help.
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
