# Image tag default — override via environment: TAG=v1.9.3-r1 make docker-build
IMAGE_REGISTRY ?= registry.ontai.dev/ontai-dev
TAG            ?= dev

.PHONY: build test test-unit test-integration test-all e2e lint lint-docs lint-images install-hooks clean docker-build docker-push

build:
	go build ./...

test:
	go test ./test/unit/...

test-unit:
	go test ./test/unit/...

test-integration:
	KUBEBUILDER_ASSETS=$(KUBEBUILDER_ASSETS) go test ./test/integration/...

test-all: test-unit test-integration

e2e:
	MGMT_KUBECONFIG=$(MGMT_KUBECONFIG) TENANT_KUBECONFIG=$(TENANT_KUBECONFIG) \
	REGISTRY_ADDR=$(REGISTRY_ADDR) MGMT_CLUSTER_NAME=$(MGMT_CLUSTER_NAME) \
	TENANT_CLUSTER_NAME=$(TENANT_CLUSTER_NAME) \
	go test ./test/e2e/... -v -timeout 30m

lint: lint-docs lint-images install-hooks
	golangci-lint run ./...

lint-docs:
	@echo ">>> lint-docs: verifying no unintended tracked .md files"
	@bad=$$(git ls-files '*.md' | grep -v '^README\.md$$' | grep -v -- '-schema\.md$$'); \
	if [ -n "$$bad" ]; then \
		echo "FAIL: tracked .md files violating policy:"; \
		echo "$$bad"; \
		exit 1; \
	fi
	@echo "PASS: no unintended tracked .md files"
	@echo ">>> lint-docs: scanning session/1-governor-init for Co-Authored-By trailers"
	@if git log session/1-governor-init --format='%B' 2>/dev/null | grep -qE '^Co-Authored-By:|^Co-authored-by:'; then \
		echo "FAIL: Co-Authored-By trailer found in commit history"; \
		exit 1; \
	fi
	@echo "PASS: no Co-Authored-By trailers in commit history"

install-hooks:
	@echo ">>> install-hooks: installing commit-msg hook"
	@cp scripts/commit-msg .git/hooks/commit-msg
	@chmod +x .git/hooks/commit-msg
	@echo "PASS: commit-msg hook installed at .git/hooks/commit-msg"

clean:
	rm -rf bin/

# docker-build builds all three conductor images with distinct tags.
# compiler: compile-mode tool (debian-slim, never deployed to cluster)
# execute:  execute-mode Kueue Jobs (debian-slim + SOPS/Helm/Kustomize)
# agent:    agent-mode Deployment (distroless, deployed to every cluster)
#
# Usage: make docker-build TAG=v1.9.3-r1
#        make docker-build TAG=dev IMAGE_REGISTRY=10.20.0.1:5000/ontai-dev
docker-build:
	docker build \
		--platform linux/amd64 \
		-f Dockerfile.compiler \
		-t $(IMAGE_REGISTRY)/compiler:$(TAG) \
		..
	docker build \
		--platform linux/amd64 \
		-f Dockerfile.execute \
		-t $(IMAGE_REGISTRY)/conductor-execute:$(TAG) \
		..
	docker build \
		--platform linux/amd64 \
		-f Dockerfile.agent \
		-t $(IMAGE_REGISTRY)/conductor:$(TAG) \
		..

# docker-push pushes all three already-built conductor images to the registry.
docker-push:
	docker push $(IMAGE_REGISTRY)/compiler:$(TAG)
	docker push $(IMAGE_REGISTRY)/conductor-execute:$(TAG)
	docker push $(IMAGE_REGISTRY)/conductor:$(TAG)

# lint-images verifies all three conductor images exist in the local OCI registry.
lint-images:
	@echo ">>> lint-images: checking conductor images in registry"
	@for img in compiler conductor-execute conductor; do \
		status=$$(curl -fsS -o /dev/null -w "%{http_code}" \
			"http://10.20.0.1:5000/v2/ontai-dev/$$img/manifests/$(TAG)" 2>/dev/null); \
		if [ "$$status" != "200" ]; then \
			echo "FAIL: 10.20.0.1:5000/ontai-dev/$$img:$(TAG) not found in registry (HTTP $$status)"; \
			exit 1; \
		fi; \
		echo "PASS: 10.20.0.1:5000/ontai-dev/$$img:$(TAG) present in registry"; \
	done
