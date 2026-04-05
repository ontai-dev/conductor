# Image tag default — override via environment: TAG=v1.9.3-r1 make docker-build
IMAGE_REGISTRY ?= registry.ontai.dev/ontai-dev
TAG            ?= dev

.PHONY: build test lint lint-docs install-hooks clean docker-build

build:
	go build ./...

test:
	go test ./...

lint: lint-docs install-hooks
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
		-f Dockerfile.compiler \
		-t $(IMAGE_REGISTRY)/compiler:$(TAG) \
		.
	docker build \
		-f Dockerfile.execute \
		-t $(IMAGE_REGISTRY)/conductor-execute:$(TAG) \
		.
	docker build \
		-f Dockerfile.agent \
		-t $(IMAGE_REGISTRY)/conductor:$(TAG) \
		.
