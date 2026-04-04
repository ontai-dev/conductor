.PHONY: build test lint lint-docs clean

build:
	go build ./...

test:
	go test ./...

lint: lint-docs
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

clean:
	rm -rf bin/
