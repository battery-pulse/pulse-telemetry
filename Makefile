.PHONY: install
install:
	python -m pip install --upgrade pip
	pip install -e . --upgrade --upgrade-strategy eager

.PHONY: install-dev
install-dev:
	python -m pip install --upgrade pip
	pip install -e .[dev] --upgrade --upgrade-strategy eager

.PHONY: format
format:
	ruff format .
	ruff check . --fix
	mypy . --install-types --ignore-missing-imports --non-interactive

.PHONY: test-format
test-format:
	ruff format . --check
	ruff check .
	mypy . --install-types --ignore-missing-imports --non-interactive

.PHONY: test-unit
test-unit:
	pytest tests/unit/

.PHONY: test-integration
test-integration:
	pytest tests/integration/ -s --log-cli-level=INFO

.PHONY: test-e2e
test-e2e:
	pytest tests/e2e/ -s --log-cli-level=INFO

.PHONY: docker-image
docker-image:
	docker build -t pulse-telemetry:latest .