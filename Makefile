.PHONY: install
install:
	python -m pip install --upgrade pip
	pip install -e . --upgrade --upgrade-strategy eager

.PHONY: dev_install
dev_install:
	python -m pip install --upgrade pip
	pip install -e .[dev] --upgrade --upgrade-strategy eager

.PHONY: format
format:
	ruff format .
	ruff check . --fix
	mypy . --install-types --ignore-missing-imports --non-interactive

.PHONY: test_format
test_format:
	ruff format . --check
	ruff check .
	mypy . --install-types --ignore-missing-imports --non-interactive
