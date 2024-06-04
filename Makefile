.PHONY: install
install:
	pip install -r requirements.txt
# 	pip install -r tests/requirements.txt

.PHONY: lint
lint:
	ruff check app/ tests/
	ruff format app/ tests/ --check

.PHONY: format
format:
	ruff check app/ tests/ --fix
	ruff format app/ tests/

.PHONY: test
test:
	pytest --cov=app

.PHONY: reset-db
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE chronos"

.PHONY: install-dev
install-dev:
	pip install -r requirements.txt
	pip install -r tests/requirements.txt
	pip install devtools
