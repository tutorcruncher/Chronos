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
	pytest

.PHONY: full-test
full-test:
	make reset-db
	python -m app.scripts.create_db_tables
	pytest

.PHONY: reset-db
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE chronos"
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS test_chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE test_chronos"

.PHONY: install-dev
install-dev:
	pip install -r requirements.txt
	pip install -r tests/requirements.txt
	pip install devtools

.PHONY: run-server
run-server:
	python -m uvicorn app.main:app --reload --port=5000

.PHONY: run-worker
run-worker:
	celery -A app.worker worker --loglevel=info
