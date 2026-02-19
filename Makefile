.PHONY: install
install:
	pip install -r requirements.txt

.PHONY: install-test
install-test:
	pip install -r tests/requirements.txt

.PHONY: lint
lint:
	ruff check chronos/ tests/
	ruff format chronos/ tests/ --check

.PHONY: format
format:
	ruff check chronos/ tests/ --fix
	ruff format chronos/ tests/

.PHONY: test
test:
	testing=True pytest --cov=chronos

.PHONY: reset-db-dev
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE chronos"
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS test_chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE test_chronos"
	python -m chronos.scripts.create_db_tables

.PHONY: create-db-tables
create-db-tables:
	python -m chronos.scripts.create_db_tables

.PHONY: install-dev
install-dev:
	pip install -r tests/requirements.txt
	pip install -r requirements.txt
	pip install devtools

.PHONY: run-server-dev
run-server-dev:
	python -m uvicorn chronos.main:app --reload --port=5000

.PHONY: run-server
run-server:
	python -m uvicorn chronos.main:app --port=${PORT} --host=${HOST}

.PHONY: run-worker
run-worker:
	celery -A chronos.worker worker --loglevel=info --autoscale 4,2 -E

.PHONY: run-dispatcher
run-dispatcher:
	celery -A chronos.worker worker -Q dispatcher -c 1 --without-heartbeat --without-mingle --soft-time-limit=0 --time-limit=0
