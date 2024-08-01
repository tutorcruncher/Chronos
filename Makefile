.PHONY: install
install:
	pip install -r requirements.txt
# 	pip install -r tests/requirements.txt

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


.PHONY: reset-db
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE chronos"
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS test_chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE test_chronos"

.PHONY: install-dev
install-dev:
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
	celery -A chronos.worker worker --loglevel=info --concurrency 2
