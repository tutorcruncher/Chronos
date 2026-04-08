.PHONY: install install-dev lint format test reset-db create-db-tables run-server-dev run-server run-worker run-dispatcher run-webhook-lab-receiver

# Install dependencies (normal packages only)
install:
	uv sync

# Install dependencies (including dev packages)
install-dev:
	uv sync --dev

# Lint code
lint:
	uv run --active ruff check chronos/ tests/
	uv run --active ruff format chronos/ tests/ --check

# Format code
format:
	uv run --active ruff check chronos/ tests/ --fix
	uv run --active ruff format chronos/ tests/

# Run tests
test:
	TESTING=True uv run pytest --cov=chronos

# Reset database
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE chronos"
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS test_chronos"
	psql -h localhost -U postgres -c "CREATE DATABASE test_chronos"
	uv run python -m chronos.scripts.create_db_tables

# Create database tables
create-db-tables:
	uv run python -m chronos.scripts.create_db_tables

# Run development server
run-server-dev:
	uv run uvicorn chronos.main:app --reload --port=5000

# Run production server
run-server:
	uv run uvicorn chronos.main:app --port=${PORT} --host=${HOST}

# Run Celery worker
run-worker:
	uv run celery -A chronos.worker worker --loglevel=info --autoscale 4,2 -E

# Run Celery dispatcher
run-dispatcher:
	uv run celery -A chronos.worker worker -Q dispatcher -c 1 --without-heartbeat --without-mingle --soft-time-limit=0 --time-limit=0

# Mock webhook receiver for manual retry/disable lab (see chronos/scripts/webhook_retry_disable_lab.py)
run-webhook-lab-receiver:
	uv run uvicorn chronos.scripts.mock_webhook_receiver:app --host 127.0.0.1 --port 18080
