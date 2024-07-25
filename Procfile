web: uvicorn chronos.main:app --host=0.0.0.0 --port=${PORT:-5000}
worker: celery -A chronos.worker worker --loglevel=info