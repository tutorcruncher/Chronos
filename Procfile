web: uvicorn chronos.main:app --host=0.0.0.0 --port=${PORT:-5000}
worker: celery -A chronos.worker worker --loglevel=error --concurrency 2 -E
dispatcher: celery -A chronos.worker worker -Q dispatcher -c 1 --without-heartbeat --without-mingle --soft-time-limit=0 --time-limit=0 --loglevel=error
