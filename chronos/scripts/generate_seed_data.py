"""
Seed data generator for testing partition migration.

Generates realistic webhook log data:
- Creates test webhook endpoint(s)
- Generates logs spanning 20 days (to test 15-day cutoff)
- Varies status codes, headers, and bodies
- Configurable number of rows

Usage:
    python -m chronos.scripts.generate_seed_data --rows 10000
    python -m chronos.scripts.generate_seed_data --rows 100000 --days 25
"""

import argparse
import random
from datetime import datetime, timedelta, UTC

from sqlmodel import Session, select

from chronos.db import engine, init_db
from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.utils import app_logger


# Sample data for generating realistic webhook logs
SAMPLE_EVENTS = [
    'job.created', 'job.updated', 'job.completed', 'job.cancelled',
    'client.created', 'client.updated', 'client.deleted',
    'invoice.created', 'invoice.paid', 'invoice.overdue',
    'lesson.scheduled', 'lesson.completed', 'lesson.cancelled',
]

SAMPLE_STATUSES = [
    ('Success', [200, 201, 204]),
    ('Client Error', [400, 404, 422]),
    ('Server Error', [500, 502, 503]),
    ('Timeout', [None]),
]


def generate_request_body(event_type: str) -> dict:
    """Generate realistic webhook request body."""
    return {
        'event': event_type,
        'timestamp': datetime.now(UTC).isoformat(),
        'data': {
            'id': random.randint(1000, 9999),
            'branch_id': random.randint(1, 10),
            'status': random.choice(['active', 'pending', 'completed', 'cancelled']),
            'amount': round(random.uniform(10, 1000), 2) if 'invoice' in event_type else None,
        }
    }


def generate_response_body(status_code: int) -> dict:
    """Generate realistic webhook response body."""
    if status_code and 200 <= status_code < 300:
        return {
            'status': 'success',
            'message': 'Webhook processed successfully',
            'processed_at': datetime.now(UTC).isoformat(),
        }
    elif status_code and 400 <= status_code < 500:
        return {
            'status': 'error',
            'message': 'Invalid webhook payload',
            'error_code': f'ERR_{status_code}',
        }
    elif status_code and status_code >= 500:
        return {
            'status': 'error',
            'message': 'Internal server error',
            'error_code': f'ERR_{status_code}',
        }
    else:
        return None


def create_test_endpoint(db: Session) -> WebhookEndpoint:
    """Create or get a test webhook endpoint."""
    # Check if test endpoint exists
    existing = db.exec(
        select(WebhookEndpoint).where(WebhookEndpoint.tc_id == 99999)
    ).first()

    if existing:
        app_logger.info(f"Using existing test endpoint: {existing.id}")
        return existing

    # Create new test endpoint
    endpoint = WebhookEndpoint(
        tc_id=99999,
        name="Test Webhook Endpoint",
        branch_id=1,
        webhook_url="https://example.com/webhook",
        api_key="test-api-key-12345",
        active=True,
    )

    db.add(endpoint)
    db.commit()
    db.refresh(endpoint)

    app_logger.info(f"Created test endpoint: {endpoint.id}")
    return endpoint


def generate_webhook_logs(
    db: Session,
    endpoint_id: int,
    total_rows: int,
    days_back: int = 20,
    batch_size: int = 1000
) -> None:
    """
    Generate webhook log entries using raw psycopg2 for better performance.

    Args:
        db: Database session (only used for getting DSN)
        endpoint_id: WebhookEndpoint ID to associate logs with
        total_rows: Total number of logs to generate
        days_back: How many days back to spread the data
        batch_size: Number of rows to insert per batch
    """
    import json as json_lib
    import psycopg2

    app_logger.info(f"Generating {total_rows:,} webhook logs spanning {days_back} days...")

    # Create a direct psycopg2 connection
    from chronos.utils import settings
    conn = psycopg2.connect(settings.pg_dsn)
    cursor = conn.cursor()

    now = datetime.now(UTC)
    values = []

    sql = """
        INSERT INTO webhooklog (
            request_headers, request_body, response_headers, response_body,
            status, status_code, timestamp, webhook_endpoint_id
        )
        VALUES (
            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
            %s, %s, %s, %s
        )
    """

    for i in range(total_rows):
        # Generate random timestamp within the date range
        days_offset = random.uniform(0, days_back)
        timestamp = now - timedelta(days=days_offset)

        # Pick random status and status code
        status, status_codes = random.choice(SAMPLE_STATUSES)
        status_code = random.choice(status_codes)

        # Generate request/response data
        event_type = random.choice(SAMPLE_EVENTS)
        request_body = generate_request_body(event_type)
        response_body = generate_response_body(status_code) if status_code else None

        # Build value tuple for SQL
        request_headers = {
            'User-Agent': 'TutorCruncher',
            'Content-Type': 'application/json',
            'webhook-signature': f'sha256_{random.randint(100000, 999999)}',
        }
        response_headers = {
            'Content-Type': 'application/json',
            'Server': 'nginx/1.18.0',
        } if status_code else None

        values.append((
            json_lib.dumps(request_headers),
            json_lib.dumps(request_body),
            json_lib.dumps(response_headers) if response_headers else None,
            json_lib.dumps(response_body) if response_body else None,
            status,
            status_code,
            timestamp,
            endpoint_id,
        ))

        # Insert in batches
        if len(values) >= batch_size or i == total_rows - 1:
            for val in values:
                cursor.execute(sql, val)

            conn.commit()

            progress = ((i + 1) / total_rows) * 100
            app_logger.info(f"Inserted {i + 1:,} / {total_rows:,} rows ({progress:.1f}%)")

            values = []

    cursor.close()
    conn.close()

    app_logger.info("✓ Seed data generation complete!")


def print_summary(db: Session, days_back: int) -> None:
    """Print summary statistics about generated data."""
    from sqlalchemy import func, text

    app_logger.info("\n" + "=" * 60)
    app_logger.info("DATA SUMMARY")
    app_logger.info("=" * 60)

    # Total count
    total = db.exec(select(func.count()).select_from(WebhookLog)).one()
    app_logger.info(f"Total webhook logs: {total:,}")

    # Count by age
    cutoff_date = datetime.now(UTC) - timedelta(days=15)
    recent = db.exec(
        select(func.count())
        .select_from(WebhookLog)
        .where(WebhookLog.timestamp >= cutoff_date)
    ).one()
    old = total - recent

    app_logger.info(f"  Last 15 days: {recent:,} rows (will be migrated)")
    app_logger.info(f"  Older than 15 days: {old:,} rows (will stay in webhook_log_old)")

    # Count by status
    app_logger.info("\nBy Status:")
    status_counts = db.exec(
        text("""
            SELECT status, COUNT(*) as count
            FROM webhooklog
            GROUP BY status
            ORDER BY count DESC
        """)
    ).all()

    for status, count in status_counts:
        app_logger.info(f"  {status}: {count:,}")

    # Date range
    min_date = db.exec(select(func.min(WebhookLog.timestamp))).one()
    max_date = db.exec(select(func.max(WebhookLog.timestamp))).one()

    app_logger.info(f"\nDate range:")
    app_logger.info(f"  Oldest: {min_date}")
    app_logger.info(f"  Newest: {max_date}")

    app_logger.info("=" * 60 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate seed data for partition migration testing'
    )
    parser.add_argument(
        '--rows',
        type=int,
        default=10000,
        help='Number of webhook log rows to generate (default: 10000)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=20,
        help='Number of days to spread data across (default: 20)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for inserts (default: 1000)'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset database tables before generating data'
    )

    args = parser.parse_args()

    app_logger.info("=" * 60)
    app_logger.info("WEBHOOK LOG SEED DATA GENERATOR")
    app_logger.info("=" * 60)
    app_logger.info(f"Configuration:")
    app_logger.info(f"  Rows to generate: {args.rows:,}")
    app_logger.info(f"  Days to span: {args.days}")
    app_logger.info(f"  Batch size: {args.batch_size:,}")
    app_logger.info(f"  Reset tables: {args.reset}")
    app_logger.info("=" * 60 + "\n")

    # Initialize database
    if args.reset:
        app_logger.info("Resetting database tables...")
        init_db()
        app_logger.info("✓ Tables reset\n")

    with Session(engine) as db:
        # Create test endpoint
        endpoint = create_test_endpoint(db)

        # Generate logs
        generate_webhook_logs(
            db=db,
            endpoint_id=endpoint.id,
            total_rows=args.rows,
            days_back=args.days,
            batch_size=args.batch_size,
        )

        # Print summary
        print_summary(db, args.days)

    app_logger.info("Ready for migration testing!")
    app_logger.info("Run: python -m chronos.scripts.migrate_to_partitioned_webhook_log\n")


if __name__ == "__main__":
    main()
