"""
Migration script to convert webhook_log table from standard table to partitioned table.

This script:
1. Renames existing webhook_log table to webhook_log_old
2. Creates new partitioned webhook_log table (daily partitions by timestamp)
3. Creates partitions for last 15 days + next 30 days
4. Migrates data from last 15 days in batches
5. Creates SQL functions for partition management

Run during low traffic period (recommended: 2-4 AM).
Estimated duration: 20-30 minutes for ~72M rows.

Usage:
    python -m chronos.scripts.migrate_to_partitioned_webhook_log
"""

import sys
from datetime import datetime, timedelta, UTC
from typing import List, Tuple

from sqlalchemy import text
from sqlmodel import Session

from chronos.db import engine
from chronos.utils import app_logger, settings

# Optional logfire import
try:
    import logfire
    # Configure logfire if available
    if settings.logfire_token:
        logfire.configure(
            service_name='chronos-migration',
            token=settings.logfire_token,
            send_to_logfire=True,
        )
except ImportError:
    # Create a no-op logfire object for testing
    class NoOpLogfire:
        class span:
            def __init__(self, *args, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
    logfire = NoOpLogfire()


def get_partition_name(date: datetime) -> str:
    """Generate partition table name for a given date."""
    return f"webhook_log_y{date.year}m{date.month:02d}d{date.day:02d}"


def get_partition_date_ranges(start_date: datetime, end_date: datetime) -> List[Tuple[datetime, datetime, str]]:
    """
    Generate list of (start, end, name) tuples for daily partitions.

    Args:
        start_date: First date to create partition for
        end_date: Last date to create partition for

    Returns:
        List of (range_start, range_end, partition_name) tuples
    """
    partitions = []
    current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    while current_date <= end_date:
        next_date = current_date + timedelta(days=1)
        partition_name = get_partition_name(current_date)
        partitions.append((current_date, next_date, partition_name))
        current_date = next_date

    return partitions


def create_partitioned_table(session: Session) -> None:
    """
    Create the new partitioned webhook_log table structure.

    This creates the parent table partitioned by RANGE on timestamp.
    """
    app_logger.info("Creating partitioned webhook_log table...")

    create_table_sql = text("""
        CREATE TABLE webhooklog (
            id SERIAL,
            request_headers JSONB,
            request_body JSONB,
            response_headers JSONB,
            response_body JSONB,
            status VARCHAR NOT NULL,
            status_code INTEGER,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            webhook_endpoint_id INTEGER,
            PRIMARY KEY (id, timestamp),
            CONSTRAINT webhook_log_webhook_endpoint_id_fkey
                FOREIGN KEY (webhook_endpoint_id)
                REFERENCES webhookendpoint(id)
        ) PARTITION BY RANGE (timestamp);
    """)

    session.exec(create_table_sql)

    # Create indexes on the partitioned table
    # These will be automatically created on each partition
    create_indexes_sql = [
        text("CREATE INDEX ix_webhook_log_timestamp ON webhooklog (timestamp);"),
        text("CREATE INDEX ix_webhook_log_webhook_endpoint_id ON webhooklog (webhook_endpoint_id);"),
    ]

    for sql in create_indexes_sql:
        session.exec(sql)

    app_logger.info("Partitioned table created successfully")


def create_partition(session: Session, range_start: datetime, range_end: datetime, partition_name: str) -> None:
    """Create a single partition for the specified date range."""
    create_partition_sql = text(f"""
        CREATE TABLE {partition_name} PARTITION OF webhooklog
        FOR VALUES FROM ('{range_start.isoformat()}') TO ('{range_end.isoformat()}');
    """)

    session.exec(create_partition_sql)
    app_logger.info(f"Created partition: {partition_name} ({range_start.date()} to {range_end.date()})")


def create_all_partitions(session: Session) -> None:
    """Create partitions for last 15 days + next 30 days."""
    app_logger.info("Creating partitions...")

    now = datetime.now(UTC)
    start_date = now - timedelta(days=15)
    end_date = now + timedelta(days=30)

    partitions = get_partition_date_ranges(start_date, end_date)

    with logfire.span(f"Creating {len(partitions)} partitions"):
        for range_start, range_end, partition_name in partitions:
            create_partition(session, range_start, range_end, partition_name)

    app_logger.info(f"Created {len(partitions)} partitions successfully")


def migrate_data_in_batches(session: Session, batch_size: int = 1000) -> None:
    """
    Migrate data from webhook_log_old to webhook_log in batches.
    Only migrates data from the last 15 days.
    """
    app_logger.info("Starting data migration...")

    # Calculate cutoff date (15 days ago)
    cutoff_date = datetime.now(UTC) - timedelta(days=15)

    # Get total count to migrate
    count_sql = text("""
        SELECT COUNT(*) FROM webhooklog_old
        WHERE timestamp >= :cutoff_date
    """)
    total_rows = session.exec(count_sql, {"cutoff_date": cutoff_date}).scalar()

    app_logger.info(f"Total rows to migrate: {total_rows:,}")

    if total_rows == 0:
        app_logger.info("No data to migrate")
        return

    # Migrate in batches
    migrated = 0
    offset = 0

    with logfire.span(f"Migrating {total_rows:,} rows in batches of {batch_size}"):
        while offset < total_rows:
            # Insert batch
            insert_sql = text(f"""
                INSERT INTO webhooklog
                    (id, request_headers, request_body, response_headers, response_body,
                     status, status_code, timestamp, webhook_endpoint_id)
                SELECT
                    id, request_headers, request_body, response_headers, response_body,
                    status, status_code, timestamp, webhook_endpoint_id
                FROM webhooklog_old
                WHERE timestamp >= :cutoff_date
                ORDER BY timestamp, id
                LIMIT {batch_size} OFFSET {offset}
            """)

            result = session.exec(insert_sql, {"cutoff_date": cutoff_date})
            batch_count = result.rowcount if hasattr(result, 'rowcount') else batch_size

            migrated += batch_count
            offset += batch_size

            # Commit each batch
            session.commit()

            # Log progress
            progress_pct = (migrated / total_rows) * 100
            app_logger.info(f"Migrated {migrated:,} / {total_rows:,} rows ({progress_pct:.1f}%)")

            # Break if we got fewer rows than batch_size (end of data)
            if batch_count < batch_size:
                break

    app_logger.info(f"Data migration complete: {migrated:,} rows migrated")


def create_partition_management_functions(session: Session) -> None:
    """
    Create SQL functions for automated partition management.

    Creates:
    1. create_future_webhook_log_partitions(days_ahead INT)
    2. drop_old_webhook_log_partitions(retention_days INT)
    """
    app_logger.info("Creating partition management functions...")

    # Function to create future partitions
    create_future_partitions_func = text("""
        CREATE OR REPLACE FUNCTION create_future_webhook_log_partitions(days_ahead INT)
        RETURNS void AS $$
        DECLARE
            partition_date DATE;
            partition_name TEXT;
            start_date TIMESTAMP;
            end_date TIMESTAMP;
        BEGIN
            -- Create partitions for each day from tomorrow to days_ahead
            FOR i IN 1..days_ahead LOOP
                partition_date := CURRENT_DATE + i;
                partition_name := 'webhooklog_y' ||
                                 TO_CHAR(partition_date, 'YYYY') || 'm' ||
                                 TO_CHAR(partition_date, 'MM') || 'd' ||
                                 TO_CHAR(partition_date, 'DD');

                start_date := partition_date::TIMESTAMP;
                end_date := (partition_date + INTERVAL '1 day')::TIMESTAMP;

                -- Check if partition already exists
                IF NOT EXISTS (
                    SELECT 1 FROM pg_tables
                    WHERE tablename = partition_name
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE %I PARTITION OF webhooklog FOR VALUES FROM (%L) TO (%L)',
                        partition_name,
                        start_date,
                        end_date
                    );
                    RAISE NOTICE 'Created partition: %', partition_name;
                END IF;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Function to drop old partitions
    drop_old_partitions_func = text("""
        CREATE OR REPLACE FUNCTION drop_old_webhook_log_partitions(retention_days INT)
        RETURNS void AS $$
        DECLARE
            partition_record RECORD;
            partition_date DATE;
            cutoff_date DATE;
        BEGIN
            cutoff_date := CURRENT_DATE - retention_days;

            -- Find and drop old partitions
            FOR partition_record IN
                SELECT tablename
                FROM pg_tables
                WHERE tablename LIKE 'webhooklog_y%'
                ORDER BY tablename
            LOOP
                -- Extract date from partition name (format: webhook_log_yYYYYmMMdDD)
                BEGIN
                    partition_date := TO_DATE(
                        SUBSTRING(partition_record.tablename FROM 'y(\d{4})m(\d{2})d(\d{2})'),
                        'YYYYMMDD'
                    );

                    IF partition_date < cutoff_date THEN
                        EXECUTE format('DROP TABLE IF EXISTS %I', partition_record.tablename);
                        RAISE NOTICE 'Dropped partition: %', partition_record.tablename;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        RAISE WARNING 'Could not process partition: %', partition_record.tablename;
                END;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
    """)

    session.exec(create_future_partitions_func)
    session.exec(drop_old_partitions_func)

    app_logger.info("Partition management functions created successfully")


def verify_migration(session: Session) -> bool:
    """
    Verify that migration was successful by comparing row counts.

    Returns:
        True if verification passed, False otherwise
    """
    app_logger.info("Verifying migration...")

    # Count rows in old table (last 15 days)
    cutoff_date = datetime.now(UTC) - timedelta(days=15)
    old_count_sql = text("""
        SELECT COUNT(*) FROM webhooklog_old
        WHERE timestamp >= :cutoff_date
    """)
    old_count = session.exec(old_count_sql, {"cutoff_date": cutoff_date}).scalar()

    # Count rows in new table
    new_count_sql = text("SELECT COUNT(*) FROM webhook_log")
    new_count = session.exec(new_count_sql).scalar()

    app_logger.info(f"Old table (last 15 days): {old_count:,} rows")
    app_logger.info(f"New table: {new_count:,} rows")

    if old_count == new_count:
        app_logger.info("✓ Verification passed: row counts match")
        return True
    else:
        app_logger.error(f"✗ Verification failed: row count mismatch ({old_count:,} vs {new_count:,})")
        return False


def run_migration() -> None:
    """
    Main migration function. Orchestrates the entire migration process.

    Steps:
    1. Rename webhook_log to webhook_log_old
    2. Create new partitioned webhook_log table
    3. Create all partitions (15 days back + 30 days forward)
    4. Migrate data from last 15 days
    5. Create partition management functions
    6. Verify migration success

    Raises:
        Exception: If any step fails, rolls back the transaction
    """
    app_logger.info("=" * 80)
    app_logger.info("Starting PostgreSQL Partitioning Migration")
    app_logger.info("=" * 80)

    if settings.testing:
        app_logger.error("Cannot run migration in testing mode")
        sys.exit(1)

    with Session(engine) as session:
        try:
            with logfire.span("Partitioning Migration"):
                # Step 1: Rename existing table
                app_logger.info("\n[Step 1/6] Renaming existing webhook_log table...")
                rename_sql = text("""
                    ALTER TABLE webhooklog RENAME TO webhook_log_old;
                """)
                session.exec(rename_sql)
                session.commit()
                app_logger.info("✓ Table renamed: webhook_log -> webhook_log_old")

                # Step 2: Create new partitioned table
                app_logger.info("\n[Step 2/6] Creating new partitioned table...")
                create_partitioned_table(session)
                session.commit()
                app_logger.info("✓ Partitioned table created")

                # Step 3: Create all partitions
                app_logger.info("\n[Step 3/6] Creating partitions...")
                create_all_partitions(session)
                session.commit()
                app_logger.info("✓ All partitions created")

                # Step 4: Migrate data
                app_logger.info("\n[Step 4/6] Migrating data from last 15 days...")
                migrate_data_in_batches(session, batch_size=1000)
                app_logger.info("✓ Data migration complete")

                # Step 5: Create management functions
                app_logger.info("\n[Step 5/6] Creating partition management functions...")
                create_partition_management_functions(session)
                session.commit()
                app_logger.info("✓ Management functions created")

                # Step 6: Verify migration
                app_logger.info("\n[Step 6/6] Verifying migration...")
                verification_passed = verify_migration(session)

                if not verification_passed:
                    raise Exception("Migration verification failed - see logs above")

                app_logger.info("\n" + "=" * 80)
                app_logger.info("MIGRATION COMPLETED SUCCESSFULLY!")
                app_logger.info("=" * 80)
                app_logger.info("\nNext steps:")
                app_logger.info("1. Monitor application logs for any errors")
                app_logger.info("2. Verify new partition management jobs run successfully")
                app_logger.info("3. After 7 days of stability, you can drop webhook_log_old:")
                app_logger.info("   DROP TABLE webhook_log_old CASCADE;")

        except Exception as e:
            app_logger.error(f"\n{'=' * 80}")
            app_logger.error("MIGRATION FAILED!")
            app_logger.error(f"{'=' * 80}")
            app_logger.error(f"Error: {e}")
            app_logger.error("\nRolling back changes...")

            try:
                session.rollback()

                # Attempt to restore original table
                app_logger.error("Attempting to restore original table...")
                restore_sql = text("""
                    DROP TABLE IF EXISTS webhook_log CASCADE;
                    ALTER TABLE webhooklog_old RENAME TO webhooklog;
                """)
                session.exec(restore_sql)
                session.commit()
                app_logger.error("✓ Original table restored")

            except Exception as rollback_error:
                app_logger.error(f"Rollback failed: {rollback_error}")
                app_logger.error("Manual intervention required!")
                app_logger.error("Run: DROP TABLE IF EXISTS webhook_log CASCADE; ALTER TABLE webhooklog_old RENAME TO webhooklog;")

            sys.exit(1)


if __name__ == "__main__":
    # Safety check: require confirmation in production
    print("\n" + "=" * 80)
    print("PostgreSQL Webhook Log Partitioning Migration")
    print("=" * 80)
    print("\nThis script will:")
    print("  1. Rename webhook_log to webhook_log_old")
    print("  2. Create new partitioned webhook_log table")
    print("  3. Migrate last 15 days of data")
    print("  4. Create partition management functions")
    print("\nEstimated duration: 20-30 minutes")
    print("Recommended timing: During low traffic (2-4 AM)")
    print("\n" + "=" * 80)

    confirmation = input("\nType 'MIGRATE' to proceed: ")

    if confirmation != "MIGRATE":
        print("Migration cancelled.")
        sys.exit(0)

    print("\nStarting migration...\n")
    run_migration()
