# PostgreSQL Partitioning Migration Guide

## Overview

This guide provides step-by-step instructions for migrating the `webhook_log` table from a standard deletion-based cleanup approach to PostgreSQL native partitioning.

**Problem Solved:** The current deletion job deletes 200k+ rows/hour (4.8M/day) and causes database crashes with 3-4 minute freezes and massive WAL generation.

**Solution:** Use PostgreSQL native table partitioning with daily partitions. Dropping old partitions is instant and generates minimal WAL.

---

## Pre-Migration Checklist

### 1. Test in Development/Staging First
```bash
# Ensure you're on development database
export TESTING=true  # Or set in .env

# Run migration
python -m chronos.scripts.migrate_to_partitioned_webhook_log
```

### 2. Verify Application Compatibility
- Test webhook logging still works
- Verify queries return expected data
- Check application logs for any errors

### 3. Backup Production Database
```bash
# Create a backup before migration
pg_dump -h your-host -U your-user -d chronos > backup_before_partition_$(date +%Y%m%d).sql
```

### 4. Review Changes
- `chronos/sql_models.py` - WebhookLog model updated with composite primary key
- `chronos/worker.py` - Two new partition management jobs added, old delete job commented out
- `chronos/scripts/migrate_to_partitioned_webhook_log.py` - Migration script

---

## Migration Procedure

### Step 1: Choose Migration Window

**Recommended:** 2:00 AM - 4:00 AM (lowest traffic)

**Estimated Duration:** 20-30 minutes for ~72M rows (15 days at 200k/hour)

### Step 2: Deploy Code Changes

```bash
# Pull latest code with partition changes
git pull origin master

# Install any new dependencies (if needed)
pip install -r requirements.txt

# Restart services to load new code (but don't run migration yet)
# This deploys the new jobs but they won't run until migration completes
make restart-server
make restart-worker
```

**Important:** The new partition management jobs will fail gracefully if run before migration. They check for the existence of the SQL functions.

### Step 3: Run Migration Script

**Production Command:**
```bash
python -m chronos.scripts.migrate_to_partitioned_webhook_log
```

You'll be prompted to type `MIGRATE` to confirm.

**What the script does:**
1. Renames `webhook_log` → `webhook_log_old` (preserves existing data)
2. Creates new partitioned `webhook_log` table with composite PK `(id, timestamp)`
3. Creates 45 daily partitions (15 days back + 30 days forward)
4. Migrates last 15 days of data in batches of 1,000 rows
5. Creates SQL functions for partition management:
   - `create_future_webhook_log_partitions(days_ahead INT)`
   - `drop_old_webhook_log_partitions(retention_days INT)`
6. Verifies row counts match

### Step 4: Monitor Migration Progress

The script logs progress to stdout and logfire (if configured):
- Partition creation status
- Data migration progress (rows migrated, percentage complete)
- Verification results

**Expected Output:**
```
================================================================================
Starting PostgreSQL Partitioning Migration
================================================================================

[Step 1/6] Renaming existing webhook_log table...
✓ Table renamed: webhook_log -> webhook_log_old

[Step 2/6] Creating new partitioned table...
✓ Partitioned table created

[Step 3/6] Creating partitions...
Created partition: webhook_log_y2025m10d22 (2025-10-22 to 2025-10-23)
...
✓ All partitions created

[Step 4/6] Migrating data from last 15 days...
Total rows to migrate: 72,000,000
Migrated 1,000 / 72,000,000 rows (0.0%)
...
✓ Data migration complete

[Step 5/6] Creating partition management functions...
✓ Management functions created

[Step 6/6] Verifying migration...
Old table (last 15 days): 72,000,000 rows
New table: 72,000,000 rows
✓ Verification passed: row counts match

================================================================================
MIGRATION COMPLETED SUCCESSFULLY!
================================================================================
```

### Step 5: Verify Migration Success

**Check row counts:**
```sql
-- Count in new table
SELECT COUNT(*) FROM webhook_log;

-- Count in old table (last 15 days)
SELECT COUNT(*) FROM webhook_log_old
WHERE timestamp >= NOW() - INTERVAL '15 days';

-- Should match!
```

**Verify partitions exist:**
```sql
SELECT tablename
FROM pg_tables
WHERE tablename LIKE 'webhook_log_y%'
ORDER BY tablename;
```

**Test application:**
- Generate a webhook log entry
- Query webhook logs via API
- Check logs for any errors

### Step 6: Monitor for 24 Hours

- Watch application logs for any errors
- Monitor database performance (should see massive improvement)
- Verify new partition jobs run successfully:
  - 1:00 AM: Create future partitions job
  - 2:00 AM: Drop old partitions job

**Check job execution:**
```bash
# Check Redis for job locks
redis-cli
> GET create_future_partitions_job
> GET drop_old_partitions_job
```

### Step 7: Cleanup (After 7+ Days of Stability)

Once you're confident the migration is stable:

```sql
-- Drop the old table
DROP TABLE webhook_log_old CASCADE;
```

**Also clean up the commented code in worker.py:**
- Remove commented `delete_old_logs_job` code
- Remove commented `get_count` function
- Remove commented `_delete_old_logs_job` task

---

## Rollback Procedure

### If Migration Fails During Execution

The script automatically rolls back on error:
1. Drops the new `webhook_log` table
2. Renames `webhook_log_old` back to `webhook_log`

### Manual Rollback (If Needed)

```sql
-- Connect to database
psql -h your-host -U your-user -d chronos

-- Drop new table and restore old
DROP TABLE IF EXISTS webhook_log CASCADE;
ALTER TABLE webhook_log_old RENAME TO webhook_log;
```

### Rollback Code Changes

```bash
# Revert code changes
git revert <commit-hash>

# Or manually:
# 1. Edit chronos/sql_models.py - remove composite PK, restore single PK
# 2. Edit chronos/worker.py - uncomment old delete job, remove new partition jobs
# 3. Restart services
```

---

## Post-Migration Benefits

### Performance Improvements
- **No more database freezes:** Partition drops are instant (milliseconds vs 3-4 minutes)
- **Minimal WAL generation:** DROP TABLE vs DELETE millions of rows
- **Better query performance:** Partition pruning automatically limits scans to relevant partitions
- **No lock contention:** Dropping a partition doesn't lock the entire table

### Operational Benefits
- **Safer operations:** Old table preserved for 7+ days
- **Automated management:** Jobs handle partition lifecycle automatically
- **Predictable performance:** No more surprise performance issues from deletions
- **Easy monitoring:** Check partition counts to verify job execution

### Expected Metrics
- **Database CPU:** Should drop significantly during cleanup times
- **WAL generation:** Reduce from GB to MB during cleanup
- **Lock wait time:** Eliminate 3-4 minute database freezes
- **SSL SYSCALL EOF errors:** Should disappear completely

---

## Troubleshooting

### Migration Script Fails

**Error: "Table webhook_log_old already exists"**
- Previous migration attempt may have failed
- Check if old table exists: `\dt webhook_log*`
- If migration failed, manually clean up and retry

**Error: "Cannot create partition for past date"**
- System clock may be incorrect
- Verify server time: `date`
- Adjust partition date ranges if needed

### New Jobs Failing

**Error: "Function create_future_webhook_log_partitions does not exist"**
- Migration didn't complete successfully
- Verify functions exist:
  ```sql
  \df create_future_webhook_log_partitions
  \df drop_old_webhook_log_partitions
  ```
- Re-run migration if needed

**Error: "Partition already exists"**
- This is normal - function checks for existing partitions
- Job should complete successfully despite warning

### Application Errors

**Error: "Column 'id' is not part of primary key"**
- SQLModel not recognizing composite primary key
- Verify both `id` and `timestamp` have `primary_key=True` in model
- Restart application

**Error: "No partition exists for value"**
- Trying to insert data for a date without a partition
- Run partition creation manually:
  ```sql
  SELECT create_future_webhook_log_partitions(30);
  ```

---

## Monitoring Commands

### Check Partition Count
```sql
SELECT COUNT(*)
FROM pg_tables
WHERE tablename LIKE 'webhook_log_y%';
-- Should be around 45 (15 past + 30 future + today)
```

### View Partition Sizes
```sql
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename LIKE 'webhook_log_y%'
ORDER BY tablename DESC
LIMIT 20;
```

### Check Oldest and Newest Partitions
```sql
-- Oldest partition
SELECT MIN(tablename) FROM pg_tables WHERE tablename LIKE 'webhook_log_y%';

-- Newest partition
SELECT MAX(tablename) FROM pg_tables WHERE tablename LIKE 'webhook_log_y%';
```

### Verify Data Distribution
```sql
SELECT
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE tablename LIKE 'webhook_log_y%'
ORDER BY tablename DESC
LIMIT 20;
```

### Monitor Job Execution
```bash
# Check Celery worker logs
tail -f /var/log/chronos/worker.log | grep partition

# Check Redis for locks
redis-cli
> KEYS *partition*
```

---

## FAQs

**Q: Can I run the migration during business hours?**
A: Not recommended. While the migration processes data in batches, it's safer to run during low-traffic periods (2-4 AM).

**Q: What happens to data older than 15 days?**
A: It remains in `webhook_log_old` but is not migrated to the new partitioned table. You can drop `webhook_log_old` after verifying the migration.

**Q: Can I change the retention period?**
A: Yes. Edit the `drop_old_partitions_job` in `worker.py` and change the parameter:
```python
sql = text("SELECT drop_old_webhook_log_partitions(30)")  # 30 days instead of 15
```

**Q: What if I need to query data older than 15 days?**
A: Query `webhook_log_old` directly until you're ready to drop it.

**Q: Can I pause the migration?**
A: No. Once started, let it complete. It's designed to be atomic with rollback on failure.

**Q: How do I manually create/drop partitions?**
```sql
-- Create future partitions
SELECT create_future_webhook_log_partitions(60);  -- 60 days ahead

-- Drop old partitions
SELECT drop_old_webhook_log_partitions(30);  -- Older than 30 days
```

**Q: What happens if partition creation job fails?**
A: Webhook logging will fail for dates without partitions. Manually run:
```sql
SELECT create_future_webhook_log_partitions(30);
```

---

## Support

If you encounter issues during migration:

1. Check the troubleshooting section above
2. Review application logs and database logs
3. Verify all prerequisites are met
4. If needed, follow rollback procedure
5. Contact database administrator or development team

**Key Files:**
- Migration script: `chronos/scripts/migrate_to_partitioned_webhook_log.py`
- Model changes: `chronos/sql_models.py`
- Job definitions: `chronos/worker.py`
- This guide: `PARTITION_MIGRATION_GUIDE.md`

---

## Summary

This migration solves critical database performance issues by replacing deletion-based cleanup with PostgreSQL native partitioning. The approach:

✅ Eliminates 3-4 minute database freezes
✅ Reduces WAL generation from GB to MB
✅ Provides instant partition drops
✅ Maintains 15-day retention policy
✅ Includes automated partition management
✅ Preserves existing data for safe rollback

**Next Steps:** Test in development, schedule production migration, monitor results.
