"""
Round-robin job dispatcher for fair processing across branches.

The dispatcher runs as a continuous while loop on a dedicated worker,
cycling through branches with pending jobs and dispatching them to
Celery workers with backpressure control.

#### IMPORTANT  ######
The dispatcher worker must be run separately with time limits
disabled via CLI flags (--soft-time-limit=0 --time-limit=0). Setting
soft_time_limit=0 on the task decorator alone is NOT sufficient because
billiard's pool.apply_async uses `or` (not `is None`) to check overrides,
so 0 is treated as falsy and falls back to the global default.

COMMAND TO RUN:

celery -A chronos.worker worker -Q dispatcher -c 1 \
    --without-heartbeat --without-mingle \
    --soft-time-limit=0 --time-limit=0
"""

import json
import logging
from bisect import bisect_right

from pydantic import ValidationError

dispatch_logger = logging.getLogger('chronos.dispatcher')

DEFAULT_BATCH_LIMIT = 100  # TODO think over this limit
DEFAULT_MAX_CELERY_QUEUE = 50
DEFAULT_CYCLE_DELAY = 0.01
DEFAULT_IDLE_DELAY = 1.0


def dispatch_cycle(batch_limit: int = DEFAULT_BATCH_LIMIT):
    """
    Execute one round-robin dispatch cycle.

    Cycles through all branches with pending jobs, starting from the cursor
    position, and dispatches one job from each branch to Celery workers.

    Uses bisect_right to find the correct start position even when the cursor
    points to a branch that is no longer active. This ensures fairness: if
    cursor=7 and active branches are [5, 10], we start from 10 (the next
    branch after 7 in sorted order), not 5.
    """
    # avoids circular import here
    from chronos.worker import celery_app, job_queue

    # get all the active branches here
    active_branches = job_queue.get_active_branches()
    if not active_branches:
        dispatch_logger.info('No active branches found by dispatcher')
        return 0

    # then get the cursor
    start_index = 0
    cursor = job_queue.get_cursor()
    if cursor is not None:
        # we use bisect right to get the insertion point from the sorted active branches list
        start_index = bisect_right(active_branches, cursor) % len(active_branches)

    dispatched = 0

    # this here does the re-ordering of the active branch list
    # For example consider [1, 2 , 3 , 4 ,5] and start is at 4
    # so we do [4, 5, 1, 2 , 3]
    branches_to_process = active_branches[start_index:] + active_branches[:start_index]
    for branch_id in branches_to_process:
        if dispatched >= batch_limit:
            # we will continue from here again in the next cycle if we have dispatched past
            # the per cycle dispatch limit
            break

        # Phase 1 is peek and validate poison payloads are acked or discarded.
        # Only catches deserialization errors such as corrupt json object which
        # are permanent and can never succeed. Redis errors are not caught
        # here as they propagate to the outer while-loop which sleeps
        # and retries the whole cycle. This prevents a transient peek() failure from
        # discarding a valid job.
        try:
            payload = job_queue.peek(branch_id)
            if payload is None:
                continue

            task = celery_app.tasks.get(payload.task_name)
            if task is None:
                dispatch_logger.error('Unknown task %s for branch %d, skipping', payload.task_name, branch_id)
                job_queue.ack(branch_id)
                continue
        except (json.JSONDecodeError, ValidationError):
            dispatch_logger.exception('Poison payload for branch %d, discarding', branch_id)
            try:
                job_queue.ack(branch_id)
            except Exception:
                dispatch_logger.exception('Failed to ack poison job for branch %d', branch_id)
            continue

        # Phase 2 is dispatch where failures here DON'T ack.
        # apply_async() can fail from broker errors or permanent
        # serialization errors. We can't distinguish them, so we leave the job
        # in the queue and skip this branch for now. Transient errors resolve
        # next cycle. Permanent errors stall only this branch (other branches
        # are unaffected) and produce repeated log errors for operator attention.
        try:
            task.apply_async(kwargs=payload.kwargs)
        except Exception:
            dispatch_logger.exception(
                'Failed to dispatch %s for branch %d, will retry next cycle', payload.task_name, branch_id
            )
            # so the concept is that the transient error will hopefully resolve next cycle
            # don't really think we'll get a serialisation error here? Because they should be discarded in phase 1
            continue

        # Phase 3 Post-dispatch ob was dispatched, ack it off the queue.
        job_queue.ack(branch_id)
        try:
            job_queue.set_cursor(branch_id)
        except Exception:
            dispatch_logger.exception('Failed to update cursor after dispatching for branch %d', branch_id)
        dispatched += 1
        dispatch_logger.debug('Dispatched %s for branch %d', payload.task_name, branch_id)

    return dispatched
