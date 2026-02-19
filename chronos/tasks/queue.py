import json
from datetime import UTC, datetime
from typing import Any, Optional

import redis
from pydantic import BaseModel

from chronos.utils import settings

BRANCH_KEY_TEMPLATE = 'jobs:branch:{}'
ACTIVE_BRANCHES_KEY = 'jobs:branches:active'
CURSOR_KEY = 'jobs:dispatcher:cursor'


class JobPayload(BaseModel):
    """
    This is used to serialise the Job Payload stored in Redis
    """

    task_name: str
    branch_id: int
    kwargs: dict[str, Any]
    enqueued_at: datetime


class JobQueue:
    """
    Redis LIST queue for per branch Job storage
    """

    # this is a Lua script for atomic pop and remove
    _ACK_SCRIPT = """
    redis.call('LPOP', KEYS[1])
    if redis.call('LLEN', KEYS[1]) == 0 then
        redis.call('SREM', KEYS[2], ARGV[1])
    end
    return 1
    """

    _ack_script_sha: Optional[str] = None

    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client

    def _get_queue_key(self, branch_id: int) -> str:
        """
        gives the key for the queue using the branch id
        """
        return BRANCH_KEY_TEMPLATE.format(branch_id)

    def enqueue(self, task_name: str, branch_id: int, **kwargs):
        """Add a job to a branch's queue.

        Args:
            task_name: Name of the Celery task to execute.
            routing_branch_id: Branch ID for queue routing (not passed to the task).
            **kwargs: Arguments to pass to the task.
        """
        payload = JobPayload(
            task_name=task_name,
            branch_id=branch_id,
            kwargs=kwargs,
            enqueued_at=datetime.now(UTC),
        )
        queue_key = self._get_queue_key(branch_id)

        # we create a pipeline to execute the enqueue related commands
        pipeline = self.redis_client.pipeline()
        pipeline.rpush(queue_key, payload.model_dump_json())
        pipeline.sadd(ACTIVE_BRANCHES_KEY, str(branch_id))
        pipeline.execute()

    def peek(self, branch_id: int) -> JobPayload | None:
        """
        Uses a non-destructive peek to return the JobPayload ob given a branch id
        """
        queue_key = self._get_queue_key(branch_id)
        data = self.redis_client.lindex(queue_key, 0)
        if data is None:
            return None
        return JobPayload(**json.loads(data))

    def _run_ack_script(self, queue_key: str, branch_id: int) -> None:
        """
        Runs the atomic Lua ack script
        """
        try:
            self.redis_client.evalsha(
                JobQueue._ack_script_sha,
                2,
                queue_key,
                ACTIVE_BRANCHES_KEY,
                str(branch_id),
            )
        except redis.exceptions.NoScriptError:
            JobQueue._ack_script_sha = self.redis_client.script_load(JobQueue._ACK_SCRIPT)
            self.redis_client.evalsha(
                JobQueue._ack_script_sha,
                2,
                queue_key,
                ACTIVE_BRANCHES_KEY,
                str(branch_id),
            )

    def ack(self, branch_id: int) -> None:
        """
        Acknowledge and remove the oldest job from a branch's queue. Also removes the branch from
        active set if it has no jobs in the queue
        """
        queue_key = self._get_queue_key(branch_id)
        if JobQueue._ack_script_sha is None:
            JobQueue._ack_script_sha = self.redis_client.script_load(JobQueue._ACK_SCRIPT)
        self._run_ack_script(queue_key, branch_id)

    def get_active_branches(self) -> list[int]:
        """
        Get sorted list of branch IDs with pending jobs.
        """
        branch_ids = self.redis_client.smembers(ACTIVE_BRANCHES_KEY)
        return sorted(int(bid) for bid in branch_ids)

    def has_active_jobs(self) -> bool:
        # checks against the cardinality or length of the set
        return self.redis_client.scard(ACTIVE_BRANCHES_KEY) > 0

    def get_cursor(self) -> Optional[int]:
        cursor = self.redis_client.get(CURSOR_KEY)
        return int(cursor) if cursor else None

    def set_cursor(self, branch_id: int) -> None:
        self.redis_client.set(CURSOR_KEY, str(branch_id))

    def get_queue_length(self, branch_id: int) -> int:
        """
        Get the length of a branch's queue. O(1) Redis operation.
        """
        return self.redis_client.llen(self._get_queue_key(branch_id))

    def get_celery_queue_length(self) -> int:
        """Get pending tasks in the Celery default queue. O(1) Redis operation.

        IMPORTANT: This measures only the broker queue (tasks waiting to be picked
        up by a worker). It does NOT count tasks currently being executed by workers.
        With 2 workers busy and LLEN=0, real system load is 2, not 0. See Edge Case 26
        for analysis of why this is acceptable and Appendix B for future improvements.
        """
        return self.redis_client.llen('celery')

    def clear_all(self) -> None:
        """
        Clear all job queue data. Testing/dev only.
        """
        assert settings.testing or settings.dev_mode
        branch_ids = self.get_active_branches()
        pipe = self.redis_client.pipeline()
        for branch_id in branch_ids:
            pipe.delete(self._get_queue_key(branch_id))
        pipe.delete(ACTIVE_BRANCHES_KEY)
        pipe.delete(CURSOR_KEY)
        pipe.execute()
