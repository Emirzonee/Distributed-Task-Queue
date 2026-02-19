import os
import logging
from typing import Optional, List

import redis
from dotenv import load_dotenv

from models import Task, setup_logger

load_dotenv()


class RedisBroker:
    """
    Manages all Redis interactions for the task queue.

    Implements the Reliable Queue pattern: tasks are atomically moved
    from the shared pending queue into a per-worker processing queue on
    pickup, and only removed once the worker explicitly marks them done.
    This ensures no task is silently dropped if a worker crashes.
    """

    PENDING_QUEUE = "queue:pending"

    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL")

        if not redis_url:
            raise EnvironmentError(
                "REDIS_URL is not set. Add it to your .env file and try again."
            )

        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.logger: logging.Logger = setup_logger("BROKER")

    # ------------------------------------------------------------------
    # Core queue operations
    # ------------------------------------------------------------------

    def push_task(self, task: Task) -> None:
        """
        Adds a new task to the left end of the pending queue.

        Args:
            task: Task instance to enqueue.
        """
        self.redis.lpush(self.PENDING_QUEUE, task.to_json())
        self.logger.info("Task enqueued", extra={"task_id": task.task_id})

    def fetch_task(self, worker_id: str, timeout: int = 1) -> Optional[Task]:
        """
        Atomically moves the next task from the pending queue into the
        worker's personal processing queue (Reliable Queue pattern).

        Blocks for up to `timeout` seconds waiting for a task. A timeout
        of 1 second is used by default so the async event loop is not
        starved while workers wait for new work.

        Args:
            worker_id: Unique identifier of the calling worker.
            timeout:   Seconds to block before returning None.

        Returns:
            The next Task, or None if the queue was empty after timeout.
        """
        processing_queue = self._processing_queue(worker_id)

        task_data = self.redis.blmove(
            self.PENDING_QUEUE,
            processing_queue,
            timeout=timeout,
            src="RIGHT",
            dest="LEFT",
        )

        return Task.from_json(task_data) if task_data else None

    def complete_task(self, worker_id: str, task: Task) -> None:
        """
        Removes a successfully processed task from the worker's queue.

        Args:
            worker_id: Worker that finished the task.
            task:      The completed Task instance.
        """
        processing_queue = self._processing_queue(worker_id)
        self.redis.lrem(processing_queue, 1, task.to_json())
        self.logger.info("Task completed", extra={"task_id": task.task_id})

    # ------------------------------------------------------------------
    # Heartbeat & orchestration helpers
    # ------------------------------------------------------------------

    def set_heartbeat(self, worker_id: str, ttl: int = 5) -> None:
        """
        Refreshes the worker's liveness key in Redis.

        The key expires automatically after `ttl` seconds. Workers must
        call this more frequently than the TTL to stay considered alive.

        Args:
            worker_id: Unique identifier of the worker.
            ttl:       Key expiry in seconds (default: 5).
        """
        self.redis.set(f"worker:heartbeat:{worker_id}", "ALIVE", ex=ttl)

    def is_worker_alive(self, worker_id: str) -> bool:
        """
        Returns True if the worker's heartbeat key is still present.

        Args:
            worker_id: Worker to check.
        """
        return bool(self.redis.exists(f"worker:heartbeat:{worker_id}"))

    def get_all_processing_workers(self) -> List[str]:
        """
        Returns the IDs of all workers that currently hold at least one task.

        Scans Redis for keys matching 'queue:processing:*' and extracts
        the worker ID suffix.
        """
        keys = self.redis.keys("queue:processing:*")
        return [k.split(":")[-1] for k in keys]

    def requeue_orphaned_tasks(self, worker_id: str) -> int:
        """
        Moves all tasks from a crashed worker's processing queue back to
        the shared pending queue so they can be picked up again.

        Args:
            worker_id: ID of the worker whose tasks should be recovered.

        Returns:
            Number of tasks that were requeued.
        """
        processing_queue = self._processing_queue(worker_id)
        requeued = 0

        while True:
            task_data = self.redis.rpoplpush(processing_queue, self.PENDING_QUEUE)
            if not task_data:
                break
            requeued += 1

        self.logger.info(
            "Orphaned tasks requeued",
            extra={"worker_id": worker_id, "count": requeued},
        )
        return requeued

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _processing_queue(worker_id: str) -> str:
        return f"queue:processing:{worker_id}"