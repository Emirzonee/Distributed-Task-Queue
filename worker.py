import asyncio
import logging
import sys

from broker import RedisBroker
from models import setup_logger

HEARTBEAT_INTERVAL = 2   # seconds between heartbeat refreshes
HEARTBEAT_TTL = 5        # Redis key expiry; must be > HEARTBEAT_INTERVAL
IDLE_SLEEP = 0.5         # seconds to wait when the queue is empty


class Worker:
    """
    Async task worker that implements the Reliable Queue pattern.

    Two coroutines run concurrently via asyncio.gather():
      - heartbeat_loop: refreshes the liveness key in Redis every
        HEARTBEAT_INTERVAL seconds so the master knows this worker is up.
      - process_tasks:  polls the queue, executes tasks, and acknowledges
        completion so the task is removed from the processing queue.

    If this process dies, the heartbeat key expires (TTL = HEARTBEAT_TTL)
    and the master orchestrator will requeue any tasks still in this
    worker's processing queue.
    """

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.broker = RedisBroker()
        self.logger: logging.Logger = setup_logger(f"WORKER-{worker_id}")
        self._running = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Starts the worker and runs until interrupted."""
        self.logger.info(f"Worker [{self.worker_id}] is ready.")
        self._running = True

        await asyncio.gather(
            self._heartbeat_loop(),
            self._process_tasks(),
        )

    def stop(self) -> None:
        """Signals the worker to finish its current task and shut down."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal coroutines
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Refreshes the Redis heartbeat key on a fixed interval."""
        while self._running:
            self.broker.set_heartbeat(self.worker_id, ttl=HEARTBEAT_TTL)
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _process_tasks(self) -> None:
        """
        Continuously fetches and processes tasks from the queue.

        Uses a 1-second blocking timeout on fetch so the event loop is
        not starved — this allows _heartbeat_loop to keep running while
        the worker waits for new work.
        """
        while self._running:
            task = self.broker.fetch_task(self.worker_id, timeout=1)

            if task:
                self.logger.info(
                    "Task received",
                    extra={"task_id": task.task_id, "payload": task.payload},
                )

                await self._execute(task)
                self.broker.complete_task(self.worker_id, task)

                self.logger.info("Task done", extra={"task_id": task.task_id})
            else:
                await asyncio.sleep(IDLE_SLEEP)

    async def _execute(self, task) -> None:
        """
        Placeholder for real task execution logic.

        Replace the sleep below with actual work: calling an external API,
        processing a file, sending an email, etc.
        """
        await asyncio.sleep(3)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

if __name__ == "__main__":
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "W1"
    worker = Worker(worker_id)

    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        worker.stop()
        worker.logger.info(f"Worker [{worker_id}] stopped.")