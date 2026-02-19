import logging
import time

from broker import RedisBroker
from models import setup_logger

POLL_INTERVAL = 3  # seconds between health check cycles


class MasterOrchestrator:
    """
    Monitors worker health and recovers tasks from crashed workers.

    On each cycle the orchestrator:
      1. Finds all workers that currently hold at least one task.
      2. Checks whether each worker's heartbeat key is still alive.
      3. For any worker whose key has expired, calls requeue_orphaned_tasks()
         to move their in-flight tasks back to the shared pending queue.

    The orchestrator does not assign tasks — it only acts as a watchdog.
    """

    def __init__(self) -> None:
        self.broker = RedisBroker()
        self.logger: logging.Logger = setup_logger("MASTER")

    def run(self) -> None:
        """Starts the monitoring loop. Blocks until interrupted."""
        self.logger.info("Master orchestrator started. Monitoring worker heartbeats.")

        while True:
            self._check_workers()
            time.sleep(POLL_INTERVAL)

    def _check_workers(self) -> None:
        """Single health-check cycle across all active workers."""
        active_workers = self.broker.get_all_processing_workers()

        for worker_id in active_workers:
            if not self.broker.is_worker_alive(worker_id):
                self.logger.warning(
                    "Worker is unresponsive, starting recovery.",
                    extra={"worker_id": worker_id},
                )
                recovered = self.broker.requeue_orphaned_tasks(worker_id)
                self.logger.info(
                    "Recovery complete.",
                    extra={"worker_id": worker_id, "tasks_recovered": recovered},
                )


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

if __name__ == "__main__":
    master = MasterOrchestrator()

    try:
        master.run()
    except KeyboardInterrupt:
        master.logger.info("Master orchestrator stopped.")