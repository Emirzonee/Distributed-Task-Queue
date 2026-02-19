import time
import argparse
import logging

from broker import RedisBroker
from models import Task, setup_logger

DEFAULT_TASK_COUNT = 5
SEND_INTERVAL = 0.5  # seconds between task submissions


def build_payload(index: int) -> dict:
    """
    Constructs a sample task payload.

    Replace this function with whatever data your workers actually need.

    Args:
        index: Sequential task number used to differentiate payloads.

    Returns:
        Dict representing the task's input data.
    """
    return {
        "operation": "data_analysis",
        "data_id": 100 + index,
        "priority": "high",
    }


def send_tasks(count: int = DEFAULT_TASK_COUNT) -> None:
    """
    Sends `count` tasks to the pending queue.

    Args:
        count: Number of tasks to enqueue.
    """
    broker = RedisBroker()
    logger: logging.Logger = setup_logger("CLIENT")

    logger.info(f"Sending {count} tasks to the queue.")

    for i in range(count):
        task = Task(payload=build_payload(i))
        broker.push_task(task)
        time.sleep(SEND_INTERVAL)

    logger.info("All tasks submitted successfully.")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit tasks to the distributed queue.")
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_TASK_COUNT,
        help=f"Number of tasks to send (default: {DEFAULT_TASK_COUNT})",
    )
    args = parser.parse_args()

    send_tasks(args.count)