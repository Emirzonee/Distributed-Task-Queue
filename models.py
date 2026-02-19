import json
import logging
from dataclasses import dataclass, asdict, field
from typing import Any, Dict
from uuid import uuid4


class JSONFormatter(logging.Formatter):
    """Formats log records as structured JSON for easy parsing and filtering."""

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        })


def setup_logger(name: str) -> logging.Logger:
    """
    Creates and returns a named logger with JSON output.

    Args:
        name: Identifier shown in the 'logger' field of each log entry.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)

    return logger


@dataclass
class Task:
    """
    Represents a unit of work passed through the queue.

    Attributes:
        payload: Arbitrary dict containing task-specific data.
        task_id: Unique identifier, auto-generated if not provided.
        status:  Current state of the task (pending, processing, done).
    """

    payload: Dict[str, Any]
    task_id: str = field(default_factory=lambda: str(uuid4()))
    status: str = "pending"

    def to_json(self) -> str:
        """Serializes the task to a JSON string for Redis storage."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "Task":
        """
        Deserializes a JSON string back into a Task instance.

        Args:
            data: JSON string previously produced by to_json().

        Returns:
            Reconstructed Task object.
        """
        return cls(**json.loads(data))