import json
import logging
from dataclasses import dataclass, asdict
from typing import Any, Dict
from uuid import uuid4

# Profesyonel JSON Loglama Ayarı
class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt)
        }
        return json.dumps(log_record)

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
    return logger

@dataclass
class Task:
    payload: Dict[str, Any]
    task_id: str = ""
    status: str = "pending"

    def __post_init__(self):
        if not self.task_id:
            self.task_id = str(uuid4())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> 'Task':
        return cls(**json.loads(data))