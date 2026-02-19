# Distributed Task Queue with Fault Tolerance

## Project Overview

A distributed task queue system built on top of Redis, using the Reliable Queue pattern to guarantee that no task is silently dropped — even when a worker process crashes mid-execution. Workers run asynchronously and a master orchestrator monitors their health via TTL-based heartbeat keys in Redis. Built as a lightweight, Redis-only alternative to heavier solutions like Celery.

---

## Why I Built This

Most task queue tutorials show you how to push a job into a list and pop it on the other side. None of them explain what happens to the job if the worker dies while processing it. I built this to answer that question concretely: the task should never disappear. It should go back to the queue and get picked up again automatically, with no manual intervention.

---

## Core Tech Stack

- Python 3.10+
- Redis via [Upstash](https://upstash.com)
- `redis-py` — Redis client
- `python-dotenv` — environment variable management
- `asyncio` — concurrent worker execution

---

## Key Engineering Decisions

**Reliable Queue over simple LPUSH/RPOP**

A naive queue deletes the task the moment a worker picks it up. If the worker crashes during processing, that task is gone. To fix this, I used Redis's `BLMOVE` command to atomically move the task from the shared `queue:pending` list into a worker-specific `queue:processing:<worker_id>` list in a single operation. The task stays there until `complete_task()` explicitly removes it with `LREM`. This means the master can always inspect what each worker is holding, and recover it if needed.

**TTL-based heartbeat instead of persistent connections**

Each worker runs a background coroutine that writes a Redis key with a 5-second TTL every 2 seconds. The master polls these keys every 3 seconds. If a worker process is killed, it stops refreshing the key and Redis expires it automatically. The master then moves everything from that worker's processing queue back to `queue:pending` via `RPOPLPUSH` — no manual cleanup needed.

**Async workers with a synchronous broker**

I kept `RedisBroker` synchronous because `redis-py`'s blocking commands release the GIL during the wait. Running the heartbeat loop and the task processor as two parallel coroutines via `asyncio.gather()` was enough without pulling in `aioredis` as an extra dependency.

---

## Challenges & Lessons

**`BLMOVE` timeout was blocking the entire event loop**

The first version used `timeout=0` on `BLMOVE`, which blocks indefinitely. Because this is a synchronous call inside an async function, it froze the event loop completely — the heartbeat coroutine stopped running alongside it, meaning the worker appeared dead to the master even while it was running fine. Switching to `timeout=1` lets the worker yield back to the event loop every second, so both coroutines run concurrently. This was the first real bug I hit.

**Ctrl+C is not a crash**

When testing fault tolerance, killing the worker with Ctrl+C triggers Python's normal shutdown, which gives the process time to clean up. The heartbeat key gets implicitly abandoned but the test isn't realistic — the master may not catch it in time. Forcefully killing the process (via `taskkill /F` on Windows) simulates a real crash and consistently triggers the recovery flow in the master within one poll cycle (3 seconds).

**Structured logging paid off immediately**

I added `JSONFormatter` from the start. When running master, worker, and client across three terminals simultaneously, being able to read clean JSON lines and filter by `"logger"` field made it easy to follow what each component was doing without the output becoming unreadable.

---

## How to Run

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Configure environment**
```bash
cp .env.example .env
# Open .env and paste your Upstash Redis URL
```

**3. Start the master orchestrator** — Terminal 1
```bash
python master.py
```

**4. Start one or more workers** — separate terminal per worker
```bash
python worker.py W1
python worker.py W2
```

**5. Send tasks**
```bash
python client.py --count 5
```

**Fault tolerance test:** Send tasks, then forcefully kill a worker terminal while it is processing. The master will log a recovery event within 3 seconds and requeue the orphaned tasks. Start a new worker to see it pick them up.

---

## Project Structure

```
.
├── broker.py        # Redis connection, queue operations, heartbeat management
├── worker.py        # Async worker: task processing + heartbeat loop
├── master.py        # Orchestrator: worker health monitoring + redelivery
├── client.py        # Task producer (supports --count argument)
├── models.py        # Task dataclass, JSON serialization, logger setup
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## License

MIT — Emircan Bingöl, 2026