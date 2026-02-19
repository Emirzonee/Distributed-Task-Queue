# Distributed Task Queue with Fault Tolerance

A high-performance, distributed task queue system built with Python and Redis. The system implements the "Reliable Queue" pattern to ensure zero task loss during worker failures. It features an automated monitoring orchestrator (Master) that detects worker health via heartbeats and handles task redelivery automatically.

## Key Features

* **Reliable Queue Architecture:** Utilizes the Redis `BLMOVE` pattern to ensure tasks are moved atomically between queues, preventing data loss if a process terminates unexpectedly.
* **Heartbeat Mechanism:** Distributed workers maintain an active state in Redis using TTL-based heartbeats for real-time health monitoring.
* **Automatic Fault Recovery:** The Master Orchestrator monitors worker heartbeats and automatically re-queues orphaned tasks from failed workers back to the main queue.
* **Asynchronous Execution:** Workers leverage `asyncio` for non-blocking task processing and concurrent heartbeat management.
* **Clean Code Standards:** Implementation follows strict Type Hinting, modular class structures, and professional JSON-formatted logging.
* **Security:** Environment variable support via `.env` for sensitive credential management.

## System Architecture



* **Broker:** Managed Redis (Upstash) serving as the centralized message bus.
* **Worker:** Asynchronous units that fetch and execute tasks while maintaining heartbeats.
* **Master (Orchestrator):** Monitoring service responsible for fault detection and task redelivery.
* **Client:** Task producer that serializes and pushes payloads to the broker.

## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/Emirzonee/Distributed-Task-Queue.git](https://github.com/Emirzonee/Distributed-Task-Queue.git)
    cd Distributed-Task-Queue
    ```

2.  **Install Dependencies:**
    ```bash
    pip install redis python-dotenv
    ```

3.  **Configure Environment:**
    Create a `.env` file in the root directory:
    ```env
    REDIS_URL="your_redis_connection_url"
    ```

## Running the System

1.  **Start the Master Orchestrator:**
    ```bash
    python master.py
    ```

2.  **Start one or more Workers:**
    ```bash
    python worker.py W1
    ```

3.  **Produce Tasks:**
    ```bash
    python client.py
    ```

## Fault Tolerance Test (Chaos Engineering)

To verify the system's reliability, terminate an active worker process (`Ctrl+C`) while it is processing a task. The Master Orchestrator will detect the heartbeat timeout within 5 seconds and automatically move the uncompleted task back to the pending queue for other workers.

## License
MIT License