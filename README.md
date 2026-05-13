# Distributed Job Queue System

A production-oriented distributed job processing system built using:

* Django
* Celery
* Redis
* PostgreSQL

This project is focused on learning real distributed systems engineering concepts instead of only implementing the happy path.

The goal is to understand:

* asynchronous job processing
* distributed locking
* retries and backoff
* dead letter queues (DLQ)
* idempotency
* worker crash recovery
* stale job detection
* delivery guarantees
* race conditions
* fault tolerance

---

# Current Architecture

```text
API Request
    ↓
Django Service Layer
    ↓
PostgreSQL (source of truth)
    ↓
Redis Queue
    ↓
Celery Workers
    ↓
Redis Distributed Lock
    ↓
Job Execution
    ↓
SUCCESS / FAILED / DLQ
```

---

# Features Implemented

## 1. Persistent Job Tracking

All jobs are stored in PostgreSQL.

The database acts as the source of truth instead of relying only on the queue.

Each job tracks:

* status
* retries
* payload
* result
* errors
* timestamps

### Job Lifecycle

```text
PENDING
   ↓
QUEUED
   ↓
RUNNING
   ↓
SUCCESS / FAILED
```

---

## 2. Asynchronous Processing Using Celery

Jobs are processed asynchronously using Celery workers.

This allows:

* background execution
* non-blocking APIs
* horizontal worker scaling
* retry support

---

## 3. Distributed Locking

Redis-based distributed locks prevent duplicate execution.

### Why this matters

In distributed systems, duplicate task execution can happen naturally due to:

* worker crashes
* message redelivery
* retries
* network failures

The distributed lock ensures only one worker processes a job at a time.

---

## 4. Row-Level Database Locking

`select_for_update()` is used inside database transactions.

This prevents race conditions where multiple workers attempt to update the same job simultaneously.

---

## 5. Retry Mechanism

Celery retries failed jobs automatically using:

* exponential backoff
* retry jitter

### Why this matters

Retries help recover from temporary failures such as:

* database outages
* network timeouts
* external API failures

Backoff and jitter prevent retry storms and thundering herd problems.

---

## 6. Idempotency Safeguards

The system prevents duplicate execution by checking:

```python
if job.status == Job.Status.SUCCESS:
    return job.result
```

### Why this matters

Distributed systems usually provide at-least-once delivery.

This means duplicate task delivery is possible.

Idempotency ensures repeated execution does not corrupt data.

---

## 7. Dead Letter Queue (DLQ)

Jobs that fail after exhausting retries are moved into a Dead Letter Queue.

The DLQ stores:

* original job ID
* payload
* error details
* failure timestamps

### Why this matters

Without a DLQ:

* permanently failing jobs disappear silently
* debugging becomes difficult
* failed jobs cannot be inspected or replayed

---

## 8. Worker Crash Recovery

The system can recover jobs stuck in `RUNNING` state.

### Problem

If a worker crashes while processing:

```text
RUNNING forever
```

The job becomes a zombie job.

### Solution

A periodic recovery task scans for stale jobs:

```text
RUNNING + old started_at timestamp
```

and safely requeues them.

### Why this matters

This simulates real production failure recovery.

---

# Database Models

## Job Model

Tracks:

* payload
* retries
* result
* execution status
* timestamps

## DeadLetterJob Model

Stores permanently failed jobs for inspection and replay.

---

# Reliability Concepts Learned

## At-Least-Once Delivery

The system guarantees:

```text
A job may execute more than once,
but should not be silently lost.
```

---

## Idempotency

Operations are designed to safely handle duplicate execution.

---

## Distributed Locking

Prevents concurrent execution across workers.

---

## Race Conditions

Handled using:

* Redis locks
* database row locks
* transactions

---

## Fault Tolerance

The system can recover from:

* worker crashes
* duplicate deliveries
* temporary failures
* poison pill jobs

---

# Failure Scenarios Tested

## Worker Crash During Execution

### Simulation

* worker killed during `RUNNING`
* stale job recovery task detects zombie job
* job requeued automatically

### Result

System successfully self-heals.

---

## Duplicate Execution Protection

### Simulation

* same task queued multiple times

### Result

Redis lock prevents duplicate processing.

---

## Permanent Failures

### Simulation

* forced exceptions
* retries exhausted

### Result

Job moved into DLQ.

---

# Tech Stack

| Component  | Purpose                    |
| ---------- | -------------------------- |
| Django     | Application framework      |
| Celery     | Distributed task queue     |
| Redis      | Broker + distributed locks |
| PostgreSQL | Durable source of truth    |

---

# Current Project Structure

```text
jobs/
├── models.py
├── services.py
├── tasks.py
├── admin.py
├── views.py
└── migrations/
```

---

# Upcoming Improvements

The project will gradually evolve toward a more production-grade architecture.

## Planned Enhancements

### Heartbeat-Based Worker Monitoring

Workers will periodically update heartbeat timestamps.

This avoids false stale-job recovery for genuinely long-running jobs.

---

### Transactional Outbox Pattern

Will solve dual-write consistency issues between:

* database writes
* queue publishing

---

### Smarter Retry Policies

Different retry strategies for:

* transient errors
* permanent failures

---

### Observability and Metrics

Planned metrics:

* queue depth
* retry count
* success rate
* DLQ size
* processing latency

---

### Structured Logging

Production-grade searchable logs with correlation IDs.

---

### Replayable DLQ

Ability to replay failed jobs safely from the DLQ.

---

### Priority Queues

Separate queues for:

* high priority jobs
* normal jobs
* low priority jobs

---

# Key Distributed Systems Concepts Covered

This project focuses heavily on real production engineering concepts:

* distributed systems
* eventual consistency
* retries and backoff
* duplicate delivery
* at-least-once processing
* stale job recovery
* fault tolerance
* concurrency control
* recovery workflows
* DLQ architecture
* idempotency
* worker crash handling

---

# Running the Project

## Start Redis

```bash
redis-server
```

---

## Start Django

```bash
python manage.py runserver
```

---

## Start Celery Worker

```bash
celery -A config worker -l info
```

---

## Start Celery Beat

```bash
celery -A config beat -l info
```

---

# Example Job Creation

```python
from jobs.services import create_job

create_job(
    name="double-number",
    payload={"number": 10}
)
```

---

# Educational Goal

This project is intentionally designed as a learning-oriented distributed systems backend.

Instead of focusing only on frameworks and APIs, the project emphasizes:

* failure handling
* recovery mechanisms
* reliability engineering
* production system thinking

The objective is to understand how real backend systems behave under failure conditions.

---

# Current Status

## Implemented

* job persistence
* Celery workers
* retries
* DLQ
* distributed locks
* row-level locking
* stale job recovery
* idempotency safeguards

## In Progress

* heartbeat system
* advanced recovery
* observability
* replayable DLQ
* transactional outbox

---

# Learning Outcome

By building this project incrementally, the goal is to gain practical understanding of:

* production backend systems
* distributed job processing
* system reliability
* failure recovery
* concurrency control
* distributed coordination

instead of relying on copy-paste implementations.
