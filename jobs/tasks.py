from celery import shared_task
from django.db import transaction
from django.db.models import F
from django.utils import timezone

from datetime import timedelta
import random
import time
import logging

from config.lock import acquire_lock, release_lock
from config.worker_identity import WORKER_ID

from jobs.models import Job
from .services import move_to_dlq
from .lease import assert_lease_owner
from .exceptions import (
    PermanentError,
    LeaseLostException,
)
from .publisher import publish
from jobs.models import OutboxEvent

logger = logging.getLogger(__name__)


@shared_task
def execute_job(job_id):

    lock_key = f"job-lock:{job_id}"

    # distributed lock
    # prevents duplicate execution across workers
    if not acquire_lock(lock_key, timeout=600):
        logger.warning(f"Duplicate execution prevented for {job_id}")
        return {"status": "duplicate_prevented"}

    try:

        # transition QUEUED -> RUNNING
        with transaction.atomic():

            job = Job.objects.select_for_update().get(id=job_id)

            # idempotency safeguard
            if job.status == Job.Status.SUCCESS:
                return job.result

            # already being processed
            if job.status == Job.Status.RUNNING:
                return {"status": "already_running"}

            now = timezone.now()

            job.status = Job.Status.RUNNING
            job.lease_owner = WORKER_ID
            job.lease_expires_at = now + timedelta(minutes=3)
            job.started_at = now
            job.last_heartbeat_at = now

            job.save(
                update_fields=[
                    "status",
                    "lease_owner",
                    "lease_expires_at",
                    "started_at",
                    "last_heartbeat_at",
                ]
            )

        logger.info(f"Started processing job {job_id}")

        # simulate long-running task + heartbeat

        for _ in range(30):

            time.sleep(2)

            assert_lease_owner(
                job_id,
                WORKER_ID
            )

            updated = Job.objects.filter(
                id=job_id,
                lease_owner=WORKER_ID
            ).update(
                last_heartbeat_at=timezone.now(),
                lease_expires_at=timezone.now() + timedelta(minutes=1)
            )

            if not updated:
                raise LeaseLostException()

            logger.info(f"Heartbeat updated for {job_id}")

        # business logic

        assert_lease_owner(
            job_id,
            WORKER_ID
        )

        job.refresh_from_db()

        number = job.payload.get("number", 0)

        # permanent failure example
        if number == 2:
            raise PermanentError("invalid payload")

        # temporary failure example
        if number == 1:
            raise Exception("temporary API timeout")

        result = number * 2

        # transition RUNNING -> SUCCESS

        with transaction.atomic():

            assert_lease_owner(
                job_id,
                WORKER_ID
            )

            Job.objects.filter(
                id=job_id
            ).update(
                status=Job.Status.SUCCESS,
                result={"result": result},
                retries=0,
                error=None,
                started_at=None,
                last_heartbeat_at=None,
                lease_owner=None,
                lease_expires_at=None,
            )

            OutboxEvent.objects.create(
                aggregate_id=str(job.id),
                event_type="job_completed",
                payload={
                    "job_id":str(job.id),
                    "result":result
                }

            )
        logger.info(
            f"Job {job_id} completed successfully"
        )

        return result

    except LeaseLostException as e:

        logger.error(
            f"Lease lost for job {job_id}: {e}"
        )

        return {"status": "lease_lost"}

    except PermanentError as e:

        logger.error(
            f"Permanent failure for job {job_id}: {e}"
        )

        assert_lease_owner(
            job_id,
            WORKER_ID
        )

        job = Job.objects.get(id=job_id)

        # transition RUNNING -> FAILED

        job.status = Job.Status.FAILED
        job.error = str(e)
        job.result = None
        job.started_at = None
        job.last_heartbeat_at = None
        job.lease_owner = None
        job.lease_expires_at = None

        job.save(
            update_fields=[
                "status",
                "error",
                "result",
                "started_at",
                "last_heartbeat_at",
                "lease_owner",
                "lease_expires_at",
            ]
        )

        # move once to DLQ

        move_to_dlq(
            job,
            str(e)
        )

        return {"status": "failed_permanently"}

    except Exception as e:

        logger.warning(
            f"Temporary failure for job {job_id}: {e}"
        )

        # atomic retry increment

        Job.objects.filter(
            id=job_id
        ).update(
            retries=F("retries") + 1,
            error=str(e),
        )

        job = Job.objects.get(id=job_id)

        # retries exhausted -> DLQ
        # transition RUNNING -> FAILED

        if job.retries >= job.max_retries:

            assert_lease_owner(
                job_id,
                WORKER_ID
            )

            job.status = Job.Status.FAILED
            job.result = None
            job.started_at = None
            job.last_heartbeat_at = None
            job.lease_owner = None
            job.lease_expires_at = None

            job.save(
                update_fields=[
                    "status",
                    "result",
                    "started_at",
                    "last_heartbeat_at",
                    "lease_owner",
                    "lease_expires_at",
                ]
            )

            logger.error(
                f"Job {job.id} exhausted retries. Moving to DLQ."
            )

            move_to_dlq(
                job,
                str(e)
            )

            return {"status": "moved_to_dlq"}

        # transition RUNNING -> QUEUED
        # schedule retry with exponential backoff + jitter

        assert_lease_owner(
            job_id,
            WORKER_ID
        )

        retry_delay = (
            2 ** job.retries
        ) + random.randint(1, 5)

        job.status = Job.Status.QUEUED
        job.error = None
        job.started_at = None
        job.last_heartbeat_at = None
        job.lease_owner = None
        job.lease_expires_at = None

        job.save(
            update_fields=[
                "status",
                "error",
                "started_at",
                "last_heartbeat_at",
                "lease_owner",
                "lease_expires_at",
            ]
        )

        logger.warning(
            f"Retrying job {job.id}. "
            f"Attempt={job.retries}, "
            f"Delay={retry_delay}s"
        )

        execute_job.apply_async(
            args=[str(job.id)],
            countdown=retry_delay
        )

        return {"status": "retry_scheduled"}

    finally:

        # always release lock
        release_lock(lock_key)


@shared_task
def recover_stale_jobs():

    expired_jobs = list(
        Job.objects.filter(
            status=Job.Status.RUNNING,
            lease_expires_at__lt=timezone.now()
        )
    )

    logger.warning(
        f"Found {len(expired_jobs)} expired jobs"
    )

    for job in expired_jobs:

        # transition RUNNING -> QUEUED

        updated = Job.objects.filter(
            id=job.id,
            status=Job.Status.RUNNING
        ).update(
            status=Job.Status.QUEUED,
            started_at=None,
            last_heartbeat_at=None,
            lease_owner=None,
            lease_expires_at=None,
        )

        if updated:

            logger.warning(
                f"Recovered expired job {job.id}"
            )

            execute_job.delay(
                str(job.id)
            )

@shared_task
def process_job_completed_event(payload):

    logger.info(
        f"Processing event: {payload}"
    )


@shared_task
def publish_outbox_events():

    with transaction.atomic():

        events = list(
            OutboxEvent.objects
            .select_for_update(skip_locked=True)
            .filter(published=False)[:100]
        )

    for event in events:

        try:

            publish(event)

            OutboxEvent.objects.filter(
                id=event.id,
                published=False
            ).update(
                published=True,
                published_at=timezone.now()
            )

        except Exception as e:

            logger.exception(
                f"Failed publishing event {event.id}: {e}"
            )