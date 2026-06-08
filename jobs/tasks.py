# tasks.py

from celery import shared_task
from django.db import transaction
from django.db.models import F
from django.utils import timezone

from datetime import timedelta
import random
import time
import logging

from config.lock import acquire_lock, release_lock
from jobs.models import Job
from .services import move_to_dlq
from .exceptions import PermanentError

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
            job.started_at = now
            job.last_heartbeat_at = now

            job.save(
                update_fields=[
                    "status",
                    "started_at",
                    "last_heartbeat_at",
                ]
            )

        logger.info(f"Started processing job {job_id}")

        
        # simulate long-running task + heartbeat
         

        for _ in range(30):

            time.sleep(2)

            Job.objects.filter(id=job_id).update(
                last_heartbeat_at=timezone.now()
            )

            logger.info(f"Heartbeat updated for {job_id}")

         
        # business logic
         

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

            Job.objects.filter(id=job_id).update(
                status=Job.Status.SUCCESS,
                result={"result": result},
                retries=0,
                error=None,
                started_at=None,
                last_heartbeat_at=None,
            )

        logger.info(f"Job {job_id} completed successfully")

        return result

    except PermanentError as e:

        logger.error(
            f"Permanent failure for job {job_id}: {e}"
        )

        job = Job.objects.get(id=job_id)

        # transition RUNNING -> FAILED

        job.status = Job.Status.FAILED
        job.error = str(e)
        job.result = None
        job.started_at = None
        job.last_heartbeat_at = None

        job.save(
            update_fields=[
                "status",
                "error",
                "result",
                "started_at",
                "last_heartbeat_at",
            ]
        )

        # move once to DLQ
        move_to_dlq(job, str(e))

        return {"status": "failed_permanently"}

    except Exception as e:

        logger.warning(
            f"Temporary failure for job {job_id}: {e}"
        )

        # atomic retry increment
        Job.objects.filter(id=job_id).update(
            retries=F("retries") + 1,
            error=str(e),
        )

        job = Job.objects.get(id=job_id)

         
        # retries exhausted -> DLQ
        # transition RUNNING -> FAILED
         

        if job.retries >= job.max_retries:

            job.status = Job.Status.FAILED
            job.result = None
            job.started_at = None
            job.last_heartbeat_at = None

            job.save(
                update_fields=[
                    "status",
                    "result",
                    "started_at",
                    "last_heartbeat_at",
                ]
            )

            logger.error(
                f"Job {job.id} exhausted retries. Moving to DLQ."
            )

            move_to_dlq(job, str(e))

            return {"status": "moved_to_dlq"}

        
        # transition RUNNING -> QUEUED
        # schedule retry with exponential backoff + jitter
    

        retry_delay = (
            2 ** job.retries
        ) + random.randint(1, 5)

        job.status = Job.Status.QUEUED
        job.error = None
        job.started_at = None
        job.last_heartbeat_at = None

        job.save(
            update_fields=[
                "status",
                "error",
                "started_at",
                "last_heartbeat_at",
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

    # jobs that stopped sending heartbeat
    timeout = timezone.now() - timedelta(minutes=1)

    stale_jobs = list(
        Job.objects.filter(
            status=Job.Status.RUNNING,
            last_heartbeat_at__lt=timeout
        )
    )

    logger.warning(
        f"Found {len(stale_jobs)} stale jobs"
    )

    for job in stale_jobs:

        # transition RUNNING -> QUEUED
        updated = Job.objects.filter(
            id=job.id,
            status=Job.Status.RUNNING
        ).update(
            status=Job.Status.QUEUED,
            started_at=None,
            last_heartbeat_at=None,
        )

        if updated:

            logger.warning(
                f"Recovered stale job {job.id}"
            )

            execute_job.delay(str(job.id))