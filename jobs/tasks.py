from celery import shared_task
from django.db import transaction
from django.db.models import F
from config.lock import acquire_lock, release_lock
from jobs.models import Job
from .services import move_to_dlq
import time


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def execute_job(self, job_id):

    lock_key = f"job-lock:{job_id}"

    # acquire distributed lock FIRST
    if not acquire_lock(lock_key, timeout=30):
        return {"status": "duplicate_prevented"}

    try:

        with transaction.atomic():

            # row-level lock
            job = Job.objects.select_for_update().get(id=job_id)

            # idempotency safeguard
            if job.status == Job.Status.SUCCESS:
                return job.result

            # already being processed
            if job.status == Job.Status.RUNNING:
                return {"status": "already_running"}

            # mark running
            job.status = Job.Status.RUNNING
            job.save(update_fields=["status"])

        # simulate long-running task
        time.sleep(10)

        # actual business logic
        number = job.payload.get("number", 0)
        result = number * 2

        # mark success
        Job.objects.filter(id=job_id).update(
            status=Job.Status.SUCCESS,
            result={"result": result},
        )

        return result

    except Exception as e:

        # atomic retry increment
        Job.objects.filter(id=job_id).update(
            retries=F("retries") + 1,
            error=str(e),
        )

        # fetch updated job
        job = Job.objects.get(id=job_id)

        # move to DLQ only AFTER max retries exhausted
        if job.retries >= job.max_retries:

            job.status = Job.Status.FAILED
            job.save(update_fields=["status"])

            move_to_dlq(job, str(e))

        raise e

    finally:
        # always release distributed lock
        release_lock(lock_key)