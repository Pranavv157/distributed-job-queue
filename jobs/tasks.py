from celery import shared_task
from django.db import transaction
from django.db.models import F
from config.lock import acquire_lock, release_lock
from jobs.models import Job
import time 


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def execute_job(self, job_id):

    lock_key = f"job-lock:{job_id}"

    time.sleep(10)

    #  DISTRIBUTED LOCK (CRITICAL)
    if not acquire_lock(lock_key, timeout=30):
        return {"status": "duplicate_prevented"}

    try:
        with transaction.atomic():
            job = Job.objects.select_for_update().get(id=job_id)

            # idempotency
            if job.status == Job.Status.SUCCESS:
                return job.result

            if job.status == Job.Status.RUNNING:
                return {"status": "already_running"}

            job.status = Job.Status.RUNNING
            job.save(update_fields=["status"])

        #  ACTUAL WORK
        number = job.payload.get("number", 0)
        result = number * 2

        Job.objects.filter(id=job_id).update(
            status=Job.Status.SUCCESS,
            result={"result": result}
        )

        return result

    except Exception as e:

        Job.objects.filter(id=job_id).update(
            retries=F("retries") + 1,
            error=str(e)
        )

        job = Job.objects.get(id=job_id)

        if job.retries >= job.max_retries:
            job.status = Job.Status.FAILED
            job.save(update_fields=["status"])

        raise e

    finally:
        #  always release
        release_lock(lock_key)