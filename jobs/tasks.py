from celery import shared_task
from django.db import transaction
from django.db.models import F
from jobs.models import Job


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def execute_job(self, job_id):

    try:
        #  LOCK ROW (CRITICAL)
        with transaction.atomic():
            job = Job.objects.select_for_update().get(id=job_id)

            # idempotency + concurrency safety
            if job.status == Job.Status.SUCCESS:
                return job.result

            if job.status == Job.Status.RUNNING:
                return {"status": "processing"}

            job.status = Job.Status.RUNNING
            job.save(update_fields=["status"])

        #  ACTUAL WORK
        number = job.payload.get("number", 0)
        result = number * 2

        #  SUCCESS UPDATE
        Job.objects.filter(id=job_id).update(
            status=Job.Status.SUCCESS,
            result={"result": result}
        )

        return result

    except Exception as e:

        #  atomic increment (no race)
        Job.objects.filter(id=job_id).update(
            retries=F("retries") + 1,
            error=str(e)
        )

        # mark failed if exceeded
        job = Job.objects.get(id=job_id)

        if job.retries >= job.max_retries:
            job.status = Job.Status.FAILED
            job.save(update_fields=["status"])

        raise e