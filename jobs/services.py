from .models import Job
from .tasks import execute_job


def create_job(name, payload):
    job = Job.objects.create(
        name=name,
        payload=payload,
        status=Job.Status.PENDING
    )

    enqueue_job(job)   

    return job


def enqueue_job(job: Job):
    job.status = Job.Status.QUEUED
    job.save(update_fields=["status"])

    execute_job.delay(job.id)   
