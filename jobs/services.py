from .models import Job , DeadLetterJob


def create_job(name, payload):
    job = Job.objects.create(
        name=name,
        payload=payload,
        status=Job.Status.PENDING
    )

    enqueue_job(job)   

    return job


def enqueue_job(job: Job):

    from .tasks import execute_job
    job.status = Job.Status.QUEUED
    job.save(update_fields=["status"])

    execute_job.delay(job.id)   

def move_to_dlq(job,error_message):

    DeadLetterJob.objects.create(
        original_job_id=job.id,
        name=job.name,
        payload=job.payload,
        error=error_message
    )