from django.utils import timezone

from jobs.models import Job
from jobs.exceptions import LeaseLostException


def assert_lease_owner(job_id, worker_id):

    job = Job.objects.only(
        "lease_owner",
        "lease_expires_at"
    ).get(id=job_id)

    if job.lease_owner != worker_id:
        raise LeaseLostException(
            f"Lease transferred to {job.lease_owner}"
        )

    if (
        job.lease_expires_at
        and job.lease_expires_at < timezone.now()
    ):
        raise LeaseLostException(
            "Lease expired"
        )