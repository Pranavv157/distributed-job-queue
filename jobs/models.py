
from django.db import models
import uuid
# Create your models here.

class Job(models.Model):

    class Status(models.TextChoices):
        PENDING = "PENDING"
        QUEUED = "QUEUED"
        RUNNING = "RUNNING"
        SUCCESS = "SUCCESS"
        FAILED = "FAILED"
    
    id = models.UUIDField(primary_key=True,default=uuid.uuid4,editable=False)
    name=models.CharField(max_length=255)
    payload = models.JSONField()
    status=models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True #because of frequent query for status
    )

    retries = models.IntegerField(default=0)
    max_retries=models.IntegerField(default=3)

    result=models.JSONField(null=True,blank=True)
    error = models.TextField(null=True, blank=True)

    scheduled_at = models.DateField(null=True,blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class DeadLetterJob(models.Model):

    original_job_id = models.UUIDField()

    name = models.CharField(max_length=255)

    payload = models.JSONField()

    error = models.TextField()
    last_retry_at = models.DateTimeField(null=True, blank=True)
    failed_at = models.DateTimeField(auto_now_add=True)








