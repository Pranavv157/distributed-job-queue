import os
import redis

redis_client = redis.Redis.from_url(
    os.getenv("CELERY_BROKER_URL")
)