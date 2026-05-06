import time
from .redis_client import redis_client

def acquire_lock(key, timeout=10):
    return redis_client.set(key, "locked", nx=True, ex=timeout)


def release_lock(key):
    redis_client.delete(key)