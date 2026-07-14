import redis

from .config import REDIS_URL

_pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=_pool)
