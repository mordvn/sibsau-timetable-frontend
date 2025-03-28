from config import settings
import redis.asyncio as redis
import functools
import pickle
import hashlib
from typing import Any, Callable, TypeVar
from logger import logger
from profiler import profile

T = TypeVar("T")


class Cacher:
    _redis_client = None

    @classmethod
    async def get_redis_client(cls):
        if cls._redis_client is None:
            cls._redis_client = redis.from_url(settings.REDIS_URI)
        return cls._redis_client

    @classmethod
    def cache(cls, expire: int):
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                redis_client = await cls.get_redis_client()

                key = cls._generate_cache_key(func, *args, **kwargs)

                cached_result = await redis_client.get(key)
                if cached_result:
                    logger.debug(f"Cache hit for {key}")
                    try:
                        return pickle.loads(cached_result)
                    except Exception:
                        pass

                logger.debug(f"Cache miss for {key}")
                result = await func(*args, **kwargs)

                if result is not None:
                    try:
                        await redis_client.set(key, pickle.dumps(result), ex=expire)
                        logger.debug(f"Cache set for {key}")
                    except Exception:
                        pass

                return result

            return wrapper

        return decorator

    @staticmethod
    @profile(func_name="cacher_generate_cache_key")
    def _generate_cache_key(func: Callable[..., Any], *args, **kwargs) -> str:
        key_parts = [func.__module__, func.__qualname__]

        for arg in args:
            key_parts.append(repr(arg))

        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{repr(v)}")

        key = ":".join(key_parts)
        return f"cache:{hashlib.md5(key.encode()).hexdigest()}"

    @classmethod
    @profile(func_name="cacher_delete_cache_by_pattern")
    async def delete_cache_by_pattern(cls, pattern: str) -> int:
        redis_client = await cls.get_redis_client()
        keys = []

        async for key in redis_client.scan_iter(f"cache:{pattern}*"):
            keys.append(key)

        if keys:
            return await redis_client.delete(*keys)
        return 0

    @classmethod
    @profile(func_name="cacher_clear_all_cache")
    async def clear_all_cache(cls) -> int:
        redis_client = await cls.get_redis_client()
        keys = []

        async for key in redis_client.scan_iter("cache:*"):
            keys.append(key)

        if keys:
            return await redis_client.delete(*keys)
        return 0
