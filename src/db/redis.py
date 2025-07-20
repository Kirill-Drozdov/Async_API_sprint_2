from redis.asyncio import Redis

redis: Redis | None = None


async def get_redis() -> Redis:
    """Функция-провайдер соединения с Redis.

    Returns:
        Соединение с Redis.
    """
    return redis
