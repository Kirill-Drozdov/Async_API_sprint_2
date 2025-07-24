import time

from redis.asyncio import Redis

from tests.functional.settings import test_settings

if __name__ == '__main__':
    redis = Redis(
        host=test_settings.redis_host,
        port=test_settings.redis_port,
    )
    while True:
        print('Check Redis connection...')
        if redis.ping():
            break
        time.sleep(1)
