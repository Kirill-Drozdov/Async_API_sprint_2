from redis.asyncio import Redis

from tests.functional.settings import test_settings
from tests.functional.utils.helpers import ping_remote_service_host

if __name__ == '__main__':
    ping_remote_service_host(
        service=Redis(
            host=test_settings.redis_host,
            port=test_settings.redis_port,
        ),
        service_name='Redis',
    )
