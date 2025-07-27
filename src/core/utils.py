import asyncio
from functools import wraps
import logging


def async_backoff(
    start_sleep_time: float = 1.0,
    factor: int = 2,
    border_sleep_time: int = 15,
    max_retries: int = 15,
):
    """Декоратор для повторного выполнения функции через некоторое время,
        если возникла ошибка.

    Использует экспоненциальный рост времени повтора (factor) до граничного
        времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе

    Args:
        start_sleep_time (int, optional): начальное время ожидания.
        factor (int, optional): во сколько раз нужно увеличивать время
            ожидания.
        border_sleep_time (int, optional): максимальное время ожидания.
        max_retries (int, optional): максимальное число попыток.

    Returns:
        Результат выполнения функции.
    """
    _logger = logging.getLogger(__name__)

    def func_wrapper(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            attempt = 1

            while True:
                retries_left = max_retries - attempt
                try:
                    return await func(*args, **kwargs)
                except Exception as error:
                    if retries_left <= 0:
                        _logger.warning(
                            f'Превышено максимальное число попыток'
                            f' ({max_retries}) выполнения функции'
                            f' "{func.__name__}".',
                        )
                        raise error
                    _logger.error(
                        f'Ошибка при выполнении: {error}. Повторная попытка'
                        f' через {sleep_time} сек. '
                        f'Осталось попыток: {retries_left}',
                    )

                    # Ожидаем перед следующей попыткой.
                    await asyncio.sleep(sleep_time)

                    # Рассчитываем время следующего ожидания.
                    next_sleep = start_sleep_time * (factor ** attempt)
                    if next_sleep > border_sleep_time:
                        sleep_time = border_sleep_time
                    else:
                        sleep_time = next_sleep

                    attempt += 1
        return inner
    return func_wrapper
