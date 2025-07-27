import contextlib
import logging

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import film, genre
from core.config import settings
from db import elastic, redis


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Асинхронный контекстный менеджер для управления событиями запуска
        и завершения работы приложения.

    Args:
        app: Экземпляр приложения FastAPI.
    """
    _logger = logging.getLogger(__name__)

    # STARTUP: Подключаемся к базам данных.
    _logger.info('Начало подключения к серверу Redis.')
    redis.redis = Redis(host=settings.redis_host, port=settings.redis_port)
    _logger.info('Успешное подключение к серверу Redis.')

    _logger.info('Начало подключения к серверу ES.')
    elastic.es = AsyncElasticsearch(
        hosts=[
            f'{settings.elastic_schema}{settings.elastic_host}'
            f':{settings.elastic_port}',
        ],
    )
    _logger.info('Успешное подключение к серверу ES.')

    # Передаем управление приложению.
    yield

    # SHUTDOWN: Корректно закрываем соединения.
    if redis.redis is not None:
        await redis.redis.close()
    if elastic.es is not None:
        await elastic.es.close()


def get_app() -> FastAPI:
    """Производит инициализацию приложения.

    Returns:
        Объект приложения FastAPI.
    """

    app = FastAPI(
        title=settings.project_name,
        docs_url='/api/openapi',
        openapi_url='/api/openapi.json',
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
    )

    # Подключение роутеров.
    app.include_router(film.router, prefix='/api/v1/films', tags=['Films'])
    app.include_router(genre.router, prefix='/api/v1/genres', tags=['Genres'])

    return app


app = get_app()
