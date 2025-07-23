"""Модуль с фикстурами тестов."""
import asyncio

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import pytest

from tests.functional.settings import test_settings


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(name='es_client', scope='session')
async def es_client():
    async with AsyncElasticsearch(
        hosts=test_settings.es_host,
        verify_certs=False,
    ) as es_client:
        yield es_client


@pytest.fixture(name='es_write_data')
def es_write_data(es_client: AsyncElasticsearch):
    """Фикстура для загрузки данных в ElasticSearch."""
    async def inner(data: list[dict]):
        if await es_client.indices.exists(index=test_settings.es_index):
            await es_client.indices.delete(index=test_settings.es_index)
        await es_client.indices.create(
            index=test_settings.es_index,
            **test_settings.es_index_mapping,
        )

        _, errors = await async_bulk(
            client=es_client,
            actions=data,
        )

        await es_client.indices.refresh(index=test_settings.es_index)

        if errors:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner
