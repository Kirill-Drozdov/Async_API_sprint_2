"""Модуль с фикстурами тестов."""
import asyncio
from typing import Callable

import aiohttp
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import pytest

from tests.functional.settings import test_settings

BASE_API_V1_URL: str = '/api/v1'

MAX_FILMS_DATA_SIZE: int = 60


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(name='es_client', scope='session')
async def es_client():
    """Фикстура для предоставления клиента ES."""
    async with AsyncElasticsearch(
        hosts=test_settings.es_host,
        verify_certs=False,
    ) as es_client:
        yield es_client


@pytest.fixture(name='aiohttp_session', scope='session')
async def aiohttp_session():
    """Фикстура для предоставления клиентской сессии."""
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture(name='es_delete_index')
def es_delete_index(es_client: AsyncElasticsearch) -> Callable:
    """Фикстура для для удаления индекса из ElasticSearch."""
    async def inner(index: str):
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)
    return inner


@pytest.fixture(name='es_write_data')
def es_write_data(es_client: AsyncElasticsearch) -> Callable:
    """Фикстура для загрузки данных в ElasticSearch."""
    async def inner(data: list[dict], index: str, index_mapping: dict):
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)
        await es_client.indices.create(
            index=index,
            **index_mapping,
        )
        _, errors = await async_bulk(
            client=es_client,
            actions=data,
        )
        await es_client.indices.refresh(index=index)

        if errors:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner


@pytest.fixture(name='make_get_request')
def make_get_request(aiohttp_session: aiohttp.ClientSession) -> Callable:
    """Фикстура для выполнения запроса к API."""
    async def inner(url: str, query_data: dict[str, str]):
        async with aiohttp_session.get(url, params=query_data) as response:
            body = await response.json()
            status = response.status
            return body, status
    return inner
