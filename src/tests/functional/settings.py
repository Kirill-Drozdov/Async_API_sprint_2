"""Пакет функциональными тестами проекта."""

from pydantic_settings import BaseSettings

from tests.functional.testdata.es_mapping import es_index_mapping


class TestSettings(BaseSettings):
    es_host: str = 'http://elasticsearch_test:9200'
    es_index: str = 'movies'
    es_id_field: str = ''
    es_index_mapping: dict = es_index_mapping
    redis_host: str = 'redis_test'
    redis_port: int = 6379
    service_url: str = 'http://api_test:8000'


test_settings = TestSettings()
