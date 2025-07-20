"""Пакет функциональными тестами проекта."""

from pydantic_settings import BaseSettings


class TestSettings(BaseSettings):
    es_host: str = 'http://127.0.0.1:9200'
    es_index: str = 'movies'
    es_id_field: str = ''
    es_index_mapping: dict = {}

    redis_host: str = '127.0.0.1'
    redis_port: int = 6379
    service_url: str = 'http://127.0.0.1:8000'


test_settings = TestSettings()
