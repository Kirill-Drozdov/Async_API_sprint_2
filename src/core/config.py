from logging import config as logging_config
import os

from pydantic_settings import BaseSettings, SettingsConfigDict

from core.logger import LOGGING

# Применяем настройки логирования.
logging_config.dictConfig(LOGGING)

# Корень проекта.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Settings(BaseSettings):
    """Настройки проекта."""
    # Настройки Redis.
    redis_host: str = '127.0.0.1'
    redis_port: int = 6379
    # Время кеширования запросов. 5 мин. по умолчанию.
    film_cache_expire_in_seconds: int = 300
    # Настройки Elasticsearch.
    elastic_host: str = '127.0.0.1'
    elastic_port: int = 9200
    elastic_schema: str = 'http://'
    # Общие настройки проекта.
    app_port: int = 8000
    project_name: str = 'movies'
    app_version: str = 'v0.0.1'

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )


settings = Settings()
