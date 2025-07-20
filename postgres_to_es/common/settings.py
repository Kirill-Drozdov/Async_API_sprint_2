from pydantic_settings import BaseSettings, SettingsConfigDict


class SettingsETL(BaseSettings):
    """Настройки проекта."""
    POSTGRES_HOST: str = '127.0.0.1'
    POSTGRES_USER: str = 'postgres'
    POSTGRES_DB: str = 'postgres'
    POSTGRES_PASSWORD: str = 'postgres'
    PGPORT: int = 5432
    LOAD_LIMIT: int = 100
    ELASTIC_HOST: str = '127.0.0.1'
    ELASTIC_PORT: int = 9200
    ETL_RATE: int = 5

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )


settings_etl = SettingsETL()
