"""Модуль с логикой для подключения к БД."""
import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.settings import settings_etl


def make_db_uri(
    db_ip: str,
    db_port: int,
    db_name: str,
    db_login: str,
    db_password: str,
    async_driver: bool = False,
) -> str:
    """Генерирует URI до базы данных.

    Args:
        db_ip: ip базы данных.
        db_port: порт базы данных.
        db_name: имя базы данных.
        db_login: логин от базы данных.
        db_password: пароль от базы данных.
        async_driver: асинхронный драйвер базы данных.

    Returns:
        URI до базы данных.
    """
    (
        db_ip,
        db_name,
        db_login,
        db_password,
    ) = map(
        urllib.parse.quote_plus,
        (
            db_ip,
            db_name,
            db_login,
            db_password,
        ),
    )
    driver_db = 'postgresql'
    if async_driver:
        return (
            f'{driver_db}+asyncpg://{db_login}:{db_password}'
            f'@{db_ip}:{db_port}/{db_name}'
        )
    return (
        f'{driver_db}://{db_login}:{db_password}'
        f'@{db_ip}:{db_port}/{db_name}'
    )


def init_db(connection_str: str, echo: bool = False) -> sessionmaker:
    """Инициализация подключения к базе данных.

    Args:
        connection_str: Строка с параметрами подключения к БД.
        echo: Включает отладочный режим SQLAlchemy с расширенным
            логированием отправляемых запросов

    Returns:
        Класс "генератор" сессий.
    """

    engine = create_engine(
        connection_str,
        echo=echo,
    )
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def get_db_connection() -> sessionmaker:
    """Отдает объект асинхронного генератора сессий, содержащий
        подключение к БД.

    Returns:
        Класс "генератор" сессий.
    """
    db_uri = make_db_uri(
        db_ip=settings_etl.POSTGRES_HOST,
        db_login=settings_etl.POSTGRES_USER,
        db_name=settings_etl.POSTGRES_DB,
        db_password=settings_etl.POSTGRES_PASSWORD,
        db_port=settings_etl.PGPORT,
    )
    return init_db(connection_str=db_uri)
