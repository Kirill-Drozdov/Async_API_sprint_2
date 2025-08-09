import logging

from elasticsearch import Elasticsearch, helpers

from common.settings import settings_etl
from common.utils import backoff


class DataLoader:
    def __init__(self) -> None:
        """Инициализирует подключение к ElasticSearch."""
        self._index_name = 'movies'
        self._es_client = Elasticsearch(
            self._generate_es_url(),
            max_retries=3,
            retry_on_timeout=True,
            meta_header=False,
        )
        self._loger = logging.getLogger(__name__)

    @staticmethod
    def _generate_es_url(method: str = 'http') -> str | None:
        """Генерирует URL для подключения к ElasticSearch.

        Args:
            method (str, optional): тип протокола при подключении.

        Raises:
            ValueError: если передан невалидный тип протокола.

        Returns:
            URL для подключения к ElasticSearch или None, если был передан
                невалидный тип протокола.
        """
        if method not in ('http', 'https'):
            raise ValueError(
                'Параметр method может принимать значения: "http", "https".'
                f' Ваше значение: {method}',
            )
        es_host = settings_etl.ELASTIC_HOST
        es_port = settings_etl.ELASTIC_PORT
        return f'{method}://{es_host}:{es_port}'

    def load_data(
        self,
        data: tuple[list[dict], list[dict], list[dict]],
    ) -> None:
        """
        Загружает данные в ElasticSearch.

        Args:
            data: кортеж со списком словарей с данными фильмов и жанров.
        """
        (
            transformed_films_data,
            transformed_genres_data,
            transformed_persons_data,
        ) = data

        index_to_data = (
            (transformed_films_data, 'movies'),
            (transformed_genres_data, 'genres'),
            (transformed_persons_data, 'persons'),
        )
        for transformed_data, index in index_to_data:
            self._load_by_index(
                data=transformed_data,
                index=index,
            )

    def _load_by_index(self, data: list[dict], index: str) -> None:
        """Загружает данные в ElasticSearch по указанному индексу.

        Args:
            data (list[dict]): данные для загрузки.
            index (str): индекс.
        """
        data_size = len(data)
        if not data_size:
            self._loger.debug('Нет данных для загрузки.')
            return

        self._loger.info(
            f'Начало загрузки {data_size} документов'
            f' в индекс "{index}"',
        )
        # Подготавливаем действия для bulk API.
        actions = [
            {
                '_op_type': 'index',
                '_index': index,
                '_id': doc.get('id'),
                '_source': doc,
            }
            for doc in data
        ]
        self._bulk_request_to_es(
            actions=actions,
            data_size=data_size,
        )

    @backoff()
    def _bulk_request_to_es(self, actions, data_size: int) -> None:
        """Выполняет bulk-запрос к серверу ElasticSearch.

        Args:
            actions: список действий с документами.
            data_size: объем данных.
        """
        success_count, errors = helpers.bulk(
            self._es_client,
            actions,
            stats_only=True,
            raise_on_error=True,
            max_retries=3,
            initial_backoff=2,
            request_timeout=30,
        )

        if errors:
            self._loger.error(
                f'Ошибки при загрузке: {errors} документов не обработано',
            )
        self._loger.info(
            f'Успешно загружено {success_count}/{data_size} документов',
        )
