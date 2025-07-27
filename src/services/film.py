from functools import lru_cache
import hashlib
import json
import logging

from elastic_transport import ObjectApiResponse
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from core.config import settings
from db.elastic import get_elastic
from db.redis import get_redis
from models.film import Film, FilmShort

_FILM_CACHE_EXPIRE_IN_SECONDS = settings.film_cache_expire_in_seconds


class FilmService:
    """Класс, описывающий бизнес-логику взаимодействия с кинопроизведениями.
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self._redis = redis
        self._elastic = elastic
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def __generate_base_body(
        page_size: int,
        page_number: int,
    ) -> dict:
        """Формируем базовое тело запроса к Elasticsearch.

        Args:
            page_size: Количество элементов на странице.
            page_number: Номер страницы (начинается с 1).

        Returns:
            Тело запроса к ES.
        """
        return {
            'from': (page_number - 1) * page_size,
            'size': page_size,
            '_source': ['id', 'title', 'imdb_rating'],
        }

    @staticmethod
    def __generate_cache_key(  # noqa
        page_size: int,
        page_number: int,
        sort_field: str | None = None,
        genre: str | None = None,
        sort_order: str | None = None,
        query: str | None = None,
    ) -> str:
        """Генерирует уникальный ключ для кэширования запроса."""
        cache_data = {
            'query': query,
            'sort_field': sort_field,
            'genre': genre,
            'sort_order': sort_order,
            'page_size': page_size,
            'page_number': page_number,
        }
        cache_str = json.dumps(cache_data, sort_keys=True)
        return f'films:{hashlib.md5(cache_str.encode()).hexdigest()}'

    @staticmethod
    def __serialize_es_response(
        response: ObjectApiResponse,
        films: list,
    ) -> list[FilmShort]:
        """Преобразуем результат ответа от ES в объекты FilmShort.

        Args:
            response (ObjectApiResponse): Результат ответа от ES.
            films (list): пустой список для заполнения фильмами.

        Returns:
            list[FilmShort]: Список сериализованных объектов.
        """

        for hit in response['hits']['hits']:
            source = hit['_source']
            film = FilmShort(
                id=source['id'],
                title=source['title'],
                imdb_rating=source['imdb_rating'],
            )
            films.append(film)
        return films

    async def get_film_by_id(self, film_id: str) -> Film | None:
        """Получить кинопроизведение по уникальному идентификатору.

        Args:
            film_id (str): уникальный идентификатор.

        Returns:
            Optional[Film]: кинопроизведение, если оно нашлой в БД.
        """
        # Пытаемся получить данные из кеша, потому что оно работает быстрее.
        film = await self._get_film_from_cache(film_id)
        if film:
            return film
        # Если фильма нет в кеше, то ищем его в Elasticsearch.
        film = await self._get_film_from_elastic(film_id)
        if not film:
            # Если он отсутствует в Elasticsearch, значит, фильма вообще
            # нет в базе.
            return None
        # Сохраняем фильм в кеш.
        await self._put_film_to_cache(film)

        return film

    async def get_films(
        self,
        sort_field: str,
        genre: str | None,
        sort_order: str,
        page_size: int,
        page_number: int,
    ) -> list[FilmShort]:
        """Получает список фильмов с пагинацией, сортировкой и фильтрацией
            по жанру.

        Args:
            sort_field: Поле для сортировки (imdb_rating).
            genre: UUID жанра для фильтрации (опционально).
            sort_order: Порядок сортировки (asc/desc).
            page_size: Количество элементов на странице.
            page_number: Номер страницы (начинается с 1).

        Returns:
            Список кинопроизведений в виде объекта FilmShort.
        """

        # Создаем уникальный ключ для кэширования запроса.
        cache_key = self.__generate_cache_key(
            sort_field=sort_field,
            genre=genre,
            sort_order=sort_order,
            page_size=page_size,
            page_number=page_number,
        )

        # Проверяем кэш.
        cached_result = await self._get_films_from_cache(cache_key)
        if cached_result:
            return cached_result

        body = self.__generate_base_body(
            page_size=page_size,
            page_number=page_number,

        )
        films = await self._get_films_from_elastic(
            sort_field=sort_field,
            sort_order=sort_order,
            genre=genre,
            body=body,
        )

        # Сохраняем в кэш.
        await self._put_films_to_cache(cache_key, films)

        return films

    async def get_films_by_search(
        self,
        query: str,
        page_size: int,
        page_number: int,
    ) -> list[FilmShort]:
        """Получает список фильмов с пагинацией в соответствии с результатом
            поискового запроса по названию фильма.

        Args:
            query: Поисковой запрос по названию фильма.
            page_size: Количество элементов на странице.
            page_number: Номер страницы (начинается с 1).

        Returns:
            Список кинопроизведений в виде объекта FilmShort.
        """
        # Создаем уникальный ключ для кэширования запроса.
        cache_key = self.__generate_cache_key(
            query=query,
            page_size=page_size,
            page_number=page_number,
        )
        # Проверяем кэш.
        cached_result = await self._get_films_from_cache(cache_key)
        if cached_result:
            return cached_result

        body = self.__generate_base_body(
            page_size=page_size,
            page_number=page_number,
        )
        films = await self._get_films_from_elastic_by_title(
            query=query,
            body=body,
        )
        # Сохраняем в кэш.
        await self._put_films_to_cache(cache_key, films)

        return films

    async def _get_film_from_cache(self, film_id: str) -> Film | None:
        """Пытается получить данные о кинопроизведении из кеша.

        Args:
            film_id (str): уникальный идентификатор.

        Returns:
            Кинопроизведение, если оно было найдено в кеше.
        """
        try:
            data = await self._redis.get(film_id)
            if not data:
                return None
            film_data = json.loads(data)
            return Film.model_validate(film_data)
        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из кеша: {error}',
            )
            return None

    async def _get_films_from_cache(
        self,
        cache_key: str,
    ) -> list[FilmShort] | None:
        """Получает список фильмов из кэша.

        Args:
            cache_key (str): ключ, по которому будет получен закешированный
                результат.

        Returns:
            Список кинопроизведений в виде объекта FilmShort.
        """
        try:
            cached_data = await self._redis.get(cache_key)
            if cached_data:
                films_data = json.loads(cached_data)
                return [
                    FilmShort.model_validate(film_data)
                    for film_data in films_data
                ]
        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из кеша: {error}',
            )
        return None

    async def _put_films_to_cache(
        self,
        cache_key: str,
        films: list[FilmShort],
    ):
        """Сохраняет список фильмов в кэш.

        Args:
            cache_key (str): ключ, по которому будет закеширован результат.
            films: list[FilmShort]: кинопроизведения для кеширования.

        Returns:
            Список кинопроизведений в виде объекта FilmShort.
        """
        try:
            films_data = [film.model_dump(by_alias=False) for film in films]
            await self._redis.setex(
                cache_key,
                _FILM_CACHE_EXPIRE_IN_SECONDS,
                json.dumps(films_data),
            )
        except Exception as error:
            self._logger.error(
                f'Ошибка при кешировании результата: {error}',
            )

    async def _put_film_to_cache(self, film: Film):
        """Кеширует результат запроса на поиск кинопроизведения.

        Args:
            film (Film): кинопроизведение.
        """
        try:
            await self._redis.set(
                film.id,
                film.model_dump_json(by_alias=False),
                _FILM_CACHE_EXPIRE_IN_SECONDS,
            )
        except Exception as error:
            self._logger.error(
                f'Ошибка при кешировании результата: {error}',
            )

    async def _get_film_from_elastic(self, film_id: str) -> Film | None:
        """Возвращает кинопроизведение из ES.

        Args:
            film_id (str): уникальный идентификатор.

        Returns:
            Кинопроизведение в виде объекта Film, если он был найден.
        """
        try:
            doc = await self._elastic.get(index='movies', id=film_id)
        except NotFoundError:
            return None
        return Film(**doc['_source'])

    async def _get_films_from_elastic(
        self,
        sort_order: str,
        sort_field: str,
        genre: str | None,
        body: dict,
    ) -> list[FilmShort]:
        """Возвращает кинопроизведение из ES.

        Args:
            sort_order: Порядок сортировки (asc/desc).
            sort_field: Поле для сортировки (imdb_rating).
            genre: UUID жанра для фильтрации (опционально).
            body: Тело запроса к ES.

        Returns:
            Кинопроизведения в виде объектов FilmShort, если они были найдены.
        """
        films = []
        try:
            body['sort'] = [
                {sort_field: {'order': sort_order}},
            ]
            # Добавляем фильтрацию по жанру, если она указана.
            if genre:
                body['query'] = {
                    'bool': {
                        'filter': [
                            {
                                'nested': {
                                    'path': 'genres',
                                    'query': {
                                        'term': {'genres.id': genre},
                                    },
                                },
                            },
                        ],
                    },
                }
            else:
                body['query'] = {'match_all': {}}

            # Выполняем запрос к Elasticsearch.
            response = await self._elastic.search(
                index='movies',
                body=body,
            )
            return self.__serialize_es_response(
                response=response,
                films=films,
            )

        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из ES: {error}',
            )
            return films

    async def _get_films_from_elastic_by_title(
        self,
        query: str,
        body: dict,
    ) -> list[FilmShort]:
        """Реализует поиск кинопроизведений в ES по названию фильма.

        Args:
            query: полное или частичное название фильма.
            body: Тело запроса к ES.

        Returns:
            Кинопроизведения в виде объектов FilmShort, если они были найдены.
        """
        films = []
        try:
            # Формируем тело запроса с поиском по частичному совпадению
            # к Elasticsearch.
            body['query'] = {
                'match': {
                    'title': {
                        'query': query,
                        'fuzziness': 'AUTO',
                        'operator': 'and',
                    },
                },
            }

            # Выполняем запрос к Elasticsearch.
            response = await self._elastic.search(
                index='movies',
                body=body,
            )
            return self.__serialize_es_response(
                response=response,
                films=films,
            )

        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из ES: {error}',
            )
            return films


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    """Функция-провайдер для предоставления сервиса.

    Args:
        redis (Redis, optional): объект, содержащий соединение с Redis.
        elastic (AsyncElasticsearch, optional): объект, содержащий соединение
            с AsyncElasticsearch.

    Returns:
        Объект FilmService.
    """
    return FilmService(redis, elastic)
