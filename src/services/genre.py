from functools import lru_cache
import json
import logging

from elastic_transport import ObjectApiResponse
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from core.config import settings
from core.utils import async_backoff
from db.elastic import get_elastic
from db.redis import get_redis
from models.film import Genre
from repository.es_repository import ElasticSearchRepository

_GENRE_CACHE_EXPIRE_IN_SECONDS = settings.genre_cache_expire_in_seconds


class GenreEsRepository(ElasticSearchRepository[Genre]):
    pass


class GenreService:
    """Класс, описывающий бизнес-логику взаимодействия с жанрами.
    """

    def __init__(self, redis: Redis, repository: GenreEsRepository):
        self._redis = redis
        self._repository = repository
        self._logger = logging.getLogger(__name__)
        self._es_index = 'genres'

    @staticmethod
    def __generate_base_body() -> dict:
        """Формируем базовое тело запроса к Elasticsearch.

        Returns:
            Тело запроса к ES.
        """
        return {
            '_source': ['id', 'name'],
        }

    @staticmethod
    def __serialize_es_response(
        response: ObjectApiResponse,
        genres: list,
    ) -> list[Genre]:
        """Преобразуем результат ответа от ES в объекты Genre.

        Args:
            response (ObjectApiResponse): Результат ответа от ES.
            genres (list): пустой список для заполнения жанрами.

        Returns:
            list[Genre]: Список сериализованных объектов.
        """

        for hit in response['hits']['hits']:
            source = hit['_source']
            genre = Genre(
                id=source['id'],
                name=source['name'],
            )
            genres.append(genre)
        return genres

    async def get_genres(self) -> list[Genre]:
        """Получает все жанры, доступные в кинотеатре.

        Returns:
            Список жанров.
        """
        cache_key = 'all_genres'
        # Проверяем кэш.
        cached_result = await self._get_genres_from_cache(cache_key)
        if cached_result:
            return cached_result

        body = self.__generate_base_body()
        genres = await self._get_genres_from_elastic(body=body)

        # Сохраняем в кэш.
        await self._put_genres_to_cache(cache_key, genres)

        return genres

    async def get_genre_by_id(self, genre_id: str) -> Genre | None:
        """Получить жанр по уникальному идентификатору.

        Args:
            genre_id (str): уникальный идентификатор.

        Returns:
            Optional[Genre]: жанр, если он нашелся в БД.
        """
        # Пытаемся получить данные из кеша, потому что оно работает быстрее.
        genre = await self._get_genre_from_cache(genre_id)
        if genre:
            return genre
        # Если жанра нет в кеше, то ищем его в Elasticsearch.
        genre = await self._repository.get(
            index=self._es_index,
            object_id=genre_id,
        )
        if not genre:
            # Если он отсутствует в Elasticsearch, значит, жанра вообще
            # нет в базе.
            return None
        # Сохраняем жанр в кеш.
        await self._put_genre_to_cache(genre)

        return genre

    async def _get_genres_from_elastic(
        self,
        body: dict,
    ) -> list[Genre]:
        """Возвращает жанры из ES.

        Args:
            body: Тело запроса к ES.

        Returns:
            Жанры в виде объектов Genre, если они были найдены.
        """
        genres = []
        try:
            response = await self._repository.get_multi(
                body=body,
                index=self._es_index,
            )
            if response is None:
                return genres

            return self.__serialize_es_response(
                response=response,
                genres=genres,
            )
        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из ES: {error}',
            )
            return genres

    @async_backoff()
    async def __get_row_genre_from_redis(self, genre_id: str):
        return await self._redis.get(genre_id)

    async def _get_genre_from_cache(self, genre_id: str) -> Genre | None:
        """Пытается получить данные о жанре из кеша.

        Args:
            genre_id (str): уникальный идентификатор.

        Returns:
            Жанр, если он был найден в кеше.
        """
        try:
            data = await self.__get_row_genre_from_redis(genre_id=genre_id)
            if not data:
                return None
            genre_data = json.loads(data)
            return Genre.model_validate(genre_data)
        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из кеша: {error}',
            )
            return None

    @async_backoff()
    async def __put_genre_to_redis(self, genre: Genre):
        await self._redis.set(
            genre.id,
            genre.model_dump_json(by_alias=False),
            _GENRE_CACHE_EXPIRE_IN_SECONDS,
        )

    async def _put_genre_to_cache(self, genre: Genre):
        """Кеширует результат запроса на поиск жанра.

        Args:
            genre (Genre): жанр.
        """
        try:
            await self.__put_genre_to_redis(genre=genre)
        except Exception as error:
            self._logger.error(
                f'Ошибка при кешировании результата: {error}',
            )

    @async_backoff()
    async def __get_row_genres_from_redis(self, cache_key: str):
        return await self._redis.get(cache_key)

    async def _get_genres_from_cache(
        self,
        cache_key: str,
    ) -> list[Genre] | None:
        """Получает список жанров из кэша.

        Args:
            cache_key (str): ключ, по которому будет получен закешированный
                результат.

        Returns:
            Список жанров в виде объекта Genre.
        """
        try:
            cached_data = await self.__get_row_genres_from_redis(
                cache_key=cache_key,
            )
            if cached_data:
                genres_data = json.loads(cached_data)
                return [
                    Genre.model_validate(genre_data)
                    for genre_data in genres_data
                ]
        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из кеша: {error}',
            )
        return None

    @async_backoff()
    async def __put_genres_to_redis(
        self,
        cache_key: str,
        genres_data: list[dict],
    ) -> None:
        await self._redis.setex(
            cache_key,
            _GENRE_CACHE_EXPIRE_IN_SECONDS,
            json.dumps(genres_data),
        )

    async def _put_genres_to_cache(
        self,
        cache_key: str,
        genres: list[Genre],
    ):
        """Сохраняет список жанров в кэш.

        Args:
            cache_key (str): ключ, по которому будет закеширован результат.
            genres: list[Genre]: жанры для кеширования.

        Returns:
            Список жанров в виде объекта Genre.
        """
        try:
            genres_data = [
                genre.model_dump(by_alias=False)
                for genre in genres
            ]
            await self.__put_genres_to_redis(
                cache_key=cache_key,
                genres_data=genres_data,
            )
        except Exception as error:
            self._logger.error(
                f'Ошибка при кешировании результата: {error}',
            )


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    """Функция-провайдер для предоставления сервиса.

    Args:
        redis (Redis, optional): объект, содержащий соединение с Redis.
        elastic (AsyncElasticsearch, optional): объект, содержащий соединение
            с AsyncElasticsearch.

    Returns:
        Объект GenreService.
    """
    return GenreService(
        redis=redis,
        repository=GenreEsRepository(
            response_model=Genre,
            elastic=elastic,
        ),
    )
