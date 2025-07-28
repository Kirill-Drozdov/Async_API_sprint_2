from collections import defaultdict
from functools import lru_cache
import hashlib
import json
import logging

from elastic_transport import ObjectApiResponse
from elasticsearch import AsyncElasticsearch, BadRequestError, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from core.config import settings
from core.utils import async_backoff
from db.elastic import get_elastic
from db.redis import get_redis
from models.person import PersonDetail, PersonFilms, PersonRole

_PERSON_CACHE_EXPIRE_IN_SECONDS = settings.person_cache_expire_in_seconds


class PersonService:
    """Класс, описывающий бизнес-логику взаимодействия с персонажами.
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self._redis = redis
        self._elastic = elastic
        self._logger = logging.getLogger(__name__)
        self._es_index = 'persons'
        self._es_movies_index = 'movies'

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
            '_source': ['id', 'name'],
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
        return f'persons:{hashlib.md5(cache_str.encode()).hexdigest()}'

    @staticmethod
    def __serialize_es_response(
        response: ObjectApiResponse,
        persons: list,
    ) -> list[PersonDetail]:
        """Преобразуем результат ответа от ES в объекты PersonDetail.

        Args:
            response (ObjectApiResponse): Результат ответа от ES.
            persons (list): пустой список для заполнения персонажами.

        Returns:
            list[PersonDetail]: Список сериализованных объектов.
        """

        for hit in response['hits']['hits']:
            source = hit['_source']
            person = PersonDetail(
                id=source['id'],
                name=source['name'],
            )
            persons.append(person)
        return persons

    async def get_persons_by_search(
        self,
        query: str,
        page_size: int,
        page_number: int,
    ) -> list[PersonDetail]:
        """Получает список персонажей с пагинацией в соответствии с результатом
            поискового запроса по имени персонажа.

        Args:
            query: Поисковой запрос по имени персонажа.
            page_size: Количество элементов на странице.
            page_number: Номер страницы (начинается с 1).

        Returns:
            Список персонажей в виде объекта PersonDetail.
        """
        # Создаем уникальный ключ для кэширования запроса.
        cache_key = self.__generate_cache_key(
            query=query,
            page_size=page_size,
            page_number=page_number,
        )
        # Проверяем кэш.
        cached_result = await self._get_persons_from_cache(cache_key)
        if cached_result:
            return cached_result

        body = self.__generate_base_body(
            page_size=page_size,
            page_number=page_number,
        )
        persons = await self._get_persons_from_elastic_by_name(
            query=query,
            body=body,
        )
        # Сохраняем в кэш.
        await self._put_persons_to_cache(cache_key, persons)

        return persons

    @async_backoff()
    async def __get_row_persons_from_elastic(
        self,
        body: dict,
        index: str,
    ) -> ObjectApiResponse | None:
        try:
            return await self._elastic.search(
                index=index,
                body=body,
            )
        except (BadRequestError, NotFoundError):
            return None

    async def _get_persons_from_elastic_by_name(  # noqa
        self,
        query: str,
        body: dict,
    ) -> list[PersonDetail]:
        """Реализует поиск персонажей в ES по имени.

        Args:
            query: полное или частичное имя персонажа.
            body: Тело запроса к ES.

        Returns:
            Персонажи в виде объектов PersonDetail, если они были найдены.
        """
        persons = []
        try:
            # Формируем тело запроса с поиском по частичному совпадению
            # к Elasticsearch.
            body['query'] = {
                'match': {
                    'name': {
                        'query': query,
                        'fuzziness': 'AUTO',
                        'operator': 'and',
                    },
                },
            }

            # Выполняем запрос к Elasticsearch.
            response = await self.__get_row_persons_from_elastic(
                body=body,
                index=self._es_index,
            )
            if response is None:
                return persons

            persons = self.__serialize_es_response(response, persons)

            if not persons:
                return persons

            # Собираем ID всех найденных персон
            person_ids = [person.id for person in persons]

            # Запрашиваем фильмы, где участвуют найденные персоны
            movies_response = await self._get_movies_by_person_ids(person_ids)
            if not movies_response:
                return persons

            # Создаем промежуточную структуру:
            # person_id -> {film_id: set(roles)}
            person_films_dict = defaultdict(lambda: defaultdict(set))
            person_ids_set = set(person_ids)

            for hit in movies_response['hits']['hits']:
                film_id = hit['_source']['id']

                # Обрабатываем режиссеров
                for director in hit['_source'].get('directors', []):
                    d_id = director.get('id')
                    if d_id in person_ids_set:
                        person_films_dict[d_id][film_id].add(PersonRole.DIRECTOR)  # noqa

                # Обрабатываем актеров
                for actor in hit['_source'].get('actors', []):
                    a_id = actor.get('id')
                    if a_id in person_ids_set:
                        person_films_dict[a_id][film_id].add(PersonRole.ACTOR)

                # Обрабатываем сценаристов
                for writer in hit['_source'].get('writers', []):
                    w_id = writer.get('id')
                    if w_id in person_ids_set:
                        person_films_dict[w_id][film_id].add(PersonRole.WRITER)

            # Обогащаем персоны данными о фильмах
            for person in persons:
                films_list = []
                if person.id in person_films_dict:
                    for film_id, roles in person_films_dict[person.id].items():
                        films_list.append(
                            PersonFilms(id=film_id, roles=list(roles)),
                        )
                person.films = films_list

            return persons

        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из ES: {error}',
            )
            return persons

    async def _get_movies_by_person_ids(
        self,
        person_ids: list[str],
    ) -> ObjectApiResponse | None:
        """Получает фильмы, где участвуют указанные персоны."""
        if not person_ids:
            return None
        body = {
            'size': 10000,  # Увеличили размер выборки
            '_source': ['id', 'directors.id', 'actors.id', 'writers.id'],
            'query': {
                'bool': {
                    'should': [
                        # Исправлено: используем nested-запросы для directors
                        {
                            'nested': {
                                'path': 'directors',
                                'query': {
                                    'terms': {'directors.id': person_ids},
                                },
                            },
                        },
                        # Исправлено: используем nested-запросы для actors
                        {
                            'nested': {
                                'path': 'actors',
                                'query': {
                                    'terms': {'actors.id': person_ids},
                                },
                            },
                        },
                        # Исправлено: используем nested-запросы для writers
                        {
                            'nested': {
                                'path': 'writers',
                                'query': {
                                    'terms': {'writers.id': person_ids},
                                },
                            },
                        },
                    ],
                },
            },
        }

        try:
            return await self._elastic.search(
                index=self._es_movies_index,
                body=body,
            )
        except Exception as e:
            self._logger.error(f'Ошибка при запросе фильмов: {e}')
            return None

    @async_backoff()
    async def __get_row_persons_from_redis(self, cache_key: str):
        return await self._redis.get(cache_key)

    async def _get_persons_from_cache(
        self,
        cache_key: str,
    ) -> list[PersonDetail] | None:
        """Получает список персонажей из кэша.

        Args:
            cache_key (str): ключ, по которому будет получен закешированный
                результат.

        Returns:
            Список персонажей в виде объекта PersonDetail.
        """
        try:
            cached_data = await self.__get_row_persons_from_redis(
                cache_key=cache_key,
            )
            if cached_data:
                persons_data = json.loads(cached_data)
                return [
                    PersonDetail.model_validate(person_data)
                    for person_data in persons_data
                ]
        except Exception as error:
            self._logger.error(
                f'Ошибка при получении данных из кеша: {error}',
            )
        return None

    @async_backoff()
    async def __put_fims_to_redis(
        self,
        cache_key: str,
        persons_data: list[dict],
    ) -> None:
        await self._redis.setex(
            cache_key,
            _PERSON_CACHE_EXPIRE_IN_SECONDS,
            json.dumps(persons_data),
        )

    async def _put_persons_to_cache(
        self,
        cache_key: str,
        persons: list[PersonDetail],
    ):
        """Сохраняет список персонажей в кэш.

        Args:
            cache_key (str): ключ, по которому будет закеширован результат.
            persons: list[PersonDetail]: персонажи для кеширования.

        Returns:
            Список персонажей в виде объекта PersonDetail.
        """
        try:
            persons_data = [
                person.model_dump(by_alias=False)
                for person in persons
            ]
            await self.__put_fims_to_redis(
                cache_key=cache_key,
                persons_data=persons_data,
            )
        except Exception as error:
            self._logger.error(
                f'Ошибка при кешировании результата: {error}',
            )


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    """Функция-провайдер для предоставления сервиса.

    Args:
        redis (Redis, optional): объект, содержащий соединение с Redis.
        elastic (AsyncElasticsearch, optional): объект, содержащий соединение
            с AsyncElasticsearch.

    Returns:
        Объект PersonService.
    """
    return PersonService(redis, elastic)
