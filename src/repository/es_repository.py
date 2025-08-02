from typing import Generic, Type, TypeVar

from elastic_transport import ObjectApiResponse
from elasticsearch import AsyncElasticsearch, BadRequestError, NotFoundError
from pydantic import BaseModel

from core.utils import async_backoff
from repository.abstract_repository import Repository

ResponseModelType = TypeVar('ResponseModelType', bound=BaseModel)


class ElasticSearchRepository(Repository, Generic[ResponseModelType]):
    """Осуществляет взаимодействие с хранилищем на базе Elasticsearch."""

    def __init__(
        self,
        elastic: AsyncElasticsearch,
        response_model: Type[ResponseModelType],
    ) -> None:
        self._elastic: AsyncElasticsearch = elastic
        self._response_model = response_model
        super().__init__()

    @async_backoff()
    async def get(
        self,
        index: str,
        object_id: str,
    ) -> ResponseModelType | None:
        """Возвращает объект из ES по id.

        Args:
            index (str): индекс для поиска в ES.
            film_id (str): уникальный идентификатор.

        Returns:
            Объект из ES по id, если он был найден.
        """
        try:
            doc = await self._elastic.get(index=index, id=object_id)
        except NotFoundError:
            return None
        return self._response_model(**doc['_source'])

    async def get_multi(
        self,
        body: dict,
        index: str,
    ) -> ObjectApiResponse | None:
        """Возвращает множество документов из ES по заданному телу запроса
            и индексу.

        Args:
            body (dict): тело запроса.
            index (str): индекс для поиска в ES

        Returns:
            ObjectApiResponse, содержащий документы или ничего.
        """
        try:
            return await self._elastic.search(
                index=index,
                body=body,
            )
        except (BadRequestError, NotFoundError):
            return None
