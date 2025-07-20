from elasticsearch import AsyncElasticsearch

es: AsyncElasticsearch | None = None


async def get_elastic() -> AsyncElasticsearch:
    """Функция-провайдер соединения с ES.

    Returns:
        Соединение с ES.
    """
    return es
