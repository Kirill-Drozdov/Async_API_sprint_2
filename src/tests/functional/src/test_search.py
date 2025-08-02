
from http import HTTPStatus
from typing import Callable
import uuid

import pytest

from tests.functional.conftest import BASE_API_V1_URL, MAX_FILMS_DATA_SIZE
from tests.functional.settings import test_settings

_FILMS_SEARCH_URL = (
    test_settings.service_url + f'{BASE_API_V1_URL}/films/search'
)


@pytest.mark.parametrize(
    ('query_data', 'expected_answer'),
    [
        (   # Валидный запрос с незаданными параметрами пагинации.
            {'query': 'The Star'},
            {'status': HTTPStatus.OK, 'length': 50},
        ),
        (  # Валидный запрос с параметрами пагинации.
            {'query': 'The Star', 'page[number]': 2, 'page[size]': 10},
            {'status': HTTPStatus.OK, 'length': 10},
        ),
        (   # Запрос несуществующей записи.
            {'query': 'Mashed potato'},
            {'status': HTTPStatus.NOT_FOUND, 'detail': 'Кинопроизведения не найдены'},  # noqa
        ),
        (  # Пустой запрос.
            {'query': ''},
            {'status': HTTPStatus.NOT_FOUND, 'detail': 'Кинопроизведения не найдены'},  # noqa
        ),
        (
            # Несуществующая страница.
            {'query': 'The Star', 'page[number]': 1000},
            {'status': HTTPStatus.NOT_FOUND, 'detail': 'Кинопроизведения не найдены'},  # noqa
        ),
        (
            # Минимальный размер страницы.
            {'query': 'The Star', 'page[size]': 1},
            {'status': HTTPStatus.OK, 'length': 1},
        ),
        (
            # Максимальный размер страницы.
            {'query': 'The Star', 'page[size]': 100},
            {'status': HTTPStatus.OK, 'length': MAX_FILMS_DATA_SIZE},
        ),
    ],
)
@pytest.mark.asyncio
async def test_search(
    es_write_data: Callable,
    es_delete_index: Callable,
    make_get_request: Callable,
    query_data: dict[str, str],
    expected_answer: dict[str, int],
):
    """Проверка поиска кинопроизведений."""
    # Генерируем фиксированные ID для повторяющихся сущностей.
    genre_action_id = str(uuid.uuid4())
    genre_scifi_id = str(uuid.uuid4())
    director_id = str(uuid.uuid4())

    # 1. Генерируем данные для ES (соответствующие схеме индекса).
    es_data = [
        {
            'id': str(uuid.uuid4()),
            'imdb_rating': 8.5,
            'genres': [
                {'id': genre_action_id, 'name': 'Action'},
                {'id': genre_scifi_id, 'name': 'Sci-Fi'},
            ],
            'title': 'The Star',
            'description': 'New World',
            'directors_names': ['Stan'],
            'actors_names': ['Ann', 'Bob'],
            'writers_names': ['Ben', 'Howard'],
            'directors': [
                {'id': director_id, 'name': 'Stan'},
            ],
            'actors': [
                {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},
                {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'},
            ],
            'writers': [
                {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
                {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'}  # noqa
            ],
        } for _ in range(MAX_FILMS_DATA_SIZE)
    ]

    bulk_query: list[dict] = []
    for row in es_data:
        bulk_query.append(
            {
                '_index': test_settings.es_index,
                '_id': row['id'],
                '_source': row,
            },
        )

    # 2. Загружаем данные в ES.
    await es_write_data(
        data=bulk_query,
        index=test_settings.es_index,
        index_mapping=test_settings.es_index_mapping,
    )

    # 3. Запрашиваем данные из ES по API.
    body, status = await make_get_request(_FILMS_SEARCH_URL, query_data)

    # 4. Проверяем ответ.
    assert status == expected_answer.get('status')
    if status == HTTPStatus.OK:
        assert len(body) == expected_answer.get('length')
        # Проверка структуры каждого элемента
        for film in body:
            assert 'uuid' in film
            assert 'title' in film
            assert 'imdb_rating' in film
    else:
        assert body == {'detail': expected_answer.get('detail')}

    # 5 Чистим ES от индекса, чтобы проверить кеширование.
    es_delete_index(index=test_settings.es_index)

    body_cached, status_cached = await make_get_request(
        _FILMS_SEARCH_URL,
        query_data,
    )

    # 6. Проверяем закешированный ответ.
    assert status_cached == expected_answer.get('status')
    if status_cached == HTTPStatus.OK:
        assert len(body_cached) == expected_answer.get('length')
    else:
        assert body_cached == {'detail': expected_answer.get('detail')}


@pytest.mark.asyncio
async def test_search_without_query(make_get_request: Callable):
    """Проверка вызова без параметра query."""
    body, status = await make_get_request(_FILMS_SEARCH_URL, {})

    assert status == HTTPStatus.NOT_FOUND
    assert 'detail' in body
    assert body == {'detail': 'Кинопроизведения не найдены'}


@pytest.mark.parametrize(
    'query_data',
    [
        {'query': 'Star', 'page[number]': 'invalid'},
        {'query': 'Star', 'page[size]': 'invalid'},
        {'query': 'Star', 'page[number]': -1},
        {'query': 'Star', 'page[size]': 0},
        {'query': 'Star', 'page[size]': 101},
    ],
)
@pytest.mark.asyncio
async def test_search_validation_errors(
    make_get_request: Callable,
    query_data: dict[str, str],
):
    """Проверка ошибок валидации параметров."""
    body, status = await make_get_request(_FILMS_SEARCH_URL, query_data)

    assert status == HTTPStatus.UNPROCESSABLE_ENTITY
    assert 'detail' in body
