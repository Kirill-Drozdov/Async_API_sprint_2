
import uuid
from http import HTTPStatus
from typing import Callable

import pytest

from tests.functional.settings import test_settings


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
            {'query': 'The Star'},
            {'status': HTTPStatus.OK, 'length': 50},
        ),
        (
            {'query': 'Mashed potato'},
            {'status': HTTPStatus.NOT_FOUND, 'length': 1},
        ),
    ],
)
@pytest.mark.asyncio
async def test_search(
    es_write_data: Callable,
    make_get_request: Callable,
    query_data: dict[str, str],
    expected_answer: dict[str, int],
):
    """Проверка поиска кинопроизведений."""
    # Генерируем фиксированные ID для повторяющихся сущностей
    genre_action_id = str(uuid.uuid4())
    genre_scifi_id = str(uuid.uuid4())
    director_id = str(uuid.uuid4())

    # 1. Генерируем данные для ES (соответствующие схеме индекса)
    es_data = [
        {
            'id': str(uuid.uuid4()),
            'imdb_rating': 8.5,
            'genres': [
                {'id': genre_action_id, 'name': 'Action'},
                {'id': genre_scifi_id, 'name': 'Sci-Fi'}
            ],
            'title': 'The Star',
            'description': 'New World',
            'directors_names': ['Stan'],
            'actors_names': ['Ann', 'Bob'],
            'writers_names': ['Ben', 'Howard'],
            'directors': [
                {'id': director_id, 'name': 'Stan'}
            ],
            'actors': [
                {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},
                {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'}
            ],
            'writers': [
                {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
                {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'}  # noqa
            ]
        } for _ in range(60)
    ]

    bulk_query: list[dict] = []
    for row in es_data:
        bulk_query.append(
            {
                '_index': test_settings.es_index,
                '_id': row['id'],
                '_source': row,
            }
        )

    # 2. Загружаем данные в ES
    await es_write_data(bulk_query)

    # 3. Запрашиваем данные из ES по API
    url = test_settings.service_url + '/api/v1/films/search'

    body, status = await make_get_request(url, query_data)

    # 4. Проверяем ответ
    assert status == expected_answer.get('status')
    assert len(body) == expected_answer.get('length')
