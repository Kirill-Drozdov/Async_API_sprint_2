
from http import HTTPStatus
from typing import Callable
import uuid

import pytest

from tests.functional.conftest import BASE_API_V1_URL, MAX_FILMS_DATA_SIZE
from tests.functional.settings import test_settings
from tests.functional.utils.helpers import generate_es_data, genre_action_id

_FILMS_API_URL = (
    test_settings.service_url + f'{BASE_API_V1_URL}/films'
)


# Подготавливаем разнообразные данные.
es_data, action_films_id = generate_es_data(data_size=MAX_FILMS_DATA_SIZE)


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (   # Валидный запрос с незаданными параметрами пагинации.
            {},
            {'status': HTTPStatus.OK, 'length': 50},
        ),
        (  # Валидный запрос с параметрами пагинации.
            {'page[number]': 2, 'page[size]': 10},
            {'status': HTTPStatus.OK, 'length': 10},
        ),
        (
            # Несуществующая страница.
            {'page[number]': 1000},
            {'status': HTTPStatus.NOT_FOUND, 'detail': 'Кинопроизведения не найдены'},  # noqa
        ),
        (
            # Минимальный размер страницы.
            {'page[size]': 1},
            {'status': HTTPStatus.OK, 'length': 1},
        ),
        (
            # Максимальный размер страницы.
            {'page[size]': 100},
            {'status': HTTPStatus.OK, 'length': MAX_FILMS_DATA_SIZE},
        ),
        # Новые тесты сортировки и фильтрации
        (
            {'sort': 'imdb_rating'},
            {'status': HTTPStatus.OK, 'length': 50, 'first_rating': 1.0, 'last_rating': 7.5}  # noqa
        ),
        (
            {'sort': '-imdb_rating'},
            {'status': HTTPStatus.OK, 'length': 50, 'first_rating': 9.0, 'last_rating': 2.0}  # noqa
        ),
        (
            {'genre': genre_action_id},
            {'status': HTTPStatus.OK, 'length': 30, 'all_genres': ['Action']}
        ),
        (
            {'genre': genre_action_id, 'sort': '-imdb_rating'},
            {'status': HTTPStatus.OK, 'length': 30, 'first_rating': 9.0, 'last_rating': 1.0, 'all_genres': ['Action']}  # noqa
        ),
        (
            {'genre': 'Non-Existing-Genre'},
            {'status': HTTPStatus.NOT_FOUND, 'detail': 'Кинопроизведения не найдены'}  # noqa
        ),
        (
            {'sort': 'invalid_field'},
            {'status': HTTPStatus.UNPROCESSABLE_ENTITY, 'detail': 'Invalid sort parameter'}  # noqa
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_films(  # noqa
    es_write_data: Callable,
    es_delete_index: Callable,
    make_get_request: Callable,
    query_data: dict[str, str],
    expected_answer: dict[str, int],
):
    """Проверка поиска кинопроизведений."""
    # 1. Генерируем данные для ES (соответствующие схеме индекса).
    bulk_query: list[dict] = []
    for row in es_data:
        bulk_query.append(
            {
                '_index': test_settings.es_index,
                '_id': row['id'],
                '_source': row,
            }
        )

    # 2. Загружаем данные в ES.
    await es_write_data(
        data=bulk_query,
        index=test_settings.es_index,
        index_mapping=test_settings.es_index_mapping,
    )

    # 3. Запрашиваем данные из ES по API.
    body, status = await make_get_request(_FILMS_API_URL, query_data)

    # 4. Проверяем ответ.
    assert status == expected_answer.get('status')
    if status == HTTPStatus.OK:
        assert len(body) == expected_answer.get('length')

        # Проверка структуры
        for film in body:
            assert 'uuid' in film
            assert 'title' in film
            assert 'imdb_rating' in film

        # Проверка сортировки
        if 'first_rating' in expected_answer:
            if expected_answer.get('sort_order') == 'asc':
                assert body[0]['imdb_rating'] == expected_answer['first_rating']  # noqa
                assert body[-1]['imdb_rating'] == expected_answer['last_rating']  # noqa
            else:
                assert body[0]['imdb_rating'] == expected_answer['first_rating']  # noqa
                assert body[-1]['imdb_rating'] == expected_answer['last_rating']  # noqa

        # Проверка фильтрации по жанру
        if 'all_genres' in expected_answer:
            for film in body:
                assert film.get('uuid') in action_films_id

    # 5 Чистим ES от индекса, чтобы проверить кеширование.
    es_delete_index(index=test_settings.es_index)

    body_cached, status_cached = await make_get_request(
        _FILMS_API_URL,
        query_data,
    )

    # 6. Проверяем закешированный ответ.
    assert status_cached == expected_answer.get('status')
    if status_cached == HTTPStatus.OK:
        assert len(body_cached) == expected_answer.get('length')


@pytest.mark.asyncio
async def test_empty_index(
    es_write_data: Callable,
    es_delete_index: Callable,
    make_get_request: Callable
):
    """Проверка пустой базы данных."""
    # Создаем пустой индекс.
    await es_write_data(
        data=[],
        index=test_settings.es_index,
        index_mapping=test_settings.es_index_mapping,
    )
    query_params = {'page[number]': 3, 'page[size]': 7}
    body, status = await make_get_request(_FILMS_API_URL, query_params)
    assert status == HTTPStatus.NOT_FOUND
    assert body.get('detail') == 'Кинопроизведения не найдены'

    # Проверка кеширования.
    await es_delete_index(index=test_settings.es_index)
    body_cached, status_cached = await make_get_request(
        _FILMS_API_URL,
        query_params,
    )
    assert status_cached == HTTPStatus.NOT_FOUND
    assert body_cached.get('detail') == 'Кинопроизведения не найдены'


@pytest.mark.parametrize(
    "query_data, expected_status",
    [
        (
            {'sort': 'invalid_field'}, HTTPStatus.UNPROCESSABLE_ENTITY,
        ),
        (
            {'page[size]': 0}, HTTPStatus.UNPROCESSABLE_ENTITY,
        ),
        (
            {'page[size]': 101}, HTTPStatus.UNPROCESSABLE_ENTITY,
        ),
        (
            {'page[number]': 0}, HTTPStatus.UNPROCESSABLE_ENTITY,
        ),
        (
            {'page[number]': -1}, HTTPStatus.UNPROCESSABLE_ENTITY,
        ),
    ]
)
@pytest.mark.asyncio
async def test_validation_errors(
    make_get_request: Callable,
    query_data: dict,
    expected_status: int
):
    """Проверка ошибок валидации параметров."""
    body, status = await make_get_request(_FILMS_API_URL, query_data)
    assert status == expected_status
    assert 'detail' in body


@pytest.mark.asyncio
async def test_film_details(
    es_write_data: Callable,
    es_delete_index: Callable,
    make_get_request: Callable,
):
    """Проверка получения детальной информации о фильме."""
    film_id = str(uuid.uuid4())
    film_detail = {
        'id': film_id,
        'imdb_rating': 8.5,
        'genres': [
            {'id': str(uuid.uuid4()), 'name': 'Action'},
            {'id': str(uuid.uuid4()), 'name': 'Sci-Fi'}
        ],
        'title': 'The Star',
        'description': 'New World',
        'directors_names': ['Stan'],
        'actors_names': ['Ann', 'Bob'],
        'writers_names': ['Ben', 'Howard'],
        'directors': [
            {'id': str(uuid.uuid4()), 'name': 'Stan'}
        ],
        'actors': [
            {'id': str(uuid.uuid4()), 'name': 'Ann'},
            {'id': str(uuid.uuid4()), 'name': 'Bob'}
        ],
        'writers': [
            {'id': str(uuid.uuid4()), 'name': 'Ben'},
            {'id': str(uuid.uuid4()), 'name': 'Howard'}
        ]
    }
    bulk_query = [
        {
            '_index': test_settings.es_index,
            '_id': film_id,
            '_source': film_detail,
        }
    ]
    await es_write_data(
        data=bulk_query,
        index=test_settings.es_index,
        index_mapping=test_settings.es_index_mapping,
    )

    url = f'{_FILMS_API_URL}/{film_id}'

    # Первый запрос.
    body, status = await make_get_request(url, {})

    # Проверка ответа.
    assert status == HTTPStatus.OK

    # Проверка полной структуры ответа.
    assert body['uuid'] == film_detail['id']
    assert body['title'] == film_detail['title']
    assert body['imdb_rating'] == film_detail['imdb_rating']
    assert body['description'] == film_detail['description']

    # Проверка вложенных структур.
    assert len(body['genres']) == len(film_detail['genres'])
    assert len(body['actors']) == len(film_detail['actors'])
    assert len(body['writers']) == len(film_detail['writers'])
    assert len(body['directors']) == len(film_detail['directors'])

    # Проверка преобразования полей.
    for i, genre in enumerate(film_detail['genres']):
        assert body['genres'][i]['uuid'] == genre['id']
        assert body['genres'][i]['name'] == genre['name']

    for i, actor in enumerate(film_detail['actors']):
        assert body['actors'][i]['uuid'] == actor['id']
        assert body['actors'][i]['full_name'] == actor['name']

    await es_delete_index(index=test_settings.es_index)
    body_cached, status_cached = await make_get_request(url, {})

    assert status_cached == HTTPStatus.OK
    assert body_cached['uuid'] == film_detail['id']


@pytest.mark.asyncio
async def test_film_details_empty_index(
    es_write_data: Callable,
    es_delete_index: Callable,
    make_get_request: Callable,
):
    """Проверка получения информации из пустого индекса."""
    film_id = str(uuid.uuid4())
    await es_write_data(
        data=[],
        index=test_settings.es_index,
        index_mapping=test_settings.es_index_mapping,
    )
    url = f'{_FILMS_API_URL}/{film_id}'

    # Первый запрос.
    body, status = await make_get_request(url, {})

    # Проверка ответа.
    assert status == HTTPStatus.NOT_FOUND
    assert body.get('detail') == 'Кинопроизведение не найдено'

    # Проверка кеширования.
    await es_delete_index(index=test_settings.es_index)
    body_cached, status_cached = await make_get_request(url, {})

    assert status_cached == HTTPStatus.NOT_FOUND
    assert body_cached.get('detail') == 'Кинопроизведение не найдено'
