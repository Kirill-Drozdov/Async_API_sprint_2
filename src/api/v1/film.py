from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query

from models.film import Film, FilmShort
from services.film import FilmService, get_film_service

router = APIRouter()


@router.get(
    '/',
    response_model=list[FilmShort],
    summary='Список всех кинопроизведений',
    response_description='Информация по кинопроизведениям',
    status_code=HTTPStatus.OK,
)
async def get_films(
    film_service: FilmService = Depends(get_film_service),
    sort: str = Query(
        default='-imdb_rating',
        regex='^-?imdb_rating$',
        description='Сортировка по рейтингу (imdb_rating или -imdb_rating)',
    ),
    genre: str | None = Query(None, description='Фильтр по жанру'),
    page_size: int = Query(
        default=50,
        alias='page[size]',
        ge=1,
        le=100,
        description='Количество элементов на странице',
    ),
    page_number: int = Query(
        default=1,
        alias='page[number]',
        ge=1,
        description='Номер страницы',
    ),
) -> list[FilmShort]:
    """Список всех кинопроизведений.

    - **uuid**: уникальный идентификатор кинопроизведения.
    - **title**: название.
    - **imdb_rating**: рейтинг с платформы imdb.
    """
    # Получаем направление сортировки.
    sort_order = 'desc' if sort.startswith('-') else 'asc'
    sort_field = sort.lstrip('-')

    films = await film_service.get_films(
        sort_field=sort_field,
        genre=genre,
        sort_order=sort_order,
        page_size=page_size,
        page_number=page_number,
    )

    if not films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Кинопроизведения не найдены',
        )

    return films


@router.get(
    '/search',
    response_model=list[FilmShort],
    summary='Поиск кинопроизведений',
    response_description='Информация по найденным кинопроизведениям',
    status_code=HTTPStatus.OK,
)
async def get_films_by_search(
    film_service: FilmService = Depends(get_film_service),
    query: str = Query(None, description='Поиск по названию фильма'),
    page_size: int = Query(
        default=50,
        alias='page[size]',
        ge=1,
        le=100,
        description='Количество элементов на странице',
    ),
    page_number: int = Query(
        default=1,
        alias='page[number]',
        ge=1,
        description='Номер страницы',
    ),
) -> list[FilmShort]:
    """Результат поиска кинопроизведений.

    - **uuid**: уникальный идентификатор кинопроизведения.
    - **title**: название.
    - **imdb_rating**: рейтинг с платформы imdb.
    """

    films = await film_service.get_films_by_search(
        query=query,
        page_size=page_size,
        page_number=page_number,
    )

    if not films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Кинопроизведения не найдены',
        )

    return films


@router.get(
    '/{film_uuid}',
    response_model=Film,
    summary='Получить информацию по кинопроизведению',
    response_description='Информация по кинопроизведению',
    status_code=HTTPStatus.OK,
)
async def get_film_details(
    film_uuid: str,
    film_service: FilmService = Depends(get_film_service),
) -> Film:
    """Подробная информация по кинопроизведению.

    - **uuid**: уникальный идентификатор кинопроизведения.
    - **title**: название.
    - **imdb_rating**: рейтинг с платформы imdb.
    - **description**: описание.
    - **genres**: жанры:
        - **uuid**: уникальный идентификатор жанра.
        - **name**: название.
    - **actors**: актеры:
        - **uuid**: уникальный идентификатор актера.
        - **full_name**: имя актера.
    - **writers**: сценаристы:
        - **uuid**: уникальный идентификатор сценариста.
        - **full_name**: имя сценариста.
    - **directors**: режиссеры:
        - **uuid**: уникальный идентификатор режиссера.
        - **full_name**: имя режиссера.
    """
    film = await film_service.get_film_by_id(film_uuid)
    if not film:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Кинопроизведение не найдено',
        )
    return film
