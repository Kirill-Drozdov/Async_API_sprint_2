from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query

from models.film import FilmShort
from models.person import PersonDetail
from services.person import PersonService, get_person_service

router = APIRouter()


@router.get(
    '/search',
    response_model=list[PersonDetail],
    summary='Поиск персонажей',
    response_description='Информация по найденным персонажам',
    status_code=HTTPStatus.OK,
)
async def get_persons_by_search(
    person_service: PersonService = Depends(get_person_service),
    query: str = Query(None, description='Поиск по имени персонажа'),
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
) -> list[PersonDetail]:
    """Результат поиска персонажей.

    - **uuid**: уникальный идентификатор персонажа.
    - **full_name**: имя персонажа.
    - **films**: фильмы, в которых принимал участие:
      - **uuid**: уникальный идентификатор кинопроизведения.
      - **roles**: список ролей на площадке, которые исполнял персонаж.
    """

    persons = await person_service.get_persons_by_search(
        query=query,
        page_size=page_size,
        page_number=page_number,
    )

    if not persons:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Персонажи не найдены',
        )

    return persons


@router.get(
    '/{person_uuid}/film',
    response_model=list[FilmShort],
    summary='Получить фильмы по персонажу',
    response_description='Фильмы по персонажу',
    status_code=HTTPStatus.OK,
)
async def get_films_by_person(
    person_uuid: str,
    person_service: PersonService = Depends(get_person_service),
) -> list[FilmShort]:
    """Фильмы по персонажу.

    - **uuid**: уникальный идентификатор кинопроизведения.
    - **title**: название.
    - **imdb_rating**: рейтинг с платформы imdb.
    """

    films = await person_service.get_films_by_person(
        person_id=person_uuid,
    )

    if not films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Фильмы по персонажу не найдены',
        )

    return films


@router.get(
    '/{person_uuid}',
    response_model=PersonDetail,
    summary='Получить информацию по персонажу',
    response_description='Информация по персонажу',
    status_code=HTTPStatus.OK,
)
async def get_person_by_id(
    person_uuid: str,
    person_service: PersonService = Depends(get_person_service),
) -> PersonDetail:
    """Информация по персонажу.

    - **uuid**: уникальный идентификатор персонажа.
    - **full_name**: имя персонажа.
    - **films**: фильмы, в которых принимал участие:
      - **uuid**: уникальный идентификатор кинопроизведения.
      - **roles**: список ролей на площадке, которые исполнял персонаж.
    """

    person = await person_service.get_person_by_id(
        person_id=person_uuid,
    )

    if not person:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Персонаж не найден',
        )

    return person
