from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from models.film import Genre
from services.genre import GenreService, get_genre_service

router = APIRouter()


@router.get(
    '/',
    response_model=list[Genre],
    summary='Список всех жанров',
    response_description='Информация по жанрам',
    status_code=HTTPStatus.OK,
)
async def get_genres(
    genre_service: GenreService = Depends(get_genre_service),
) -> list[Genre]:
    """Список всех жанров.

    - **uuid**: уникальный идентификатор жанра.
    - **name**: название жанра.
    """

    genres = await genre_service.get_genres()

    if not genres:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Жанры не найдены',
        )

    return genres


@router.get(
    '/{genre_uuid}',
    response_model=Genre,
    summary='Получить информацию по жанру',
    response_description='Информация по жанру',
    status_code=HTTPStatus.OK,
)
async def get_genre_details(
    genre_uuid: str,
    genre_service: GenreService = Depends(get_genre_service),
) -> Genre:
    """Информация по жанру.

    - **uuid**: уникальный идентификатор жанра.
    - **name**: название жанра.
    """

    genre = await genre_service.get_genre_by_id(genre_id=genre_uuid)

    if not genre:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Жанр не найден',
        )

    return genre
