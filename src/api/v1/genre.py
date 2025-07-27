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
