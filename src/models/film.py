from models.base import BaseResponseModel
from models.genre import Genre
from models.person import Person


class FilmShort(BaseResponseModel):
    """Модель кинопроизведения (сокращенная)."""
    title: str
    imdb_rating: float


class Film(FilmShort):
    """Модель кинопроизведения (развернутая)."""
    description: str
    genres: list[Genre]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]
