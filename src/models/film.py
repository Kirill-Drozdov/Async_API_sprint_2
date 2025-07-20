from pydantic import BaseModel, ConfigDict, Field


class BaseResponseModel(BaseModel):
    """Базовая модель, в которой повторяются поля и настройки для
    других моделей.
    """
    id: str = Field(serialization_alias='uuid')

    model_config = ConfigDict(
        serialize_by_alias=True,
    )


class Person(BaseResponseModel):
    """Модель персоны."""
    name: str = Field(serialization_alias='full_name')


class Genre(BaseResponseModel):
    """Модель жанра."""
    name: str


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
