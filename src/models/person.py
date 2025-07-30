from enum import StrEnum

from pydantic import Field

from models.film import BaseResponseModel


class PersonRole(StrEnum):
    DIRECTOR = 'director'
    ACTOR = 'actor'
    WRITER = 'writer'


class Person(BaseResponseModel):
    """Модель персоны."""
    name: str = Field(serialization_alias='full_name')


class PersonFilms(BaseResponseModel):
    roles: list[PersonRole]


class PersonDetail(Person):
    """Подробная информация о персонаже и фильмах,
        в которых он принимал участие.
    """
    films: list[PersonFilms] = Field(default=[])
