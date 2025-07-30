from enum import StrEnum

from pydantic import Field

from models.film import BaseResponseModel
from models.film import Person as BasePerson


class PersonRole(StrEnum):
    DIRECTOR = 'director'
    ACTOR = 'actor'
    WRITER = 'writer'


class PersonFilms(BaseResponseModel):
    roles: list[PersonRole]


class PersonDetail(BasePerson):
    """Подробная информация о персонаже и фильмах,
        в которых он принимал участие.
    """
    films: list[PersonFilms] = Field(default=[])
