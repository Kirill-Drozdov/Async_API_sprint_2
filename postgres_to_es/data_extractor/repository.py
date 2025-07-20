"""Модуль взаимодействия с БД."""
from dataclasses import dataclass
import datetime as dt
from typing import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from common.settings import settings_etl
from common.utils import backoff
from data_extractor.db import get_db_connection
from data_extractor.models_db import (
    FilmWork,
    Genre,
    GenreFilmWork,
    Person,
    PersonFilmWork,
)


@dataclass
class BaseDataState:
    model: str
    new_state: str


@dataclass
class FilmWorkDataState(BaseDataState):
    data: Sequence[FilmWork]


@dataclass
class GenreDataState(BaseDataState):
    data: Sequence[Genre]


@dataclass
class PersonDataState(BaseDataState):
    data: Sequence[Person]


class Repository:
    """Класс используемый для взаимодействия с БД."""

    def __init__(self) -> None:
        """Инициализирует подключение к базе данных."""
        self._session: Session = get_db_connection()()
        self._load_limit: int = settings_etl.LOAD_LIMIT

    @backoff()
    def get_updated_persons(
        self,
        last_modified: dt.datetime,
    ) -> Sequence[Person]:
        """Запрос обновленных персон.

        Args:
            last_modified (dt.datetime): временная отметка последнего
                обновления записи, взятая из состояния.

        Returns:
            Обновленные персоны.
        """
        stmt_persons = (
            select(Person)
            .where(Person.modified > last_modified)
            .order_by(Person.modified)
            .limit(self._load_limit)
        )
        return self._session.execute(stmt_persons).scalars().all()

    @backoff()
    def get_updated_genres(
        self,
        last_modified: dt.datetime,
    ) -> Sequence[Genre]:
        """Запрос обновленных жанров.

        Args:
            last_modified (dt.datetime): временная отметка последнего
                обновления записи, взятая из состояния.

        Returns:
            Обновленные жанры.
        """
        stmt_genres = (
            select(Genre)
            .where(Genre.modified > last_modified)
            .order_by(Genre.modified)
            .limit(self._load_limit)
        )
        return self._session.execute(stmt_genres).scalars().all()

    @backoff()
    def get_fw_by_updated_persons(
        self,
        person_ids: list[UUID],
    ) -> Sequence[FilmWork]:
        """Находим фильмы, связанные c обновленными персонами.

        Args:
            person_ids (list[UUID]): идентификаторы обновленных персон.

        Returns:
            Фильмы по обновленным персонам.
        """
        stmt_film_works = (
            select(FilmWork)
            .join(PersonFilmWork, FilmWork.id == PersonFilmWork.film_work_id)
            .where(PersonFilmWork.person_id.in_(person_ids))
            .order_by(FilmWork.modified)
            .limit(self._load_limit)
            .distinct()
        )
        return self._session.execute(stmt_film_works).scalars().all()

    @backoff()
    def get_fw_by_updated_genres(
        self,
        genre_ids: list[UUID],
    ) -> Sequence[FilmWork]:
        """Находим фильмы, связанные c обновленными жанрами.

        Args:
            genre_ids (list[UUID]): идентификаторы обновленных жанров.

        Returns:
            Фильмы по обновленным жанрам.
        """
        stmt_film_works = (
            select(FilmWork)
            .join(GenreFilmWork, FilmWork.id == GenreFilmWork.film_work_id)
            .where(GenreFilmWork.genre_id.in_(genre_ids))
            .order_by(FilmWork.modified)
            .limit(self._load_limit)
            .distinct()
        )
        return self._session.execute(stmt_film_works).scalars().all()

    @backoff()
    def get_updated_filmworks_by_id(
        self,
        film_work_ids: list[UUID],
    ) -> Sequence[FilmWork]:
        """Находим обновленные фильмы по UUID.

        Args:
            film_work_ids (list[UUID]): идентификаторы обновленных фильмов.

        Returns:
            Обновленные фильмы по UUID.
        """
        stmt_details = (
            select(FilmWork).where(FilmWork.id.in_(film_work_ids))
        ).options(
            selectinload(FilmWork.genres).joinedload(GenreFilmWork.genre),
            selectinload(FilmWork.persons).joinedload(PersonFilmWork.person),
        )

        film_works = self._session.execute(stmt_details)
        return film_works.scalars().all()

    @backoff()
    def get_updated_filmworks_by_timestamp(
        self,
        last_modified: dt.datetime,
    ) -> Sequence[FilmWork]:
        """Запрос обновленных фильмов по времени последнего обновления.

        Args:
            last_modified (dt.datetime): временная отметка последнего
                обновления записи, взятая из состояния.

        Returns:
            Обновленные фильмы.
        """
        stmt_filmforks = (
            select(FilmWork)
            .where(FilmWork.modified > last_modified)
            .order_by(FilmWork.modified)
            .limit(self._load_limit)
        ).options(
            selectinload(FilmWork.genres).joinedload(GenreFilmWork.genre),
            selectinload(FilmWork.persons).joinedload(PersonFilmWork.person),
        )

        film_works = self._session.execute(stmt_filmforks)
        return film_works.scalars().all()
