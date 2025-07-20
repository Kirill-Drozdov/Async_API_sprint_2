import datetime as dt
import logging
import os
from pathlib import Path

from data_extractor.repository import (
    FilmWorkDataState,
    GenreDataState,
    PersonDataState,
    Repository,
)
from data_extractor.state import JsonFileStorage, State

BASE_DIR = Path(__file__).resolve().parent


class DataExtractor:
    """Осуществляет работу с данными из БД."""

    def __init__(self) -> None:
        """Инициализирует подключение к базе данных."""
        self._repository = Repository()
        self.state = State(
            storage=JsonFileStorage(
                file_path=os.path.join(BASE_DIR, 'state.json'),
            ),
        )
        self._loger = logging.getLogger(__name__)

    def extract_data(self) -> tuple[
        tuple[
            FilmWorkDataState,
            FilmWorkDataState,
            FilmWorkDataState,
        ],
        GenreDataState,
        PersonDataState,
    ]:
        """Получает всю необходимую информацию по обновленным таблицам в БД.

        Returns:
            Кортеж с обновленными в БД данными.
        """
        film_works_by_updated_persons = self._get_fw_by_updated_persons()
        film_works_by_updated_genres = self._get_fw_by_updated_genres()
        updated_film_works = self._get_updated_filmworks()

        updated_genres = self._get_updated_genres()
        updated_persons = self._get_updated_persons()

        return (
            (
                film_works_by_updated_persons,
                film_works_by_updated_genres,
                updated_film_works,
            ),
            updated_genres,
            updated_persons,
        )

    def _get_updated_genres(self) -> GenreDataState:
        """Получает обновленные данные по жанрам."""
        current_state = self.state.get_state(key='genres')
        last_modified = (
            dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            if current_state is None
            else dt.datetime.fromisoformat(current_state)
        )

        genres = self._repository.get_updated_genres(
            last_modified=last_modified,
        )

        if not genres:
            return GenreDataState(
                model='genres',
                new_state=current_state,
                data=[],
            )

        new_state = max(genre.modified for genre in genres).isoformat()
        return GenreDataState(
            model='genres',
            new_state=new_state,
            data=genres,
        )

    def _get_updated_persons(self) -> PersonDataState:
        """Получает обновленные данные по персонам."""
        current_state = self.state.get_state(key='persons')
        last_modified = (
            dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            if current_state is None
            else dt.datetime.fromisoformat(current_state)
        )

        persons = self._repository.get_updated_persons(
            last_modified=last_modified,
        )

        if not persons:
            return PersonDataState(
                model='persons',
                new_state=current_state,
                data=[],
            )

        new_state = max(person.modified for person in persons).isoformat()
        return PersonDataState(
            model='persons',
            new_state=new_state,
            data=persons,
        )

    def _get_fw_by_updated_persons(self) -> FilmWorkDataState:
        """Получает обновленные данные фильмов через обновленных персон."""
        # Получаем последнее состояние.
        current_state = self.state.get_state(key='fw_persons')
        last_modified = (
            dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            if current_state is None
            else dt.datetime.fromisoformat(current_state)
        )

        persons = self._repository.get_updated_persons(
            last_modified=last_modified,
        )

        if not persons:
            return FilmWorkDataState(
                model='fw_persons',
                new_state=current_state,
                data=[],
            )

        person_ids = [person.id for person in persons]
        new_state = max(person.modified for person in persons).isoformat()

        film_works = self._repository.get_fw_by_updated_persons(
            person_ids=person_ids,
        )

        if not film_works:
            return FilmWorkDataState(
                model='fw_persons',
                new_state=new_state,
                data=[],
            )

        film_work_ids = [fw.id for fw in film_works]

        film_works = self._repository.get_updated_filmworks_by_id(
            film_work_ids=film_work_ids,
        )

        return FilmWorkDataState(
            model='fw_persons',
            new_state=new_state,
            data=film_works,
        )

    def _get_fw_by_updated_genres(self) -> FilmWorkDataState:
        """Получает обновленные данные фильмов через обновленные жанры."""
        # Получаем последнее состояние.
        current_state = self.state.get_state(key='fw_genres')
        last_modified = (
            dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            if current_state is None
            else dt.datetime.fromisoformat(current_state)
        )

        genres = self._repository.get_updated_genres(
            last_modified=last_modified,
        )

        if not genres:
            return FilmWorkDataState(
                model='fw_genres',
                new_state=current_state,
                data=[],
            )

        genre_ids = [genre.id for genre in genres]
        new_state = max(genre.modified for genre in genres).isoformat()

        film_works = self._repository.get_fw_by_updated_genres(
            genre_ids=genre_ids,
        )

        if not film_works:
            return FilmWorkDataState(
                model='fw_genres',
                new_state=new_state,
                data=[],
            )

        film_work_ids = [fw.id for fw in film_works]

        film_works = self._repository.get_updated_filmworks_by_id(
            film_work_ids=film_work_ids,
        )

        return FilmWorkDataState(
            model='fw_genres',
            new_state=new_state,
            data=film_works,
        )

    def _get_updated_filmworks(self) -> FilmWorkDataState:
        """Получает последние обновленные данные фильмов."""
        # Получаем последнее состояние.
        current_state = self.state.get_state(key='filmworks')
        last_modified = (
            dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            if current_state is None
            else dt.datetime.fromisoformat(current_state)
        )

        film_works = self._repository.get_updated_filmworks_by_timestamp(
            last_modified=last_modified,
        )

        if not film_works:
            return FilmWorkDataState(
                model='filmworks',
                new_state=current_state,
                data=film_works,
            )

        new_state = max(
            filmworks.modified for filmworks in film_works
        ).isoformat()

        return FilmWorkDataState(
            model='filmworks',
            new_state=new_state,
            data=film_works,
        )
