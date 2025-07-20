import logging
from typing import Sequence
from uuid import UUID

from data_extractor.models_db import FilmWork, Genre, Person, PersonFilmWork


class DataTransformer:
    def __init__(self) -> None:
        self._loger = logging.getLogger(__name__)

    def transform_data(
        self,
        data_films: list[Sequence[FilmWork]],
        data_genres: Sequence[Genre],
        data_persons: Sequence[Person],
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Преобразует данные фильмов для индексации в ElasticSearch.
        Args:
            data: данные для преобразования.

        Returns:
            Преобразованные данные.
        """
        # Объединяем фильмы из всех источников, устраняя дубликаты по ID.
        unique_films: dict[UUID, FilmWork] = {}
        for film_list in data_films:
            for film in film_list:
                unique_films.setdefault(film.id, film)

        # Преобразуем каждый фильм в формат для ES.
        transformed_films_data = []
        for film in unique_films.values():
            # Собираем итоговый документ
            doc = self._transform_by_film(film=film)
            transformed_films_data.append(doc)

        transformed_genres_data = [
            {
                'id': str(genre.id),
                'name': genre.name,
            }
            for genre in data_genres
        ]
        transformed_persons_data = [
            {
                'id': str(person.id),
                'name': person.full_name,
            }
            for person in data_persons
        ]

        return (
            transformed_films_data,
            transformed_genres_data,
            transformed_persons_data,
        )

    def _transform_by_film(self, film: FilmWork):
        """Преобразует по одному фильму.

        Args:
            film (FilmWork): фильм.

        Returns:
            Преобразованный в документ в виде словаря фильм.
        """
        # Собираем жанры.
        genres = [
            {
                'id': str(gfw.genre.id),
                'name': gfw.genre.name,
            }
            for gfw in film.genres
            if gfw.genre and gfw.genre.name
        ]

        (
            directors,
            actors,
            writers,
        ) = self._group_persons_by_role(film.persons)

        # Формируем поля для полнотекстового поиска.
        directors_names = ', '.join(p['name'] for p in directors)
        actors_names = ', '.join(p['name'] for p in actors)
        writers_names = ', '.join(p['name'] for p in writers)

        # Собираем итоговый документ.
        return {
            'id': str(film.id),
            'imdb_rating': film.rating,
            'genres': genres,
            'title': film.title,
            'description': film.description or '',
            'directors_names': directors_names,
            'actors_names': actors_names,
            'writers_names': writers_names,
            'directors': directors,
            'actors': actors,
            'writers': writers,
        }

    def _group_persons_by_role(
        self,
        persons: list[PersonFilmWork],
    ) -> tuple[
        list[dict[str, str]],
        list[dict[str, str]],
        list[dict[str, str]],
    ]:
        """Группирует участников по ролям.

        Args:
            persons (list[PersonFilmWork]): участники фильмов.

        Returns:
            Кортеж со сгруппированными участниками фильмов.
        """
        directors, actors, writers = [], [], []

        for pfw in persons:
            if not pfw.person:
                continue

            person_data = {
                'id': str(pfw.person.id),
                'name': pfw.person.full_name,
            }

            if pfw.role == 'director':
                directors.append(person_data)
            elif pfw.role == 'actor':
                actors.append(person_data)
            elif pfw.role == 'writer':
                writers.append(person_data)

        return directors, actors, writers
