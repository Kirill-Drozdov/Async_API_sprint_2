import uuid


# Генерируем фиксированные ID для повторяющихся сущностей.
genre_action_id = str(uuid.uuid4())
genre_scifi_id = str(uuid.uuid4())
director_id = str(uuid.uuid4())


def generate_es_data(data_size: int) -> tuple[list[dict], list[str]]:
    """Генерирует тестовые данные для загрузки в индекс.

    Args:
        data_size (int): размер данных для загрузки.
    """
    action_films_id, es_data = [], []
    for i in range(data_size):
        # Чередуем жанры и рейтинги.
        main_genre = 'Action' if i % 2 == 0 else 'Sci-Fi'
        # Рейтинги от 1.0 до 9.5.
        rating = 1.0 + (i % 17) * 0.5

        film_id = str(uuid.uuid4())
        if main_genre == 'Action':
            action_films_id.append(film_id)

        film_data = {
            'id': film_id,
            'imdb_rating': rating,
            'genres': [
                {
                    'id': genre_action_id if main_genre == 'Action'
                    else genre_scifi_id,
                    'name': main_genre,
                },
                {'id': genre_scifi_id, 'name': 'Sci-Fi'}
            ],
            'title': f'Film {i}',
            'description': 'Description',
            'directors_names': ['Director'],
            'actors_names': ['Actor 1', 'Actor 2'],
            'writers_names': ['Writer'],
            'directors': [{'id': director_id, 'name': 'Director'}],
            'actors': [
                {'id': str(uuid.uuid4()), 'name': 'Actor 1'},
                {'id': str(uuid.uuid4()), 'name': 'Actor 2'}
            ],
            'writers': [{'id': str(uuid.uuid4()), 'name': 'Writer'}]
        }
        es_data.append(film_data)

    return es_data, action_films_id
