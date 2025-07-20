import logging
from time import sleep
from typing import NoReturn

from common.settings import settings_etl
from data_extractor.main import DataExtractor
from data_loader.main import DataLoader
from data_transformer.main import DataTransformer


class ETLProcess:
    def __init__(self) -> None:
        self._etl_rate: int = settings_etl.ETL_RATE
        self._loger = logging.getLogger(__name__)

    def start_process(self) -> NoReturn:
        """Запускает жизненный цикл ETL процесса.

        Returns:
            Ничего не возвращает. Крутится в вечном цикле.
        """
        self._loger.info('Запуск жизненного цикла ETL процесса...')
        data_extractor = DataExtractor()
        data_transformer = DataTransformer()
        data_loader = DataLoader()
        self._loger.info('Основные модули ETL процесса сконфигурированы.')

        while True:
            try:
                raw_data = data_extractor.extract_data()
                data_films, data_genres, data_persons = raw_data
                transformed_data = data_transformer.transform_data(
                    data_films=[
                        data.data for data in data_films
                    ],
                    data_genres=data_genres.data,
                    data_persons=data_persons.data,
                )
                data_loader.load_data(data=transformed_data)

                # Фиксируем состояние после успешной отправки данных в ES.
                for data in data_films:
                    data_extractor.state.set_state(
                        key=data.model,
                        value=data.new_state,
                    )
                data_extractor.state.set_state(
                    key=data_genres.model,
                    value=data_genres.new_state,
                )
                data_extractor.state.set_state(
                    key=data_persons.model,
                    value=data_persons.new_state,
                )
            except Exception as error:
                self._loger.error(
                    f'В ETL процессе произошла непредвиденная ошибка: {error}',
                )
            finally:
                sleep(self._etl_rate)
