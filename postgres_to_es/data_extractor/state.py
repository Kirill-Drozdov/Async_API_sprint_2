import abc
import json
import os
from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                json.dump({}, file)

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self._file_path, 'r', encoding='utf-8') as file:
            state_file: Dict[str, Any] = json.load(file)
        state_file.update(state)
        with open(self._file_path, 'w', encoding='utf-8') as file:
            json.dump(state_file, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        with open(self._file_path, 'r', encoding='utf-8') as file:
            return json.load(file)


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self._storage.save_state(state={key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self._storage.retrieve_state().get(key)
