from abc import ABC, abstractmethod


class Repository(ABC):
    """Абстрактный класс для взаимодействия с хранилищем данных.
    """
    @abstractmethod
    async def get(self, *args, **kwargs):
        ...

    @abstractmethod
    async def get_multi(self, *args, **kwargs):
        ...
