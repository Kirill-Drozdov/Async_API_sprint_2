from models.base import BaseResponseModel


class Genre(BaseResponseModel):
    """Модель жанра."""
    name: str
