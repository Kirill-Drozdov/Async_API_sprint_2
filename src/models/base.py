from pydantic import BaseModel, ConfigDict, Field


class BaseResponseModel(BaseModel):
    """Базовая модель, в которой повторяются поля и настройки для
    других моделей.
    """
    id: str = Field(serialization_alias='uuid')

    model_config = ConfigDict(
        serialize_by_alias=True,
    )
