from pydantic import Field, SecretStr

from config.base_config import BaseAppSettings


class WbSettings(BaseAppSettings):
    BASE_URL: str = Field(...)
    TOKEN: SecretStr = Field(...)
