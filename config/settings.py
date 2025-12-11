from pydantic import Field

from config.base_config import BaseAppSettings
from config.db_config import DatabaseSettings
from config.logging_config import LoggerSettings
from config.wb_config import WbSettings


class Settings(BaseAppSettings):
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    logging: LoggerSettings = Field(default_factory=LoggerSettings)
    wb: WbSettings = Field(default_factory=WbSettings)
