from enum import Enum
from pathlib import Path

from pydantic import Field

from config.base_config import BaseAppSettings


class LogLevel(
    str,
    Enum
):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class LoggerSettings(BaseAppSettings):
    LOGGER_NAME: str = Field(...)
    LOGGING_LEVEL: LogLevel = Field(default=LogLevel.INFO)
    LOGGER_PATH: Path
    LOGGER_FILE_MAX_BYTES: int = Field(default=1_000_000)
    LOGGER_FILE_BACKUP_COUNT: int = Field(default=10)
    LOGGER_ENCODING: str = Field(default='utf-8')
    LOGGER_FORMAT: str = Field(default='[%(levelname)s] | [%(message)s] | [%(asctime)s] | [%(filename)s:%(lineno)d]')
    LOGGER_DATE_FORMAT: str = Field(default='%Y-%m-%d %H:%M:%S')

    LOGGER_DIR_PATH: Path

    def get_log_file_path(
        self,
        log_name: str
    ) -> Path:
        """Формирует полный путь для лог-файла.

        Args:
            log_name (str): Имя лог-файла (без расширения).

        Returns:
            Path: Полный путь до лог-файла.
        """
        return self.LOGGER_DIR_PATH / f'{log_name}.log'
