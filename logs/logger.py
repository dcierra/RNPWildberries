import logging
from logging.handlers import RotatingFileHandler

from config.logging_config import LoggerSettings, LogLevel


class Logger:
    def __init__(
        self,
        settings: LoggerSettings,
        log_name: str = None,
        log_level: LogLevel = None,
        log_path: str = None,
        log_file_max_bytes: int = None,
        log_file_backup_count: int = None,
        log_encoding: str = None,
        log_format: str = None,
        log_date_format: str = None,
    ):
        self._settings = settings

        self._log_level = log_level or self._settings.LOGGING_LEVEL
        self._log_name = log_name or self._settings.LOGGER_NAME
        self._log_path = log_path or self._settings.LOGGER_PATH
        self._log_file_max_bytes = log_file_max_bytes or self._settings.LOGGER_FILE_MAX_BYTES
        self._log_file_backup_count = log_file_backup_count or self._settings.LOGGER_FILE_BACKUP_COUNT
        self._log_encoding = log_encoding or self._settings.LOGGER_ENCODING
        self._log_format = log_format or self._settings.LOGGER_FORMAT
        self._log_date_format = log_date_format or self._settings.LOGGER_DATE_FORMAT

        self.logger = None
        self.setup_logger()

    def setup_logger(
        self
    ) -> None:
        try:
            match self._log_level:
                case 'DEBUG':
                    log_level = logging.DEBUG
                case 'INFO':
                    log_level = logging.INFO
                case 'WARNING':
                    log_level = logging.WARNING
                case 'ERROR':
                    log_level = logging.ERROR
                case 'CRITICAL':
                    log_level = logging.CRITICAL
                case _:
                    log_level = logging.INFO

            logger = logging.getLogger(
                name=self._log_name
            )

            logger.setLevel(
                level=log_level
            )

            file_handler = RotatingFileHandler(
                filename=self._log_path,
                maxBytes=self._log_file_max_bytes,
                backupCount=self._log_file_backup_count,
                encoding=self._log_encoding
            )

            file_formatter = logging.Formatter(
                fmt=self._log_format,
                datefmt=self._log_date_format
            )

            file_handler.setFormatter(
                fmt=file_formatter
            )

            logger.addHandler(
                hdlr=file_handler
            )

            self.logger = logger
        except Exception:
            raise

    def get_logger(
        self
    ) -> logging.Logger:
        try:
            return self.logger
        except Exception:
            raise
