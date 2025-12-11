from enum import Enum

from pydantic import Field, SecretStr

from config.base_config import BaseAppSettings


class DatabaseType(
    str,
    Enum
):
    POSTGRESQL = 'postgresql'
    MYSQL = 'mysql'
    SQLITE = 'sqlite'
    OTHER = 'other'


class DatabaseSettings(BaseAppSettings):
    DB_TYPE: DatabaseType = Field(default=DatabaseType.POSTGRESQL)

    DB_USER: str = Field(...)
    DB_PASSWORD: SecretStr = Field(...)
    DB_HOST: str = Field(default='localhost')
    DB_PORT: int = Field(default=5432)
    DB_NAME: str = Field(...)
    DB_SCHEMA_NAME: str = Field(default='public')

    DB_USE_NATIVE_UPSERT: bool = Field(default=False)
    DB_USE_UNIQUE_COLUMNS_IN_IDENTIFIER_KEYS: bool = Field(default=False)

    DB_INCLUDE_BASE_ID: bool = Field(default=True)
    DB_INCLUDE_BASE_CREATED_AT: bool = Field(default=True)
    DB_INCLUDE_BASE_UPDATED_AT: bool = Field(default=True)

    DB_ECHO: bool = Field(default=False)
    DB_POOL_SIZE: int = Field(default=5)
    DB_MAX_OVERFLOW: int = Field(default=10)
    DB_POOL_TIMEOUT: int = Field(default=30)
    DB_POOL_RECYCLE: int = Field(default=1800)
    DB_POOL_PRE_PING: bool = Field(default=True)

    DB_DEFAULT_PORTS: dict[DatabaseType, int] = {
        DatabaseType.POSTGRESQL: 5432,
        DatabaseType.MYSQL: 3306,
        DatabaseType.SQLITE: 0,
    }

    def _driver(
        self
    ) -> str:
        if self.DB_TYPE == DatabaseType.POSTGRESQL:
            return 'postgresql+psycopg2'
        if self.DB_TYPE == DatabaseType.MYSQL:
            return 'mysql+pymysql'
        if self.DB_TYPE == DatabaseType.SQLITE:
            return 'sqlite'
        return self.DB_TYPE.value

    @property
    def database_url(
        self
    ) -> str:
        db_driver = self._driver()

        if self.DB_TYPE == DatabaseType.SQLITE:
            return f'{db_driver}:///{self.DB_NAME}'

        port = self.DB_PORT or self.DB_DEFAULT_PORTS.get(self.DB_TYPE)

        if self.DB_USER and self.DB_PASSWORD:
            auth_part = f'{self.DB_USER}:{self.DB_PASSWORD.get_secret_value()}@'
        else:
            auth_part = ''

        return (
            f'{db_driver}://{auth_part}{self.DB_HOST}'
            f'{f":{port}" if port else ""}/{self.DB_NAME}'
        )

    @property
    def engine_settings(
        self
    ) -> dict:
        settings = {
            'echo': self.DB_ECHO,
            'pool_pre_ping': self.DB_POOL_PRE_PING,
        }

        if self.DB_TYPE != DatabaseType.SQLITE:
            settings.update(
                {
                    'pool_size': self.DB_POOL_SIZE,
                    'max_overflow': self.DB_MAX_OVERFLOW,
                    'pool_timeout': self.DB_POOL_TIMEOUT,
                    'pool_recycle': self.DB_POOL_RECYCLE,
                }
            )

        return settings
