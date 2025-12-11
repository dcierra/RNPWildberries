from .logging_config import LogLevel
from .settings import Settings

settings = Settings()

__all__ = [
    'settings',
    'LogLevel'
]
