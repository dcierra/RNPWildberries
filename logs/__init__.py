from config import settings as app_settings

from .logger import Logger


app_logger = Logger(
    settings=app_settings.logging
).get_logger()


__all__ = [
    'app_logger',
    'Logger'
]
