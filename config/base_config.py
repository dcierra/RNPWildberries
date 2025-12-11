from dotenv import find_dotenv, load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings


load_dotenv(
    dotenv_path=find_dotenv(
        usecwd=True
    )
)


class BaseAppSettings(BaseSettings):
    APP_NAME: str
    APP_VERSION: str
    DEBUG: bool = Field(default=False)

    model_config = {
        'env_file': '.env',
        'env_file_encoding': 'utf-8',
        'extra': 'ignore',
        'env_files': [
            '.env.default',
            '.env',
            '.env.local'
        ]
    }
